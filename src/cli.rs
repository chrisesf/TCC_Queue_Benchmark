//! Parametros de linha de comando, relatorio e exportacao CSV comuns aos binarios.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use crate::bench::{run_once, Arrival, RunConfig, RunOutcome};
use crate::clock::measure_clock_overhead_ns;
use crate::message::BenchMessage;
use crate::queue::Queue;
use crate::stats::{fmt_ns, RunAggregate};

pub const USAGE: &str = "\
Opcoes:
  --producers N        threads produtoras (padrao 4)
  --consumers N        threads consumidoras (padrao 4)
  --messages N         mensagens por produtor na fase medida (padrao 200000)
  --warmup N           mensagens por produtor no warm-up de cada execucao
                       (padrao: messages/10, minimo 1000; 0 desativa)
  --payload N          bytes de payload: 64, 1024 ou 65536 (padrao 64)
  --payload-kind K     heap (Vec<u8>) | inline ([u8; N]) (padrao depende da fila)
  --capacity N         capacidade da fila (padrao 8192; 0 = ilimitada)
  --repetitions N      execucoes independentes medidas (padrao 30)
  --arrival MODE       saturated | burst | paced (padrao saturated)
  --rate N             mensagens/s por produtor, para --arrival paced (padrao 100000)
  --batch N            tamanho da rajada, para --arrival burst (padrao 1000)
  --pause-ns N         pausa entre rajadas em ns, para --arrival burst (padrao 100000)
  --csv ARQUIVO        acrescenta uma linha por execucao medida ao CSV";

const FLAGS: [&str; 13] = [
    "--producers",
    "--consumers",
    "--messages",
    "--warmup",
    "--payload",
    "--payload-kind",
    "--capacity",
    "--repetitions",
    "--arrival",
    "--rate",
    "--batch",
    "--pause-ns",
    "--csv",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    /// `Message` com `Vec<u8>`: inclui custo de alocacao em heap.
    Heap,
    /// `FixedMessage<N>` com `[u8; N]`: sem alocacao por mensagem.
    Inline,
}

impl PayloadKind {
    pub fn label(&self) -> &'static str {
        match self {
            PayloadKind::Heap => "heap",
            PayloadKind::Inline => "inline",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Args {
    pub producers: usize,
    pub consumers: usize,
    pub messages: u64,
    pub warmup: u64,
    pub payload: usize,
    pub payload_kind: PayloadKind,
    pub capacity: Option<usize>,
    pub repetitions: usize,
    pub arrival: Arrival,
    pub csv: Option<PathBuf>,
}

impl Args {
    pub fn parse(default_kind: PayloadKind) -> Self {
        let argv: Vec<String> = std::env::args().skip(1).collect();
        if argv.iter().any(|a| a == "--help" || a == "-h") {
            println!("{USAGE}");
            std::process::exit(0);
        }
        for (i, a) in argv.iter().enumerate() {
            if i % 2 == 0 && !FLAGS.contains(&a.as_str()) {
                fail(&format!("opcao desconhecida ou sem valor: {a}"));
            }
        }
        if !argv.len().is_multiple_of(2) {
            fail(&format!("opcao sem valor: {}", argv[argv.len() - 1]));
        }

        let get = |name: &str| -> Option<String> {
            argv.iter()
                .position(|a| a == name)
                .and_then(|i| argv.get(i + 1))
                .cloned()
        };
        let num = |name: &str, default: u64| -> u64 {
            match get(name) {
                Some(v) => v
                    .parse()
                    .unwrap_or_else(|_| fail(&format!("valor invalido para {name}: {v}"))),
                None => default,
            }
        };

        let messages = num("--messages", 200_000);
        let capacity = num("--capacity", 8192);
        let arrival = match get("--arrival").as_deref().unwrap_or("saturated") {
            "saturated" => Arrival::Saturated,
            "burst" => Arrival::Burst {
                batch: num("--batch", 1_000),
                pause_ns: num("--pause-ns", 100_000),
            },
            "paced" => Arrival::Paced {
                rate_per_producer: num("--rate", 100_000) as f64,
            },
            other => fail(&format!("--arrival invalido: {other}")),
        };
        let payload_kind = match get("--payload-kind").as_deref() {
            None => default_kind,
            Some("heap") => PayloadKind::Heap,
            Some("inline") => PayloadKind::Inline,
            Some(other) => fail(&format!("--payload-kind invalido: {other}")),
        };

        let args = Self {
            producers: num("--producers", 4) as usize,
            consumers: num("--consumers", 4) as usize,
            messages,
            warmup: num("--warmup", (messages / 10).max(1_000)),
            payload: num("--payload", 64) as usize,
            payload_kind,
            capacity: (capacity != 0).then_some(capacity as usize),
            repetitions: num("--repetitions", 30) as usize,
            arrival,
            csv: get("--csv").map(PathBuf::from),
        };
        if args.producers == 0 || args.consumers == 0 || args.repetitions == 0 {
            fail("--producers, --consumers e --repetitions devem ser >= 1");
        }
        if args.producers as u64 > crate::message::MAX_PRODUCERS {
            fail("numero de produtores excede o limite do id (16 bits)");
        }
        args
    }

    pub fn run_config(&self) -> RunConfig {
        RunConfig {
            producers: self.producers,
            consumers: self.consumers,
            messages_per_producer: self.messages,
            warmup_per_producer: self.warmup,
            payload_bytes: self.payload,
            arrival: self.arrival,
        }
    }
}

pub fn fail(msg: &str) -> ! {
    eprintln!("erro: {msg}\n\n{USAGE}");
    std::process::exit(2);
}

/// Executa `M` com o tipo de mensagem escolhido: `Message` para payload em heap ou
/// `FixedMessage<N>` para payload inline (N precisa ser conhecido em compilacao).
#[macro_export]
macro_rules! with_message_type {
    ($args:expr, $M:ident => $body:expr) => {
        match ($args.payload_kind, $args.payload) {
            ($crate::cli::PayloadKind::Heap, _) => {
                type $M = $crate::message::Message;
                $body
            }
            ($crate::cli::PayloadKind::Inline, 64) => {
                type $M = $crate::message::FixedMessage<64>;
                $body
            }
            ($crate::cli::PayloadKind::Inline, 1024) => {
                type $M = $crate::message::FixedMessage<1024>;
                $body
            }
            ($crate::cli::PayloadKind::Inline, 65536) => {
                type $M = $crate::message::FixedMessage<65536>;
                $body
            }
            ($crate::cli::PayloadKind::Inline, n) => $crate::cli::fail(&format!(
                "payload inline suporta apenas 64, 1024 ou 65536 bytes (recebido {n})"
            )),
        }
    };
}

pub fn run_benchmark<Q, M, F>(title: &str, args: &Args, make_queue: F)
where
    Q: Queue<M> + 'static,
    M: BenchMessage,
    F: Fn() -> Q,
{
    let cfg = args.run_config();
    let clock_ns = measure_clock_overhead_ns(200_000);

    println!("=== {title} ===");
    println!(
        "topologia={} produtores={} consumidores={} payload={}B ({}) capacidade={} chegada={}",
        cfg.topology(),
        cfg.producers,
        cfg.consumers,
        cfg.payload_bytes,
        args.payload_kind.label(),
        args.capacity
            .map(|c| c.to_string())
            .unwrap_or_else(|| "ilimitada".into()),
        cfg.arrival.label()
    );
    println!(
        "mensagens/produtor={} warm-up/produtor={} total/execucao={} repeticoes={}",
        cfg.messages_per_producer,
        cfg.warmup_per_producer,
        cfg.total_messages(),
        args.repetitions
    );
    println!("custo do relogio: {:.1} ns/chamada", clock_ns);
    println!();

    let mut csv = args.csv.as_ref().map(open_csv);

    println!(
        "{:>4}  {:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>6}  {:>6}",
        "#", "msg/s", "p50", "p95", "p99", "max", "cpu", "erros"
    );

    let mut outcomes: Vec<RunOutcome> = Vec::with_capacity(args.repetitions);
    for i in 1..=args.repetitions {
        let queue = Arc::new(make_queue());
        let queue_name = queue.name();
        let o = run_once(queue, &cfg);
        println!(
            "{:>4}  {:>12.0}  {:>10}  {:>10}  {:>10}  {:>10}  {:>6.2}  {:>6}",
            i,
            o.throughput,
            fmt_ns(o.latency.p50 as f64),
            fmt_ns(o.latency.p95 as f64),
            fmt_ns(o.latency.p99 as f64),
            fmt_ns(o.latency.max as f64),
            o.cpu_cores_used(),
            o.errors()
        );
        if let Some(f) = csv.as_mut() {
            write_csv_row(f, queue_name, args, &cfg, i, &o);
        }
        outcomes.push(o);
    }
    println!();

    // --- Agregacao entre execucoes ---
    let agg = |f: fn(&RunOutcome) -> f64| -> RunAggregate {
        RunAggregate::from(&outcomes.iter().map(f).collect::<Vec<_>>())
    };
    let a_thr = agg(|o| o.throughput);
    let a_mib = agg(|o| o.byte_throughput / (1024.0 * 1024.0));
    let a_p50 = agg(|o| o.latency.p50 as f64);
    let a_p95 = agg(|o| o.latency.p95 as f64);
    let a_p99 = agg(|o| o.latency.p99 as f64);
    let a_max = agg(|o| o.latency.max as f64);
    let a_cores = agg(|o| o.cpu_cores_used());
    let a_cpu_msg = agg(|o| o.cpu_ns_per_message());
    let a_peak = agg(|o| o.peak_rss_kib as f64 / 1024.0);
    let a_extra = agg(|o| o.peak_rss_kib.saturating_sub(o.baseline_rss_kib) as f64 / 1024.0);

    println!("--- Agregado de {} execucoes (media +/- IC 95%, t de Student) ---", a_thr.n);
    println!(
        "vazao:        {:.0} +/- {:.0} msg/s  (desvio {:.0})",
        a_thr.mean, a_thr.ci95_half_width, a_thr.stddev
    );
    println!("vazao:        {:.2} +/- {:.2} MiB/s", a_mib.mean, a_mib.ci95_half_width);
    for (label, a) in [("p50", &a_p50), ("p95", &a_p95), ("p99", &a_p99), ("max", &a_max)] {
        println!(
            "latencia {label}: {} +/- {}",
            fmt_ns(a.mean),
            fmt_ns(a.ci95_half_width)
        );
    }
    println!(
        "cpu:          {:.2} +/- {:.2} nucleos ocupados | {:.0} ns de CPU por mensagem",
        a_cores.mean, a_cores.ci95_half_width, a_cpu_msg.mean
    );
    println!(
        "memoria:      pico de RSS {:.1} MiB (+{:.1} MiB acima do inicio da fase medida)",
        a_peak.mean, a_extra.mean
    );

    // --- Corretude ---
    let sum = |f: fn(&RunOutcome) -> u64| -> u64 { outcomes.iter().map(f).sum() };
    let (lost, dup, ooo, bad, disc) = (
        sum(|o| o.lost()),
        sum(|o| o.duplicated()),
        sum(|o| o.out_of_order),
        sum(|o| o.corrupted),
        sum(|o| o.discarded_samples),
    );
    let sent = sum(|o| o.sent);
    println!();
    println!(
        "corretude: perdidas={lost} duplicadas={dup} fora_de_ordem={ooo} corrompidas={bad} amostras_descartadas={disc} taxa_de_erro={:.2e}",
        (lost + dup + ooo + bad) as f64 / sent.max(1) as f64
    );
    if lost == 0 && dup == 0 && ooo == 0 && bad == 0 {
        println!("           OK: sem perda, sem duplicacao, ordem FIFO por produtor preservada, payload integro");
    }
    if let Some(path) = &args.csv {
        println!("resultados por execucao em {}", path.display());
    }
}

const CSV_HEADER: &str = "queue,payload_kind,topology,producers,consumers,payload_bytes,capacity,\
arrival,messages_per_producer,warmup_per_producer,repetition,sent,received,lost,duplicated,\
out_of_order,corrupted,elapsed_s,throughput_msg_s,throughput_mib_s,lat_mean_ns,lat_stddev_ns,lat_min_ns,\
lat_p50_ns,lat_p95_ns,lat_p99_ns,lat_p999_ns,lat_max_ns,cpu_s,cpu_cores,cpu_ns_per_msg,\
rss_baseline_kib,rss_peak_kib";

fn open_csv(path: &PathBuf) -> fs::File {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        fs::create_dir_all(dir)
            .unwrap_or_else(|e| fail(&format!("nao foi possivel criar {}: {e}", dir.display())));
    }
    let is_new = fs::metadata(path).map(|m| m.len() == 0).unwrap_or(true);
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|e| fail(&format!("nao foi possivel abrir {}: {e}", path.display())));
    if is_new {
        writeln!(f, "{CSV_HEADER}").expect("falha ao escrever CSV");
    }
    f
}

fn write_csv_row(
    f: &mut fs::File,
    queue: &str,
    args: &Args,
    cfg: &RunConfig,
    repetition: usize,
    o: &RunOutcome,
) {
    let l = &o.latency;
    writeln!(
        f,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.1},{:.3},{:.1},{:.1},{},{},{},{},{},{},{:.6},{:.3},{:.1},{},{}",
        queue,
        args.payload_kind.label(),
        cfg.topology(),
        cfg.producers,
        cfg.consumers,
        cfg.payload_bytes,
        args.capacity.map(|c| c.to_string()).unwrap_or_else(|| "unbounded".into()),
        cfg.arrival.label(),
        cfg.messages_per_producer,
        cfg.warmup_per_producer,
        repetition,
        o.sent,
        o.received,
        o.lost(),
        o.duplicated(),
        o.out_of_order,
        o.corrupted,
        o.elapsed_secs,
        o.throughput,
        o.byte_throughput / (1024.0 * 1024.0),
        l.mean,
        l.stddev,
        l.min,
        l.p50,
        l.p95,
        l.p99,
        l.p999,
        l.max,
        o.cpu_secs,
        o.cpu_cores_used(),
        o.cpu_ns_per_message(),
        o.baseline_rss_kib,
        o.peak_rss_kib,
    )
    .expect("falha ao escrever CSV");
}
