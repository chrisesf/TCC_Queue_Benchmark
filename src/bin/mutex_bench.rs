//! Executa o benchmark da fila lock-based (`MutexQueue`).
//!
//! Uso:
//!   cargo run --release --bin mutex_bench -- [opcoes]
//!
//! Opcoes:
//!   --producers N        threads produtoras (padrão 4)
//!   --consumers N        threads consumidoras (padrão 4)
//!   --messages N         mensagens por produtor na fase medida (padrão 200000)
//!   --payload N          bytes de payload: 64, 1024 ou 65536 (padrão 64)
//!   --capacity N         capacidade da fila (padrão 8192; 0 = ilimitada)
//!   --repetitions N      execuções independentes medidas (padrão 10)
//!   --arrival MODE       saturated | burst | paced (padrão saturated)
//!   --rate N             mensagens/s por produtor, para --arrival paced
//!   --batch N            tamanho da rajada, para --arrival burst (padrão 1000)
//!   --pause-ns N         pausa entre rajadas em ns (padrão 100000)

use std::sync::Arc;

use tcc_bench::bench::{run_once, Arrival, RunConfig, RunOutcome};
use tcc_bench::clock::measure_clock_overhead_ns;
use tcc_bench::queues::MutexQueue;
use tcc_bench::stats::{fmt_ns, RunAggregate};
use tcc_bench::Message;

fn main() {
    let args = Args::parse();

    let cfg = RunConfig {
        producers: args.producers,
        consumers: args.consumers,
        messages_per_producer: args.messages,
        payload_bytes: args.payload,
        arrival: args.arrival,
    };

    let clock_ns = measure_clock_overhead_ns(200_000);

    println!("=== Fila lock-based (Mutex) ===");
    println!(
        "topologia={} produtores={} consumidores={} payload={}B capacidade={} chegada={:?}",
        cfg.topology(),
        cfg.producers,
        cfg.consumers,
        cfg.payload_bytes,
        args.capacity
            .map(|c| c.to_string())
            .unwrap_or_else(|| "ilimitada".into()),
        cfg.arrival
    );
    println!(
        "mensagens/produções={} total/execução={} repetições={}",
        cfg.messages_per_producer,
        cfg.total_messages(),
        args.repetitions
    );
    println!("custo do relógio: {:.1} ns/chamada", clock_ns);
    println!();

    let warmup_cfg = RunConfig {
        messages_per_producer: (cfg.messages_per_producer / 10).max(1_000),
        ..cfg.clone()
    };
    let w = run_once(Arc::new(make_queue(args.capacity)), &warmup_cfg);
    println!(
        "warmup: {} msgs, {:.0} msg/s (descartado)",
        w.received, w.throughput
    );
    println!();

    // --- execuções medidas ---
    println!(
        "{:>4}  {:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>6}",
        "#", "msg/s", "p50", "p95", "p99", "max", "erros"
    );

    let mut outcomes: Vec<RunOutcome> = Vec::with_capacity(args.repetitions);
    for i in 1..=args.repetitions {
        let q: Arc<MutexQueue<Message>> = Arc::new(make_queue(args.capacity));
        let o = run_once(q, &cfg);
        println!(
            "{:>4}  {:>12.0}  {:>10}  {:>10}  {:>10}  {:>10}  {:>6}",
            i,
            o.throughput,
            fmt_ns(o.latency.p50 as f64),
            fmt_ns(o.latency.p95 as f64),
            fmt_ns(o.latency.p99 as f64),
            fmt_ns(o.latency.max as f64),
            o.lost().abs() + o.out_of_order as i64
        );
        outcomes.push(o);
    }
    println!();

    // --- Agregacao ---
    let thr: Vec<f64> = outcomes.iter().map(|o| o.throughput).collect();
    let p50: Vec<f64> = outcomes.iter().map(|o| o.latency.p50 as f64).collect();
    let p99: Vec<f64> = outcomes.iter().map(|o| o.latency.p99 as f64).collect();

    let a_thr = RunAggregate::from(&thr);
    let a_p50 = RunAggregate::from(&p50);
    let a_p99 = RunAggregate::from(&p99);

    println!("--- Agregado de {} execuções (IC 95%) ---", a_thr.n);
    println!(
        "vazão:      {:.0} +/- {:.0} msg/s   (desvio {:.0})",
        a_thr.mean, a_thr.ci95_half_width, a_thr.stddev
    );
    println!(
        "vazão:      {:.2} MiB/s",
        outcomes.iter().map(|o| o.byte_throughput).sum::<f64>()
            / outcomes.len() as f64
            / (1024.0 * 1024.0)
    );
    println!(
        "latência p50: {} +/- {}",
        fmt_ns(a_p50.mean),
        fmt_ns(a_p50.ci95_half_width)
    );
    println!(
        "latência p99: {} +/- {}",
        fmt_ns(a_p99.mean),
        fmt_ns(a_p99.ci95_half_width)
    );

    let last = outcomes.last().unwrap();
    println!(
        "cpu: {:.2} núcleos ocupados em média | pico de RSS: {:.1} MiB",
        outcomes.iter().map(|o| o.cpu_cores_used()).sum::<f64>() / outcomes.len() as f64,
        last.peak_rss_kib as f64 / 1024.0
    );

    // --- Corretude ---
    let total_lost: i64 = outcomes.iter().map(|o| o.lost()).sum();
    let total_ooo: u64 = outcomes.iter().map(|o| o.out_of_order).sum();
    let total_disc: u64 = outcomes.iter().map(|o| o.discarded_samples).sum();
    println!();
    println!(
        "corretude: perdidas={} fora_de_ordem={} amostras_descartadas={}",
        total_lost, total_ooo, total_disc
    );
    if total_lost == 0 && total_ooo == 0 {
        println!("           OK: sem perda, sem duplicacao, ordem FIFO por produtor preservada");
    }
}

fn make_queue(capacity: Option<usize>) -> MutexQueue<Message> {
    match capacity {
        Some(c) => MutexQueue::with_capacity(c),
        None => MutexQueue::unbounded(),
    }
}

struct Args {
    producers: usize,
    consumers: usize,
    messages: u64,
    payload: usize,
    capacity: Option<usize>,
    repetitions: usize,
    arrival: Arrival,
}

impl Args {
    fn parse() -> Self {
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let get = |name: &str| -> Option<String> {
            argv.iter()
                .position(|a| a == name)
                .and_then(|i| argv.get(i + 1))
                .cloned()
        };
        let num = |name: &str, default: u64| -> u64 {
            get(name)
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };

        let capacity = num("--capacity", 8192);
        let mode = get("--arrival").unwrap_or_else(|| "saturated".into());
        let arrival = match mode.as_str() {
            "burst" => Arrival::Burst {
                batch: num("--batch", 1_000),
                pause_ns: num("--pause-ns", 100_000),
            },
            "paced" => Arrival::Paced {
                rate_per_producer: num("--rate", 100_000) as f64,
            },
            _ => Arrival::Saturated,
        };

        Self {
            producers: num("--producers", 4) as usize,
            consumers: num("--consumers", 4) as usize,
            messages: num("--messages", 200_000),
            payload: num("--payload", 64) as usize,
            capacity: if capacity == 0 {
                None
            } else {
                Some(capacity as usize)
            },
            repetitions: num("--repetitions", 10) as usize,
            arrival,
        }
    }
}
