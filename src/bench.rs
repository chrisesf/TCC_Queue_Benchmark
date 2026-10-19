use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use crate::clock::now_ns;
use crate::message::{payload_checksum, payload_seed, BenchMessage, PayloadFactory};
use crate::queue::Queue;
use crate::resource::{self, ContainerMonitor, ResourceSnapshot};
use crate::stats::LatencySummary;
use crate::transport::{Consumer, InMemory, Producer, Recv, Transport};

/// Padrao de chegada das mensagens
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Arrival {
    /// Produtores enviam o mais rapido possivel (vazao maxima).
    Saturated,
    /// Rajadas de `batch` mensagens separadas por `pause_ns`.
    Burst { batch: u64, pause_ns: u64 },
    /// Taxa fixa por produtor ("equilibrado", producao ~ consumo). O timestamp e o
    /// instante agendado, nao o real, para nao esconder atrasos (coordinated omission).
    Paced { rate_per_producer: f64 },
}

impl Arrival {
    /// Rotulo sem virgulas, adequado para CSV.
    pub fn label(&self) -> String {
        match self {
            Arrival::Saturated => "saturated".into(),
            Arrival::Burst { batch, pause_ns } => format!("burst(batch={batch};pause_ns={pause_ns})"),
            Arrival::Paced { rate_per_producer } => format!("paced(rate={rate_per_producer})"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub producers: usize,
    pub consumers: usize,
    pub messages_per_producer: u64,
    /// Mensagens por produtor enviadas antes da medicao, na mesma fila, e descartadas.
    pub warmup_per_producer: u64,
    pub payload_bytes: usize,
    pub arrival: Arrival,
    /// Tempo maximo sem progresso: no warm-up (se estourar, a medicao comeca assim mesmo,
    /// com aviso) e, com brokers, apos o fim da producao, para concluir que as mensagens
    /// que faltam se perderam.
    pub idle_timeout: Duration,
}

impl RunConfig {
    pub fn total_messages(&self) -> u64 {
        self.messages_per_producer * self.producers as u64
    }

    pub fn topology(&self) -> &'static str {
        match (self.producers, self.consumers) {
            (1, 1) => "SPSC",
            (_, 1) => "MPSC",
            (1, _) => "SPMC",
            _ => "MPMC",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub sent: u64,
    pub received: u64,
    /// Ids distintos recebidos na fase medida.
    pub unique: u64,
    pub elapsed_secs: f64,
    pub throughput: f64,
    pub byte_throughput: f64,
    pub latency: LatencySummary,
    pub out_of_order: u64,
    /// Mensagens cujo payload nao confere com o gerado pelo produtor.
    pub corrupted: u64,
    pub discarded_samples: u64,
    /// CPU do processo cliente (produtores, consumidores e threads das bibliotecas).
    pub cpu_secs: f64,
    /// RSS no inicio da fase medida.
    pub baseline_rss_kib: u64,
    /// Pico de RSS durante a fase medida (zerado antes dela quando o kernel permite).
    pub peak_rss_kib: u64,
    /// Mensagens aceitas por `send` e depois rejeitadas pelo transporte (nack, falha de
    /// entrega); ja descontadas de `sent`.
    pub rejected: u64,
    /// CPU do container do broker entre as amostras que cercam a fase medida (`None`
    /// para filas em memoria ou sem acesso a API do Docker).
    pub broker_cpu_secs: Option<f64>,
    /// Nucleos ocupados pelo broker no intervalo real entre essas amostras, que pode ser
    /// bem maior que a fase medida em execucoes curtas.
    pub broker_cpu_cores: Option<f64>,
    pub broker_peak_mem_kib: Option<u64>,
}

impl RunOutcome {
    pub fn lost(&self) -> u64 {
        self.sent.saturating_sub(self.unique)
    }

    pub fn duplicated(&self) -> u64 {
        self.received.saturating_sub(self.unique)
    }

    pub fn errors(&self) -> u64 {
        self.lost() + self.duplicated() + self.out_of_order + self.corrupted
    }

    pub fn error_rate(&self) -> f64 {
        if self.sent > 0 {
            self.errors() as f64 / self.sent as f64
        } else {
            0.0
        }
    }

    /// Nucleos ocupados em media durante a execucao.
    pub fn cpu_cores_used(&self) -> f64 {
        if self.elapsed_secs > 0.0 {
            self.cpu_secs / self.elapsed_secs
        } else {
            0.0
        }
    }

    /// Tempo de CPU do cliente por mensagem entregue, somando todas as threads.
    pub fn cpu_ns_per_message(&self) -> f64 {
        if self.received > 0 {
            self.cpu_secs * 1e9 / self.received as f64
        } else {
            0.0
        }
    }

    /// CPU do cliente mais a do broker, por mensagem entregue.
    pub fn total_cpu_ns_per_message(&self) -> f64 {
        if self.received > 0 {
            (self.cpu_secs + self.broker_cpu_secs.unwrap_or(0.0)) * 1e9 / self.received as f64
        } else {
            0.0
        }
    }
}

/// Executa uma medicao sobre uma fila em memoria.
pub fn run_once<Q, M>(queue: Arc<Q>, cfg: &RunConfig) -> RunOutcome
where
    Q: Queue<M> + 'static,
    M: BenchMessage,
{
    run(&InMemory(queue), cfg)
}

/// Executa uma medicao sobre qualquer transporte. O tempo medido vai da liberacao dos
/// produtores ate a ultima mensagem medida ser consumida.
pub fn run<T, M>(transport: &T, cfg: &RunConfig) -> RunOutcome
where
    T: Transport<M>,
    M: BenchMessage,
{
    assert!(cfg.producers >= 1 && cfg.consumers >= 1);
    now_ns();
    // Antes de qualquer alocacao desta execucao, para que ela nao reaproveite paginas
    // ainda residentes da anterior.
    resource::release_free_memory();

    let start_barrier = Arc::new(Barrier::new(cfg.producers + cfg.consumers + 1));
    let measure_barrier = Arc::new(Barrier::new(cfg.producers + 1));
    let warmup_consumed = Arc::new(AtomicU64::new(0));
    let warmup_total = cfg.warmup_per_producer * cfg.producers as u64;
    let end = Arc::new(EndState::new());

    // --- Consumidores ---
    let total = cfg.total_messages() as usize;
    let bitmap_words = total.div_ceil(64);

    let mut consumer_handles = Vec::with_capacity(cfg.consumers);
    for index in 0..cfg.consumers {
        let mut q = transport.consumer(index);
        let b = Arc::clone(&start_barrier);
        let warm = Arc::clone(&warmup_consumed);
        let end = Arc::clone(&end);
        let idle_timeout = cfg.idle_timeout;
        let n_producers = cfg.producers;
        let warmup = cfg.warmup_per_producer;
        let per_producer = cfg.messages_per_producer;
        let payload_bytes = cfg.payload_bytes;
        consumer_handles.push(thread::spawn(move || {
            // Capacidade para o pior caso (um consumidor recebe tudo): evita realocacao
            // durante a medicao; o SO so materializa as paginas efetivamente escritas.
            let mut latencies: Vec<u64> = Vec::with_capacity(total);
            let mut seen = vec![0u64; bitmap_words];
            let mut last_seq: Vec<Option<u64>> = vec![None; n_producers];
            let expected_checksum: Vec<u64> = (0..n_producers)
                .map(|p| {
                    let f = PayloadFactory::new(payload_bytes, payload_seed(p as u16));
                    payload_checksum(f.template())
                })
                .collect();
            let mut received: u64 = 0;
            let mut out_of_order: u64 = 0;
            let mut corrupted: u64 = 0;
            let mut discarded: u64 = 0;
            let mut last_recv_ns: u64 = 0;
            // Medidas recebidas ainda nao somadas em `end.measured`: o contador global so
            // e atualizado quando o consumidor fica ocioso, para nao acrescentar uma
            // operacao atomica compartilhada por mensagem.
            let mut unpublished: u64 = 0;
            let mut idle_since: Option<Instant> = None;

            b.wait();

            loop {
                let msg = match q.recv(RECV_POLL) {
                    Recv::Message(m) => m,
                    Recv::Closed => break,
                    Recv::Empty => {
                        if end.should_stop(&mut unpublished, &mut idle_since, idle_timeout) {
                            break;
                        }
                        continue;
                    }
                };
                idle_since = None;
                let now = now_ns();
                let latency = now.checked_sub(msg.timestamp_ns());
                let pid = msg.producer_id() as usize;
                let seq = msg.sequence();

                // Todo mecanismo paga a leitura completa do payload, que tambem valida
                // sua integridade.
                let checksum = payload_checksum(msg.payload());
                if pid >= n_producers || checksum != expected_checksum[pid] {
                    corrupted += 1;
                }

                if pid < n_producers {
                    if let Some(prev) = last_seq[pid] {
                        if seq <= prev {
                            out_of_order += 1;
                        }
                    }
                    last_seq[pid] = Some(seq);
                }

                if seq < warmup {
                    warm.fetch_add(1, Ordering::Release);
                    continue;
                }

                match latency {
                    Some(l) => latencies.push(l),
                    None => discarded += 1,
                }
                let offset = seq - warmup;
                if pid < n_producers && offset < per_producer {
                    let idx = pid * per_producer as usize + offset as usize;
                    seen[idx / 64] |= 1 << (idx % 64);
                }
                received += 1;
                unpublished += 1;
                last_recv_ns = now;
                drop(msg);
            }

            ConsumerReport {
                latencies,
                seen,
                received,
                out_of_order,
                corrupted,
                discarded,
                last_recv_ns,
            }
        }));
    }

    // --- Produtores ---
    let mut producer_handles = Vec::with_capacity(cfg.producers);
    for pid in 0..cfg.producers {
        let mut q = transport.producer(pid);
        let b = Arc::clone(&start_barrier);
        let go = Arc::clone(&measure_barrier);
        let n = cfg.messages_per_producer;
        let warmup = cfg.warmup_per_producer;
        let arrival = cfg.arrival;
        let payload_bytes = cfg.payload_bytes;
        producer_handles.push(thread::spawn(move || {
            let factory = PayloadFactory::new(payload_bytes, payload_seed(pid as u16));
            let template = factory.template();
            let producer_id = pid as u16;
            let mut sent: u64 = 0;

            b.wait();

            for seq in 0..warmup {
                let mut msg = M::build(producer_id, seq, template);
                msg.set_timestamp(now_ns());
                if q.send(msg).is_err() {
                    break;
                }
            }
            // Com brokers, garante que o warm-up foi confirmado antes da fase medida.
            q.flush();

            go.wait();

            let start = now_ns();
            let interval_ns = match arrival {
                Arrival::Paced { rate_per_producer } if rate_per_producer > 0.0 => {
                    1_000_000_000.0 / rate_per_producer
                }
                _ => 0.0,
            };

            for i in 0..n {
                let seq = warmup + i;
                let msg = match arrival {
                    Arrival::Paced { .. } => {
                        let deadline = start + (interval_ns * i as f64) as u64;
                        spin_until(deadline);
                        let mut msg = M::build(producer_id, seq, template);
                        msg.set_timestamp(deadline);
                        msg
                    }
                    Arrival::Burst { batch, pause_ns } => {
                        if i > 0 && batch > 0 && i % batch == 0 && pause_ns > 0 {
                            spin_until(now_ns() + pause_ns);
                        }
                        let mut msg = M::build(producer_id, seq, template);
                        msg.set_timestamp(now_ns());
                        msg
                    }
                    Arrival::Saturated => {
                        let mut msg = M::build(producer_id, seq, template);
                        msg.set_timestamp(now_ns());
                        msg
                    }
                };

                if q.send(msg).is_err() {
                    break;
                }
                sent += 1;
            }
            // Dentro do tempo medido: com confirmacoes, o envio so conta quando confirmado.
            let rejected = q.flush();
            (sent.saturating_sub(rejected), rejected)
        }));
    }

    start_barrier.wait();

    // --- Warm-up: aguarda o consumo antes de liberar a fase medida ---
    let mut progress = (0u64, Instant::now());
    loop {
        let consumed = warmup_consumed.load(Ordering::Acquire);
        if consumed >= warmup_total {
            break;
        }
        if consumed != progress.0 {
            progress = (consumed, Instant::now());
        } else if progress.1.elapsed() >= cfg.idle_timeout {
            eprintln!(
                "aviso: warm-up incompleto ({consumed} de {warmup_total} consumidas); medindo assim mesmo"
            );
            break;
        }
        thread::sleep(Duration::from_micros(100));
    }

    let broker_monitor = transport.external_container().and_then(ContainerMonitor::start);
    resource::reset_peak_rss();
    let res_before = ResourceSnapshot::capture();
    let start_ns = now_ns();
    measure_barrier.wait();

    let mut sent: u64 = 0;
    let mut rejected: u64 = 0;
    for h in producer_handles {
        let (s, r) = h.join().expect("thread produtora entrou em panico");
        sent += s;
        rejected += r;
    }

    end.expected.store(sent, Ordering::Release);
    end.producers_done.store(true, Ordering::Release);
    transport.finish();

    let reports: Vec<ConsumerReport> = consumer_handles
        .into_iter()
        .map(|h| h.join().expect("thread consumidora entrou em panico"))
        .collect();

    let end_ns = now_ns();
    let res = ResourceSnapshot::capture().delta_since(&res_before);
    let broker = broker_monitor.and_then(ContainerMonitor::finish);

    let last_recv_ns = reports.iter().map(|r| r.last_recv_ns).max().unwrap_or(0);
    let measured_end = if last_recv_ns > start_ns { last_recv_ns } else { end_ns };
    let elapsed_secs = (measured_end - start_ns) as f64 * 1e-9;

    // --- Agregacao (fora do tempo medido) ---
    let mut latencies: Vec<u64> = Vec::with_capacity(total);
    let mut union = vec![0u64; bitmap_words];
    let mut received = 0u64;
    let mut out_of_order = 0u64;
    let mut corrupted = 0u64;
    let mut discarded = 0u64;
    for r in reports {
        latencies.extend_from_slice(&r.latencies);
        for (acc, word) in union.iter_mut().zip(&r.seen) {
            *acc |= word;
        }
        received += r.received;
        out_of_order += r.out_of_order;
        corrupted += r.corrupted;
        discarded += r.discarded;
    }
    let unique: u64 = union.iter().map(|w| w.count_ones() as u64).sum();

    RunOutcome {
        sent,
        received,
        unique,
        elapsed_secs,
        throughput: received as f64 / elapsed_secs,
        byte_throughput: (received as f64 * cfg.payload_bytes as f64) / elapsed_secs,
        latency: LatencySummary::from_samples(&mut latencies),
        out_of_order,
        corrupted,
        discarded_samples: discarded,
        cpu_secs: res.cpu_secs,
        baseline_rss_kib: res_before.rss_kib,
        peak_rss_kib: res.peak_rss_kib,
        rejected,
        broker_cpu_secs: broker.map(|b| b.cpu_secs),
        broker_cpu_cores: broker.map(|b| b.cpu_secs / b.wall_secs.max(1e-9)),
        broker_peak_mem_kib: broker.map(|b| b.peak_mem_kib),
    }
}

/// Espera maxima de `Consumer::recv` antes de reavaliar o encerramento (brokers).
const RECV_POLL: Duration = Duration::from_millis(10);

/// Encerramento dos consumidores de transportes sem fechamento (brokers): param quando
/// todas as mensagens medidas aceitas chegaram ou, se houve perda, apos `idle_timeout`
/// sem mensagens depois do fim da producao. Mensagens do warm-up nao contam, entao a
/// perda delas nao atrasa o fim.
struct EndState {
    measured: AtomicU64,
    /// Mensagens medidas aceitas; `u64::MAX` ate os produtores terminarem.
    expected: AtomicU64,
    producers_done: AtomicBool,
    done: AtomicBool,
}

impl EndState {
    fn new() -> Self {
        Self {
            measured: AtomicU64::new(0),
            expected: AtomicU64::new(u64::MAX),
            producers_done: AtomicBool::new(false),
            done: AtomicBool::new(false),
        }
    }

    /// Chamado por um consumidor ocioso: publica sua contagem e decide se encerra.
    fn should_stop(
        &self,
        unpublished: &mut u64,
        idle_since: &mut Option<Instant>,
        idle_timeout: Duration,
    ) -> bool {
        if *unpublished > 0 {
            self.measured.fetch_add(*unpublished, Ordering::AcqRel);
            *unpublished = 0;
        }
        if self.done.load(Ordering::Acquire) {
            return true;
        }
        if self.measured.load(Ordering::Acquire) >= self.expected.load(Ordering::Acquire) {
            self.done.store(true, Ordering::Release);
            return true;
        }
        self.producers_done.load(Ordering::Acquire)
            && idle_since.get_or_insert_with(Instant::now).elapsed() >= idle_timeout
    }
}

struct ConsumerReport {
    latencies: Vec<u64>,
    /// Bitmap dos ids medidos recebidos por este consumidor.
    seen: Vec<u64>,
    received: u64,
    out_of_order: u64,
    corrupted: u64,
    discarded: u64,
    /// Instante (`now_ns`) da ultima mensagem medida recebida; 0 se nenhuma.
    last_recv_ns: u64,
}

#[inline]
fn spin_until(deadline_ns: u64) {
    while now_ns() < deadline_ns {
        std::hint::spin_loop();
    }
}
