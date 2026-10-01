use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use crate::clock::{now_ns, Stopwatch};
use crate::message::{payload_checksum, payload_seed, BenchMessage, PayloadFactory};
use crate::queue::Queue;
use crate::resource::{self, ResourceSnapshot};
use crate::stats::LatencySummary;

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
    pub cpu_secs: f64,
    /// RSS no inicio da fase medida.
    pub baseline_rss_kib: u64,
    /// Pico de RSS durante a fase medida (zerado antes dela quando o kernel permite).
    pub peak_rss_kib: u64,
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

    /// Tempo de CPU gasto por mensagem entregue, somando todas as threads.
    pub fn cpu_ns_per_message(&self) -> f64 {
        if self.received > 0 {
            self.cpu_secs * 1e9 / self.received as f64
        } else {
            0.0
        }
    }
}

pub fn run_once<Q, M>(queue: Arc<Q>, cfg: &RunConfig) -> RunOutcome
where
    Q: Queue<M> + 'static,
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

    // --- Consumidores ---
    let total = cfg.total_messages() as usize;
    let bitmap_words = total.div_ceil(64);

    let mut consumer_handles = Vec::with_capacity(cfg.consumers);
    for _ in 0..cfg.consumers {
        let q = Arc::clone(&queue);
        let b = Arc::clone(&start_barrier);
        let warm = Arc::clone(&warmup_consumed);
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

            b.wait();

            while let Some(msg) = q.pop() {
                let latency = msg.latency_ns();
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
                drop(msg);
            }

            ConsumerReport {
                latencies,
                seen,
                received,
                out_of_order,
                corrupted,
                discarded,
            }
        }));
    }

    // --- Produtores ---
    let mut producer_handles = Vec::with_capacity(cfg.producers);
    for pid in 0..cfg.producers {
        let q = Arc::clone(&queue);
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
                if q.push(msg).is_err() {
                    break;
                }
            }

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

                if q.push(msg).is_err() {
                    break;
                }
                sent += 1;
            }
            sent
        }));
    }

    start_barrier.wait();

    // --- Warm-up: aguarda a fila drenar antes de liberar a fase medida ---
    while warmup_consumed.load(Ordering::Acquire) < warmup_total {
        thread::sleep(Duration::from_micros(100));
    }

    resource::reset_peak_rss();
    let res_before = ResourceSnapshot::capture();
    let sw = Stopwatch::start();
    measure_barrier.wait();

    let mut sent: u64 = 0;
    for h in producer_handles {
        sent += h.join().expect("thread produtora entrou em panico");
    }

    queue.close();

    let reports: Vec<ConsumerReport> = consumer_handles
        .into_iter()
        .map(|h| h.join().expect("thread consumidora entrou em panico"))
        .collect();

    let elapsed_secs = sw.elapsed_secs();
    let res = ResourceSnapshot::capture().delta_since(&res_before);

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
}

#[inline]
fn spin_until(deadline_ns: u64) {
    while now_ns() < deadline_ns {
        std::hint::spin_loop();
    }
}
