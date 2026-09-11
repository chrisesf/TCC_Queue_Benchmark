use std::sync::{Arc, Barrier};
use std::thread;

use crate::clock::{now_ns, Stopwatch};
use crate::message::{Message, PayloadFactory};
use crate::queue::Queue;
use crate::resource::ResourceSnapshot;
use crate::stats::LatencySummary;

/// Padrao de chegada das mensagens
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Arrival {
    Saturated,
    Burst { batch: u64, pause_ns: u64 },
    Paced { rate_per_producer: f64 },
}

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub producers: usize,
    pub consumers: usize,
    pub messages_per_producer: u64,
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
    pub elapsed_secs: f64,
    pub throughput: f64,
    pub byte_throughput: f64,
    pub latency: LatencySummary,
    pub out_of_order: u64,
    pub discarded_samples: u64,
    pub cpu_secs: f64,
    pub peak_rss_kib: u64,
}

impl RunOutcome {
    pub fn lost(&self) -> i64 {
        self.sent as i64 - self.received as i64
    }

    /// Nucleos ocupados em media durante a execucao.
    pub fn cpu_cores_used(&self) -> f64 {
        if self.elapsed_secs > 0.0 {
            self.cpu_secs / self.elapsed_secs
        } else {
            0.0
        }
    }
}

pub fn run_once<Q>(queue: Arc<Q>, cfg: &RunConfig) -> RunOutcome
where
    Q: Queue<Message> + 'static,
{
    assert!(cfg.producers >= 1 && cfg.consumers >= 1);

    let barrier = Arc::new(Barrier::new(cfg.producers + cfg.consumers + 1));

    // --- Consumidores ---
    let expected_per_consumer =
        (cfg.total_messages() as usize / cfg.consumers).saturating_add(1024);

    let mut consumer_handles = Vec::with_capacity(cfg.consumers);
    for _ in 0..cfg.consumers {
        let q = Arc::clone(&queue);
        let b = Arc::clone(&barrier);
        let n_producers = cfg.producers;
        consumer_handles.push(thread::spawn(move || {
            let mut latencies: Vec<u64> = Vec::with_capacity(expected_per_consumer);
            let mut last_seq: Vec<Option<u64>> = vec![None; n_producers];
            let mut received: u64 = 0;
            let mut out_of_order: u64 = 0;
            let mut discarded: u64 = 0;

            b.wait();

            while let Some(msg) = q.pop() {
                match msg.latency_ns() {
                    Some(l) => latencies.push(l),
                    None => discarded += 1,
                }
                let pid = msg.producer_id() as usize;
                if pid < last_seq.len() {
                    let seq = msg.sequence();
                    if let Some(prev) = last_seq[pid] {
                        if seq <= prev {
                            out_of_order += 1;
                        }
                    }
                    last_seq[pid] = Some(seq);
                }
                received += 1;
                drop(msg);
            }

            ConsumerReport {
                latencies,
                received,
                out_of_order,
                discarded,
            }
        }));
    }

    // --- Produtores ---
    let mut producer_handles = Vec::with_capacity(cfg.producers);
    for pid in 0..cfg.producers {
        let q = Arc::clone(&queue);
        let b = Arc::clone(&barrier);
        let n = cfg.messages_per_producer;
        let arrival = cfg.arrival;
        let payload_bytes = cfg.payload_bytes;
        producer_handles.push(thread::spawn(move || {
            let factory = PayloadFactory::new(payload_bytes, 0x9E3779B9 ^ (pid as u64 + 1));
            let producer_id = pid as u16;
            let mut sent: u64 = 0;

            b.wait();

            let start = now_ns();
            let interval_ns = match arrival {
                Arrival::Paced { rate_per_producer } if rate_per_producer > 0.0 => {
                    1_000_000_000.0 / rate_per_producer
                }
                _ => 0.0,
            };

            for seq in 0..n {
                let msg = match arrival {
                    Arrival::Paced { .. } => {
                        let deadline = start + (interval_ns * seq as f64) as u64;
                        spin_until(deadline);
                        Message::with_timestamp(producer_id, seq, deadline.max(start), factory.make())
                    }
                    Arrival::Burst { batch, pause_ns } => {
                        if seq > 0 && batch > 0 && seq % batch == 0 && pause_ns > 0 {
                            spin_until(now_ns() + pause_ns);
                        }
                        Message::new(producer_id, seq, factory.make())
                    }
                    Arrival::Saturated => Message::new(producer_id, seq, factory.make()),
                };

                if q.push(msg).is_err() {
                    break;
                }
                sent += 1;
            }
            sent
        }));
    }

    barrier.wait();
    let res_before = ResourceSnapshot::capture();
    let sw = Stopwatch::start();

    let mut sent: u64 = 0;
    for h in producer_handles {
        sent += h.join().expect("thread produtora entrou em panico");
    }

    queue.close();

    let mut latencies: Vec<u64> = Vec::with_capacity(cfg.total_messages() as usize);
    let mut received = 0u64;
    let mut out_of_order = 0u64;
    let mut discarded = 0u64;
    for h in consumer_handles {
        let r = h.join().expect("thread consumidora entrou em panico");
        latencies.extend_from_slice(&r.latencies);
        received += r.received;
        out_of_order += r.out_of_order;
        discarded += r.discarded;
    }

    let elapsed_secs = sw.elapsed_secs();
    let res = ResourceSnapshot::capture().delta_since(&res_before);

    RunOutcome {
        sent,
        received,
        elapsed_secs,
        throughput: received as f64 / elapsed_secs,
        byte_throughput: (received as f64 * cfg.payload_bytes as f64) / elapsed_secs,
        latency: LatencySummary::from_samples(&mut latencies),
        out_of_order,
        discarded_samples: discarded,
        cpu_secs: res.cpu_secs,
        peak_rss_kib: res.peak_rss_kib,
    }
}

struct ConsumerReport {
    latencies: Vec<u64>,
    received: u64,
    out_of_order: u64,
    discarded: u64,
}

#[inline]
fn spin_until(deadline_ns: u64) {
    while now_ns() < deadline_ns {
        std::hint::spin_loop();
    }
}
