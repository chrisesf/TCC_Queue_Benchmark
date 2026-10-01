use std::sync::OnceLock;
use std::time::Instant;

static ANCHOR: OnceLock<Instant> = OnceLock::new();

/// Nanossegundos desde uma ancora monotona do processo.
///
/// Usa `Instant` (CLOCK_MONOTONIC no Linux), imune a ajustes de NTP. Os valores so
/// sao comparaveis dentro do mesmo processo, que e onde produtores e consumidores
/// do benchmark rodam, inclusive nos cenarios com broker.
#[inline(always)]
pub fn now_ns() -> u64 {
    ANCHOR.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

pub struct Stopwatch {
    start: Instant,
}

impl Stopwatch {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }
}

pub fn measure_clock_overhead_ns(samples: u32) -> f64 {
    // Aquecimento.
    for _ in 0..1_000 {
        std::hint::black_box(now_ns());
    }
    let sw = Stopwatch::start();
    for _ in 0..samples {
        std::hint::black_box(now_ns());
    }
    sw.elapsed_ns() as f64 / samples as f64
}
