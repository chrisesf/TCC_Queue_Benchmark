//! Estatistica descritiva das amostras coletadas (Secao 4.6).

/// Resumo de uma distribuicao de latencias, em nanossegundos.
#[derive(Debug, Clone, Default)]
pub struct LatencySummary {
    pub count: usize,
    pub mean: f64,
    pub stddev: f64,
    pub min: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

impl LatencySummary {
    pub fn from_samples(samples: &mut [u64]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        samples.sort_unstable();
        let n = samples.len();
        let mean = samples.iter().map(|&v| v as f64).sum::<f64>() / n as f64;
        let var = if n > 1 {
            samples
                .iter()
                .map(|&v| {
                    let d = v as f64 - mean;
                    d * d
                })
                .sum::<f64>()
                / (n - 1) as f64
        } else {
            0.0
        };
        Self {
            count: n,
            mean,
            stddev: var.sqrt(),
            min: samples[0],
            p50: percentile_sorted(samples, 50.0),
            p95: percentile_sorted(samples, 95.0),
            p99: percentile_sorted(samples, 99.0),
            p999: percentile_sorted(samples, 99.9),
            max: samples[n - 1],
        }
    }
}

pub fn percentile_sorted(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = (p / 100.0) * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = rank - lo as f64;
        (sorted[lo] as f64 + frac * (sorted[hi] as f64 - sorted[lo] as f64)).round() as u64
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunAggregate {
    pub n: usize,
    pub mean: f64,
    pub stddev: f64,
    pub ci95_half_width: f64,
}

impl RunAggregate {
    pub fn from(values: &[f64]) -> Self {
        let n = values.len();
        if n == 0 {
            return Self::default();
        }
        let mean = values.iter().sum::<f64>() / n as f64;
        if n == 1 {
            return Self {
                n,
                mean,
                stddev: 0.0,
                ci95_half_width: 0.0,
            };
        }
        let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
        let stddev = var.sqrt();
        let z = 1.96;
        Self {
            n,
            mean,
            stddev,
            ci95_half_width: z * stddev / (n as f64).sqrt(),
        }
    }
}

pub fn fmt_ns(ns: f64) -> String {
    if ns < 1_000.0 {
        format!("{:.0} ns", ns)
    } else if ns < 1_000_000.0 {
        format!("{:.2} us", ns / 1_000.0)
    } else if ns < 1_000_000_000.0 {
        format!("{:.2} ms", ns / 1_000_000.0)
    } else {
        format!("{:.2} s", ns / 1_000_000_000.0)
    }
}
