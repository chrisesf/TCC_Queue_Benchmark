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
        Self {
            n,
            mean,
            stddev,
            ci95_half_width: t_critical_95(n - 1) * stddev / (n as f64).sqrt(),
        }
    }
}

/// Valor critico bicaudal de t de Student para 95% de confianca. Com poucas execucoes
/// o z = 1,96 subestima o intervalo (df = 9 exige 2,262).
pub fn t_critical_95(df: usize) -> f64 {
    const TABLE: [f64; 30] = [
        12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179,
        2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064,
        2.060, 2.056, 2.052, 2.048, 2.045, 2.042,
    ];
    match df {
        0 => f64::NAN,
        1..=30 => TABLE[df - 1],
        31..=40 => 2.021,
        41..=60 => 2.000,
        61..=120 => 1.980,
        _ => 1.960,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentis_interpolam_entre_amostras() {
        let mut v: Vec<u64> = (1..=100).collect();
        let s = LatencySummary::from_samples(&mut v);
        assert_eq!(s.min, 1);
        assert_eq!(s.max, 100);
        assert_eq!(s.p50, 51); // rank 49.5 -> (50 + 51) / 2 arredondado
        assert_eq!(s.p99, 99);
        assert!((s.mean - 50.5).abs() < 1e-9);
    }

    #[test]
    fn ic95_usa_t_de_student() {
        let values = [10.0, 12.0, 11.0, 13.0, 9.0, 10.0, 12.0, 11.0, 10.0, 12.0];
        let a = RunAggregate::from(&values);
        let expected = 2.262 * a.stddev / (10f64).sqrt();
        assert!((a.ci95_half_width - expected).abs() < 1e-9);
    }

    #[test]
    fn t_converge_para_z() {
        assert_eq!(t_critical_95(29), 2.045);
        assert_eq!(t_critical_95(1_000), 1.960);
    }
}
