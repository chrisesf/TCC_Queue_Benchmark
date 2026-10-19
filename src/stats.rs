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

/// Resultado de um teste bicaudal entre duas amostras de execucoes independentes.
#[derive(Debug, Clone, PartialEq)]
pub struct TwoSampleTest {
    /// `t` (Welch) ou `mann-whitney`, escolhido pela normalidade (Secao 4.6).
    pub name: &'static str,
    pub statistic: f64,
    pub significant_95: bool,
}

/// Verdadeiro quando a amostra nao rejeita normalidade a 5% (Jarque-Bera). Amostras
/// curtas ou constantes nao sustentam o teste t.
pub fn looks_normal(values: &[f64]) -> bool {
    let n = values.len();
    if n < 8 {
        return false;
    }
    let mean = values.iter().sum::<f64>() / n as f64;
    let mut m2 = 0.0;
    let mut m3 = 0.0;
    let mut m4 = 0.0;
    for v in values {
        let d = v - mean;
        let d2 = d * d;
        m2 += d2;
        m3 += d2 * d;
        m4 += d2 * d2;
    }
    let n = n as f64;
    m2 /= n;
    m3 /= n;
    m4 /= n;
    if m2 == 0.0 {
        return true;
    }
    let skew = m3 / m2.powf(1.5);
    let kurt = m4 / (m2 * m2);
    let jb = n / 6.0 * (skew * skew + (kurt - 3.0).powi(2) / 4.0);
    jb <= 5.99146454710798
}

/// Teste t de Welch (variancias diferentes). `statistic` e t com sinal de media(a)-media(b).
pub fn welch_t_test(a: &[f64], b: &[f64]) -> Option<TwoSampleTest> {
    if a.len() < 2 || b.len() < 2 {
        return None;
    }
    let (mean_a, std_a) = mean_stddev(a);
    let (mean_b, std_b) = mean_stddev(b);
    let var_a = std_a * std_a / a.len() as f64;
    let var_b = std_b * std_b / b.len() as f64;
    let se2 = var_a + var_b;
    if se2 == 0.0 {
        return Some(TwoSampleTest {
            name: "t",
            statistic: 0.0,
            significant_95: false,
        });
    }
    let t = (mean_a - mean_b) / se2.sqrt();
    let df = se2 * se2 / (var_a * var_a / (a.len() - 1) as f64 + var_b * var_b / (b.len() - 1) as f64);
    Some(TwoSampleTest {
        name: "t",
        statistic: t,
        significant_95: t.abs() > t_critical_interp(df),
    })
}

/// Mann-Whitney bicaudal, com correcao de empates e de continuidade.
/// `statistic` e z com sinal: positivo quando `a` tende a valores maiores que `b`.
pub fn mann_whitney(a: &[f64], b: &[f64]) -> Option<TwoSampleTest> {
    let n1 = a.len();
    let n2 = b.len();
    if n1 == 0 || n2 == 0 {
        return None;
    }
    let mut tagged: Vec<(f64, u8)> = a.iter().map(|&v| (v, 0u8)).chain(b.iter().map(|&v| (v, 1u8))).collect();
    tagged.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap_or(std::cmp::Ordering::Equal));

    let mut rank_sum_a = 0.0;
    let mut tie_correction = 0.0;
    let mut i = 0;
    while i < tagged.len() {
        let mut j = i + 1;
        while j < tagged.len() && tagged[j].0 == tagged[i].0 {
            j += 1;
        }
        let count = (j - i) as f64;
        let average_rank = (i + 1 + j) as f64 / 2.0;
        for item in &tagged[i..j] {
            if item.1 == 0 {
                rank_sum_a += average_rank;
            }
        }
        if count > 1.0 {
            tie_correction += count.powi(3) - count;
        }
        i = j;
    }

    let n1f = n1 as f64;
    let n2f = n2 as f64;
    let total = n1f + n2f;
    let u1 = rank_sum_a - n1f * (n1f + 1.0) / 2.0;
    let mu = n1f * n2f / 2.0;
    let sigma2 = if total > 1.0 {
        n1f * n2f / 12.0 * ((total + 1.0) - tie_correction / (total * (total - 1.0)))
    } else {
        0.0
    };
    let z = if sigma2 <= 0.0 {
        0.0
    } else {
        let diff = u1 - mu;
        let corrected = if diff > 0.0 {
            diff - 0.5
        } else if diff < 0.0 {
            diff + 0.5
        } else {
            0.0
        };
        corrected / sigma2.sqrt()
    };
    Some(TwoSampleTest {
        name: "mann-whitney",
        statistic: z,
        significant_95: z.abs() > 1.959963984540054,
    })
}

/// Welch se as duas amostras parecem normais; caso contrario Mann-Whitney (Secao 4.6).
pub fn compare_samples(a: &[f64], b: &[f64]) -> Option<TwoSampleTest> {
    if looks_normal(a) && looks_normal(b) {
        welch_t_test(a, b)
    } else {
        mann_whitney(a, b)
    }
}

fn mean_stddev(values: &[f64]) -> (f64, f64) {
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
    (mean, var.sqrt())
}

fn t_critical_interp(df: f64) -> f64 {
    if df <= 1.0 {
        return t_critical_95(1);
    }
    let lo = df.floor() as usize;
    let hi = df.ceil() as usize;
    if lo == hi {
        t_critical_95(lo)
    } else {
        let frac = df - lo as f64;
        t_critical_95(lo) * (1.0 - frac) + t_critical_95(hi) * frac
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

    #[test]
    fn welch_detecta_medias_separadas_e_poupa_iguais() {
        let a = [1.0, 2.0, 3.0, 2.0, 1.5, 2.5, 1.8, 2.2];
        let b = [1.1, 1.9, 2.4, 1.6, 2.1, 1.7, 2.3, 1.8];
        let same = welch_t_test(&a, &b).unwrap();
        assert_eq!(same.name, "t");
        assert!(!same.significant_95);

        let c = [10.0, 11.0, 12.0, 10.5, 11.5, 12.5, 10.2, 11.8];
        let diff = welch_t_test(&a, &c).unwrap();
        assert!(diff.significant_95);
        assert!(diff.statistic < 0.0);
    }

    #[test]
    fn mann_whitney_ordena_amostras_sem_sobreposicao() {
        let low = [1.0, 2.0, 3.0, 4.0];
        let high = [10.0, 11.0, 12.0, 13.0];
        let t = mann_whitney(&high, &low).unwrap();
        assert!(t.significant_95);
        assert!(t.statistic > 0.0);
        let tied = mann_whitney(&low, &low).unwrap();
        assert!(!tied.significant_95);
    }

    #[test]
    fn amostra_constante_nao_rejeita_normalidade() {
        assert!(looks_normal(&[2.0; 10]));
        assert!(!looks_normal(&[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 50.0]));
    }
}
