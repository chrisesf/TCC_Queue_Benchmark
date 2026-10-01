//! CPU e memoria do processo (Linux). Em outros sistemas as leituras retornam zero.

#[derive(Debug, Clone, Copy, Default)]
pub struct ResourceSnapshot {
    /// Tempo de CPU (usuario + sistema) de todas as threads, inclusive as ja encerradas.
    pub cpu_secs: f64,
    /// RSS atual (`VmRSS`).
    pub rss_kib: u64,
    /// Pico de RSS (`VmHWM`) desde o inicio do processo ou do ultimo `reset_peak_rss`.
    pub peak_rss_kib: u64,
}

impl ResourceSnapshot {
    pub fn capture() -> Self {
        let (rss_kib, peak_rss_kib) = read_rss_kib();
        Self {
            cpu_secs: read_cpu_secs(),
            rss_kib,
            peak_rss_kib,
        }
    }

    pub fn delta_since(&self, earlier: &Self) -> Self {
        Self {
            cpu_secs: (self.cpu_secs - earlier.cpu_secs).max(0.0),
            rss_kib: self.rss_kib,
            peak_rss_kib: self.peak_rss_kib,
        }
    }
}

/// Devolve ao SO a memoria livre retida pelo alocador (glibc). Sem isso, o heap liberado
/// por uma repeticao continua residente e a seguinte comeca com a base inflada e sem
/// crescimento aparente de RSS.
///
/// Fixar `M_MMAP_THRESHOLD` desliga o ajuste dinamico do glibc, que apos liberar um bloco
/// grande passa a servir blocos desse tamanho a partir de arenas retidas. Com 128 KiB,
/// vetores de metricas e buffers das filas vao para `mmap` e voltam ao SO no `free`,
/// enquanto payloads (ate 64 KiB) continuam no caminho normal.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn release_free_memory() {
    extern "C" {
        fn malloc_trim(pad: usize) -> i32;
        fn mallopt(param: i32, value: i32) -> i32;
    }
    const M_MMAP_THRESHOLD: i32 = -3;
    static FIXED_THRESHOLD: std::sync::Once = std::sync::Once::new();
    FIXED_THRESHOLD.call_once(|| unsafe {
        mallopt(M_MMAP_THRESHOLD, 128 * 1024);
    });
    unsafe { malloc_trim(0) };
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn release_free_memory() {}

/// Zera o pico de RSS (`VmHWM`) para o valor atual, escrevendo 5 em
/// `/proc/self/clear_refs` (Linux >= 4.0). Retorna `false` se nao suportado.
pub fn reset_peak_rss() -> bool {
    std::fs::write("/proc/self/clear_refs", "5").is_ok()
}

#[cfg(target_os = "linux")]
fn read_cpu_secs() -> f64 {
    #[repr(C)]
    struct Timespec {
        tv_sec: i64,
        tv_nsec: i64,
    }
    extern "C" {
        fn clock_gettime(clock_id: i32, tp: *mut Timespec) -> i32;
    }
    const CLOCK_PROCESS_CPUTIME_ID: i32 = 2;

    let mut ts = Timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &mut ts) } != 0 {
        return 0.0;
    }
    ts.tv_sec as f64 + ts.tv_nsec as f64 * 1e-9
}

#[cfg(not(target_os = "linux"))]
fn read_cpu_secs() -> f64 {
    0.0
}

fn read_rss_kib() -> (u64, u64) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return (0, 0);
    };
    let field = |line: &str, prefix: &str| -> Option<u64> {
        line.strip_prefix(prefix)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    };
    let mut rss = 0;
    let mut hwm = 0;
    for line in status.lines() {
        if let Some(v) = field(line, "VmRSS:") {
            rss = v;
        } else if let Some(v) = field(line, "VmHWM:") {
            hwm = v;
        }
    }
    (rss, hwm)
}
