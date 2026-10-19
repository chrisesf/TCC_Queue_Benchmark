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

/// CPU e memoria de um container Docker (o broker) durante a fase medida, lidos da API
/// do Docker em `/var/run/docker.sock`. A memoria e amostrada a cada `SAMPLE_INTERVAL`
/// e reportada como pico (uso menos page cache inativo, como no `docker stats`).
pub struct ContainerMonitor {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    sampler: std::thread::JoinHandle<u64>,
    container: String,
    first: docker_stats::Sample,
}

#[derive(Debug, Clone, Copy)]
pub struct ContainerUsage {
    pub cpu_secs: f64,
    /// Intervalo real entre a primeira e a ultima amostra.
    pub wall_secs: f64,
    pub peak_mem_kib: u64,
}

impl ContainerMonitor {
    const SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

    /// `None` se a API do Docker nao estiver acessivel.
    pub fn start(container: &str) -> Option<Self> {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let first = docker_stats::sample(container)?;
        let first_mem = first.mem_bytes;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let name = container.to_owned();
        let sampler = std::thread::spawn(move || {
            let mut peak = first_mem;
            while !stop_flag.load(Ordering::Acquire) {
                std::thread::sleep(Self::SAMPLE_INTERVAL);
                if let Some(s) = docker_stats::sample(&name) {
                    peak = peak.max(s.mem_bytes);
                }
            }
            peak
        });
        Some(Self {
            stop,
            sampler,
            container: container.to_owned(),
            first,
        })
    }

    pub fn finish(self) -> Option<ContainerUsage> {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        let last = docker_stats::sample(&self.container);
        let peak = self.sampler.join().ok()?;
        let last = last?;
        let first = &self.first;
        // Preferencia pelo relogio do daemon (CPU total do host / nucleos, como no
        // `docker stats`): nao inclui a latencia das proprias requisicoes.
        let wall_secs = match (first.system_ns, last.system_ns, last.online_cpus) {
            (Some(a), Some(b), Some(n)) if b > a && n > 0 => (b - a) as f64 / n as f64 * 1e-9,
            _ => last.at.duration_since(first.at).as_secs_f64(),
        };
        Some(ContainerUsage {
            cpu_secs: last.cpu_ns.saturating_sub(first.cpu_ns) as f64 * 1e-9,
            wall_secs,
            peak_mem_kib: peak.max(last.mem_bytes) / 1024,
        })
    }
}

#[cfg(all(unix, feature = "docker-stats"))]
mod docker_stats {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    use std::time::{Duration, Instant};

    pub struct Sample {
        pub cpu_ns: u64,
        pub mem_bytes: u64,
        /// CPU acumulada de todo o host (ns) e nucleos visiveis, segundo o daemon.
        pub system_ns: Option<u64>,
        pub online_cpus: Option<u64>,
        /// Ponto medio da requisicao, no relogio local.
        pub at: Instant,
    }

    pub fn sample(container: &str) -> Option<Sample> {
        let before = Instant::now();
        let mut stream = UnixStream::connect("/var/run/docker.sock").ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
        // HTTP/1.0: resposta sem chunked encoding, terminada pelo fechamento da conexao.
        write!(
            stream,
            "GET /containers/{container}/stats?stream=false&one-shot=true HTTP/1.0\r\nHost: docker\r\n\r\n"
        )
        .ok()?;
        let mut response = String::new();
        stream.read_to_string(&mut response).ok()?;
        let at = before + before.elapsed() / 2;
        let (head, body) = response.split_once("\r\n\r\n")?;
        if !head.starts_with("HTTP/1.0 200") && !head.starts_with("HTTP/1.1 200") {
            return None;
        }
        let v: serde_json::Value = serde_json::from_str(body).ok()?;
        let cpu_ns = v["cpu_stats"]["cpu_usage"]["total_usage"].as_u64()?;
        let mem = &v["memory_stats"];
        let usage = mem["usage"].as_u64()?;
        // cgroup v2 expoe `inactive_file`; v1, `total_inactive_file`.
        let inactive = mem["stats"]["inactive_file"]
            .as_u64()
            .or_else(|| mem["stats"]["total_inactive_file"].as_u64())
            .unwrap_or(0);
        Some(Sample {
            cpu_ns,
            mem_bytes: usage.saturating_sub(inactive),
            system_ns: v["cpu_stats"]["system_cpu_usage"].as_u64(),
            online_cpus: v["cpu_stats"]["online_cpus"].as_u64(),
            at,
        })
    }
}

#[cfg(not(all(unix, feature = "docker-stats")))]
#[allow(dead_code)]
mod docker_stats {
    pub struct Sample {
        pub cpu_ns: u64,
        pub mem_bytes: u64,
        pub system_ns: Option<u64>,
        pub online_cpus: Option<u64>,
        pub at: std::time::Instant,
    }

    pub fn sample(_container: &str) -> Option<Sample> {
        None
    }
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
