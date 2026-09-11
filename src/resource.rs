#[derive(Debug, Clone, Copy, Default)]
pub struct ResourceSnapshot {
    pub cpu_secs: f64,
    pub peak_rss_kib: u64,
}

impl ResourceSnapshot {
    pub fn capture() -> Self {
        Self {
            cpu_secs: read_cpu_secs(),
            peak_rss_kib: read_peak_rss_kib(),
        }
    }

    pub fn delta_since(&self, earlier: &Self) -> Self {
        Self {
            cpu_secs: (self.cpu_secs - earlier.cpu_secs).max(0.0),
            peak_rss_kib: self.peak_rss_kib,
        }
    }
}

fn read_cpu_secs() -> f64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0.0;
    };
    let Some(idx) = stat.rfind(')') else {
        return 0.0;
    };
    let fields: Vec<&str> = stat[idx + 1..].split_whitespace().collect();
    let utime: f64 = fields.get(11).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let stime: f64 = fields.get(12).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let hz = 100.0;
    (utime + stime) / hz
}

fn read_peak_rss_kib() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }
    0
}
