//! Process memory probes (Linux `/proc`); zero elsewhere.

use serde::Serialize;

/// Resident and peak-resident memory of this process, in bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Rss {
    pub rss_bytes: u64,
    pub peak_bytes: u64,
}

fn status_kb(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with(field)).and_then(|l| {
                l.split_whitespace().nth(1).and_then(|v| v.parse().ok())
            })
        })
        .unwrap_or(0)
}

pub fn now() -> Rss {
    Rss {
        rss_bytes: status_kb("VmRSS:") * 1024,
        peak_bytes: status_kb("VmHWM:") * 1024,
    }
}

/// Reset the peak (VmHWM) to the current RSS, so a following measurement
/// reports the peak of one phase only. Returns false if unsupported.
pub fn reset_peak() -> bool {
    std::fs::write("/proc/self/clear_refs", "5").is_ok()
}
