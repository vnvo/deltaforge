//! The process's memory limit, from which caches size their default bounds.

/// The process's cgroup memory limit (v2 `memory.max`, else v1
/// `memory.limit_in_bytes`), `None` when unlimited or unreadable.
pub fn memory_limit() -> Option<u64> {
    [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .find_map(|p| std::fs::read_to_string(p).ok())
    .and_then(|s| parse_limit(&s))
}

/// A cgroup limit file's value: `max` (v2) and v1's page-rounded
/// `i64::MAX` mean no limit.
fn parse_limit(text: &str) -> Option<u64> {
    let n: u64 = text.trim().parse().ok()?;
    (n < 1 << 60).then_some(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_limit_file_reads_as_its_bytes_and_no_limit_as_none() {
        assert_eq!(parse_limit("6442450944\n"), Some(6 << 30));
        assert_eq!(parse_limit("max\n"), None);
        assert_eq!(parse_limit("9223372036854771712\n"), None);
        assert_eq!(parse_limit(""), None);
    }
}
