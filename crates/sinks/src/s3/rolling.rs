//! File rolling thresholds.
//!
//! A writer is closed (rolled) when **any** of:
//! - `max_bytes` of accumulated payload (Parquet estimate / JSONL exact)
//! - `max_events` since open
//! - `max_age` since first event in the file (wall clock)
//! - `idle_age` since the last event in the file (low-volume partitions)
//!
//! Plus an implicit roll on partition-key change, which is handled by the
//! writer pool routing rather than this module.

use std::time::{Duration, Instant};

/// Rolling configuration. The defaults aim at ~256 MiB / 1M events / 5 min
/// per file, with a 10 min idle window for low-volume partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RollingConfig {
    pub max_bytes: u64,
    pub max_events: u64,
    pub max_age: Duration,
    pub idle_age: Duration,
}

impl Default for RollingConfig {
    fn default() -> Self {
        Self {
            max_bytes: 256 * 1024 * 1024,
            max_events: 1_000_000,
            max_age: Duration::from_secs(300),
            idle_age: Duration::from_secs(600),
        }
    }
}

/// Reason a writer was rolled. Used by metrics and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollReason {
    Bytes,
    Events,
    Age,
    Idle,
}

/// Compute whether a writer should roll given its current state.
pub fn should_roll(
    cfg: &RollingConfig,
    bytes: u64,
    events: u64,
    opened_at: Instant,
    last_event_at: Instant,
    now: Instant,
) -> Option<RollReason> {
    if events == 0 {
        return None;
    }
    if bytes >= cfg.max_bytes {
        return Some(RollReason::Bytes);
    }
    if events >= cfg.max_events {
        return Some(RollReason::Events);
    }
    if now.saturating_duration_since(opened_at) >= cfg.max_age {
        return Some(RollReason::Age);
    }
    if now.saturating_duration_since(last_event_at) >= cfg.idle_age {
        return Some(RollReason::Idle);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RollingConfig {
        RollingConfig {
            max_bytes: 1_000,
            max_events: 100,
            max_age: Duration::from_secs(60),
            idle_age: Duration::from_secs(30),
        }
    }

    #[test]
    fn does_not_roll_empty_writer() {
        let now = Instant::now();
        assert_eq!(
            should_roll(
                &cfg(),
                999,
                0,
                now,
                now,
                now + Duration::from_secs(120)
            ),
            None
        );
    }

    #[test]
    fn rolls_on_bytes() {
        let now = Instant::now();
        assert_eq!(
            should_roll(&cfg(), 1_000, 1, now, now, now),
            Some(RollReason::Bytes)
        );
        assert_eq!(
            should_roll(&cfg(), 9_999, 1, now, now, now),
            Some(RollReason::Bytes)
        );
    }

    #[test]
    fn rolls_on_event_count() {
        let now = Instant::now();
        assert_eq!(
            should_roll(&cfg(), 0, 100, now, now, now),
            Some(RollReason::Events)
        );
    }

    #[test]
    fn rolls_on_age() {
        let now = Instant::now();
        let later = now + Duration::from_secs(60);
        assert_eq!(
            should_roll(&cfg(), 0, 1, now, later, later),
            Some(RollReason::Age)
        );
    }

    #[test]
    fn rolls_on_idle() {
        let opened = Instant::now();
        let last = opened + Duration::from_secs(1);
        let now = last + Duration::from_secs(31);
        assert_eq!(
            should_roll(&cfg(), 0, 5, opened, last, now),
            Some(RollReason::Idle)
        );
    }

    #[test]
    fn idle_does_not_trigger_before_threshold() {
        let opened = Instant::now();
        let last = opened + Duration::from_secs(1);
        let now = last + Duration::from_secs(15);
        assert_eq!(should_roll(&cfg(), 0, 5, opened, last, now), None);
    }

    #[test]
    fn bytes_takes_precedence_over_age() {
        let now = Instant::now();
        let later = now + Duration::from_secs(120);
        // Both bytes (>=1000) and age (>=60s) trigger; bytes wins.
        assert_eq!(
            should_roll(&cfg(), 5_000, 50, now, later, later),
            Some(RollReason::Bytes)
        );
    }
}
