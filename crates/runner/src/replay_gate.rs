//! Per-sink replay gate (Event Replay, Slice 3 checkpoint c).
//!
//! While a replay job holds a live sink paused, or a staged sink is being backfilled before
//! it joins the live set, that sink must be excluded from the coordinator's LIVE delivery
//! and from its commit-policy evaluation. The coordinator consults this shared gate on each
//! batch; the replay controller flips membership. Pause state is DERIVED from the active
//! job (the controller installs and removes it), never persisted per sink, so a crash
//! cannot strand a pause: on restart the startup barrier reinstalls the gate from the
//! durable job before the source starts.

use std::collections::HashSet;

use parking_lot::RwLock;

/// Shared set of sink ids currently excluded from live delivery for an active replay job.
#[derive(Debug, Default)]
pub struct ReplaySinkGate {
    excluded: RwLock<HashSet<String>>,
}

impl ReplaySinkGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `sink_id` is currently excluded from live delivery.
    pub fn is_excluded(&self, sink_id: &str) -> bool {
        self.excluded.read().contains(sink_id)
    }

    /// Whether any sink is excluded (a fast path so the coordinator can skip filtering when
    /// no replay job is active).
    pub fn any_excluded(&self) -> bool {
        !self.excluded.read().is_empty()
    }

    /// Add `sink_ids` to the excluded set (pause a live sink, or hold a staged sink out of
    /// the live set).
    pub fn exclude<I, S>(&self, sink_ids: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut g = self.excluded.write();
        for id in sink_ids {
            g.insert(id.into());
        }
    }

    /// Remove `sink_ids` from the excluded set (a sink rejoins the live set at handoff).
    pub fn include<I, S>(&self, sink_ids: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut g = self.excluded.write();
        for id in sink_ids {
            g.remove(id.as_ref());
        }
    }

    /// A snapshot of the excluded ids (for diagnostics/tests).
    pub fn excluded_ids(&self) -> Vec<String> {
        let mut v: Vec<String> = self.excluded.read().iter().cloned().collect();
        v.sort();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclude_include_roundtrip() {
        let gate = ReplaySinkGate::new();
        assert!(!gate.any_excluded());
        gate.exclude(["kafka".to_string(), "s3".to_string()]);
        assert!(gate.any_excluded());
        assert!(gate.is_excluded("kafka"));
        assert!(gate.is_excluded("s3"));
        assert!(!gate.is_excluded("redis"));
        assert_eq!(gate.excluded_ids(), vec!["kafka", "s3"]);

        gate.include(["kafka"]);
        assert!(!gate.is_excluded("kafka"));
        assert!(gate.is_excluded("s3"));
        gate.include(["s3"]);
        assert!(!gate.any_excluded());
    }
}
