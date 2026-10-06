//! Legacy (pre-queue) snapshot progress: whether an interrupted earlier
//! snapshot is ambiguous for a `durable_v2` sink (some tables done, some
//! pending), checked over the tables the configured patterns expand to.

/// Whether a table-level source checkpoint is an interrupted legacy snapshot
/// (some tables done, some pending, not finished) - the ambiguous case durable
/// startup must reject. Names-only, so a source can check it without resolving
/// cursor kinds. See [`LegacyProgressScan`] to check
/// discovered tables page by page.
pub fn is_ambiguous_legacy_progress(
    all_tables: &[String],
    done_tables: &[String],
    finished: bool,
) -> bool {
    let done: std::collections::HashSet<&str> =
        done_tables.iter().map(String::as_str).collect();
    let mut scan = LegacyProgressScan::default();
    for t in all_tables {
        scan.observe(done.contains(t.as_str()));
    }
    scan.ambiguous(finished)
}

/// [`is_ambiguous_legacy_progress`] over tables seen one at a time (the
/// expanded tables of a paged discovery): whether any is done and any is
/// pending.
#[derive(Debug, Default, Clone, Copy)]
pub struct LegacyProgressScan {
    any_done: bool,
    any_pending: bool,
}

impl LegacyProgressScan {
    pub fn observe(&mut self, done: bool) {
        if done {
            self.any_done = true;
        } else {
            self.any_pending = true;
        }
    }

    /// Both seen: the answer can no longer change.
    pub fn settled(&self) -> bool {
        self.any_done && self.any_pending
    }

    pub fn ambiguous(&self, finished: bool) -> bool {
        !finished && self.settled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_ambiguous_legacy_progress_only_for_interrupted() {
        let all = vec!["a".to_string(), "b".to_string()];
        // Interrupted: one done, one pending, not finished.
        assert!(is_ambiguous_legacy_progress(
            &all,
            &["a".to_string()],
            false
        ));
        // Finished: never ambiguous.
        assert!(!is_ambiguous_legacy_progress(
            &all,
            &["a".to_string()],
            true
        ));
        // Clean start: nothing done.
        assert!(!is_ambiguous_legacy_progress(&all, &[], false));
        // All done but not marked finished: not ambiguous (no pending).
        assert!(!is_ambiguous_legacy_progress(
            &all,
            &["a".to_string(), "b".to_string()],
            false
        ));
    }
}
