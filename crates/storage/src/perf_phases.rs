//! Phase timings of the store, for measurement builds only (the
//! `perf-phases` feature; `docs/design/mysql-ddl-proof-transactions.md`,
//! section 6): see the sources crate's `perf_phases`. Without the feature a
//! [`Phase`] is empty and records nothing.

/// A phase being timed; recorded when dropped.
pub(crate) struct Phase {
    #[cfg(feature = "perf-phases")]
    name: &'static str,
    #[cfg(feature = "perf-phases")]
    started: std::time::Instant,
}

impl Phase {
    #[cfg_attr(not(feature = "perf-phases"), allow(unused_variables))]
    pub(crate) fn start(name: &'static str) -> Self {
        Self {
            #[cfg(feature = "perf-phases")]
            name,
            #[cfg(feature = "perf-phases")]
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for Phase {
    fn drop(&mut self) {
        #[cfg(feature = "perf-phases")]
        metrics::histogram!(
            "deltaforge_perf_phase_latency_seconds",
            "phase" => self.name
        )
        .record(self.started.elapsed().as_secs_f64());
    }
}
