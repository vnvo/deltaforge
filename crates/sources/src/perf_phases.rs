//! Phase timings of the MySQL DDL path, for measurement builds only (the
//! `perf-phases` feature; `docs/design/mysql-ddl-proof-transactions.md`,
//! section 6). A [`Phase`] records its elapsed time when dropped, into the
//! histogram `deltaforge_perf_phase_latency_seconds{phase}`: a distribution
//! per phase over a run, not a trace of any one DDL. Without the feature it
//! is empty and records nothing.

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

// ---------------------------------------------------------------------------
// Selection-cache instrumentation (measurement builds only, the
// `perf-phases` feature; `docs/design/selection-cache-diagnosis.md`). Without
// the feature every function here is empty and the cache's bounds come from
// the code alone. Unlabelled by pipeline or source: a measurement runner runs
// one source.
// ---------------------------------------------------------------------------

/// `DELTAFORGE_PERF_SELECTION_CACHE_BYTES`: the selection cache's byte
/// bound for a measurement build.
#[cfg_attr(not(feature = "perf-phases"), allow(dead_code))]
pub(crate) const ENV_CACHE_BYTES: &str =
    "DELTAFORGE_PERF_SELECTION_CACHE_BYTES";

/// The selection cache's byte bound a measurement build was given in its
/// environment (`var` reads a variable), or `None`. Always `None` without
/// the feature: a default build ignores the variable.
#[cfg_attr(not(feature = "perf-phases"), allow(unused_variables))]
pub(crate) fn cache_bytes_override(
    var: impl Fn(&str) -> Option<String>,
) -> Option<u64> {
    #[cfg(feature = "perf-phases")]
    {
        let v = var(ENV_CACHE_BYTES)?;
        match v.trim().parse::<u64>() {
            Ok(n) if n > 0 => Some(n),
            _ => {
                tracing::error!(name = ENV_CACHE_BYTES, value = %v, "ignored: not a positive number");
                None
            }
        }
    }
    #[cfg(not(feature = "perf-phases"))]
    None
}

/// Publish the bound a cache was built with:
/// `deltaforge_perf_selection_cache_limit{bound="bytes"}`. A measurement
/// harness checks it before it measures.
#[cfg_attr(not(feature = "perf-phases"), allow(unused_variables))]
pub(crate) fn cache_limit(bytes: u64) {
    #[cfg(feature = "perf-phases")]
    metrics::gauge!("deltaforge_perf_selection_cache_limit", "bound" => "bytes")
        .set(bytes as f64);
}

/// One lookup of a cache entry: `entry` is `timeline` (a table's validated
/// history), `selection` (the schema selected for a row position and
/// TableMap) or `version` (a registered version):
/// `deltaforge_perf_selection_lookups_total{entry, result="hit"|"miss"}`.
#[cfg_attr(not(feature = "perf-phases"), allow(unused_variables))]
pub(crate) fn cache_lookup(entry: &'static str, hit: bool) {
    #[cfg(feature = "perf-phases")]
    metrics::counter!(
        "deltaforge_perf_selection_lookups_total",
        "entry" => entry,
        "result" => if hit { "hit" } else { "miss" }
    )
    .increment(1);
}

/// One entry evicted to make room:
/// `deltaforge_perf_selection_evictions_total{entry}`.
#[cfg_attr(not(feature = "perf-phases"), allow(unused_variables))]
pub(crate) fn cache_eviction(entry: &'static str) {
    #[cfg(feature = "perf-phases")]
    metrics::counter!("deltaforge_perf_selection_evictions_total", "entry" => entry)
        .increment(1);
}

/// One state-store read a miss caused: `read` is `history` (timelines a row
/// needed), `history_prefetch` (timelines an activation window read ahead),
/// `versions` (versions read ahead with them) or `version` (one version a
/// selection needed). Counts the reads (one per load, which may issue
/// several statements), the tables the store was asked for (a load that
/// stops once nothing more fits leaves the rest unread and uncounted) and
/// the stored bytes of the timelines read:
/// `deltaforge_perf_selection_store_reads_total{read}`,
/// `deltaforge_perf_selection_store_tables_total{read}`,
/// `deltaforge_perf_selection_store_bytes_total{read}`.
#[cfg_attr(not(feature = "perf-phases"), allow(unused_variables))]
pub(crate) fn store_read(read: &'static str, tables: u64, bytes: u64) {
    #[cfg(feature = "perf-phases")]
    {
        metrics::counter!("deltaforge_perf_selection_store_reads_total", "read" => read)
            .increment(1);
        metrics::counter!("deltaforge_perf_selection_store_tables_total", "read" => read)
            .increment(tables);
        metrics::counter!("deltaforge_perf_selection_store_bytes_total", "read" => read)
            .increment(bytes);
    }
}
