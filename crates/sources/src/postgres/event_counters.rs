//! Cached `deltaforge_source_events_total` handles for the PostgreSQL
//! stream, keyed by (table labels, op) so the hot path skips a
//! metrics-registry lookup per event.

use std::collections::HashMap;
use std::sync::Arc;

use deltaforge_core::table_metrics::{TableLabel, TableMetrics, with_table};
use metrics::{Counter, Label, counter};

/// The cache holds at most `ops` handles with per-table detail off and
/// `(max_tables + 1) * ops` with it on: its keys are label sets, which the
/// policy bounds, never table names.
pub(crate) struct EventCounters {
    pipeline: String,
    source: String,
    tables: Arc<TableMetrics>,
    cache: HashMap<(Option<TableLabel>, &'static str), Counter>,
}

impl EventCounters {
    pub(crate) fn new(
        pipeline: &str,
        source: &str,
        tables: Arc<TableMetrics>,
    ) -> Self {
        Self {
            pipeline: pipeline.to_string(),
            source: source.to_string(),
            tables,
            cache: HashMap::new(),
        }
    }

    /// Count one `op` event on `table`.
    pub(crate) fn increment(&mut self, table: &str, op: &'static str) {
        let key = (self.tables.label(table), op);
        let (pipeline, source) = (&self.pipeline, &self.source);
        self.cache
            .entry(key)
            .or_insert_with_key(|(table, op)| {
                let labels = with_table(
                    vec![
                        Label::new("pipeline", pipeline.clone()),
                        Label::new("source", source.clone()),
                        Label::new("op", *op),
                    ],
                    table.as_ref(),
                );
                counter!("deltaforge_source_events_total", labels)
            })
            .increment(1);
    }

    #[cfg(test)]
    pub(crate) fn cached(&self) -> usize {
        self.cache.len()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use deltaforge_core::table_metrics::PerTablePolicy;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    const OPS: [&str; 4] = ["c", "u", "d", "t"];

    fn churn(counters: &mut EventCounters, tables: usize) {
        for i in 0..tables {
            for op in OPS {
                counters.increment(&format!("public.t{i}"), op);
            }
        }
    }

    /// Every `deltaforge_source_events_total` series: its labels and value.
    fn series(rec: &DebuggingRecorder) -> Vec<(Vec<(String, String)>, u64)> {
        rec.snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(k, ..)| {
                k.key().name() == "deltaforge_source_events_total"
            })
            .map(|(k, _, _, v)| {
                let mut labels: Vec<_> = k
                    .key()
                    .labels()
                    .map(|l| (l.key().to_string(), l.value().to_string()))
                    .collect();
                labels.sort();
                let DebugValue::Counter(n) = v else {
                    panic!("a counter")
                };
                (labels, n)
            })
            .collect()
    }

    #[test]
    fn off_caches_one_handle_per_op_and_labels_no_table() {
        let rec = DebuggingRecorder::new();
        metrics::with_local_recorder(&rec, || {
            let tables = Arc::new(TableMetrics::disabled("p"));
            let mut counters = EventCounters::new("p", "s", tables);
            churn(&mut counters, 1_000);
            assert_eq!(counters.cached(), OPS.len());
        });
        let series = series(&rec);
        assert_eq!(series.len(), OPS.len());
        for (labels, n) in series {
            assert!(
                labels
                    .iter()
                    .all(|(k, _)| k != "table" && k != "table_scope")
            );
            assert_eq!(n, 1_000);
        }
    }

    #[test]
    fn on_caches_at_most_max_tables_plus_one_per_op() {
        let rec = DebuggingRecorder::new();
        metrics::with_local_recorder(&rec, || {
            let policy = PerTablePolicy {
                max_tables: 10,
                lag_idle: Duration::from_secs(300),
            };
            let tables = Arc::new(TableMetrics::new("p", Some(policy)));
            let mut counters = EventCounters::new("p", "s", tables);
            churn(&mut counters, 1_000);
            assert_eq!(counters.cached(), (10 + 1) * OPS.len());
        });
        let series = series(&rec);
        assert_eq!(series.len(), (10 + 1) * OPS.len());
        let overflow: u64 = series
            .iter()
            .filter(|(l, _)| {
                l.contains(&("table_scope".into(), "overflow".into()))
            })
            .map(|(_, n)| n)
            .sum();
        assert_eq!(overflow, 990 * OPS.len() as u64, "no event is lost");
    }
}
