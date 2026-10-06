//! A pipeline's metric registrations: its per-table label policy
//! (`deltaforge_core::table_metrics`), registered at spawn and forgotten,
//! with every per-table series it owns, on delete.

use std::sync::Arc;
use std::time::Duration;

use deltaforge_config::PipelineSpec;
use deltaforge_core::table_metrics::{self, PerTablePolicy, TableMetrics};

/// The per-table policy `spec` configures (`None` when disabled).
pub fn per_table_policy(spec: &PipelineSpec) -> Option<PerTablePolicy> {
    let cfg = &spec.spec.metrics.per_table;
    cfg.enabled.then(|| PerTablePolicy {
        max_tables: cfg.max_tables as usize,
        lag_idle: Duration::from_secs(u64::from(cfg.lag_idle_secs)),
    })
}

/// Register `spec`'s policy before its source, sinks and coordinator start.
pub fn register(spec: &PipelineSpec) -> Arc<TableMetrics> {
    table_metrics::register(&spec.metadata.name, per_table_policy(spec))
}

/// Forget a deleted pipeline's policy and per-table series.
pub fn forget(pipeline: &str) {
    table_metrics::unregister(pipeline);
    o11y::table_lag::global().remove_pipeline(pipeline);
}
