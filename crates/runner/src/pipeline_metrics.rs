//! A pipeline's metric registrations: its per-table label policy
//! (`deltaforge_core::table_metrics`), registered at spawn and forgotten,
//! with every per-table series it owns, on delete.

use std::time::Duration;

use deltaforge_config::PipelineSpec;
use deltaforge_core::table_metrics::{self, PerTablePolicy};

/// The per-table policy `spec` configures (`None` when disabled).
pub fn per_table_policy(spec: &PipelineSpec) -> Option<PerTablePolicy> {
    let cfg = &spec.spec.metrics.per_table;
    cfg.enabled.then(|| PerTablePolicy {
        max_tables: cfg.max_tables as usize,
        lag_idle: Duration::from_secs(u64::from(cfg.lag_idle_secs)),
    })
}

/// Whether `spec`'s per-table settings can apply to its pipeline name now
/// (checked before a create or patch stops or starts anything).
pub fn check(spec: &PipelineSpec) -> Result<(), String> {
    table_metrics::check(&spec.metadata.name, per_table_policy(spec))
}

/// Register `spec`'s policy before its source, sinks and coordinator start;
/// the caller rolls the registration back if the start fails.
pub fn register(
    spec: &PipelineSpec,
) -> anyhow::Result<table_metrics::Registration> {
    table_metrics::register(&spec.metadata.name, per_table_policy(spec))
        .map_err(anyhow::Error::msg)
}

/// A pipeline was deleted: forget its policy (kept, disabled, while its
/// table series exist) and remove its per-table lag series.
pub fn forget(pipeline: &str) {
    table_metrics::unregister(pipeline);
    o11y::table_lag::global().remove_pipeline(pipeline);
}

/// Test hook: an action run inside PATCH between its per-table check and
/// the stop of the running pipeline (keyed by pipeline name).
#[cfg(test)]
type PatchHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
fn patch_hooks()
-> &'static std::sync::Mutex<std::collections::HashMap<String, PatchHook>> {
    static HOOKS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, PatchHook>>,
    > = std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}

#[cfg(test)]
pub(crate) fn set_patch_hook(pipeline: &str, hook: PatchHook) {
    patch_hooks()
        .lock()
        .unwrap()
        .insert(pipeline.to_string(), hook);
}

/// Run `pipeline`'s pending hook; `false` when none was pending.
#[cfg(test)]
pub(crate) fn run_patch_hook(pipeline: &str) -> bool {
    let hook = patch_hooks().lock().unwrap().remove(pipeline);
    hook.map(|hook| hook()).is_some()
}
