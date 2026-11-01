//! Config regression: the Helm chart must keep `/health` as the liveness probe and
//! `/ready` as the readiness probe. `/health` returns 200 while the process is alive;
//! `/ready` returns 503 while a pipeline is failed, blocked or starting (removal from
//! Service endpoints, never a restart), so a chart edit that repoints, drops or adds a
//! probe would silently break failure surfacing or restart a snapshotting pod.
//! Hermetic: reads the chart file, no cluster.

use std::path::PathBuf;

fn values_yaml() -> serde_yaml::Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/helm/deltaforge/values.yaml");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_yaml::from_str(&text).expect("values.yaml is valid YAML")
}

#[test]
fn helm_liveness_probe_targets_health() {
    let v = values_yaml();
    let path = v["probes"]["liveness"]["httpGet"]["path"].as_str();
    assert_eq!(
        path,
        Some("/health"),
        "liveness probe must GET /health (values.yaml probes.liveness.httpGet.path)"
    );
}

#[test]
fn helm_readiness_probe_targets_ready() {
    let v = values_yaml();
    let path = v["probes"]["readiness"]["httpGet"]["path"].as_str();
    assert_eq!(
        path,
        Some("/ready"),
        "readiness probe must GET /ready (values.yaml probes.readiness.httpGet.path)"
    );
}

/// Readiness may stay false for a whole initial snapshot (a starting
/// pipeline): no probe may turn that into a restart. Only liveness restarts a
/// pod, and it must not use `/ready`; a startup probe on `/ready` would kill
/// a pod that is snapshotting.
#[test]
fn helm_no_restarting_probe_targets_ready() {
    let v = values_yaml();
    for probe in ["liveness", "startup"] {
        assert_ne!(
            v["probes"][probe]["httpGet"]["path"].as_str(),
            Some("/ready"),
            "{probe} probe must not GET /ready"
        );
    }
}
