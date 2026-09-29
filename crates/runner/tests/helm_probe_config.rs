//! Config regression: the Helm chart must keep `/health` as the liveness probe and
//! `/ready` as the readiness probe. These endpoints now both return 503 when a pipeline
//! has failed (liveness -> pod restart by Kubernetes; readiness -> removal from Service
//! endpoints), so a chart edit that repoints or drops either probe would silently break
//! failure surfacing. Hermetic: reads the chart file, no cluster.

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
