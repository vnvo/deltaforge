//! Runs created immediately after one another, or concurrently, never share
//! an id, a result directory or a row namespace, and a failed run's result
//! never lands in another run's directory. The server is unreachable, so
//! every run fails fast at its first stage (no docker needed).

use std::collections::BTreeSet;
use std::sync::Arc;

use fleet_harness::config::RunConfig;
use fleet_harness::run::{RunOptions, run_once};
use serde_json::Value;

fn config(out: &str, work: &str) -> RunConfig {
    let yaml = format!(
        r#"
class: exploratory
profile: cdc
output_dir: {out}
topology:
  servers: [{{name: c01, host: 127.0.0.1, port: 1}}]
  admin_user: root
  admin_password: x
  databases_per_server: 1
  tables_per_database: 1
deltaforge:
  api_url: http://127.0.0.1:1
  metrics_url: http://127.0.0.1:1/metrics
  process: {{kind: pid, pid: 1}}
  state_store: postgres
  cdc_user: df
  cdc_password: x
  kafka_brokers: 127.0.0.1:1
  key: {{mode: template, template: "${{after.id}}"}}
verifier:
  kafka_brokers: 127.0.0.1:1
  expected_key: none
  work_dir: {work}
traffic:
  changes_per_sec_avg: 1
  changes_per_sec_peak: 1
  peak_fraction: 0
  op_mix: {{insert: 1.0, update: 0.0, delete: 0.0}}
  row_bytes: 64
  rows_per_txn: 1
  zipf_exponent: 0
  active_set: {{pattern: fixed, fraction: 1.0}}
  writers_per_server: 1
measure:
  cdc_users: [df]
run:
  scenario: S1
  duration_secs: 60
  warmup_secs: 0
  sources: [1]
"#
    );
    serde_yaml::from_str(&yaml).expect("config")
}

fn results(out: &std::path::Path) -> Vec<Value> {
    std::fs::read_dir(out.join("exploratory"))
        .unwrap()
        .map(|e| {
            let p = e.unwrap().path().join("result.json");
            serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn back_to_back_and_concurrent_failed_runs_keep_their_own_evidence() {
    let out = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let cfg = Arc::new(config(
        out.path().to_str().unwrap(),
        work.path().to_str().unwrap(),
    ));
    let opts = RunOptions::default();
    // The same run (as two sweeps of one configuration would start it),
    // back to back, well within one second.
    for _ in 0..5 {
        run_once(cfg.clone(), 1, 1, &opts).await.unwrap_err();
    }
    // Concurrently.
    let concurrent: Vec<_> = (0..8)
        .map(|_| {
            let (cfg, opts) = (cfg.clone(), opts.clone());
            tokio::spawn(async move { run_once(cfg, 1, 1, &opts).await })
        })
        .collect();
    for h in concurrent {
        h.await.unwrap().unwrap_err();
    }

    let all = results(out.path());
    assert_eq!(all.len(), 13, "one result directory per run");
    let ids: BTreeSet<_> =
        all.iter().map(|r| r["run_id"].as_str().unwrap()).collect();
    let tags: BTreeSet<_> =
        all.iter().map(|r| r["run_tag"].as_u64().unwrap()).collect();
    assert_eq!(ids.len(), 13);
    assert_eq!(tags.len(), 13, "no two runs share a row namespace");
    for r in &all {
        assert_eq!(r["outcome"], "failed", "{r}");
        assert_eq!(r["stage"], "servers", "{r}");
    }
}
