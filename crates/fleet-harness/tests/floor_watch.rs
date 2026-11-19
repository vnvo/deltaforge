//! `qualification/floor-watch.sh`: a qualification invocation stopped at its
//! deadline or at the disk floor is stopped gracefully first (SIGINT), and a
//! command that ignores it is killed once the grace period has passed: a
//! hung command never holds the sequence without bound.

use std::process::{Command, Output};
use std::time::{Duration, Instant};

/// Run `python3 -c script` under floor-watch with `env`; returns the output
/// and how long floor-watch took.
fn watch(env: &[(&str, &str)], script: &str) -> (Output, Duration) {
    let path =
        concat!(env!("CARGO_MANIFEST_DIR"), "/qualification/floor-watch.sh");
    let started = Instant::now();
    let out = Command::new("bash")
        .arg(path)
        .args(["--", "python3", "-c", script])
        .envs(env.iter().copied())
        .env("POLL_SECS", "1")
        .output()
        .expect("run floor-watch");
    (out, started.elapsed())
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

/// Exits 7 on SIGINT (overriding the ignore a background job inherits).
const GRACEFUL: &str = "import signal, sys, time\n\
signal.signal(signal.SIGINT, lambda *a: sys.exit(7))\n\
time.sleep(120)";

/// Ignores SIGINT, as a hung harness effectively does.
const HUNG: &str = "import signal, time\n\
signal.signal(signal.SIGINT, signal.SIG_IGN)\n\
time.sleep(120)";

#[test]
fn the_deadline_stops_a_command_gracefully() {
    let (out, took) = watch(
        &[
            ("START_GIB", "0"),
            ("FLOOR_GIB", "0"),
            ("DEADLINE_SECS", "2"),
            ("GRACE_SECS", "30"),
        ],
        GRACEFUL,
    );
    let log = stdout(&out);
    assert_eq!(out.status.code(), Some(5), "{log}");
    assert!(log.contains("DEADLINE"), "{log}");
    assert!(!log.contains("SIGKILL"), "it exited on SIGINT: {log}");
    assert!(took < Duration::from_secs(20), "{took:?}: {log}");
}

#[test]
fn a_command_that_ignores_the_stop_is_killed_after_the_grace() {
    let (out, took) = watch(
        &[
            ("START_GIB", "0"),
            ("FLOOR_GIB", "0"),
            ("DEADLINE_SECS", "2"),
            ("GRACE_SECS", "2"),
        ],
        HUNG,
    );
    let log = stdout(&out);
    assert_eq!(out.status.code(), Some(5), "{log}");
    assert!(log.contains("SIGKILL"), "{log}");
    assert!(took < Duration::from_secs(20), "{took:?}: {log}");
}

#[test]
fn the_floor_stop_is_bounded_too() {
    let (out, took) = watch(
        &[
            ("START_GIB", "0"),
            ("FLOOR_GIB", "1000000000"),
            ("GRACE_SECS", "2"),
        ],
        HUNG,
    );
    let log = stdout(&out);
    assert_eq!(out.status.code(), Some(4), "{log}");
    assert!(log.contains("FLOOR") && log.contains("SIGKILL"), "{log}");
    assert!(took < Duration::from_secs(20), "{took:?}: {log}");
}

#[test]
fn without_a_stop_the_command_status_is_returned() {
    let (out, _) = watch(
        &[("START_GIB", "0"), ("FLOOR_GIB", "0")],
        "import sys; sys.exit(9)",
    );
    assert_eq!(out.status.code(), Some(9), "{}", stdout(&out));
}

/// The fixture builder stops on the floor watch's SIGINT, although it runs as
/// a background job that inherits SIGINT ignored: a fixture stopped at the
/// floor must not keep writing until the SIGKILL. The MySQL server here
/// accepts the connection and never answers, so the build stays in flight.
#[test]
fn a_fixture_build_stops_on_the_stop_signal() {
    let server = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = server.local_addr().expect("addr").port();
    let _held = std::thread::spawn(move || {
        let held: Vec<_> = server.incoming().take(64).collect();
        std::thread::sleep(Duration::from_secs(120));
        drop(held);
    });
    let dir = tempfile::tempdir().expect("tempdir");
    let config = dir.path().join("fixture.yaml");
    std::fs::write(
        &config,
        format!(
            r#"
class: exploratory
profile: cdc
output_dir: {out}
topology:
  servers: [{{name: c01, host: 127.0.0.1, port: {port}}}]
  admin_user: root
  admin_password: rootpw
  databases_per_server: 3
  tables_per_database: 4
  seed: 3
deltaforge:
  api_url: http://127.0.0.1:1
  metrics_url: http://127.0.0.1:1/metrics
  process: {{kind: pid, pid: 1}}
  state_store: sqlite
  cdc_user: df
  cdc_password: dfpw
  kafka_brokers: 127.0.0.1:1
  key: {{mode: template, template: "${{after.id}}"}}
verifier:
  kafka_brokers: 127.0.0.1:1
  expected_key: none
  work_dir: {out}
traffic:
  changes_per_sec_avg: 40
  changes_per_sec_peak: 40
  peak_fraction: 0
  op_mix: {{insert: 0.6, update: 0.3, delete: 0.1}}
  row_bytes: 64
  rows_per_txn: 1
  zipf_exponent: 0
  active_set: {{pattern: fixed, fraction: 1.0}}
  writers_per_server: 1
ddl:
  migration_statements: 0
  migration_pace: 1
  migration_rollout: {{kind: sequential}}
measure:
  interval_secs: 1
  state_store_file: {out}/state.db
  cdc_users: [df]
run:
  scenario: F0
  duration_secs: 10
  sources: [1]
"#,
            out = dir.path().display()
        ),
    )
    .expect("write config");
    let path =
        concat!(env!("CARGO_MANIFEST_DIR"), "/qualification/floor-watch.sh");
    let started = Instant::now();
    let out = Command::new("bash")
        .arg(path)
        .args(["--", env!("CARGO_BIN_EXE_fleet-harness"), "fixture"])
        .arg(&config)
        .envs([
            ("START_GIB", "0"),
            ("FLOOR_GIB", "0"),
            ("DEADLINE_SECS", "3"),
            ("GRACE_SECS", "30"),
            ("POLL_SECS", "1"),
        ])
        .output()
        .expect("run floor-watch");
    let took = started.elapsed();
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.status.code(), Some(5), "{log}");
    assert!(!log.contains("SIGKILL"), "it stopped on SIGINT: {log}");
    assert!(took < Duration::from_secs(20), "{took:?}: {log}");
}
