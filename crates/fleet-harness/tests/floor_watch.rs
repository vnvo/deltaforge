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
