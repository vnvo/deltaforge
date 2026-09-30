//! Process-level checks of the store gate through the real `deltaforge` binary
//! on a SQLite store: the server refuses to start while the gate is held,
//! holds it while running, releases it on a clean (SIGTERM) shutdown and keeps
//! it after a crash (SIGKILL) until an explicit break; the schema migration
//! apply is gated the same way and a dry run takes no gate.
#![cfg(unix)]

use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use storage::ArcStorageBackend;
use storage::adapters::store_gate::{self, GateRole, GateState};

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_runner"))
}

fn run(db: &Path, args: &[&str]) -> Output {
    bin()
        .args(args)
        .args(["--storage-backend", "sqlite", "--storage-path"])
        .arg(db)
        .output()
        .expect("run deltaforge")
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn backend(db: &Path) -> ArcStorageBackend {
    storage::SqliteStorageBackend::open(db).unwrap()
}

fn start_server(db: &Path) -> Child {
    bin()
        .args([
            "--api-addr",
            "127.0.0.1:0",
            "--metrics-addr",
            "127.0.0.1:0",
            "--storage-backend",
            "sqlite",
            "--storage-path",
        ])
        .arg(db)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server")
}

async fn wait_for_server_gate(b: &ArcStorageBackend, pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let GateState::Held(h) = store_gate::status(b).await.unwrap()
            && h.role == GateRole::Server
            && h.pid == pid
        {
            return;
        }
        assert!(Instant::now() < deadline, "server never acquired the gate");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn wait_exit(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "server did not exit");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn signal(child: &Child, sig: &str) {
    let ok = Command::new("kill")
        .args([sig, &child.id().to_string()])
        .status()
        .unwrap()
        .success();
    assert!(ok, "kill {sig}");
}

#[tokio::test]
async fn server_holds_the_gate_and_releases_it_only_on_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("df.db");
    let b = backend(&db);

    // Clean shutdown releases.
    let mut server = start_server(&db);
    wait_for_server_gate(&b, server.id()).await;
    let apply_while_running = run(
        &db,
        &[
            "schema-migrate",
            "--mapping",
            "/nonexistent",
            "--apply",
            "--expect-proof",
            "x",
        ],
    );
    assert!(!apply_while_running.status.success());
    signal(&server, "-TERM");
    assert!(wait_exit(&mut server).success(), "clean shutdown exits 0");
    assert_eq!(store_gate::status(&b).await.unwrap(), GateState::Unlocked);

    // A crash keeps the gate held.
    let mut server = start_server(&db);
    wait_for_server_gate(&b, server.id()).await;
    signal(&server, "-KILL");
    wait_exit(&mut server);
    let GateState::Held(holder) = store_gate::status(&b).await.unwrap() else {
        panic!("a crashed server must leave the gate held");
    };

    // A new server refuses to start, naming the holder and the break command.
    let refused = start_server(&db).wait_with_output().unwrap();
    assert!(!refused.status.success());
    let msg = text(&refused);
    assert!(msg.contains(&holder.owner_id), "{msg}");
    assert!(msg.contains("store-gate break --owner"), "{msg}");

    // Status reports it; a wrong owner does not break it; the right one does.
    let status = run(&db, &["store-gate", "status", "--json"]);
    assert!(status.status.success());
    let v: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(v["state"], "held");
    assert_eq!(v["holder"]["owner_id"], holder.owner_id.as_str());
    assert_eq!(v["holder"]["role"], "server");
    assert!(
        !run(&db, &["store-gate", "break", "--owner", "someone-else"])
            .status
            .success()
    );
    assert!(
        run(&db, &["store-gate", "break", "--owner", &holder.owner_id])
            .status
            .success()
    );
    assert_eq!(store_gate::status(&b).await.unwrap(), GateState::Unlocked);

    // And the server starts again.
    let mut server = start_server(&db);
    wait_for_server_gate(&b, server.id()).await;
    signal(&server, "-TERM");
    assert!(wait_exit(&mut server).success());
}

#[tokio::test]
async fn migration_apply_is_gated_and_dry_run_takes_no_gate() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("df.db");
    let mapping = dir.path().join("mapping.yaml");
    std::fs::write(
        &mapping,
        "mappings:\n  - tenant: t\n    source_id: s\n    lineage_hash: \
         deadbeef\n    tables:\n      - {db: app, table: users}\n",
    )
    .unwrap();
    let mapping = mapping.to_str().unwrap();
    let b = backend(&db);

    // Dry run with the server's gate held: allowed, read-only, prints a proof.
    let held = store_gate::acquire(&b, GateRole::Server).await.unwrap();
    let dry = run(&db, &["schema-migrate", "--mapping", mapping, "--json"]);
    assert!(dry.status.success(), "{}", text(&dry));
    let plan: serde_json::Value = serde_json::from_slice(&dry.stdout).unwrap();
    let proof = plan["proof"].as_str().unwrap().to_string();

    // Apply is refused while the server holds the gate.
    let apply_args = [
        "schema-migrate",
        "--mapping",
        mapping,
        "--apply",
        "--expect-proof",
        &proof,
    ];
    let refused = run(&db, &apply_args);
    assert!(!refused.status.success());
    assert!(text(&refused).contains(&held.holder().owner_id));
    held.release().await.unwrap();

    // Without the server it runs (nothing to migrate: the lineage is not
    // established) and releases the gate afterwards.
    let applied = run(&db, &apply_args);
    assert!(applied.status.success(), "{}", text(&applied));
    assert_eq!(store_gate::status(&b).await.unwrap(), GateState::Unlocked);

    // A proof mismatch fails and still releases the gate.
    let mismatch = run(
        &db,
        &[
            "schema-migrate",
            "--mapping",
            mapping,
            "--apply",
            "--expect-proof",
            "0000",
        ],
    );
    assert!(!mismatch.status.success());
    assert_eq!(store_gate::status(&b).await.unwrap(), GateState::Unlocked);

    // --apply without a proof is a usage error.
    assert!(
        !run(&db, &["schema-migrate", "--mapping", mapping, "--apply"])
            .status
            .success()
    );
}

#[test]
fn wildcard_mappings_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("df.db");
    let mapping = dir.path().join("mapping.yaml");
    std::fs::write(
        &mapping,
        "mappings:\n  - tenant: t\n    source_id: s\n    lineage_hash: \
         deadbeef\n    tables:\n      - {db: app, table: '*'}\n",
    )
    .unwrap();
    let out = run(
        &db,
        &["schema-migrate", "--mapping", mapping.to_str().unwrap()],
    );
    assert!(!out.status.success());
    assert!(text(&out).contains("wildcard"), "{}", text(&out));
}
