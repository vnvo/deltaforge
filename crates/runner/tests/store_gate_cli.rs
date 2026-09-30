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
    start_server_with(db, &[])
}

fn start_server_with(db: &Path, env: &[(&str, &str)]) -> Child {
    bin()
        .envs(env.iter().copied())
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

/// Wait until the server prints the startup-hold marker for `point`.
fn wait_for_hold(
    server: &mut Child,
    point: &str,
) -> std::thread::JoinHandle<()> {
    use std::io::{BufRead, BufReader};
    let stderr = server.stderr.take().expect("stderr piped");
    let (tx, rx) = std::sync::mpsc::channel();
    let marker = format!("holding at {point}");
    let drain = std::thread::spawn(move || {
        let mut tx = Some(tx);
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if line.contains(&marker)
                && let Some(tx) = tx.take()
            {
                let _ = tx.send(());
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(60))
        .expect("server never reached the startup hold point");
    drain
}

/// A signal that arrives during startup must end in a clean exit with the
/// gate released - both before the gate is acquired and while it is held
/// (manager built, API not yet serving). Startup is held at each point until
/// the signal arrives, so the timing is deterministic.
#[tokio::test]
async fn a_signal_during_startup_releases_the_gate() {
    for point in ["before-gate", "after-gate"] {
        for sig in ["-TERM", "-INT"] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("df.db");
            let b = backend(&db);
            let mut server = start_server_with(
                &db,
                &[("DELTAFORGE_TEST_HOLD_STARTUP", point)],
            );
            let drain = wait_for_hold(&mut server, point);
            if point == "after-gate" {
                wait_for_server_gate(&b, server.id()).await;
            }
            signal(&server, sig);
            let status = wait_exit(&mut server);
            let _ = drain.join();
            assert!(status.success(), "{sig} at {point}: {status:?}");
            assert_eq!(
                store_gate::status(&b).await.unwrap(),
                GateState::Unlocked,
                "{sig} at {point} left the gate held"
            );
        }
    }
}

/// SIGTERM at many points from process start through startup: whatever the
/// timing, the gate is never left held (a signal before the handlers exist
/// kills the process before it can acquire the gate; any later one is
/// handled), and a process that did acquire it exits cleanly.
#[tokio::test]
async fn sigterm_at_any_point_of_startup_never_leaves_the_gate_held() {
    for delay_ms in [0u64, 1, 2, 5, 10, 20, 35, 50, 75, 100, 150, 250, 400] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("df.db");
        let mut server = start_server(&db);
        std::thread::sleep(Duration::from_millis(delay_ms));
        signal(&server, "-TERM");
        let status = wait_exit(&mut server);
        let b = backend(&db);
        assert_eq!(
            store_gate::status(&b).await.unwrap(),
            GateState::Unlocked,
            "SIGTERM after {delay_ms} ms left the gate held ({status:?})"
        );
    }
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

    // A proof mismatch on a fresh run happens before any write, so it is the
    // one failure that releases the gate.
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

/// Two legacy versions of `acme/public/orders` and the source's verified
/// lineage on a SQLite store; returns the lineage hash.
async fn seed_migration(db: &Path) -> String {
    let b = backend(db);
    for (i, pk) in [(1, "[]"), (2, "[\"id\"]")] {
        let entry = serde_json::json!({
            "hash": format!("orders-h{i}"),
            "schema_json": serde_json::json!({"oid": 10, "primary_key": pk}),
            "registered_at": "2026-01-01T00:00:00Z",
            "checkpoint": null,
        });
        b.log_append(
            "schemas",
            "acme/public/orders",
            &serde_json::to_vec(&entry).unwrap(),
        )
        .await
        .unwrap();
    }
    storage::adapters::source_lineage::establish(
        &b,
        "acme",
        "orders-pg",
        storage::adapters::LineageDescriptor::postgres(7, 8).unwrap(),
    )
    .await
    .unwrap()
    .record
    .current
    .lineage_hash
}

async fn adopted_versions(db: &Path, lh: &str) -> usize {
    let r = storage::DurableSchemaRegistry::new(backend(db))
        .await
        .unwrap();
    let key = storage::adapters::SchemaKey::new(
        "acme",
        "orders-pg",
        lh,
        "public",
        "orders",
    );
    r.history_page(&key, None, 10).await.unwrap().versions.len()
}

/// A migration that fails after adopting one of two versions keeps the gate:
/// no server can start and the operator break is refused, so the partial
/// history is never read. Only a resume of the same proof (taking the gate
/// over without unlocking it) finishes the work and releases the gate.
#[tokio::test]
async fn a_failed_apply_keeps_the_gate_until_the_same_migration_is_resumed() {
    use storage::adapters::test_util::FaultBackend;

    for budget in 1..64u64 {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("df.db");
        let lh = seed_migration(&db).await;
        let mapping = dir.path().join("mapping.yaml");
        std::fs::write(
            &mapping,
            format!(
                "mappings:\n  - tenant: acme\n    source_id: orders-pg\n    \
                 lineage_hash: {lh}\n    tables:\n      - {{db: public, table: orders}}\n"
            ),
        )
        .unwrap();
        let mapping = mapping.to_str().unwrap().to_string();
        let dry =
            run(&db, &["schema-migrate", "--mapping", &mapping, "--json"]);
        let plan: serde_json::Value =
            serde_json::from_slice(&dry.stdout).unwrap();
        let proof = plan["proof"].as_str().unwrap().to_string();

        // The apply's writes go through a fault backend that fails ONE write
        // after `budget` writes (the gate acquisition is one of them) and then
        // works again: a transient failure, after which releasing the gate
        // would succeed - it must not happen.
        let fault = std::sync::Arc::new(FaultBackend::wrap(backend(&db)));
        fault.fail_one_write_after(budget);
        let err = runner::schema_migrate::run(
            runner::schema_migrate::MigrateArgs {
                mapping: mapping.clone(),
                tenant: None,
                source: None,
                apply: true,
                expect_proof: Some(proof.clone()),
                resume_owner: None,
                json: false,
            },
            fault.clone(),
        )
        .await
        .expect_err("the budget is too small for the whole apply");
        let b = backend(&db);
        let GateState::Held(holder) = store_gate::status(&b).await.unwrap()
        else {
            continue; // failed before the gate was taken
        };
        if adopted_versions(&db, &lh).await != 1 {
            // Not the partial state this test is about; try the next budget.
            continue;
        }
        assert_eq!(holder.role, GateRole::Migration);
        assert_eq!(holder.migration_proof.as_deref(), Some(proof.as_str()));
        let msg = format!("{err:#}");
        assert!(msg.contains("--resume-owner"), "{msg}");
        assert!(msg.contains(&holder.owner_id), "{msg}");

        // The partial history is unreachable: no server, no break, no new apply.
        let refused = start_server(&db).wait_with_output().unwrap();
        assert!(!refused.status.success());
        assert!(
            text(&refused).contains("--resume-owner"),
            "{}",
            text(&refused)
        );
        let broke =
            run(&db, &["store-gate", "break", "--owner", &holder.owner_id]);
        assert!(!broke.status.success());
        assert!(
            text(&broke).contains("unfinished schema migration"),
            "{}",
            text(&broke)
        );
        let apply = [
            "schema-migrate",
            "--mapping",
            &mapping,
            "--apply",
            "--expect-proof",
            &proof,
        ];
        assert!(!run(&db, &apply).status.success());
        // A resume must name the recorded owner and the same proof.
        let wrong_proof = run(
            &db,
            &[
                "schema-migrate",
                "--mapping",
                &mapping,
                "--apply",
                "--expect-proof",
                "0000",
                "--resume-owner",
                &holder.owner_id,
            ],
        );
        assert!(!wrong_proof.status.success());
        assert!(matches!(store_gate::status(&b).await.unwrap(),
            GateState::Held(h) if h.owner_id == holder.owner_id));
        assert_eq!(
            adopted_versions(&db, &lh).await,
            1,
            "nothing more was written"
        );

        // Resume under the same gate: completes, then releases.
        let mut resume: Vec<&str> = apply.to_vec();
        resume.extend(["--resume-owner", &holder.owner_id]);
        let resumed = run(&db, &resume);
        assert!(resumed.status.success(), "{}", text(&resumed));
        assert_eq!(store_gate::status(&b).await.unwrap(), GateState::Unlocked);
        assert_eq!(adopted_versions(&db, &lh).await, 2);

        // Now, and only now, a server may start.
        let mut server = start_server(&db);
        wait_for_server_gate(&b, server.id()).await;
        signal(&server, "-TERM");
        assert!(wait_exit(&mut server).success());
        return;
    }
    panic!("no write budget produced a partial adoption under a held gate");
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
