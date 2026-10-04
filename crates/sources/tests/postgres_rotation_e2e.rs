//! PostgreSQL controlled credential-rotation e2e. Run with:
//! `cargo test -p sources --test postgres_rotation_e2e -- --include-ignored --nocapture --test-threads=1`
//!
//! Exercises the full production rotation path against a live PostgreSQL and the
//! principal correctness guarantees:
//! - a new-password rotation reconnects the walsender (pid changes) and the
//!   unchanged baseline is deduped (no needless reconnect);
//! - a candidate arriving mid-transaction waits for the commit boundary, and all
//!   rows of that transaction arrive once and in order;
//! - an invalid replacement retains the old stream, and a later valid generation
//!   supersedes it and reconnects;
//! - after rotation the stream resumes from the frozen boundary with no skipped or
//!   duplicated transactions.

use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_config::{
    CredentialRotationCfg, OnSchemaDrift, PipelineSpec, PostgresSrcCfg,
    RotationTriggerCfg, SnapshotCfg, SnapshotMode, SourceCfg,
    SourceCredentialsCfg,
};
use deltaforge_core::{Op, Source, SourceHandle, SourceItem};
use secrets::{
    FileMode, FilePolicy, FileResolver, SecretProvider, SecretReference,
    SecretResolver,
};
use sources::credentials::resolve_postgres_credentials;
use sources::postgres::{PostgresSource, postgres_rotation};
use sources::{build_source, resolve_source_dsn, source_secret_resolver};
use tokio::{
    sync::mpsc,
    time::{Duration, sleep, timeout},
};

mod test_common;
use test_common::{
    PG_CDC_PASS, PG_CDC_USER, PG_CONTAINER, make_registry,
    make_storage_backend, pg_drop_db, pg_port, pg_setup,
};

#[dtor]
fn cleanup() {
    if let Some(c) = PG_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", c.0.id()])
            .output()
            .ok();
    }
}

const LIMIT: usize = 1 << 20;
static UNIQ: AtomicU64 = AtomicU64::new(0);

/// A Kubernetes-projected-Secret-like layout under a unique temp root: each key is
/// a symlink into `..data`, and a rotation atomically swaps `..data`.
struct Projected {
    root: PathBuf,
    version: u64,
}

impl Projected {
    fn new(files: &[(&str, &[u8])]) -> Self {
        let n = UNIQ.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "df-pgrot-{}-{}",
            std::process::id(),
            n
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut p = Projected { root, version: 0 };
        p.publish(files);
        p
    }

    fn publish(&mut self, files: &[(&str, &[u8])]) {
        use std::io::Write;
        self.version += 1;
        let data_dir = self.root.join(format!("..{:04}", self.version));
        std::fs::create_dir_all(&data_dir).unwrap();
        for (name, contents) in files {
            let mut f = std::fs::File::create(data_dir.join(name)).unwrap();
            f.write_all(contents).unwrap();
            f.sync_all().unwrap();
        }
        let data_link = self.root.join("..data");
        let _ = std::fs::remove_file(&data_link);
        symlink(&data_dir, &data_link).unwrap();
        for (name, _) in files {
            let key_link = self.root.join(name);
            let _ = std::fs::remove_file(&key_link);
            symlink(self.root.join("..data").join(name), &key_link).unwrap();
        }
    }

    fn key_ref(&self, name: &str) -> SecretReference {
        SecretReference::new(
            SecretProvider::File,
            self.root.join(name).to_str().unwrap(),
        )
    }

    /// Remove a key's top-level symlink, so the projected set can no longer be
    /// resolved (the watcher reports an incomplete/mid-swap state).
    fn break_key(&self, name: &str) {
        let _ = std::fs::remove_file(self.root.join(name));
    }
}

impl Drop for Projected {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn file_resolver(root: &Path) -> Arc<dyn SecretResolver> {
    Arc::new(FileResolver::new(FilePolicy {
        max_size: LIMIT,
        mode: FileMode::ProjectedVolume {
            trusted_root: root.to_path_buf(),
        },
        trim_trailing_newline: false,
    }))
}

/// Read the slot's walsender backend pid, or `None` when the slot is inactive.
async fn slot_active_pid(
    admin: &tokio_postgres::Client,
    slot: &str,
) -> Result<Option<i32>> {
    let row = admin
        .query_opt(
            // Only a walsender counts: while `pg_create_logical_replication_slot`
            // runs during snapshot anchoring, the slot is held by that ordinary
            // backend, whose pid is not the stream's.
            "SELECT s.active_pid FROM pg_replication_slots s \
             JOIN pg_stat_activity a ON a.pid = s.active_pid \
             WHERE s.slot_name = $1 AND a.backend_type = 'walsender'",
            &[&slot],
        )
        .await?;
    Ok(row.and_then(|r| r.get::<_, Option<i32>>(0)))
}

/// Poll until the slot is active with a pid different from `baseline`.
async fn wait_for_pid_change(
    admin: &tokio_postgres::Client,
    slot: &str,
    baseline: i32,
    dur: Duration,
) -> Result<i32> {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if let Some(pid) = slot_active_pid(admin, slot).await? {
            if pid != baseline {
                return Ok(pid);
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("slot walsender pid did not change within {dur:?}")
}

/// Wait until the slot becomes active, returning its pid.
async fn wait_for_active_pid(
    admin: &tokio_postgres::Client,
    slot: &str,
    dur: Duration,
) -> Result<i32> {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if let Some(pid) = slot_active_pid(admin, slot).await? {
            return Ok(pid);
        }
        sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("slot never became active within {dur:?}")
}

/// Assert the slot pid stays `expected` for `dur` (no reconnect).
async fn assert_pid_stable(
    admin: &tokio_postgres::Client,
    slot: &str,
    expected: i32,
    dur: Duration,
) -> Result<()> {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if let Some(pid) = slot_active_pid(admin, slot).await? {
            anyhow::ensure!(
                pid == expected,
                "walsender pid changed to {pid} (expected stable {expected})"
            );
        }
        sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

/// Drain until a data event arrives (skipping non-event items), or time out.
async fn wait_for_event(
    rx: &mut mpsc::Receiver<SourceItem>,
    dur: Duration,
) -> bool {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        match timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(SourceItem::Event(_))) => return true,
            Ok(Some(_)) => continue,
            Ok(None) => return false,
            Err(_) => continue,
        }
    }
    false
}

/// Collect the `id` of the next `want` Create events (insert row images), in
/// arrival order, or until `dur` elapses.
async fn collect_create_ids(
    rx: &mut mpsc::Receiver<SourceItem>,
    want: usize,
    dur: Duration,
) -> Vec<i64> {
    let mut ids = Vec::new();
    let deadline = Instant::now() + dur;
    while ids.len() < want && Instant::now() < deadline {
        match timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) if matches!(e.op, Op::Create) => {
                if let Some(id) = e
                    .after
                    .as_ref()
                    .and_then(|v| v.get("id"))
                    .and_then(|v| v.as_i64())
                {
                    ids.push(id);
                }
            }
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    ids
}

fn pg_rotation_cfg(
    db: &str,
    port: u16,
    proj: &Projected,
    publication: &str,
    slot: &str,
) -> PostgresSrcCfg {
    PostgresSrcCfg {
        id: "pg-rot".to_string(),
        dsn: Some(format!("host=127.0.0.1 port={port} dbname={db}")),
        dsn_secret: None,
        credentials: Some(SourceCredentialsCfg {
            username: Some(proj.key_ref("username")),
            password: Some(proj.key_ref("password")),
        }),
        publication: publication.to_string(),
        slot: slot.to_string(),
        tables: vec!["public.orders".to_string()],
        table_options: Default::default(),
        start_position: Default::default(),
        outbox: None,
        snapshot: SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
        on_schema_drift: OnSchemaDrift::Adapt,
        rotation: Some(CredentialRotationCfg {
            trigger: RotationTriggerCfg::File {
                trusted_root: proj.root.clone(),
            },
            poll_interval_ms: 300,
            debounce_ms: 300,
            max_secret_bytes: LIMIT,
            apply_timeout_ms: 30_000,
        }),
    }
}

/// A running source configured for controlled rotation, plus the fixtures needed to
/// drive and observe it.
struct Harness {
    db: String,
    admin: tokio_postgres::Client,
    slot: String,
    proj: Projected,
    rx: mpsc::Receiver<SourceItem>,
    handle: SourceHandle,
}

impl Harness {
    async fn rotate_password(&mut self, new_password: &str) -> Result<()> {
        self.admin
            .execute(
                &format!("ALTER ROLE {PG_CDC_USER} PASSWORD '{new_password}'"),
                &[],
            )
            .await?;
        self.proj.publish(&[
            ("username", PG_CDC_USER.as_bytes()),
            ("password", new_password.as_bytes()),
        ]);
        Ok(())
    }

    async fn finish(self) {
        self.handle.stop();
        self.handle.join().await.ok();
        pg_drop_db(&self.db).await;
    }
}

/// Set up a database + table + publication and start a source whose file-backed
/// credentials rotate through a projected volume. `channel_cap` sizes the event
/// channel (small values let a test hold the source backpressured mid-transaction).
async fn start_rotation_harness(
    suffix: &str,
    channel_cap: usize,
) -> Result<Harness> {
    let (db, admin) = pg_setup(suffix).await?;
    let port = pg_port().await;

    // The CDC role is cluster-wide and shared across the test databases in this
    // container; a prior test may have rotated its password. Reset it to the known
    // baseline so this test's initial projected credentials authenticate.
    admin
        .execute(
            &format!("ALTER ROLE {PG_CDC_USER} PASSWORD '{PG_CDC_PASS}'"),
            &[],
        )
        .await?;

    admin
        .execute(
            "CREATE TABLE orders (id SERIAL PRIMARY KEY, sku TEXT NOT NULL)",
            &[],
        )
        .await?;
    admin
        .execute(
            &format!("GRANT SELECT ON public.orders TO {PG_CDC_USER}"),
            &[],
        )
        .await?;
    let publication =
        format!("rot_pub_{}", UNIQ.fetch_add(1, Ordering::Relaxed));
    let slot = format!("rot_slot_{}", UNIQ.fetch_add(1, Ordering::Relaxed));
    admin
        .execute(
            &format!(
                "CREATE PUBLICATION {publication} FOR TABLE public.orders"
            ),
            &[],
        )
        .await?;

    let proj = Projected::new(&[
        ("username", PG_CDC_USER.as_bytes()),
        ("password", PG_CDC_PASS.as_bytes()),
    ]);
    let cfg = pg_rotation_cfg(&db, port, &proj, &publication, &slot);

    // Resolve the initial DSN through the real production path so the manager's
    // baseline compose matches it exactly (no spurious startup reconnect).
    let resolver = file_resolver(&proj.root);
    let initial_dsn = resolve_postgres_credentials(&cfg, resolver.as_ref())
        .await
        .map_err(|e| anyhow::anyhow!("resolve initial dsn: {e}"))?
        .build_dsn()
        .map_err(|e| anyhow::anyhow!("build initial dsn: {e}"))?;
    let rotation =
        postgres_rotation::build_spec(&cfg, resolver.clone()).await?;
    assert!(rotation.is_some(), "rotation spec should build");

    let src = PostgresSource {
        id: cfg.id.clone(),
        dsn: initial_dsn,
        slot: slot.clone(),
        publication: publication.clone(),
        tables: vec!["public.orders".to_string()],
        tenant: "acme".to_string(),
        pipeline: "test".to_string(),
        registry: make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend: make_storage_backend().await,
        outbox_prefixes: AllowList::default(),
        snapshot_cfg: SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
        on_schema_drift: OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation,
    };

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, rx) = mpsc::channel(channel_cap);
    let handle = src.run(tx, ckpt).await;

    Ok(Harness {
        db,
        admin,
        slot,
        proj,
        rx,
        handle,
    })
}

#[tokio::test]
#[ignore = "requires docker"]
async fn rotation_reconnects_on_new_password_and_dedups_baseline() -> Result<()>
{
    let mut h = start_rotation_harness("rot_basic", 256).await?;

    let pid_before =
        wait_for_active_pid(&h.admin, &h.slot, Duration::from_secs(30)).await?;
    eprintln!("[rotation-e2e] initial walsender pid = {pid_before}");

    h.admin
        .execute("INSERT INTO orders (sku) VALUES ('before')", &[])
        .await?;
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "should stream a CDC event before rotation"
    );

    // Unchanged baseline: the watcher observes the current credentials and must
    // dedup them, so no reconnect fires and the pid stays put.
    assert_pid_stable(&h.admin, &h.slot, pid_before, Duration::from_secs(3))
        .await?;

    h.rotate_password("newpw").await?;

    let pid_after = wait_for_pid_change(
        &h.admin,
        &h.slot,
        pid_before,
        Duration::from_secs(40),
    )
    .await?;
    eprintln!(
        "[rotation-e2e] walsender pid after rotation = {pid_after} (was {pid_before})"
    );

    h.admin
        .execute("INSERT INTO orders (sku) VALUES ('after')", &[])
        .await?;
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "should stream a CDC event after rotation on the new credentials"
    );

    h.finish().await;
    Ok(())
}

/// A stream that drops after a rotation reconnects with the rotated
/// credentials, not the ones the source started with (the server no longer
/// accepts those).
#[tokio::test]
#[ignore = "requires docker"]
async fn a_reconnect_after_rotation_uses_the_rotated_credentials() -> Result<()>
{
    let mut h = start_rotation_harness("rot_reconnect", 256).await?;
    let pid_before =
        wait_for_active_pid(&h.admin, &h.slot, Duration::from_secs(30)).await?;

    h.rotate_password("rotatedpw").await?;
    let pid_rotated = wait_for_pid_change(
        &h.admin,
        &h.slot,
        pid_before,
        Duration::from_secs(40),
    )
    .await?;

    // Drop the rotated stream: an ordinary reconnect follows.
    h.admin
        .execute("SELECT pg_terminate_backend($1)", &[&pid_rotated])
        .await?;
    wait_for_pid_change(
        &h.admin,
        &h.slot,
        pid_rotated,
        Duration::from_secs(40),
    )
    .await?;

    h.admin
        .execute("INSERT INTO orders (sku) VALUES ('after-reconnect')", &[])
        .await?;
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "CDC must continue after a reconnect that follows a rotation"
    );

    h.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn rotation_waits_for_commit_boundary_no_loss_or_dup() -> Result<()> {
    // A tiny channel lets a multi-row transaction hold the source backpressured
    // mid-transaction while the secret rotates.
    let mut h = start_rotation_harness("rot_txn", 4).await?;

    let pid_before =
        wait_for_active_pid(&h.admin, &h.slot, Duration::from_secs(30)).await?;

    // One transaction of 30 rows. The source decodes it and backpressures on the
    // small channel partway through emitting it (current_tx_id is Some).
    h.admin
        .execute(
            "INSERT INTO orders (sku) \
             SELECT 'row' || g FROM generate_series(1, 30) g",
            &[],
        )
        .await?;
    // Let the source reach the mid-transaction backpressure point.
    sleep(Duration::from_secs(2)).await;

    // Rotate while the source is mid-transaction.
    h.rotate_password("newpw").await?;

    // The candidate must NOT be applied mid-transaction: pid stays put while the
    // transaction is still being emitted.
    assert_pid_stable(&h.admin, &h.slot, pid_before, Duration::from_secs(3))
        .await?;

    // Drain the whole transaction: all 30 rows, once, in id order.
    let ids = collect_create_ids(&mut h.rx, 30, Duration::from_secs(30)).await;
    assert_eq!(
        ids,
        (1..=30).collect::<Vec<i64>>(),
        "all 30 transaction rows must arrive once and in order"
    );

    // Only after the commit boundary does the rotation apply: the pid changes.
    let pid_after = wait_for_pid_change(
        &h.admin,
        &h.slot,
        pid_before,
        Duration::from_secs(40),
    )
    .await?;
    assert_ne!(pid_before, pid_after);

    // No duplicate of the transaction's rows after the reconnect.
    let extra = collect_create_ids(&mut h.rx, 1, Duration::from_secs(3)).await;
    assert!(
        extra.is_empty(),
        "no rows should be re-delivered after the boundary reconnect, got {extra:?}"
    );

    h.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn invalid_replacement_retained_then_valid_supersedes() -> Result<()> {
    let mut h = start_rotation_harness("rot_invalid", 256).await?;

    let pid_before =
        wait_for_active_pid(&h.admin, &h.slot, Duration::from_secs(30)).await?;

    // Publish an invalid password WITHOUT changing the server: preflight cannot
    // authenticate, so the candidate is rejected and the old stream is retained.
    h.proj.publish(&[
        ("username", PG_CDC_USER.as_bytes()),
        ("password", b"not-the-password"),
    ]);
    assert_pid_stable(&h.admin, &h.slot, pid_before, Duration::from_secs(5))
        .await?;

    // CDC continues on the still-valid old connection.
    h.admin
        .execute("INSERT INTO orders (sku) VALUES ('during-invalid')", &[])
        .await?;
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "CDC must continue while the invalid replacement is rejected"
    );

    // A newer valid generation supersedes the failed one and reconnects.
    h.rotate_password("validnew").await?;
    let pid_after = wait_for_pid_change(
        &h.admin,
        &h.slot,
        pid_before,
        Duration::from_secs(40),
    )
    .await?;
    assert_ne!(pid_before, pid_after);

    h.admin
        .execute("INSERT INTO orders (sku) VALUES ('after-valid')", &[])
        .await?;
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "CDC must continue after the valid supersede"
    );

    h.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn invalid_watched_material_retains_and_recovers() -> Result<()> {
    let mut h = start_rotation_harness("rot_watch", 256).await?;

    let pid_before =
        wait_for_active_pid(&h.admin, &h.slot, Duration::from_secs(30)).await?;

    // Break the watched set (remove the password key). The watcher reports an
    // incomplete state; the active credentials must be retained (no reconnect) and
    // CDC must keep flowing on the existing connection.
    h.proj.break_key("password");
    assert_pid_stable(&h.admin, &h.slot, pid_before, Duration::from_secs(5))
        .await?;
    h.admin
        .execute("INSERT INTO orders (sku) VALUES ('during-incomplete')", &[])
        .await?;
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "CDC must continue while the watched secret is incomplete"
    );

    // Recover with valid new material: the watcher resolves again and the rotation
    // applies on the new credentials.
    h.rotate_password("recovered_pw").await?;
    let pid_after = wait_for_pid_change(
        &h.admin,
        &h.slot,
        pid_before,
        Duration::from_secs(40),
    )
    .await?;
    assert_ne!(pid_before, pid_after);

    h.admin
        .execute("INSERT INTO orders (sku) VALUES ('after-recovery')", &[])
        .await?;
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "CDC must continue after recovery on the new credentials"
    );

    h.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn rotation_resumes_from_boundary_without_skip_or_replay() -> Result<()> {
    let mut h = start_rotation_harness("rot_resume", 256).await?;

    wait_for_active_pid(&h.admin, &h.slot, Duration::from_secs(30)).await?;

    // Pre-rotation transactions establish the committed boundary (ids 1..=5).
    for _ in 1..=5 {
        h.admin
            .execute("INSERT INTO orders (sku) VALUES ('pre')", &[])
            .await?;
    }
    let pre = collect_create_ids(&mut h.rx, 5, Duration::from_secs(20)).await;
    assert_eq!(
        pre,
        (1..=5).collect::<Vec<i64>>(),
        "pre-rotation rows should arrive in order"
    );
    let boundary_max = *pre.iter().max().unwrap();

    let pid_before = slot_active_pid(&h.admin, &h.slot).await?.unwrap();

    // Rotate at an idle boundary.
    h.rotate_password("newpw").await?;
    wait_for_pid_change(&h.admin, &h.slot, pid_before, Duration::from_secs(40))
        .await?;

    // The first post-rotation transactions (ids 6..=10) must arrive, in order,
    // resuming from the frozen boundary - none skipped.
    for _ in 6..=10 {
        h.admin
            .execute("INSERT INTO orders (sku) VALUES ('post')", &[])
            .await?;
    }
    let post = collect_create_ids(&mut h.rx, 5, Duration::from_secs(30)).await;
    assert_eq!(
        post,
        (6..=10).collect::<Vec<i64>>(),
        "first post-rotation transactions must resume from the boundary in order"
    );

    // No pre-boundary row is replayed after the reconnect (resume from the frozen
    // boundary, not an earlier position).
    assert!(
        post.iter().all(|id| *id > boundary_max),
        "no pre-boundary row may be replayed after rotation"
    );

    h.finish().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Config-path tests (no docker). These run under a normal `cargo test`, so the
// resolver-policy and startup-validation blockers cannot hide behind the
// docker-gated e2e above.
// ---------------------------------------------------------------------------

fn unique_temp_dir() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "df-rotcfg-{}-{}",
        std::process::id(),
        UNIQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn pipeline_spec_with_rotation(proj: &Projected) -> PipelineSpec {
    let v = serde_json::json!({
        "metadata": { "name": "p", "tenant": "t" },
        "spec": {
            "source": {
                "type": "postgres",
                "config": {
                    "id": "pg",
                    "dsn": "host=127.0.0.1 dbname=d",
                    "credentials": {
                        "username": { "provider": "file", "location": proj.root.join("username").to_str().unwrap() },
                        "password": { "provider": "file", "location": proj.root.join("password").to_str().unwrap() }
                    },
                    "publication": "pub",
                    "slot": "slot",
                    "tables": [],
                    "rotation": {
                        "trusted_root": proj.root.to_str().unwrap(),
                        "poll_interval_ms": 300,
                        "debounce_ms": 300,
                        "max_secret_bytes": LIMIT,
                        "apply_timeout_ms": 30000
                    }
                }
            },
            "processors": [],
            "sinks": []
        }
    });
    serde_json::from_value(v).expect("valid pipeline spec")
}

fn cfg_with_file_creds(
    trusted_root: &Path,
    user_loc: &str,
    pass_loc: &str,
) -> PostgresSrcCfg {
    PostgresSrcCfg {
        id: "pg".to_string(),
        dsn: Some("host=127.0.0.1 dbname=d".to_string()),
        dsn_secret: None,
        credentials: Some(SourceCredentialsCfg {
            username: Some(SecretReference::new(
                SecretProvider::File,
                user_loc,
            )),
            password: Some(SecretReference::new(
                SecretProvider::File,
                pass_loc,
            )),
        }),
        publication: "pub".to_string(),
        slot: "slot".to_string(),
        tables: vec![],
        table_options: Default::default(),
        start_position: Default::default(),
        outbox: None,
        snapshot: Default::default(),
        on_schema_drift: OnSchemaDrift::Adapt,
        rotation: Some(CredentialRotationCfg {
            trigger: RotationTriggerCfg::File {
                trusted_root: trusted_root.to_path_buf(),
            },
            poll_interval_ms: 300,
            debounce_ms: 300,
            max_secret_bytes: LIMIT,
            apply_timeout_ms: 30_000,
        }),
    }
}

/// Blocker 1: the genuine startup path (spec-aware resolver + resolve_source_dsn +
/// build_source) must resolve projected-volume symlinks. A strict-symlink resolver
/// would fail here.
#[tokio::test]
async fn projected_symlink_resolves_through_genuine_build_path() -> Result<()> {
    let proj =
        Projected::new(&[("username", b"puser"), ("password", b"ppass")]);
    let spec = pipeline_spec_with_rotation(&proj);

    let resolver = source_secret_resolver(&spec).await?;
    let dsn = resolve_source_dsn(&spec, resolver.as_ref()).await?;
    assert!(
        dsn.expose().contains("user=puser"),
        "projected username must resolve at startup"
    );
    assert!(dsn.expose().contains("password=ppass"));

    if let SourceCfg::Postgres(c) = &spec.spec.source {
        let rot = postgres_rotation::build_spec(c, resolver.clone()).await?;
        assert!(rot.is_some(), "rotation must build via the genuine path");
    }

    let _src = build_source(
        &spec,
        dsn,
        make_registry().await,
        sources::registry_scope::SharedRegistryScope::default(),
        make_storage_backend().await,
        resolver,
    )
    .await?;
    Ok(())
}

/// Blocker 2: a valid projected symlink under the trusted root is accepted.
#[tokio::test]
async fn build_spec_accepts_valid_projected_symlink() -> Result<()> {
    let proj = Projected::new(&[("username", b"u"), ("password", b"p")]);
    let cfg = cfg_with_file_creds(
        &proj.root,
        proj.root.join("username").to_str().unwrap(),
        proj.root.join("password").to_str().unwrap(),
    );
    let resolver = file_resolver(&proj.root);
    let rot = postgres_rotation::build_spec(&cfg, resolver.clone()).await?;
    assert!(rot.is_some());
    Ok(())
}

/// Blocker 2: a `..` traversal escaping the trusted root fails startup.
#[tokio::test]
async fn build_spec_rejects_dotdot_traversal() {
    let base = unique_temp_dir();
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let outside = base.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret"), b"x").unwrap();

    let loc = root
        .join("..")
        .join("outside")
        .join("secret")
        .to_str()
        .unwrap()
        .to_string();
    let cfg = cfg_with_file_creds(&root, &loc, &loc);
    let resolver = file_resolver(&root);
    assert!(
        postgres_rotation::build_spec(&cfg, resolver.clone())
            .await
            .is_err(),
        "a `..` path escaping trusted_root must fail closed at startup"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Blocker 2: a symlink whose target escapes the trusted root fails startup.
#[tokio::test]
async fn build_spec_rejects_escaping_symlink() {
    let base = unique_temp_dir();
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let outside = base.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret"), b"x").unwrap();
    symlink(outside.join("secret"), root.join("username")).unwrap();

    let loc = root.join("username").to_str().unwrap().to_string();
    let cfg = cfg_with_file_creds(&root, &loc, &loc);
    let resolver = file_resolver(&root);
    assert!(
        postgres_rotation::build_spec(&cfg, resolver.clone())
            .await
            .is_err(),
        "a symlink escaping trusted_root must fail closed at startup"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Blocker 2: a missing watched target fails startup.
#[tokio::test]
async fn build_spec_rejects_missing_target() {
    let base = unique_temp_dir();
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let loc = root.join("nonexistent").to_str().unwrap().to_string();
    let cfg = cfg_with_file_creds(&root, &loc, &loc);
    let resolver = file_resolver(&root);
    assert!(
        postgres_rotation::build_spec(&cfg, resolver.clone())
            .await
            .is_err(),
        "a missing watched target must fail closed at startup"
    );
    let _ = std::fs::remove_dir_all(&base);
}
