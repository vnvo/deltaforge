//! MySQL controlled credential-rotation e2e. Run with:
//! `cargo test -p sources --test mysql_rotation_e2e -- --include-ignored --nocapture --test-threads=1`
//!
//! Exercises the full production rotation path against a live MySQL (GTID mode) and
//! the principal correctness guarantees: baseline dedup + new-password reconnect,
//! mid-transaction deferral, invalid-then-valid recovery, resume from the frozen
//! GTID boundary without loss/replay, and terminal rejection of a wrong server_uuid
//! or an incompatible GTID set. Reconnection is proven by the binlog dump thread id
//! changing (a fresh replica connection authenticated with the new password).

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
    CredentialRotationCfg, MysqlSrcCfg, OnSchemaDrift, PipelineSpec,
    RotationTriggerCfg, SnapshotCfg, SnapshotMode, SourceCfg,
    SourceCredentialsCfg,
};
use deltaforge_core::{Op, Source, SourceHandle, SourceItem};
use mysql_async::{Pool, prelude::Queryable};
use secrets::{
    FileMode, FilePolicy, FileResolver, SecretProvider, SecretReference,
    SecretResolver,
};
use sources::credentials::resolve_mysql_credentials;
use sources::failover::identity::{IdentityStore, ServerIdentity};
use sources::mysql::mysql_health::{
    MySqlServerIdentity, fetch_server_identity,
};
use sources::mysql::{MySqlSource, mysql_rotation};
use sources::{
    MySqlCheckpoint, RotationReject, build_source, resolve_source_dsn,
    source_secret_resolver,
};
use tokio::{
    sync::mpsc,
    time::{Duration, sleep, timeout},
};

mod test_common;
use test_common::{
    MYSQL_CDC_PASSWORD, MYSQL_CDC_USER, MYSQL_CONTAINER, make_registry,
    make_storage_backend, mysql_drop_db, mysql_port, mysql_root_dsn,
    mysql_setup,
};

#[dtor]
fn cleanup() {
    if let Some(c) = MYSQL_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", c.0.id()])
            .output()
            .ok();
    }
}

const LIMIT: usize = 1 << 20;
static UNIQ: AtomicU64 = AtomicU64::new(0);

/// A Kubernetes-projected-Secret-like layout under a unique temp root.
struct Projected {
    root: PathBuf,
    version: u64,
}

impl Projected {
    fn new(files: &[(&str, &[u8])]) -> Self {
        let n = UNIQ.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "df-myrot-{}-{}",
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

/// Ids of the binlog dump threads currently connected (via root, which sees all).
async fn dump_thread_ids(root_dsn: &str) -> Result<Vec<u64>> {
    let pool = Pool::new(root_dsn);
    let mut conn = pool.get_conn().await?;
    let rows: Vec<(u64,)> = conn
        .query(
            "SELECT ID FROM information_schema.PROCESSLIST \
             WHERE COMMAND LIKE 'Binlog Dump%'",
        )
        .await?;
    conn.disconnect().await.ok();
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Wait until at least one binlog dump thread exists; return one of its ids.
async fn wait_for_dump_thread(root_dsn: &str, dur: Duration) -> Result<u64> {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        let ids = dump_thread_ids(root_dsn).await?;
        if let Some(id) = ids.first() {
            return Ok(*id);
        }
        sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("no binlog dump thread appeared within {dur:?}")
}

/// Wait until a binlog dump thread with an id different from `baseline` exists.
async fn wait_for_dump_thread_change(
    root_dsn: &str,
    baseline: u64,
    dur: Duration,
) -> Result<u64> {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        let ids = dump_thread_ids(root_dsn).await?;
        if let Some(id) = ids.iter().find(|id| **id != baseline) {
            return Ok(*id);
        }
        sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("binlog dump thread id did not change within {dur:?}")
}

/// Assert no dump thread other than `expected` appears for `dur` (no reconnect).
async fn assert_dump_thread_stable(
    root_dsn: &str,
    expected: u64,
    dur: Duration,
) -> Result<()> {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        let ids = dump_thread_ids(root_dsn).await?;
        for id in &ids {
            anyhow::ensure!(
                *id == expected,
                "binlog dump thread changed to {id} (expected stable {expected})"
            );
        }
        sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

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

async fn gtid_executed(root_dsn: &str) -> Result<String> {
    let pool = Pool::new(root_dsn);
    let mut conn = pool.get_conn().await?;
    let row: Option<(Option<String>,)> =
        conn.query_first("SELECT @@GLOBAL.gtid_executed").await?;
    conn.disconnect().await.ok();
    Ok(row.and_then(|(s,)| s).unwrap_or_default())
}

/// The GTID(s) in the server's executed set that are not in `minus` - used to
/// isolate the single GTID a statement just produced.
async fn gtid_subtract(root_dsn: &str, minus: &str) -> Result<String> {
    let pool = Pool::new(root_dsn);
    let mut conn = pool.get_conn().await?;
    let row: Option<(Option<String>,)> = conn
        .exec_first("SELECT GTID_SUBTRACT(@@GLOBAL.gtid_executed, ?)", (minus,))
        .await?;
    conn.disconnect().await.ok();
    Ok(row.and_then(|(s,)| s).unwrap_or_default())
}

async fn gtid_subset(
    root_dsn: &str,
    subset: &str,
    superset: &str,
) -> Result<bool> {
    let pool = Pool::new(root_dsn);
    let mut conn = pool.get_conn().await?;
    let row: Option<(Option<i64>,)> = conn
        .exec_first("SELECT GTID_SUBSET(?, ?)", (subset, superset))
        .await?;
    conn.disconnect().await.ok();
    Ok(matches!(row, Some((Some(1),))))
}

/// Two GTID sets denote the same set (mutual subset), tolerant of range
/// formatting (`uuid:15` vs `uuid:15-15`).
async fn gtid_sets_equal(root_dsn: &str, a: &str, b: &str) -> Result<bool> {
    Ok(gtid_subset(root_dsn, a, b).await?
        && gtid_subset(root_dsn, b, a).await?)
}

/// The next `TxCommit` if no data `Create` event precedes it, returning its tx_id
/// and decoded checkpoint. `None` if a data event comes first or it times out.
async fn next_dataless_txcommit(
    rx: &mut mpsc::Receiver<SourceItem>,
    dur: Duration,
) -> Option<(String, MySqlCheckpoint)> {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        match timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(SourceItem::TxCommit { tx_id, boundary })) => {
                let cp: MySqlCheckpoint =
                    serde_json::from_slice(boundary.checkpoint.as_bytes())
                        .ok()?;
                return Some((tx_id, cp));
            }
            Ok(Some(SourceItem::Event(e))) if matches!(e.op, Op::Create) => {
                return None;
            }
            Ok(Some(_)) => continue,
            Ok(None) => return None,
            Err(_) => continue,
        }
    }
    None
}

/// Drain and discard every item available within `dur` (quiesce the channel).
async fn drain_for(rx: &mut mpsc::Receiver<SourceItem>, dur: Duration) {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => continue,
        }
    }
}

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

fn mysql_rotation_cfg(db: &str, port: u16, proj: &Projected) -> MysqlSrcCfg {
    MysqlSrcCfg {
        id: "my-rot".to_string(),
        dsn: Some(format!("mysql://127.0.0.1:{port}/{db}")),
        dsn_secret: None,
        credentials: Some(SourceCredentialsCfg {
            username: Some(proj.key_ref("username")),
            password: Some(proj.key_ref("password")),
        }),
        tables: vec![format!("{db}.orders")],
        table_options: Default::default(),
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

struct Harness {
    db: String,
    pool: Pool,
    root_dsn: String,
    proj: Projected,
    rx: mpsc::Receiver<SourceItem>,
    handle: SourceHandle,
}

impl Harness {
    async fn rotate_password(&mut self, new_password: &str) -> Result<()> {
        let mut conn = self.pool.get_conn().await?;
        // ALTER USER takes effect for new connections immediately in MySQL 8; no
        // FLUSH PRIVILEGES (which would be its own non-DDL GTID transaction the
        // source does not treat as a commit boundary).
        conn.query_drop(format!(
            "ALTER USER '{MYSQL_CDC_USER}'@'%' IDENTIFIED BY '{new_password}'"
        ))
        .await?;
        conn.disconnect().await.ok();
        self.proj.publish(&[
            ("username", MYSQL_CDC_USER.as_bytes()),
            ("password", new_password.as_bytes()),
        ]);
        Ok(())
    }

    async fn finish(self) {
        self.handle.stop();
        self.handle.join().await.ok();
        mysql_drop_db(&self.pool, &self.db).await;
    }
}

/// Set up a database + table and start a MySQL source whose file-backed credentials
/// rotate through a projected volume. `channel_cap` sizes the event channel.
async fn start_rotation_harness(
    suffix: &str,
    channel_cap: usize,
) -> Result<Harness> {
    let (db, pool, _cdc_dsn) = mysql_setup(suffix).await?;
    let port = mysql_port().await;
    let root_dsn = mysql_root_dsn().await;

    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!(
        "CREATE TABLE {db}.orders \
         (id INT AUTO_INCREMENT PRIMARY KEY, sku VARCHAR(64) NOT NULL)"
    ))
    .await?;
    // The CDC user is cluster-wide and shared across the test databases in this
    // container; reset it to the baseline so this test's projected credentials
    // authenticate regardless of a prior test's rotation.
    conn.query_drop(format!(
        "ALTER USER '{MYSQL_CDC_USER}'@'%' IDENTIFIED BY '{MYSQL_CDC_PASSWORD}'"
    ))
    .await?;
    conn.disconnect().await.ok();

    let proj = Projected::new(&[
        ("username", MYSQL_CDC_USER.as_bytes()),
        ("password", MYSQL_CDC_PASSWORD.as_bytes()),
    ]);
    let cfg = mysql_rotation_cfg(&db, port, &proj);

    // Resolve the initial DSN through the real production path so the manager's
    // baseline compose matches it exactly (no spurious startup reconnect).
    let resolver = file_resolver(&proj.root);
    let initial_dsn = resolve_mysql_credentials(&cfg, resolver.as_ref())
        .await
        .map_err(|e| anyhow::anyhow!("resolve initial dsn: {e}"))?
        .build_dsn()
        .map_err(|e| anyhow::anyhow!("build initial dsn: {e}"))?;
    let rotation = mysql_rotation::build_spec(&cfg, resolver.clone()).await?;
    assert!(rotation.is_some(), "rotation spec should build");

    let backend = make_storage_backend().await;
    let src = MySqlSource {
        id: cfg.id.clone(),
        dsn: initial_dsn,
        tables: vec![format!("{db}.orders")],
        tenant: "acme".to_string(),
        pipeline: "test".to_string(),
        registry: make_registry().await,
        backend: Arc::clone(&backend),
        outbox_tables: AllowList::default(),
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
        pool,
        root_dsn,
        proj,
        rx,
        handle,
    })
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_rotation_reconnects_on_new_password_and_dedups_baseline()
-> Result<()> {
    let mut h = start_rotation_harness("rot_basic", 256).await?;

    let dump_before =
        wait_for_dump_thread(&h.root_dsn, Duration::from_secs(30)).await?;
    eprintln!("[mysql-rotation-e2e] initial dump thread = {dump_before}");

    let mut conn = h.pool.get_conn().await?;
    conn.query_drop(format!(
        "INSERT INTO {}.orders (sku) VALUES ('before')",
        h.db
    ))
    .await?;
    conn.disconnect().await.ok();
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "should stream a CDC event before rotation"
    );

    // Unchanged baseline must be deduped: no reconnect (dump thread stays put).
    assert_dump_thread_stable(&h.root_dsn, dump_before, Duration::from_secs(3))
        .await?;

    h.rotate_password("newpw").await?;

    let dump_after = wait_for_dump_thread_change(
        &h.root_dsn,
        dump_before,
        Duration::from_secs(40),
    )
    .await?;
    eprintln!(
        "[mysql-rotation-e2e] dump thread after rotation = {dump_after} (was {dump_before})"
    );

    let mut conn = h.pool.get_conn().await?;
    conn.query_drop(format!(
        "INSERT INTO {}.orders (sku) VALUES ('after')",
        h.db
    ))
    .await?;
    conn.disconnect().await.ok();
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "should stream a CDC event after rotation on the new credentials"
    );

    h.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_rotation_waits_for_commit_boundary_no_loss_or_dup() -> Result<()>
{
    let mut h = start_rotation_harness("rot_txn", 4).await?;
    let dump_before =
        wait_for_dump_thread(&h.root_dsn, Duration::from_secs(30)).await?;

    // One transaction of 30 rows. The source backpressures on the small channel
    // partway through emitting it (current_gtid is Some).
    let mut conn = h.pool.get_conn().await?;
    conn.query_drop(format!(
        "INSERT INTO {}.orders (sku) \
         WITH RECURSIVE seq(n) AS \
           (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 30) \
         SELECT CONCAT('row', n) FROM seq",
        h.db
    ))
    .await?;
    conn.disconnect().await.ok();
    sleep(Duration::from_secs(2)).await;

    h.rotate_password("newpw").await?;

    // Mid-transaction: the candidate must NOT be applied; dump thread stays put.
    assert_dump_thread_stable(&h.root_dsn, dump_before, Duration::from_secs(3))
        .await?;

    // Drain the whole transaction: all 30 rows, once, in id order.
    let ids = collect_create_ids(&mut h.rx, 30, Duration::from_secs(30)).await;
    assert_eq!(
        ids,
        (1..=30).collect::<Vec<i64>>(),
        "all 30 transaction rows must arrive once and in order"
    );

    // Only after the commit boundary does rotation apply: dump thread changes.
    let dump_after = wait_for_dump_thread_change(
        &h.root_dsn,
        dump_before,
        Duration::from_secs(40),
    )
    .await?;
    assert_ne!(dump_before, dump_after);

    // No duplicate rows after the reconnect (resume from the frozen GTID set).
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
async fn mysql_invalid_replacement_retained_then_valid_supersedes() -> Result<()>
{
    let mut h = start_rotation_harness("rot_invalid", 256).await?;
    let dump_before =
        wait_for_dump_thread(&h.root_dsn, Duration::from_secs(30)).await?;

    // Publish an invalid password WITHOUT changing the server: preflight cannot
    // authenticate, so the candidate is rejected and the old stream is retained.
    h.proj.publish(&[
        ("username", MYSQL_CDC_USER.as_bytes()),
        ("password", b"not-the-password"),
    ]);
    assert_dump_thread_stable(&h.root_dsn, dump_before, Duration::from_secs(5))
        .await?;

    let mut conn = h.pool.get_conn().await?;
    conn.query_drop(format!(
        "INSERT INTO {}.orders (sku) VALUES ('during-invalid')",
        h.db
    ))
    .await?;
    conn.disconnect().await.ok();
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "CDC must continue while the invalid replacement is rejected"
    );

    // A newer valid generation supersedes and reconnects.
    h.rotate_password("validnew").await?;
    let dump_after = wait_for_dump_thread_change(
        &h.root_dsn,
        dump_before,
        Duration::from_secs(40),
    )
    .await?;
    assert_ne!(dump_before, dump_after);

    let mut conn = h.pool.get_conn().await?;
    conn.query_drop(format!(
        "INSERT INTO {}.orders (sku) VALUES ('after-valid')",
        h.db
    ))
    .await?;
    conn.disconnect().await.ok();
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "CDC must continue after the valid supersede"
    );

    h.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_rotation_resumes_from_boundary_without_skip_or_replay()
-> Result<()> {
    let mut h = start_rotation_harness("rot_resume", 256).await?;
    wait_for_dump_thread(&h.root_dsn, Duration::from_secs(30)).await?;

    // Pre-rotation transactions establish the committed boundary (ids 1..=5).
    for _ in 1..=5 {
        let mut conn = h.pool.get_conn().await?;
        conn.query_drop(format!(
            "INSERT INTO {}.orders (sku) VALUES ('pre')",
            h.db
        ))
        .await?;
        conn.disconnect().await.ok();
    }
    let pre = collect_create_ids(&mut h.rx, 5, Duration::from_secs(20)).await;
    assert_eq!(
        pre,
        (1..=5).collect::<Vec<i64>>(),
        "pre-rotation rows should arrive in order"
    );
    let boundary_max = *pre.iter().max().unwrap();

    let dump_before = dump_thread_ids(&h.root_dsn)
        .await?
        .first()
        .copied()
        .expect("dump thread present");
    h.rotate_password("newpw").await?;
    wait_for_dump_thread_change(
        &h.root_dsn,
        dump_before,
        Duration::from_secs(40),
    )
    .await?;

    // The first post-rotation transactions (ids 6..=10) must arrive in order,
    // resuming from the frozen GTID boundary - none skipped, none replayed.
    for _ in 6..=10 {
        let mut conn = h.pool.get_conn().await?;
        conn.query_drop(format!(
            "INSERT INTO {}.orders (sku) VALUES ('post')",
            h.db
        ))
        .await?;
        conn.disconnect().await.ok();
    }
    let post = collect_create_ids(&mut h.rx, 5, Duration::from_secs(30)).await;
    assert_eq!(
        post,
        (6..=10).collect::<Vec<i64>>(),
        "first post-rotation transactions must resume from the boundary in order"
    );
    assert!(
        post.iter().all(|id| *id > boundary_max),
        "no pre-boundary row may be replayed after rotation"
    );

    h.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_invalid_watched_material_retains_and_recovers() -> Result<()> {
    let mut h = start_rotation_harness("rot_watch", 256).await?;
    let dump_before =
        wait_for_dump_thread(&h.root_dsn, Duration::from_secs(30)).await?;

    // Break the watched set (remove the password key). The watcher reports an
    // incomplete state; the active credentials must be retained and CDC continues.
    h.proj.break_key("password");
    assert_dump_thread_stable(&h.root_dsn, dump_before, Duration::from_secs(5))
        .await?;
    let mut conn = h.pool.get_conn().await?;
    conn.query_drop(format!(
        "INSERT INTO {}.orders (sku) VALUES ('during-incomplete')",
        h.db
    ))
    .await?;
    conn.disconnect().await.ok();
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "CDC must continue while the watched secret is incomplete"
    );

    // Recover with valid new material: rotation applies on the new credentials.
    h.rotate_password("recovered_pw").await?;
    let dump_after = wait_for_dump_thread_change(
        &h.root_dsn,
        dump_before,
        Duration::from_secs(40),
    )
    .await?;
    assert_ne!(dump_before, dump_after);

    h.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_flush_privileges_frames_and_unblocks_rotation() -> Result<()> {
    // Regression: a GTID-backed autocommit statement (FLUSH PRIVILEGES) that is not
    // DDL and not COMMIT/ROLLBACK must still be framed as its own transaction -
    // emitting a data-less commit boundary and clearing current_gtid - otherwise
    // the transaction stays open forever and blocks the rotation boundary.
    let mut h = start_rotation_harness("rot_flush", 256).await?;
    let dump_before =
        wait_for_dump_thread(&h.root_dsn, Duration::from_secs(30)).await?;

    // Quiesce the channel after snapshot/startup.
    drain_for(&mut h.rx, Duration::from_secs(2)).await;

    // Record the executed set, then issue a data-less autocommit GTID statement
    // (FLUSH PRIVILEGES is written to the binlog with its own GTID).
    let executed_before = gtid_executed(&h.root_dsn).await?;
    let mut conn = h.pool.get_conn().await?;
    conn.query_drop("FLUSH PRIVILEGES").await?;
    conn.disconnect().await.ok();
    // Isolate the exact GTID the FLUSH produced.
    let flush_gtid = gtid_subtract(&h.root_dsn, &executed_before).await?;
    assert!(!flush_gtid.is_empty(), "FLUSH must produce a GTID");

    // (4) A data-less commit boundary is emitted with no data event before it.
    let (tx_id, checkpoint) =
        next_dataless_txcommit(&mut h.rx, Duration::from_secs(15))
            .await
            .expect("FLUSH must emit a data-less commit boundary");
    // (2) The boundary belongs to exactly the FLUSH GTID.
    assert!(
        gtid_sets_equal(&h.root_dsn, &tx_id, &flush_gtid).await?,
        "TxCommit tx_id ({tx_id}) must be the FLUSH GTID ({flush_gtid})"
    );
    // (3) Its checkpoint's accumulated GTID set includes the FLUSH GTID.
    let checkpoint_gtids = checkpoint
        .gtid_set
        .expect("FLUSH boundary checkpoint must carry a gtid set");
    assert!(
        gtid_subset(&h.root_dsn, &flush_gtid, &checkpoint_gtids).await?,
        "checkpoint accumulated set ({checkpoint_gtids}) must include the \
         FLUSH GTID ({flush_gtid})"
    );

    // With current_gtid cleared, rotation now applies while the stream is idle.
    h.rotate_password("newpw").await?;
    let dump_after = wait_for_dump_thread_change(
        &h.root_dsn,
        dump_before,
        Duration::from_secs(40),
    )
    .await?;
    assert_ne!(dump_before, dump_after);

    // The first transaction after the FLUSH boundary is delivered (its GTID was
    // framed and not skipped).
    let mut conn = h.pool.get_conn().await?;
    conn.query_drop(format!(
        "INSERT INTO {}.orders (sku) VALUES ('after-flush')",
        h.db
    ))
    .await?;
    conn.disconnect().await.ok();
    assert!(
        wait_for_event(&mut h.rx, Duration::from_secs(15)).await,
        "the transaction after the FLUSH boundary must be delivered"
    );

    h.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_preflight_rejects_wrong_server_uuid() -> Result<()> {
    let root_dsn = mysql_root_dsn().await;
    let backend = make_storage_backend().await;
    let store = IdentityStore::new(Arc::clone(&backend));
    // Durable authority says the source belongs to a different server.
    store
        .store(
            "src-wrong-uuid",
            &ServerIdentity::MySql(MySqlServerIdentity {
                server_uuid: "11111111-1111-1111-1111-111111111111".to_string(),
            }),
        )
        .await?;

    let reject =
        mysql_rotation::preflight(&root_dsn, "src-wrong-uuid", &backend, None)
            .await
            .expect_err("wrong server_uuid must be rejected");
    assert_eq!(reject, RotationReject::IdentityMismatch);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_preflight_rejects_incompatible_gtid() -> Result<()> {
    let root_dsn = mysql_root_dsn().await;
    let backend = make_storage_backend().await;
    let store = IdentityStore::new(Arc::clone(&backend));

    // Store the REAL identity so the identity gate passes and the GTID gate is
    // what rejects.
    let live = fetch_server_identity(&root_dsn)
        .await?
        .expect("server_uuid available");
    store.store("src-gtid", &ServerIdentity::from(live)).await?;

    // A GTID set from a foreign uuid is not a subset of gtid_executed.
    let foreign = "22222222-2222-2222-2222-222222222222:1-100".to_string();
    let reject = mysql_rotation::preflight(
        &root_dsn,
        "src-gtid",
        &backend,
        Some(&foreign),
    )
    .await
    .expect_err("incompatible GTID set must be rejected");
    assert_eq!(reject, RotationReject::PositionIncompatible);
    Ok(())
}

// ---------------------------------------------------------------------------
// Config-path test (no docker): the genuine startup path must resolve
// projected-volume symlinks for a MySQL source too.
// ---------------------------------------------------------------------------

fn mysql_pipeline_spec_with_rotation(proj: &Projected) -> PipelineSpec {
    let v = serde_json::json!({
        "metadata": { "name": "p", "tenant": "t" },
        "spec": {
            "source": {
                "type": "mysql",
                "config": {
                    "id": "my",
                    "dsn": "mysql://127.0.0.1:3306/d",
                    "credentials": {
                        "username": { "provider": "file", "location": proj.root.join("username").to_str().unwrap() },
                        "password": { "provider": "file", "location": proj.root.join("password").to_str().unwrap() }
                    },
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

#[tokio::test]
async fn mysql_projected_symlink_resolves_through_genuine_build_path()
-> Result<()> {
    let proj =
        Projected::new(&[("username", b"myuser"), ("password", b"mypass")]);
    let spec = mysql_pipeline_spec_with_rotation(&proj);

    // The spec-aware resolver must handle projected symlinks for MySQL too.
    let resolver = source_secret_resolver(&spec).await?;
    let dsn = resolve_source_dsn(&spec, resolver.as_ref()).await?;
    assert!(dsn.expose().contains("myuser"));
    assert!(dsn.expose().contains("mypass"));

    if let SourceCfg::Mysql(c) = &spec.spec.source {
        let rot = mysql_rotation::build_spec(c, resolver.clone()).await?;
        assert!(rot.is_some(), "rotation must build via the genuine path");
    }

    let _src = build_source(
        &spec,
        dsn,
        make_registry().await,
        make_storage_backend().await,
        resolver,
    )
    .await?;
    Ok(())
}
