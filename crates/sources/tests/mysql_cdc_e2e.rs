use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_core::{Event, Op, Source};
use mysql_async::{Pool as MySQLPool, prelude::Queryable};
use sources::mysql::MySqlSource;
use std::sync::Arc;
use std::time::Instant;
use testcontainers::runners::AsyncRunner;
use testcontainers::{
    GenericImage, ImageExt, core::IntoContainerPort, core::WaitFor,
};
use tokio::{
    io::AsyncBufReadExt,
    sync::mpsc,
    task,
    time::{Duration, sleep, timeout},
};
use tracing::{debug, info};

mod common;
use common::init_test_tracing;

/// End-to-end CDC test for the MySQL source.
///
/// This is intentionally a "fat" integration test:
/// - boots a real MySQL 8 container with GTID + ROW binlog
/// - provisions schema / user / privileges
/// - starts `MySqlSource` with an in-memory checkpoint store
/// - performs INSERT/UPDATE/DELETE
/// - asserts that corresponding CDC events are emitted with correct payloads.
///
/// NOTE: this test requires Docker and the ability to pull/run `mysql:8.4`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_cdc_end_to_end() -> Result<()> {
    init_test_tracing();

    // boot MySQL 8 with binlog/GTID
    let image = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "password")
        .with_cmd(vec![
            "--server-id=999",
            "--log-bin=/var/lib/mysql/mysql-bin.log",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
            "--binlog-checksum=NONE",
        ])
        // fixed host port keeps the DSN simple; tests are marked as E2E and
        // should not be run in parallel with other MySQL-using tests.
        .with_mapped_port(3307, 3306.tcp());

    let container = image.start().await.expect("start mysql");
    info!("Container ID: {}", container.id());

    // container log followers (helpful when this fails in CI)
    {
        let mut out = container.stdout(true); // follow=true
        task::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match out.read_line(&mut line).await {
                    Ok(0) => break, // EOF, container stopped
                    Ok(_) => print!("STDOUT: {}", line),
                    Err(e) => {
                        eprint!("stdout read error: {e}");
                        break;
                    }
                }
            }
        });
    }
    {
        let mut err = container.stderr(true);
        task::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match err.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => print!("STDERR: {}", line),
                    Err(e) => {
                        eprint!("stderr read error: {e}");
                        break;
                    }
                }
            }
        });
    }

    // give MySQL a bit of time to finish init scripts.
    // we still validate that the required binlog settings are enabled below.
    sleep(Duration::from_secs(8)).await;
    info!("starting MySQL CDC e2e test ...");

    let port = 3307;
    let root_dsn = format!("mysql://root:password@127.0.0.1:{}/", port);
    let dsn = "mysql://df:dfpw@127.0.0.1:3307/shop";

    // provision schema + user
    let pool = MySQLPool::new(root_dsn.as_str());
    let mut conn = pool.get_conn().await?;

    // sanity-check that binlog + GTID are actually enabled in the container.
    let v: (String, String) = conn
        .query_first("SHOW VARIABLES LIKE 'log_bin'")
        .await?
        .unwrap();
    assert_eq!(v.1.to_uppercase(), "ON", "log_bin must be ON for CDC");

    let gtid: (String, String) = conn
        .query_first("SHOW VARIABLES LIKE 'gtid_mode'")
        .await?
        .unwrap();
    assert!(
        gtid.1.eq_ignore_ascii_case("ON"),
        "gtid_mode must be ON for CDC"
    );

    conn.query_drop("CREATE DATABASE IF NOT EXISTS shop")
        .await?;
    debug!("database `shop` created.");

    conn.query_drop("USE shop").await?;
    conn.query_drop(
        r#"
        CREATE TABLE IF NOT EXISTS orders(
            id INT PRIMARY KEY,
            sku VARCHAR(64),
            payload JSON,
            blobz BLOB
        )"#,
    )
    .await?;
    debug!("table `orders` created.");

    conn.query_drop(
        r#"CREATE USER IF NOT EXISTS 'df'@'%' IDENTIFIED BY 'dfpw'"#,
    )
    .await?;
    debug!("user `df` created");

    conn.query_drop(
        r#"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%'"#,
    )
    .await?;
    conn.query_drop(r#"GRANT SELECT, SHOW VIEW ON shop.* TO 'df'@'%'"#)
        .await?;
    conn.query_drop("FLUSH PRIVILEGES").await?;

    // start source with df
    let src = MySqlSource {
        id: "it-mysql".into(),
        dsn: dsn.to_string(),
        tables: vec!["shop.orders".into()],
        tenant: "acme".into(),
        pipeline: "pipe-2".to_string(),
    };

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    info!("source is running ...");

    // give the source a small head-start so it can connect and subscribe
    sleep(Duration::from_secs(3)).await;

    // 1) INSERT
    conn.query_drop(
        r#"INSERT INTO orders VALUES (1,'sku-1','{"a":1}', X'DEADBEEF')"#,
    )
    .await?;

    // 2) UPDATE
    conn.query_drop("UPDATE orders SET sku='sku-1b' WHERE id=1")
        .await?;

    // 3) DELETE
    conn.query_drop("DELETE FROM orders WHERE id=1").await?;
    debug!(
        "insert, update and delete performed on `orders` - expecting CDC events ..."
    );

    // expect at least 3 events (insert/update/delete for our row), but there
    // may be additional events from DDL or other internal activity. we collect
    // all events until we see the three operations we care about or a timeout.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut got = Vec::new();
    let mut seen_insert = false;
    let mut seen_update = false;
    let mut seen_delete = false;

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Some(e)) => {
                debug!(?e, "event received");
                match e.op {
                    Op::Insert => seen_insert = true,
                    Op::Update => seen_update = true,
                    Op::Delete => seen_delete = true,
                    Op::Ddl => {}
                }
                got.push(e);

                if seen_insert && seen_update && seen_delete {
                    info!("all expected events have arrived.");
                    break;
                }
            }
            Ok(None) => {
                // source ended: stop waiting
                break;
            }
            Err(_elapsed) => {
                // deadline hit: stop waiting
                break;
            }
        }
    }

    // --- structural assertions on the event stream ---

    // we must have seen each operation at least once.
    assert!(
        got.iter().any(|e| matches!(e.op, Op::Insert)),
        "expected at least one INSERT event"
    );
    assert!(
        got.iter().any(|e| matches!(e.op, Op::Update)),
        "expected at least one UPDATE event"
    );
    assert!(
        got.iter().any(|e| matches!(e.op, Op::Delete)),
        "expected at least one DELETE event"
    );

    // filter down to events related to id=1. For DELETE, the id will be in
    // `before`; for INSERT/UPDATE it's in `after`.
    let by_id: Vec<&Event> = got
        .iter()
        .filter(|e| {
            e.after
                .as_ref()
                .and_then(|v| v.get("id"))
                .and_then(|v| v.as_i64())
                .map(|id| id == 1)
                .unwrap_or(false)
                || e.before
                    .as_ref()
                    .and_then(|v| v.get("id"))
                    .and_then(|v| v.as_i64())
                    .map(|id| id == 1)
                    .unwrap_or(false)
        })
        .collect();

    assert_eq!(
        by_id.len(),
        3,
        "expected exactly 3 CDC events for id=1 (insert, update, delete); got {by_id_len}",
        by_id_len = by_id.len()
    );

    // preserve binlog order: for our single row this should be
    // INSERT -> UPDATE -> DELETE.
    let ops: Vec<Op> = by_id.iter().map(|e| e.op).collect();
    assert_eq!(
        ops,
        vec![Op::Insert, Op::Update, Op::Delete],
        "unexpected operation order for id=1"
    );

    // --- payload assertions on the INSERT event (JSON + BLOB) ---

    let ins = by_id
        .iter()
        .find(|e| matches!(e.op, Op::Insert))
        .expect("missing INSERT event for id=1");
    let after = ins.after.as_ref().expect("INSERT must have `after`");
    assert_eq!(after["id"], 1);
    assert_eq!(after["sku"], "sku-1");
    assert_eq!(after["payload"]["a"], 1);
    assert!(
        after["blobz"]["_base64"].is_string(),
        "BLOB field should be base64-wrapped"
    );

    // assert update event shape
    let upd = by_id
        .iter()
        .find(|e| matches!(e.op, Op::Update))
        .expect("missing UPDATE event for id=1");
    let upd_before = upd.before.as_ref().expect("UPDATE must have `before`");
    let upd_after = upd.after.as_ref().expect("UPDATE must have `after`");
    assert_eq!(upd_before["id"], 1);
    assert_eq!(upd_before["sku"], "sku-1");
    assert_eq!(upd_after["id"], 1);
    assert_eq!(upd_after["sku"], "sku-1b");

    // check del event shape
    let del = by_id
        .iter()
        .find(|e| matches!(e.op, Op::Delete))
        .expect("missing DELETE event for id=1");
    let del_before = del.before.as_ref().expect("DELETE must have `before`");
    assert_eq!(del_before["id"], 1);
    assert!(del.after.is_none(), "DELETE must not have `after`");

    for e in &by_id {
        assert_eq!(e.table, "shop.orders");
        assert_eq!(e.source.db, "shop");
    }

    info!("all MySQL CDC e2e assertions are successful!");

    // clean shutdown for source
    handle.stop();
    let _ = handle.join().await;

    conn.disconnect().await?;
    pool.disconnect().await?;

    Ok(())
}
