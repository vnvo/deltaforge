//! Row decoding on worker threads changes nothing a MySQL source emits.
//!
//! The same binlog is read twice from the same checkpoint, once with rows
//! decoded on the source task (`decode_workers: 0`) and once with four
//! decode workers:
//!
//! - every item between two marker transactions is identical, in the same
//!   order: events with their images, checkpoints and event ids,
//!   transaction markers. The workload has 1-row transactions, multi-row
//!   events larger than a decode batch, updates, deletes, JSON and NULL
//!   values. (A DDL inside the window is not part of it: a restart that
//!   resumes before a DDL fails closed by design, and DDL under decode
//!   workers is covered live by the DDL and activation suites, which run
//!   with workers by default);
//! - a row the source cannot decode (an ENUM index 0) stops both runs the
//!   same way: the same items before it, no event of that row or after it,
//!   and the same error; with workers, a DDL right behind that row is never
//!   recorded.
//!
//! ```bash
//! cargo test -p sources --test mysql_decode_pool_e2e -- --include-ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::canonical::{CanonicalValue, DbValue};
use deltaforge_core::{Source, SourceItem};
use mysql_async::prelude::Queryable;
use sources::mysql::MySqlSource;
use storage::ArcStorageBackend;
use storage::adapters::{LineageDescriptor, SchemaKey};
use tokio::time::timeout;

mod test_common;

const ID: &str = "decode_pool";
/// The marker rows: the window compared starts after the first one's
/// transaction and ends before the last one's.
const START: i64 = -1;
const END: i64 = -2;

async fn source(
    dsn: &str,
    db: &str,
    workers: usize,
    backend: &ArcStorageBackend,
) -> MySqlSource {
    source_of(dsn, vec![format!("{db}.t")], workers, backend).await
}

/// A source over `tables`, decoding with `workers` workers.
async fn source_of(
    dsn: &str,
    tables: Vec<String>,
    workers: usize,
    backend: &ArcStorageBackend,
) -> MySqlSource {
    let backend = backend.clone();
    MySqlSource {
        id: ID.into(),
        dsn: dsn.to_string().into(),
        tables,
        tenant: "acme".into(),
        pipeline: "decode".into(),
        registry: test_common::registry_on(&backend).await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        outbox_tables: AllowList::default(),
        // The first run snapshots the (empty) table, which records the
        // schema its rows are proven against; later runs resume from the
        // checkpoint and do not snapshot.
        snapshot_cfg: SnapshotCfg::default(),
        backend,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
        proofs: Default::default(),
        type_mapping: deltaforge_core::TypeMapping::V2,
        decode_workers: Some(workers),
    }
}

/// The state each run resumes from: a checkpoint at the current end of the
/// binlog, left by a first run that streams for a moment and stops before
/// the workload is written, and the storage (schema history) it kept. The
/// storage is shared, as a restarted source shares it: the second run finds
/// every record the first one derived already present.
struct Resume {
    backend: ArcStorageBackend,
    ckpt: Arc<dyn CheckpointStore>,
}

async fn checkpoint_here(dsn: &str, db: &str) -> Resume {
    let backend = test_common::make_storage_backend().await;
    let ckpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    let src = source(dsn, db, 0, &backend).await;
    let (tx, _rx) = test_common::acked_channel(&src, &ckpt, ID, 1024);
    let handle = src.run(tx, ckpt.clone()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    handle.stop();
    timeout(Duration::from_secs(30), handle.join())
        .await
        .expect("the first run stops")
        .expect("the first run stops cleanly");
    assert!(
        !ckpt.list().await.unwrap().is_empty(),
        "the first run left no checkpoint"
    );
    Resume { backend, ckpt }
}

/// A separate store holding every checkpoint of `from`, so each run starts
/// from the same state and none sees what another one stored.
async fn copy_of(from: &Arc<dyn CheckpointStore>) -> Arc<dyn CheckpointStore> {
    let to: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    for key in from.list().await.unwrap() {
        let bytes = from.get_raw(&key).await.unwrap().expect("listed key");
        to.put_raw(&key, &bytes).await.unwrap();
    }
    to
}

/// The `id` of an event's row.
fn id_of(item: &SourceItem) -> Option<i64> {
    let SourceItem::Event(e) = item else {
        return None;
    };
    if e.ddl.is_some() {
        return None;
    }
    match e.after.as_ref().or(e.before.as_ref())?.get("id")? {
        CanonicalValue::Db(DbValue::Int { value, .. }) => Some(*value),
        other => panic!("id is not an integer: {other:?}"),
    }
}

/// An item as text without its receive time, the one field that is the
/// wall clock of the run rather than the binlog.
fn comparable(item: &SourceItem) -> String {
    let text = format!("{item:?}");
    let mut out = String::with_capacity(text.len());
    let mut rest = text.as_str();
    while let Some(k) = rest.find("received_at_ms: ") {
        let (head, tail) = rest.split_at(k + "received_at_ms: ".len());
        out.push_str(head);
        out.push('_');
        rest = tail.trim_start_matches(|c: char| c.is_ascii_digit());
    }
    out.push_str(rest);
    out
}

/// What a run emitted: every item from the start marker's transaction
/// commit to the end marker's event (or to the source's end), as text, and
/// the error the source stopped with, if it stopped.
struct Emitted {
    items: Vec<String>,
    ids: Vec<i64>,
    error: Option<String>,
}

/// Run from a copy of `from`'s checkpoint (the same one for each run) until
/// the end marker arrives or the source stops.
async fn read(dsn: &str, db: &str, workers: usize, from: &Resume) -> Emitted {
    let ckpt = copy_of(&from.ckpt).await;
    let src = source(dsn, db, workers, &from.backend).await;
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, ID, 1024);
    let handle = src.run(tx, ckpt.clone()).await;
    let deadline = Instant::now() + Duration::from_secs(120);
    let (mut started, mut seen_start) = (false, false);
    let mut out = Emitted {
        items: Vec::new(),
        ids: Vec::new(),
        error: None,
    };
    loop {
        let left = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| panic!("{workers} workers: no end marker"));
        let Ok(next) = timeout(left, rx.recv()).await else {
            panic!("{workers} workers: no end marker in time")
        };
        let Some(item) = next else { break };
        let id = id_of(&item);
        if !started {
            seen_start |= id == Some(START);
            started = seen_start && matches!(item, SourceItem::TxCommit { .. });
            continue;
        }
        if id == Some(END) {
            handle.stop();
            break;
        }
        out.ids.extend(id);
        out.items.push(comparable(&item));
    }
    match timeout(Duration::from_secs(60), handle.join()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => out.error = Some(e.to_string()),
        Err(_) => panic!("{workers} workers: the source did not stop"),
    }
    assert!(
        started,
        "{workers} workers: the start marker never arrived ({:?})",
        out.error
    );
    out
}

async fn exec(conn: &mut mysql_async::Conn, stmts: &[String]) {
    for s in stmts {
        conn.query_drop(s.as_str())
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
}

/// The schema key of `{db}.t` under this server's lineage.
async fn table_key(conn: &mut mysql_async::Conn, db: &str) -> String {
    table_key_of(conn, db, "t").await
}

/// The schema key of `{db}.{table}` under this server's lineage.
async fn table_key_of(
    conn: &mut mysql_async::Conn,
    db: &str,
    table: &str,
) -> String {
    let uuid: String = conn
        .query_first("SELECT @@GLOBAL.server_uuid")
        .await
        .unwrap()
        .unwrap();
    let hash = LineageDescriptor::mysql(&uuid).unwrap().lineage_hash();
    SchemaKey::new("acme", ID, &hash, db, table).backend_key()
}

/// The table's activation records, as stored.
async fn activation(backend: &ArcStorageBackend, key: &str) -> Vec<Vec<u8>> {
    backend
        .log_list("schemas.v1.activation", key)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, bytes)| bytes)
        .collect()
}

fn marker(db: &str, id: i64) -> String {
    format!("INSERT INTO {db}.t (id, v) VALUES ({id}, 'marker')")
}

/// The same workload decoded inline and by four workers emits the same
/// items in the same order.
#[tokio::test]
#[ignore = "requires docker"]
async fn the_stream_is_the_same_with_decode_workers() -> Result<()> {
    let (db, pool, dsn) = test_common::mysql_setup("decode_pool_same").await?;
    let mut conn = pool.get_conn().await?;
    exec(
        &mut conn,
        &[format!(
            "CREATE TABLE {db}.t (id BIGINT PRIMARY KEY, v VARCHAR(64), \
         j JSON, d DECIMAL(12,2), e ENUM('a','b'))"
        )],
    )
    .await;
    let ckpt = checkpoint_here(&dsn, &db).await;

    let mut w = vec![marker(&db, START)];
    // 1-row transactions.
    for i in 0..1_000 {
        w.push(format!(
            "INSERT INTO {db}.t VALUES ({i}, 'v{i}', '{{\"k\": {i}, \"s\": \"x\\\\\"y\"}}', {i}.25, 'a')"
        ));
    }
    // One rows event larger than a decode batch, NULLs included.
    let many: Vec<String> = (1_000..1_700)
        .map(|i| {
            if i % 7 == 0 {
                format!("({i}, NULL, NULL, NULL, NULL)")
            } else {
                format!("({i}, 'm{i}', '[{i}, null, true]', -{i}.5, 'b')")
            }
        })
        .collect();
    w.push(format!("INSERT INTO {db}.t VALUES {}", many.join(", ")));
    // Multi-row update and delete events, then more 1-row transactions and
    // one transaction of many statements.
    w.push(format!(
        "UPDATE {db}.t SET v = CONCAT(v, '!'), e = 'b' WHERE id BETWEEN 100 AND 499"
    ));
    w.push(format!("DELETE FROM {db}.t WHERE id BETWEEN 1200 AND 1499"));
    for i in 2_000..2_300 {
        w.push(format!(
            "INSERT INTO {db}.t (id, v, d) VALUES ({i}, 'later', {i})"
        ));
    }
    w.push("START TRANSACTION".into());
    for i in 2_300..2_310 {
        w.push(format!("INSERT INTO {db}.t (id, v) VALUES ({i}, 'txn')"));
    }
    w.push(format!(
        "UPDATE {db}.t SET d = d + 1 WHERE id BETWEEN 2000 AND 2009"
    ));
    w.push("COMMIT".into());
    w.push(marker(&db, END));
    exec(&mut conn, &w).await;

    let inline = read(&dsn, &db, 0, &ckpt).await;
    let workers = read(&dsn, &db, 4, &ckpt).await;
    assert!(inline.error.is_none(), "{:?}", inline.error);
    assert!(workers.error.is_none(), "{:?}", workers.error);
    // Inserts 0..1700 and 2000..2310, 400 updates, 300 deletes, 10 updates.
    assert_eq!(inline.ids.len(), 1_700 + 310 + 400 + 300 + 10);
    assert_eq!(inline.items.len(), workers.items.len());
    for (k, (a, b)) in inline.items.iter().zip(&workers.items).enumerate() {
        assert_eq!(a, b, "item {k} differs");
    }

    test_common::mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// A row that cannot be decoded stops the source the same way inline and
/// with workers: the same items before it, nothing of it or after it, the
/// same error.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_decode_failure_stops_both_the_same_way() -> Result<()> {
    let (db, pool, dsn) = test_common::mysql_setup("decode_pool_fail").await?;
    let mut conn = pool.get_conn().await?;
    exec(
        &mut conn,
        &[format!(
            "CREATE TABLE {db}.t (id BIGINT PRIMARY KEY, v VARCHAR(64), \
         e ENUM('a','b'))"
        )],
    )
    .await;
    let ckpt = checkpoint_here(&dsn, &db).await;

    const BAD: i64 = 600;
    let mut w = vec![marker(&db, START)];
    for i in 0..300 {
        w.push(format!("INSERT INTO {db}.t VALUES ({i}, 'v{i}', 'a')"));
    }
    let many: Vec<String> =
        (300..BAD).map(|i| format!("({i}, 'm{i}', 'b')")).collect();
    w.push(format!("INSERT INTO {db}.t VALUES {}", many.join(", ")));
    // An invalid ENUM value is stored as index 0 in a permissive session.
    w.push("SET SESSION sql_mode = ''".into());
    w.push(format!("INSERT INTO {db}.t VALUES ({BAD}, 'bad', 'zzz')"));
    w.push("SET SESSION sql_mode = DEFAULT".into());
    for i in BAD + 1..BAD + 300 {
        w.push(format!(
            "INSERT INTO {db}.t (id, v, e) VALUES ({i}, 'later', 'a')"
        ));
    }
    w.push(marker(&db, END));
    exec(&mut conn, &w).await;

    let inline = read(&dsn, &db, 0, &ckpt).await;
    let workers = read(&dsn, &db, 4, &ckpt).await;
    let error = inline.error.clone().expect("the inline run stops");
    assert!(error.contains("value_not_decodable"), "{error}");
    assert_eq!(workers.error.as_deref(), Some(error.as_str()));
    assert_eq!(inline.ids, (0..BAD).collect::<Vec<_>>());
    assert_eq!(inline.items, workers.items);

    test_common::mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// With workers, a DDL right behind a row that cannot be decoded is never
/// recorded: the source fails at the row, as without workers, before the
/// statement's activation records are written. The row ends a transaction
/// of many rows and the DDL follows its commit at once, so the row is still
/// with the workers when the reader hands over the DDL.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_ddl_behind_an_undecodable_row_is_never_recorded() -> Result<()> {
    let (db, pool, dsn) = test_common::mysql_setup("decode_pool_ddl").await?;
    let mut conn = pool.get_conn().await?;
    exec(
        &mut conn,
        &[format!(
            "CREATE TABLE {db}.t (id BIGINT PRIMARY KEY, v VARCHAR(64), \
             e ENUM('a','b'))"
        )],
    )
    .await;
    let key = table_key(&mut conn, &db).await;
    let backend = test_common::make_storage_backend().await;
    let ckpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    let src = source(&dsn, &db, 4, &backend).await;
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, ID, 1024);
    let handle = src.run(tx, ckpt.clone()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    // The table's first row, delivered: its schema is proven before the
    // DDL changes it (a table first seen behind a DDL fails closed).
    exec(&mut conn, &[marker(&db, START)]).await;
    loop {
        let item = timeout(Duration::from_secs(60), rx.recv())
            .await
            .expect("the first row arrives")
            .expect("the source runs");
        if id_of(&item) == Some(START) {
            break;
        }
    }
    let recorded = activation(&backend, &key).await;

    const ROWS: i64 = 100_000;
    exec(
        &mut conn,
        &[
            "SET SESSION cte_max_recursion_depth = 1000000".into(),
            "START TRANSACTION".into(),
            format!(
                "INSERT INTO {db}.t (id, v, e) WITH RECURSIVE s(n) AS \
                 (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < {}) \
                 SELECT n, CONCAT('v', n), 'a' FROM s",
                ROWS - 1
            ),
            "SET SESSION sql_mode = ''".into(),
            format!("INSERT INTO {db}.t VALUES ({ROWS}, 'bad', 'zzz')"),
            "COMMIT".into(),
            format!("ALTER TABLE {db}.t ADD COLUMN x INT NULL"),
            "SET SESSION sql_mode = DEFAULT".into(),
        ],
    )
    .await;

    let mut last = None;
    while let Ok(Some(item)) = timeout(Duration::from_secs(60), rx.recv()).await
    {
        last = id_of(&item).or(last);
    }
    let error = timeout(Duration::from_secs(60), handle.join())
        .await
        .expect("the source stops")
        .expect_err("the source fails at the row");
    assert!(error.to_string().contains("value_not_decodable"), "{error}");
    assert_eq!(last, Some(ROWS - 1), "every row before the failing one");
    assert_eq!(
        activation(&backend, &key).await,
        recorded,
        "a record was written past the row that could not be decoded"
    );

    test_common::mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// A table first seen behind a row that cannot be decoded is never
/// admitted: its window waits for the rows before it to be decoded, so the
/// source fails at the row and writes no admission past it.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_window_behind_an_undecodable_row_is_never_recorded() -> Result<()> {
    let (db, pool, dsn) =
        test_common::mysql_setup("decode_pool_window").await?;
    let mut conn = pool.get_conn().await?;
    exec(
        &mut conn,
        &[
            format!(
                "CREATE TABLE {db}.t (id BIGINT PRIMARY KEY, v VARCHAR(64), \
                 e ENUM('a','b'))"
            ),
            format!("CREATE TABLE {db}.u (id BIGINT PRIMARY KEY)"),
        ],
    )
    .await;
    let key = table_key_of(&mut conn, &db, "u").await;
    let backend = test_common::make_storage_backend().await;
    let ckpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    let tables = vec![format!("{db}.t"), format!("{db}.u")];
    let src = source_of(&dsn, tables, 4, &backend).await;
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, ID, 1024);
    let handle = src.run(tx, ckpt.clone()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    exec(&mut conn, &[marker(&db, START)]).await;
    loop {
        let item = timeout(Duration::from_secs(60), rx.recv())
            .await
            .expect("the first row arrives")
            .expect("the source runs");
        if id_of(&item) == Some(START) {
            break;
        }
    }
    let recorded = activation(&backend, &key).await;

    const ROWS: i64 = 100_000;
    exec(
        &mut conn,
        &[
            "SET SESSION cte_max_recursion_depth = 1000000".into(),
            "START TRANSACTION".into(),
            format!(
                "INSERT INTO {db}.t (id, v, e) WITH RECURSIVE s(n) AS \
                 (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < {}) \
                 SELECT n, CONCAT('v', n), 'a' FROM s",
                ROWS - 1
            ),
            "SET SESSION sql_mode = ''".into(),
            format!("INSERT INTO {db}.t VALUES ({ROWS}, 'bad', 'zzz')"),
            "COMMIT".into(),
            "SET SESSION sql_mode = DEFAULT".into(),
            format!("INSERT INTO {db}.u VALUES (1)"),
        ],
    )
    .await;

    let mut last = None;
    while let Ok(Some(item)) = timeout(Duration::from_secs(60), rx.recv()).await
    {
        last = id_of(&item).or(last);
    }
    let error = timeout(Duration::from_secs(60), handle.join())
        .await
        .expect("the source stops")
        .expect_err("the source fails at the row");
    assert!(error.to_string().contains("value_not_decodable"), "{error}");
    assert_eq!(last, Some(ROWS - 1), "every row before the failing one");
    assert_eq!(
        activation(&backend, &key).await,
        recorded,
        "an admission was written past the row that could not be decoded"
    );

    test_common::mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// A window full at its head (one compressed transaction first touching as
/// many tables as a window admits) closes without reading ahead; its
/// admission still waits for the rows before it to be decoded, so behind a
/// row that cannot be decoded no table is admitted.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_full_window_behind_an_undecodable_row_is_never_recorded()
-> Result<()> {
    /// As many tables as one window admits (`MAX_BATCH_TABLES`).
    const TABLES: usize = 2048;
    let (db, pool, dsn) =
        test_common::mysql_setup("decode_pool_full_window").await?;
    let mut conn = pool.get_conn().await?;
    exec(
        &mut conn,
        &[format!(
            "CREATE TABLE {db}.t (id BIGINT PRIMARY KEY, v VARCHAR(64), \
             e ENUM('a','b'))"
        )],
    )
    .await;
    let names: Vec<String> = (0..TABLES).map(|i| format!("u{i:04}")).collect();
    for chunk in names.chunks(256) {
        let ddl: Vec<String> = chunk
            .iter()
            .map(|n| format!("CREATE TABLE {db}.{n} (id BIGINT PRIMARY KEY)"))
            .collect();
        conn.query_drop(ddl.join(";\n")).await?;
    }
    let mut keys = Vec::with_capacity(TABLES);
    for n in &names {
        keys.push(table_key_of(&mut conn, &db, n).await);
    }
    let backend = test_common::make_storage_backend().await;
    let ckpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    let tables = vec![format!("{db}.t"), format!("{db}.u*")];
    let src = source_of(&dsn, tables, 4, &backend).await;
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, ID, 1024);
    let handle = src.run(tx, ckpt.clone()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    exec(&mut conn, &[marker(&db, START)]).await;
    loop {
        let item = timeout(Duration::from_secs(120), rx.recv())
            .await
            .expect("the first row arrives")
            .expect("the source runs");
        if id_of(&item) == Some(START) {
            break;
        }
    }
    for k in &keys {
        assert!(activation(&backend, k).await.is_empty(), "{k} admitted");
    }

    const ROWS: i64 = 100_000;
    exec(
        &mut conn,
        &[
            "SET SESSION cte_max_recursion_depth = 1000000".into(),
            "START TRANSACTION".into(),
            format!(
                "INSERT INTO {db}.t (id, v, e) WITH RECURSIVE s(n) AS \
                 (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < {}) \
                 SELECT n, CONCAT('v', n), 'a' FROM s",
                ROWS - 1
            ),
            "SET SESSION sql_mode = ''".into(),
            format!("INSERT INTO {db}.t VALUES ({ROWS}, 'bad', 'zzz')"),
            "COMMIT".into(),
            "SET SESSION sql_mode = DEFAULT".into(),
        ],
    )
    .await;
    // One compressed transaction: a single payload event, the window's
    // head, carrying every table's first row.
    let mut txn = vec![
        "SET SESSION binlog_transaction_compression = ON".to_string(),
        "BEGIN".into(),
    ];
    txn.extend(
        names
            .iter()
            .map(|n| format!("INSERT INTO {db}.{n} VALUES (1)")),
    );
    txn.push("COMMIT".into());
    exec(&mut conn, &txn).await;

    let mut last = None;
    while let Ok(Some(item)) = timeout(Duration::from_secs(60), rx.recv()).await
    {
        last = id_of(&item).or(last);
    }
    let error = timeout(Duration::from_secs(120), handle.join())
        .await
        .expect("the source stops")
        .expect_err("the source fails at the row");
    assert!(error.to_string().contains("value_not_decodable"), "{error}");
    assert_eq!(last, Some(ROWS - 1), "every row before the failing one");
    for k in &keys {
        assert!(
            activation(&backend, k).await.is_empty(),
            "{k}: an admission was written past the row that could not be \
             decoded"
        );
    }

    test_common::mysql_drop_db(&pool, &db).await;
    Ok(())
}
