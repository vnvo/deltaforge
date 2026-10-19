//! Live ClickHouse sink integration tests.
//!
//! Requires Docker; pulls `clickhouse/clickhouse-server`. Run:
//!   cargo test -p sinks --test clickhouse_sink_tests -- --ignored --test-threads=1

use std::sync::Arc;
use std::time::{Duration, Instant};

use deltaforge_config::{ChMode, ChVersionSource, ClickHouseSinkCfg};
use deltaforge_core::canonical::{
    CanonicalRow, CanonicalValue, DbValue, Decimal, IntWidth,
};
use deltaforge_core::{
    Event, Op, Sink, SourceInfo, SourcePosition, TypeMapping,
};
use gate_ownership::GateOwned;
use serde_json::{Value, json};
use sinks::clickhouse::types::ColDesc;
use sinks::clickhouse::{
    ClickHouseSchemaResolver, TableColumns, build_clickhouse_sink,
};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio_util::sync::CancellationToken;

const CH_HTTP: u16 = 8123;
const CH_PASSWORD: &str = "chtest";

/// Columns for a test `orders` table: `id BIGINT PK`, `amount DECIMAL(12,2)`.
fn resolver() -> ClickHouseSchemaResolver {
    Arc::new(|_key: &str| Ok(fixture_columns()))
}

fn fixture_columns() -> Option<TableColumns> {
    Some(TableColumns {
        columns: vec![
            ColDesc {
                name: "id".into(),
                data_type: "bigint".into(),
                full_type: "bigint".into(),
                nullable: false,
                unsigned: false,
                precision: None,
                scale: None,
            },
            ColDesc {
                name: "amount".into(),
                data_type: "decimal".into(),
                full_type: "decimal(12,2)".into(),
                nullable: true,
                unsigned: false,
                // Mirror the real MySQL schema loader, which leaves
                // numeric_precision/scale unset and only carries the type
                // string — so this exercises the parse-from-`full_type` path
                // (the decimal(38,0) default bug lived here).
                precision: None,
                scale: None,
            },
        ],
        primary_key: vec!["id".into()],
    })
}

fn cfg(
    url: &str,
    table: &str,
    mode: ChMode,
    auto_create: bool,
) -> ClickHouseSinkCfg {
    ClickHouseSinkCfg {
        extensions_column: "_extensions".into(),
        id: "ch-test".into(),
        url: url.into(),
        database: "default".into(),
        table: table.into(),
        mode,
        user: Some("default".into()),
        password: Some(CH_PASSWORD.into()),
        user_ref: None,
        password_ref: None,
        tls: None,
        version_source: ChVersionSource::TsMs,
        send_timeout_secs: 30,
        required: Some(true),
        auto_create,
    }
}

/// A typed `orders` row from a JSON literal: `id` is a BIGINT, `amount` a
/// DECIMAL given as its exact text (or null).
fn orders_row(v: Value) -> Option<CanonicalRow> {
    let Value::Object(m) = v else {
        return None;
    };
    let cols = m
        .into_iter()
        .map(|(k, v)| {
            let value = match (k.as_str(), v) {
                (_, Value::Null) => CanonicalValue::Null,
                ("id", Value::Number(n)) => CanonicalValue::Db(
                    DbValue::int(n.as_i64().unwrap(), IntWidth::W64).unwrap(),
                ),
                ("amount", Value::String(s)) => CanonicalValue::Db(
                    DbValue::Decimal(Decimal::parse(&s).unwrap()),
                ),
                (k, v) => panic!("no orders column {k} = {v}"),
            };
            (k, value)
        })
        .collect();
    Some(CanonicalRow::new(cols).unwrap())
}

fn mk_event(
    op: Op,
    after: Value,
    before: Value,
    table: &str,
    ts: i64,
) -> Event {
    Event {
        before: orders_row(before),
        after: orders_row(after),
        source: SourceInfo {
            version: "1".into(),
            connector: "mysql".into(),
            name: "t".into(),
            ts_ms: ts,
            db: "shop".into(),
            schema: None,
            table: table.into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        op,
        ts_ms: ts,
        transaction: None,
        type_mapping: TypeMapping::V2,
        event_id: None,
        tenant_id: None,
        schema_version: None,
        schema_sequence: None,
        ddl: None,
        trace_id: None,
        tags: None,
        synthetic: None,
        routing: None,
        tx_end: false,
        boundary: None,
        size_bytes: 0,

        received_at_ms: 0,
    }
}

/// Run a SQL statement over the ClickHouse HTTP interface, returning the body.
async fn ch(base: &str, sql: &str) -> String {
    // Send the SQL in the body (non-empty → Content-Length set). Query-in-URL
    // with an empty body returns HTTP 411 on this ClickHouse build.
    let r = reqwest::Client::new()
        .post(base)
        .header("X-ClickHouse-User", "default")
        .header("X-ClickHouse-Key", CH_PASSWORD)
        .body(sql.to_string())
        .send()
        .await
        .expect("clickhouse http request");
    let status = r.status();
    let body = r.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "clickhouse error ({status}): {body}\nsql: {sql}"
    );
    body
}

async fn wait_ready(base: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(r) = reqwest::get(format!("{base}/ping")).await {
            if r.status().is_success() {
                return;
            }
        }
        if Instant::now() > deadline {
            panic!("clickhouse not ready after 60s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn start_clickhouse()
-> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("clickhouse/clickhouse-server", "24.8")
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(3),
        })
        .with_mapped_port(0, CH_HTTP.tcp())
        .with_env_var("CLICKHOUSE_PASSWORD", CH_PASSWORD)
        // SQL-driven users and settings profiles (the async-insert test).
        .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .gate_owned()
        .start()
        .await
        .expect("start clickhouse container");
    let port = container.get_host_port_ipv4(CH_HTTP).await.unwrap();
    let base = format!("http://localhost:{port}");
    wait_ready(&base).await;
    (container, base)
}

#[tokio::test]
#[ignore]
async fn upsert_mode_auto_creates_and_reflects_current_state() {
    let (_c, base) = start_clickhouse().await;
    let table = "orders_upsert";
    let sink = build_clickhouse_sink(
        &cfg(&base, table, ChMode::Upsert, true),
        CancellationToken::new(),
        "p",
        Some(resolver()),
        &sinks::SinkOwnership::ephemeral(),
        &sinks::ResolvedSinkCreds::default(),
    )
    .unwrap();

    // insert id=1 amount=10.00 (v=1); update id=1 -> 20.00 (v=2); delete id=2 (v=3)
    let batch = vec![
        mk_event(
            Op::Create,
            json!({"id": 1, "amount": "10.00"}),
            json!(null),
            table,
            1,
        ),
        mk_event(
            Op::Update,
            // fractional value so a scale-0 mismap (the Decimal(38,0) default
            // bug) would truncate 20.50 → 20 and fail the assertion below.
            json!({"id": 1, "amount": "20.50"}),
            json!(null),
            table,
            2,
        ),
        mk_event(
            Op::Delete,
            json!(null),
            json!({"id": 2, "amount": null}),
            table,
            3,
        ),
    ];
    let res = sink.send_batch(&batch).await.unwrap();
    // Nothing stays buffered behind an acknowledged batch: the checkpoint
    // barrier acknowledges.
    sink.barrier(
        &deltaforge_core::BarrierKind::Terminal,
        &deltaforge_core::SinkBatchContext {
            checkpoint: deltaforge_core::CheckpointMeta::from_vec(
                b"cp".to_vec(),
            ),
            durable_watermark: None,
            batch_id: None,
        },
    )
    .await
    .unwrap();
    assert!(
        res.dlq_failures.is_empty(),
        "no per-row failures: {:?}",
        res.dlq_failures
    );

    let out = ch(
        &base,
        &format!("SELECT id, toFloat64(amount) FROM default.{table} FINAL ORDER BY id FORMAT TSV"),
    )
    .await;
    // Only id=1 at amount 20.00 survives; the delete of never-seen id=2 collapses.
    let lines: Vec<&str> = out.trim().lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "expected one current-state row, got {out:?}"
    );
    let cols: Vec<&str> = lines[0].split('\t').collect();
    assert_eq!(cols[0], "1", "current-state id, got {out:?}");
    assert!(
        (cols[1].parse::<f64>().unwrap() - 20.50).abs() < 1e-9,
        "current-state amount should be 20.50 (exact decimal scale), got {out:?}"
    );
}

#[tokio::test]
#[ignore]
async fn changelog_mode_retains_all_changes() {
    let (_c, base) = start_clickhouse().await;
    let table = "orders_log";
    let sink = build_clickhouse_sink(
        &cfg(&base, table, ChMode::Changelog, true),
        CancellationToken::new(),
        "p",
        Some(resolver()),
        &sinks::SinkOwnership::ephemeral(),
        &sinks::ResolvedSinkCreds::default(),
    )
    .unwrap();

    let batch = vec![
        mk_event(
            Op::Create,
            json!({"id": 1, "amount": "10.00"}),
            json!(null),
            table,
            1,
        ),
        mk_event(
            Op::Update,
            json!({"id": 1, "amount": "20.00"}),
            json!(null),
            table,
            2,
        ),
        mk_event(
            Op::Delete,
            json!(null),
            json!({"id": 1, "amount": null}),
            table,
            3,
        ),
    ];
    sink.send_batch(&batch).await.unwrap();

    let n = ch(
        &base,
        &format!("SELECT count() FROM default.{table} FORMAT TSV"),
    )
    .await;
    assert_eq!(n.trim(), "3", "change-log retains all 3 changes");
    let ops = ch(
        &base,
        &format!(
            "SELECT _op FROM default.{table} ORDER BY _version FORMAT TSV"
        ),
    )
    .await;
    assert_eq!(
        ops.split_whitespace().collect::<Vec<_>>(),
        vec!["c", "u", "d"]
    );
}

#[tokio::test]
#[ignore]
async fn dedup_token_prevents_double_insert() {
    let (_c, base) = start_clickhouse().await;
    let table = "orders_dedup";
    // The sink creates the table (an unrelated first row), then
    // non-replicated deduplication is enabled on it.
    let sink = build_clickhouse_sink(
        &cfg(&base, table, ChMode::Changelog, true),
        CancellationToken::new(),
        "p",
        Some(resolver()),
        &sinks::SinkOwnership::ephemeral(),
        &sinks::ResolvedSinkCreds::default(),
    )
    .unwrap();
    sink.send_batch(&[mk_event(
        Op::Create,
        json!({"id": 0, "amount": "0.00"}),
        json!(null),
        table,
        0,
    )])
    .await
    .unwrap();
    ch(
        &base,
        &format!(
            "ALTER TABLE default.{table} MODIFY SETTING non_replicated_deduplication_window = 1000"
        ),
    )
    .await;

    let batch = vec![
        mk_event(
            Op::Create,
            json!({"id": 1, "amount": "10.00"}),
            json!(null),
            table,
            1,
        ),
        mk_event(
            Op::Create,
            json!({"id": 2, "amount": "11.00"}),
            json!(null),
            table,
            2,
        ),
    ];
    // Send the SAME batch twice — the deterministic dedup token must suppress the replay.
    sink.send_batch(&batch).await.unwrap();
    sink.send_batch(&batch).await.unwrap();

    let n = ch(
        &base,
        &format!("SELECT count() FROM default.{table} FORMAT TSV"),
    )
    .await;
    assert_eq!(
        n.trim(),
        "3",
        "dedup token prevented the replayed batch from doubling rows"
    );
}

/// Run a statement; its status and body, whatever the status.
async fn ch_raw(base: &str, user: &str, key: &str, sql: &str) -> (u16, String) {
    let r = reqwest::Client::new()
        .post(base)
        .header("X-ClickHouse-User", user)
        .header("X-ClickHouse-Key", key)
        .body(sql.to_string())
        .send()
        .await
        .expect("clickhouse http request");
    (r.status().as_u16(), r.text().await.unwrap_or_default())
}

fn class(e: &deltaforge_core::SinkError) -> Option<String> {
    e.draft()
        .and_then(|d| d.discriminator.get("class").cloned())
}

fn ch_sink(
    base: &str,
    database: &str,
    table: &str,
    o: &sinks::SinkOwnership,
) -> sinks::clickhouse::sink::ClickHouseSink {
    let mut c = cfg(base, table, ChMode::Changelog, true);
    c.database = database.into();
    build_clickhouse_sink(
        &c,
        CancellationToken::new(),
        "p",
        Some(resolver()),
        o,
        &sinks::ResolvedSinkCreds::default(),
    )
    .unwrap()
}

fn row(source_table: &str, id: i64) -> Event {
    mk_event(
        Op::Create,
        json!({"id": id, "amount": "1.50"}),
        json!(null),
        source_table,
        id,
    )
}

/// Ownership against a live server: a restart of the owner writes; another
/// relation, another deployment and a table the sink did not create are
/// refused before inserting; a dropped table is never recreated, and one
/// recreated by another deployment does not transfer ownership.
#[tokio::test]
#[ignore]
async fn ownership_holds_across_restarts_and_refuses_others() {
    let (_c, base) = start_clickhouse().await;
    let table = "orders_owned";
    let ownership = sinks::SinkOwnership::ephemeral();
    let sink = |o: &sinks::SinkOwnership, table: &str| {
        ch_sink(&base, "default", table, o)
    };
    sink(&ownership, table)
        .send_batch(&[row("orders", 1)])
        .await
        .unwrap();
    sink(&ownership, table)
        .send_batch(&[row("orders", 2)])
        .await
        .unwrap();
    let count = |t: &str| {
        let (base, sql) =
            (base.clone(), format!("SELECT count() FROM default.{t}"));
        async move { ch(&base, &sql).await }
    };
    assert_eq!(count(table).await.trim(), "2");

    let e = sink(&ownership, table)
        .send_batch(&[row("orders_v2", 3)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("bound_to_another_relation"));
    assert!(!format!("{e:?}").contains("1.50"), "no row data");
    let e = sink(&sinks::SinkOwnership::ephemeral(), table)
        .send_batch(&[row("orders", 3)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("owned_by_other"));
    assert_eq!(count(table).await.trim(), "2", "nothing inserted");

    ch(&base, "CREATE TABLE default.user_table (id Int64) ENGINE = MergeTree ORDER BY id").await;
    let e = sink(&ownership, "user_table")
        .send_batch(&[row("orders", 1)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("marker_missing"));

    ch(&base, &format!("DROP TABLE default.{table} SYNC")).await;
    let e = sink(&ownership, table)
        .send_batch(&[row("orders", 4)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("destination_missing"));
    let (_, exists) = ch_raw(
        &base,
        "default",
        CH_PASSWORD,
        &format!("EXISTS TABLE default.{table}"),
    )
    .await;
    assert_eq!(exists.trim(), "0", "never recreated");
    sink(&sinks::SinkOwnership::ephemeral(), table)
        .send_batch(&[row("orders", 5)])
        .await
        .unwrap();
    let e = sink(&ownership, table)
        .send_batch(&[row("orders", 6)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("owned_by_other"));
}

/// A copy of the state store claiming the table fences the original: its
/// very next insert fails at the server (the fence constraint), and so does
/// its next claim. Rows of the fenced writer after the rotation never land.
#[tokio::test]
#[ignore]
async fn a_copied_store_fences_the_original_at_its_next_insert() {
    let (_c, base) = start_clickhouse().await;
    let table = "orders_clone";
    let store = std::sync::Arc::new(
        deltaforge_core::ownership::MemoryOwnershipStore::default(),
    );
    let original = sinks::SinkOwnership {
        deployment: deltaforge_core::ownership::random_token(),
        store: store.clone(),
    };
    let a = ch_sink(&base, "default", table, &original);
    a.send_batch(&[row("orders", 1)]).await.unwrap();
    let copy = sinks::SinkOwnership {
        deployment: original.deployment.clone(),
        store: std::sync::Arc::new(store.copy()),
    };
    let b = ch_sink(&base, "default", table, &copy);
    b.send_batch(&[row("orders", 2)]).await.unwrap();
    let e = a.send_batch(&[row("orders", 3)]).await.unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("fenced"), "{e:?}");
    let e = ch_sink(&base, "default", table, &original)
        .send_batch(&[row("orders", 4)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("fenced"));
    b.send_batch(&[row("orders", 5)]).await.unwrap();
    let ids = ch(
        &base,
        &format!("SELECT id FROM default.{table} ORDER BY id FORMAT TSV"),
    )
    .await;
    assert_eq!(ids.split_whitespace().collect::<Vec<_>>(), ["1", "2", "5"]);
}

/// The allowed overlap: an insert the server already accepted may complete
/// while the fence rotates; an insert submitted after the rotation
/// committed is rejected.
#[tokio::test]
#[ignore]
async fn an_insert_in_flight_during_rotation_may_complete_and_the_next_fails() {
    let (_c, base) = start_clickhouse().await;
    let table = "orders_inflight";
    let store = std::sync::Arc::new(
        deltaforge_core::ownership::MemoryOwnershipStore::default(),
    );
    let original = sinks::SinkOwnership {
        deployment: deltaforge_core::ownership::random_token(),
        store: store.clone(),
    };
    let a = std::sync::Arc::new(ch_sink(&base, "default", table, &original));
    a.send_batch(&[row("orders", 0)]).await.unwrap();
    let big: Vec<Event> = (1..=200_000).map(|i| row("orders", i)).collect();
    let a2 = a.clone();
    let inflight = tokio::spawn(async move { a2.send_batch(&big).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let copy = sinks::SinkOwnership {
        deployment: original.deployment.clone(),
        store: std::sync::Arc::new(store.copy()),
    };
    ch_sink(&base, "default", table, &copy)
        .send_batch(&[row("orders", 999_999)])
        .await
        .unwrap();
    match inflight.await.unwrap() {
        Ok(_) => eprintln!(
            "in-flight insert completed before or across the rotation"
        ),
        Err(e) => assert_eq!(class(&e).as_deref(), Some("fenced"), "{e:?}"),
    }
    let e = a.send_batch(&[row("orders", 1_000_000)]).await.unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("fenced"));
}

/// Unsupported topologies are refused: a `Distributed` table, and a server
/// on which synchronous inserts cannot be forced. A profile that merely
/// enables `async_insert` is overridden by the sink's `async_insert=0`.
#[tokio::test]
#[ignore]
async fn unsupported_topologies_and_forced_async_inserts_are_refused() {
    let (_c, base) = start_clickhouse().await;
    let ownership = sinks::SinkOwnership::ephemeral();
    ch(&base, "CREATE TABLE default.local_t (id Int64) ENGINE = MergeTree ORDER BY id").await;
    let mut unsupported = vec![
        (
            "mem_t",
            "CREATE TABLE default.mem_t (id Int64) ENGINE = Memory".to_string(),
        ),
        (
            "log_t",
            "CREATE TABLE default.log_t (id Int64) ENGINE = Log".to_string(),
        ),
    ];
    // A Distributed table over a cluster the server defines.
    let cluster = ch(
        &base,
        "SELECT cluster FROM system.clusters LIMIT 1 FORMAT TSV",
    )
    .await;
    if !cluster.trim().is_empty() {
        unsupported.push((
            "dist_t",
            format!(
                "CREATE TABLE default.dist_t (id Int64) ENGINE = Distributed('{}', default, local_t)",
                cluster.trim()
            ),
        ));
    }
    for (table, ddl) in unsupported {
        ch(&base, &ddl).await;
        let e = ch_sink(&base, "default", table, &ownership)
            .send_batch(&[row("orders", 1)])
            .await
            .unwrap_err();
        assert_eq!(
            class(&e).as_deref(),
            Some("unsupported_topology"),
            "{table}"
        );
    }

    for (profile, constant, refused) in
        [("async_on", "", false), ("async_const", " CONST", true)]
    {
        ch(
            &base,
            &format!("CREATE SETTINGS PROFILE {profile} SETTINGS async_insert = 1{constant}"),
        )
        .await;
        let user = format!("u_{profile}");
        ch(&base, &format!("CREATE USER {user} IDENTIFIED BY 'pw' SETTINGS PROFILE {profile}")).await;
        ch(
            &base,
            &format!("GRANT SELECT, INSERT, CREATE TABLE, ALTER TABLE ON default.* TO {user}"),
        )
        .await;
        let mut c =
            cfg(&base, &format!("t_{profile}"), ChMode::Changelog, true);
        c.user = Some(user.clone());
        c.password = Some("pw".into());
        let sink = build_clickhouse_sink(
            &c,
            CancellationToken::new(),
            "p",
            Some(resolver()),
            &ownership,
            &sinks::ResolvedSinkCreds::default(),
        )
        .unwrap();
        let r = sink.send_batch(&[row("orders", 1)]).await;
        if refused {
            assert_eq!(
                class(&r.unwrap_err()).as_deref(),
                Some("fencing_unenforced")
            );
        } else {
            r.unwrap();
            // Written synchronously: visible at once.
            let n =
                ch(&base, &format!("SELECT count() FROM default.t_{profile}"))
                    .await;
            assert_eq!(n.trim(), "1");
        }
    }
}

/// Database and table names that need quoting work end to end.
#[tokio::test]
#[ignore]
async fn names_that_need_quoting_are_created_and_written() {
    let (_c, base) = start_clickhouse().await;
    let db = "we`ird db";
    let table = "or.ders 'x' ü";
    let q =
        |s: &str| format!("`{}`", s.replace('\\', "\\\\").replace('`', "\\`"));
    ch(&base, &format!("CREATE DATABASE {}", q(db))).await;
    let ownership = sinks::SinkOwnership::ephemeral();
    ch_sink(&base, db, table, &ownership)
        .send_batch(&[row("orders", 1)])
        .await
        .unwrap();
    ch_sink(&base, db, table, &ownership)
        .send_batch(&[row("orders", 2)])
        .await
        .unwrap();
    let n = ch(
        &base,
        &format!("SELECT count() FROM {}.{}", q(db), q(table)),
    )
    .await;
    assert_eq!(n.trim(), "2");
}

/// A damaged enforcement shape is refused at the next claim.
#[tokio::test]
#[ignore]
async fn a_malformed_fence_constraint_or_column_is_refused() {
    let (_c, base) = start_clickhouse().await;
    for (table, damage) in [
        (
            "orders_nocons",
            "ALTER TABLE default.orders_nocons DROP CONSTRAINT {c}",
        ),
        (
            "orders_extra",
            "ALTER TABLE default.orders_extra ADD COLUMN _df_x String",
        ),
    ] {
        let ownership = sinks::SinkOwnership::ephemeral();
        ch_sink(&base, "default", table, &ownership)
            .send_batch(&[row("orders", 1)])
            .await
            .unwrap();
        let create = ch(
            &base,
            &format!("SELECT create_table_query FROM system.tables WHERE name = '{table}' FORMAT TSVRaw"),
        )
        .await;
        let c = &create[create.find("df_fence_").unwrap()..][..41];
        ch(&base, &damage.replace("{c}", c)).await;
        let e = ch_sink(&base, "default", table, &ownership)
            .send_batch(&[row("orders", 2)])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("fence_malformed"), "{table}");
    }
}

/// Claim and steady-state costs against a live server (printed).
#[tokio::test]
#[ignore]
async fn measure_claim_and_steady_state_costs() {
    let (_c, base) = start_clickhouse().await;
    let table = "orders_measure";
    let ownership = sinks::SinkOwnership::ephemeral();
    let t = Instant::now();
    let s = ch_sink(&base, "default", table, &ownership);
    s.send_batch(&[row("orders", 1)]).await.unwrap();
    let first = t.elapsed();
    let t = Instant::now();
    ch_sink(&base, "default", table, &ownership)
        .send_batch(&[row("orders", 2)])
        .await
        .unwrap();
    let restart = t.elapsed();
    let s = ch_sink(&base, "default", table, &ownership);
    s.send_batch(&[row("orders", 3)]).await.unwrap();
    let batches = 50;
    let per = 5000;
    let t = Instant::now();
    for b in 0..batches {
        let batch: Vec<Event> = (0..per)
            .map(|i| row("orders", 10_000 + b * per + i))
            .collect();
        s.send_batch(&batch).await.unwrap();
    }
    let rate = (batches * per) as f64 / t.elapsed().as_secs_f64();
    // Wide parts report per-column sizes (compact parts store one file).
    ch(
        &base,
        &format!(
            "ALTER TABLE default.{table} MODIFY SETTING min_bytes_for_wide_part = 0, \
             min_rows_for_wide_part = 0"
        ),
    )
    .await;
    ch(&base, &format!("OPTIMIZE TABLE default.{table} FINAL")).await;
    let sizes = ch(
        &base,
        &format!(
            "SELECT column, sum(column_data_compressed_bytes), \
             sum(column_data_uncompressed_bytes) FROM system.parts_columns \
             WHERE table = '{table}' AND active GROUP BY column ORDER BY column FORMAT TSV"
        ),
    )
    .await;
    // Constraint compare-and-swap alone.
    let create = ch(
        &base,
        &format!("SELECT create_table_query FROM system.tables WHERE name = '{table}' FORMAT TSVRaw"),
    )
    .await;
    let old = &create[create.find("df_fence_").unwrap() + 9..][..32];
    let new = "c".repeat(32);
    let t = Instant::now();
    ch(
        &base,
        &format!(
            "ALTER TABLE default.{table} DROP CONSTRAINT df_fence_{old}, \
             ADD CONSTRAINT df_fence_{new} CHECK _df_fence = '{new}'"
        ),
    )
    .await;
    let cas = t.elapsed();
    println!(
        "CH claim costs: first_creation={first:?} restart_claim={restart:?} \
         constraint_cas={cas:?} steady_rows_per_s={rate:.0} fence_bytes_per_row_on_wire=33\n\
         column sizes (name, compressed, uncompressed):\n{sizes}"
    );
}
