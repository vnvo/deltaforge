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
        std::sync::Arc::new(
            deltaforge_core::destination::MemoryDestinationBindings::default(),
        ),
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
        std::sync::Arc::new(
            deltaforge_core::destination::MemoryDestinationBindings::default(),
        ),
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
    // Pre-create a MergeTree with non-replicated dedup enabled; auto_create off.
    ch(
        &base,
        &format!(
            "CREATE TABLE default.{table} (`id` Int64, `amount` Nullable(Decimal(12,2)), \
             `_op` LowCardinality(String), `_version` UInt64, `_deleted` UInt8, \
             `_source_ts` DateTime64(3), `_extensions` Nullable(String)) \
             ENGINE = MergeTree ORDER BY (`id`, `_version`) \
             SETTINGS non_replicated_deduplication_window = 1000"
        ),
    )
    .await;

    let sink = build_clickhouse_sink(
        &cfg(&base, table, ChMode::Changelog, false),
        CancellationToken::new(),
        "p",
        Some(resolver()),
        std::sync::Arc::new(
            deltaforge_core::destination::MemoryDestinationBindings::default(),
        ),
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
        "2",
        "dedup token prevented the replayed batch from doubling rows"
    );
}

/// The target table's binding against a live server: a restarted sink
/// whose first event is another relation is refused before inserting; a
/// table dropped and recreated (another UUID) is refused as replaced; the
/// refusal names both relations and no row data.
#[tokio::test]
#[ignore]
async fn the_target_table_binding_holds_across_restart_and_recreation() {
    let (_c, base) = start_clickhouse().await;
    let table = "orders_bound";
    let store: std::sync::Arc<
        dyn deltaforge_core::destination::DestinationBindingStore,
    > = std::sync::Arc::new(
        deltaforge_core::destination::MemoryDestinationBindings::default(),
    );
    let sink = || {
        build_clickhouse_sink(
            &cfg(&base, table, ChMode::Changelog, true),
            CancellationToken::new(),
            "p",
            Some(resolver()),
            std::sync::Arc::clone(&store),
            &sinks::ResolvedSinkCreds::default(),
        )
        .unwrap()
    };
    let row = |source_table: &str| {
        mk_event(
            Op::Create,
            json!({"id": 7, "amount": "1.50"}),
            json!(null),
            source_table,
            1,
        )
    };
    sink().send_batch(&[row("orders")]).await.unwrap();
    let count_sql = format!("SELECT count() FROM default.{table}");
    let count = || ch(&base, &count_sql);
    assert_eq!(count().await.trim(), "1");

    // A restart: the other relation comes first.
    let e = sink().send_batch(&[row("orders_v2")]).await.unwrap_err();
    assert_eq!(
        e.draft().map(|d| d.reason_code),
        Some(deltaforge_core::incident::ReasonCode::SinkDestinationConflict)
    );
    let text = format!("{e:?}");
    assert!(
        text.contains("/orders") && text.contains("/orders_v2"),
        "{text}"
    );
    assert!(!text.contains("1.50"), "no row data: {text}");
    assert_eq!(count().await.trim(), "1", "nothing inserted");

    // Recreated by hand: another table UUID.
    ch(&base, &format!("DROP TABLE default.{table} SYNC")).await;
    let e = sink().send_batch(&[row("orders")]).await.unwrap_err();
    assert_eq!(
        e.draft()
            .and_then(|d| d.discriminator.get("class").cloned())
            .as_deref(),
        Some("target_replaced")
    );
}
