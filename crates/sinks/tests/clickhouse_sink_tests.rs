//! Live ClickHouse sink integration tests.
//!
//! Requires Docker; pulls `clickhouse/clickhouse-server`. Run:
//!   cargo test -p sinks --test clickhouse_sink_tests -- --ignored --test-threads=1

use std::sync::Arc;
use std::time::{Duration, Instant};

use deltaforge_config::{ChMode, ChVersionSource, ClickHouseSinkCfg};
use deltaforge_core::{Event, Op, Sink, SourceInfo, SourcePosition};
use serde_json::{json, Value};
use sinks::clickhouse::types::ColDesc;
use sinks::clickhouse::{build_clickhouse_sink, ClickHouseSchemaResolver, TableColumns};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio_util::sync::CancellationToken;

const CH_HTTP: u16 = 8123;

/// Columns for a test `orders` table: `id BIGINT PK`, `amount DECIMAL(12,2)`.
fn resolver() -> ClickHouseSchemaResolver {
    Arc::new(|_key: &str| {
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
                    precision: Some(12),
                    scale: Some(2),
                },
            ],
            primary_key: vec!["id".into()],
        })
    })
}

fn cfg(url: &str, table: &str, mode: ChMode, auto_create: bool) -> ClickHouseSinkCfg {
    ClickHouseSinkCfg {
        id: "ch-test".into(),
        url: url.into(),
        database: "default".into(),
        table: table.into(),
        mode,
        user: Some("default".into()),
        password: None,
        tls: None,
        version_source: ChVersionSource::TsMs,
        send_timeout_secs: 30,
        required: Some(true),
        auto_create,
    }
}

fn mk_event(op: Op, after: Value, before: Value, table: &str, ts: i64) -> Event {
    Event {
        before: if before.is_null() { None } else { Some(before) },
        after: if after.is_null() { None } else { Some(after) },
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
        checkpoint: None,
        size_bytes: 0,
        received_at_ms: 0,
    }
}

/// Run a SQL statement over the ClickHouse HTTP interface, returning the body.
async fn ch(base: &str, sql: &str) -> String {
    let r = reqwest::Client::new()
        .post(base)
        .query(&[("query", sql)])
        .send()
        .await
        .expect("clickhouse http request");
    let status = r.status();
    let body = r.text().await.unwrap_or_default();
    assert!(status.is_success(), "clickhouse error ({status}): {body}\nsql: {sql}");
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

async fn start_clickhouse() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("clickhouse/clickhouse-server", "24.8")
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(3),
        })
        .with_mapped_port(0, CH_HTTP.tcp())
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
    )
    .unwrap();

    // insert id=1 amount=10.00 (v=1); update id=1 -> 20.00 (v=2); delete id=2 (v=3)
    let batch = vec![
        mk_event(Op::Create, json!({"id": 1, "amount": "10.00"}), json!(null), table, 1),
        mk_event(Op::Update, json!({"id": 1, "amount": "20.00"}), json!(null), table, 2),
        mk_event(Op::Delete, json!(null), json!({"id": 2, "amount": null}), table, 3),
    ];
    let res = sink.send_batch(&batch).await.unwrap();
    assert!(res.dlq_failures.is_empty(), "no per-row failures: {:?}", res.dlq_failures);

    let out = ch(
        &base,
        &format!("SELECT id, toString(amount) FROM default.{table} FINAL ORDER BY id FORMAT TSV"),
    )
    .await;
    // Only id=1 at amount 20.00 survives; the delete of never-seen id=2 collapses.
    assert_eq!(out.trim(), "1\t20.00", "current state, got: {out:?}");
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
    )
    .unwrap();

    let batch = vec![
        mk_event(Op::Create, json!({"id": 1, "amount": "10.00"}), json!(null), table, 1),
        mk_event(Op::Update, json!({"id": 1, "amount": "20.00"}), json!(null), table, 2),
        mk_event(Op::Delete, json!(null), json!({"id": 1, "amount": null}), table, 3),
    ];
    sink.send_batch(&batch).await.unwrap();

    let n = ch(&base, &format!("SELECT count() FROM default.{table} FORMAT TSV")).await;
    assert_eq!(n.trim(), "3", "change-log retains all 3 changes");
    let ops = ch(
        &base,
        &format!("SELECT _op FROM default.{table} ORDER BY _version FORMAT TSV"),
    )
    .await;
    assert_eq!(ops.split_whitespace().collect::<Vec<_>>(), vec!["c", "u", "d"]);
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
             `_source_ts` DateTime64(3)) ENGINE = MergeTree ORDER BY (`id`, `_version`) \
             SETTINGS non_replicated_deduplication_window = 1000"
        ),
    )
    .await;

    let sink = build_clickhouse_sink(
        &cfg(&base, table, ChMode::Changelog, false),
        CancellationToken::new(),
        "p",
        Some(resolver()),
    )
    .unwrap();

    let batch = vec![
        mk_event(Op::Create, json!({"id": 1, "amount": "10.00"}), json!(null), table, 1),
        mk_event(Op::Create, json!({"id": 2, "amount": "11.00"}), json!(null), table, 2),
    ];
    // Send the SAME batch twice — the deterministic dedup token must suppress the replay.
    sink.send_batch(&batch).await.unwrap();
    sink.send_batch(&batch).await.unwrap();

    let n = ch(&base, &format!("SELECT count() FROM default.{table} FORMAT TSV")).await;
    assert_eq!(n.trim(), "2", "dedup token prevented the replayed batch from doubling rows");
}
