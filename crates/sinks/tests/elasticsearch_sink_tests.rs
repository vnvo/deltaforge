//! Live Elasticsearch sink integration tests.
//!
//! Requires Docker; pulls `docker.elastic.co/elasticsearch/elasticsearch`. Run:
//!   cargo test -p sinks --test elasticsearch_sink_tests -- --ignored --test-threads=1

use std::sync::Arc;
use std::time::{Duration, Instant};

use deltaforge_config::{ElasticsearchSinkCfg, EsVersionSource};
use deltaforge_core::{Event, Op, Sink, SourceInfo, SourcePosition};
use serde_json::{Value, json};
use sinks::clickhouse::types::ColDesc;
use sinks::elasticsearch::{
    EsSchemaResolver, TableColumns, build_elasticsearch_sink,
};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio_util::sync::CancellationToken;

const ES_HTTP: u16 = 9200;

/// Resolver for a test `orders` table: `id BIGINT PK`, `amount DECIMAL(12,2)`
/// with the numeric metadata *unset* (mirrors the real loader) so the mapping
/// must resolve the scale from `full_type` — proving `amount` lands as
/// `scaled_float(100)`, not a truncating fallback.
fn resolver() -> EsSchemaResolver {
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
                    precision: None,
                    scale: None,
                },
                // TEXT (binlog delivers it base64-wrapped), TIMESTAMP (binlog
                // delivers integer microseconds), and BLOB — the real CDC value
                // shapes that must be normalized to match the mapping.
                ColDesc {
                    name: "data".into(),
                    data_type: "text".into(),
                    full_type: "text".into(),
                    nullable: true,
                    unsigned: false,
                    precision: None,
                    scale: None,
                },
                ColDesc {
                    name: "created_at".into(),
                    data_type: "timestamp".into(),
                    full_type: "timestamp".into(),
                    nullable: true,
                    unsigned: false,
                    precision: None,
                    scale: None,
                },
                ColDesc {
                    name: "blobby".into(),
                    data_type: "blob".into(),
                    full_type: "blob".into(),
                    nullable: true,
                    unsigned: false,
                    precision: None,
                    scale: None,
                },
            ],
            primary_key: vec!["id".into()],
        })
    })
}

fn b64(s: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

fn cfg(url: &str, index: &str, auto_create: bool) -> ElasticsearchSinkCfg {
    ElasticsearchSinkCfg {
        id: "es-test".into(),
        url: url.into(),
        index: index.into(),
        auto_create_index: auto_create,
        id_fields: vec![],
        id_separator: "_".into(),
        // Deterministic external version from ts_ms so we can drive ordering.
        version_source: EsVersionSource::TsMs,
        auth: None,
        tls: None,
        send_timeout_secs: 30,
        required: Some(true),
    }
}

fn mk_event(op: Op, after: Value, before: Value, ts: i64) -> Event {
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
            table: "orders".into(),
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

/// Read a numeric value that ES `_source` may keep as a number or as the
/// original numeric string (decimals often arrive as strings to keep precision).
fn as_f64_loose(v: &Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
        .unwrap_or_else(|| panic!("not a number-ish value: {v}"))
}

async fn es_get(base: &str, path: &str) -> (reqwest::StatusCode, String) {
    let r = reqwest::Client::new()
        .get(format!("{base}{path}"))
        .send()
        .await
        .expect("es GET");
    let status = r.status();
    (status, r.text().await.unwrap_or_default())
}

async fn refresh(base: &str, index: &str) {
    reqwest::Client::new()
        .post(format!("{base}/{index}/_refresh"))
        .send()
        .await
        .expect("refresh");
}

async fn wait_ready(base: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(r) = reqwest::get(format!("{base}/_cluster/health")).await {
            if r.status().is_success() {
                return;
            }
        }
        if Instant::now() > deadline {
            panic!("elasticsearch not ready after 120s");
        }
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

async fn start_elasticsearch()
-> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new(
        "docker.elastic.co/elasticsearch/elasticsearch",
        "8.15.0",
    )
    .with_wait_for(WaitFor::Duration {
        length: Duration::from_secs(3),
    })
    .with_mapped_port(0, ES_HTTP.tcp())
    .with_env_var("discovery.type", "single-node")
    .with_env_var("xpack.security.enabled", "false")
    .with_env_var("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
    .start()
    .await
    .expect("start elasticsearch container");
    let port = container.get_host_port_ipv4(ES_HTTP).await.unwrap();
    let base = format!("http://localhost:{port}");
    wait_ready(&base).await;
    (container, base)
}

#[tokio::test]
#[ignore]
async fn upsert_delete_and_typed_mapping() {
    let (_c, base) = start_elasticsearch().await;
    let index = "orders_upsert";
    let sink = build_elasticsearch_sink(
        &cfg(&base, index, true),
        CancellationToken::new(),
        "p",
        Some(resolver()),
    )
    .unwrap();

    // insert id=1 amount 10.00 (v=1); update id=1 -> 20.50 (v=2); delete id=2 (v=3)
    let batch = vec![
        mk_event(
            Op::Create,
            json!({"id": 1, "amount": "10.00"}),
            json!(null),
            1,
        ),
        mk_event(
            Op::Update,
            json!({"id": 1, "amount": "20.50"}),
            json!(null),
            2,
        ),
        mk_event(Op::Delete, json!(null), json!({"id": 2}), 3),
    ];
    let res = sink.send_batch(&batch).await.unwrap();
    assert!(res.dlq_failures.is_empty(), "{:?}", res.dlq_failures);
    refresh(&base, index).await;

    // Generated mapping is typed: amount is scaled_float, not float/text.
    let (_s, mapping) = es_get(&base, &format!("/{index}/_mapping")).await;
    let m: Value = serde_json::from_str(&mapping).unwrap();
    let amount_type = &m[index]["mappings"]["properties"]["amount"]["type"];
    assert_eq!(amount_type, "scaled_float", "mapping: {mapping}");

    // Current state: id=1 at 20.50; id=2 never existed so the delete is a no-op.
    let (_s, doc) = es_get(&base, &format!("/{index}/_doc/1")).await;
    let d: Value = serde_json::from_str(&doc).unwrap();
    let amt = as_f64_loose(&d["_source"]["amount"]);
    assert!(
        (amt - 20.50).abs() < 1e-9,
        "amount should be 20.50, got {doc}"
    );

    let (status2, _b) = es_get(&base, &format!("/{index}/_doc/2")).await;
    assert_eq!(
        status2,
        reqwest::StatusCode::NOT_FOUND,
        "id=2 must not exist"
    );
}

#[tokio::test]
#[ignore]
async fn real_cdc_value_shapes_land_and_map_correctly() {
    // Regression guard for the DLQ-everything bug: real MySQL CDC payloads carry
    // base64-wrapped TEXT/BLOB and microsecond TIMESTAMP integers, which ES
    // rejected against the generated text/date mappings. Feed those exact shapes
    // and assert the documents actually land, decoded/typed correctly.
    let (_c, base) = start_elasticsearch().await;
    let index = "orders_shapes";
    let sink = build_elasticsearch_sink(
        &cfg(&base, index, true),
        CancellationToken::new(),
        "p",
        Some(resolver()),
    )
    .unwrap();

    let batch = vec![mk_event(
        Op::Create,
        json!({
            "id": 1,
            "amount": "20.50",
            "data": { "_base64": b64("backlog-hello") },   // TEXT via binlog
            "created_at": 1789934706000000i64,              // TIMESTAMP micros
            "blobby": { "_base64": b64("\u{0}\u{1}raw") },  // BLOB
        }),
        json!(null),
        1,
    )];
    let res = sink.send_batch(&batch).await.unwrap();
    assert!(
        res.dlq_failures.is_empty(),
        "must not DLQ: {:?}",
        res.dlq_failures
    );
    refresh(&base, index).await;

    // Document landed with decoded text and a normalized timestamp.
    let (status, doc) = es_get(&base, &format!("/{index}/_doc/1")).await;
    assert_eq!(status, reqwest::StatusCode::OK, "doc must land: {doc}");
    let d: Value = serde_json::from_str(&doc).unwrap();
    assert_eq!(d["_source"]["data"], json!("backlog-hello"), "text decoded");
    assert_eq!(
        d["_source"]["blobby"],
        json!(b64("\u{0}\u{1}raw")),
        "blob b64"
    );
    assert_eq!(
        d["_source"]["created_at"].as_i64().unwrap(),
        1789934706000i64,
        "timestamp micros normalized to millis"
    );

    // Mapping is correctly typed for the new columns.
    let (_s, mapping) = es_get(&base, &format!("/{index}/_mapping")).await;
    let m: Value = serde_json::from_str(&mapping).unwrap();
    let props = &m[index]["mappings"]["properties"];
    assert_eq!(props["data"]["type"], "text");
    assert_eq!(props["blobby"]["type"], "binary");
    assert_eq!(props["created_at"]["type"], "date");
}

#[tokio::test]
#[ignore]
async fn replay_and_out_of_order_are_idempotent() {
    let (_c, base) = start_elasticsearch().await;
    let index = "orders_replay";
    let sink = build_elasticsearch_sink(
        &cfg(&base, index, true),
        CancellationToken::new(),
        "p",
        Some(resolver()),
    )
    .unwrap();

    // Establish id=1 at version 10, amount 10.00.
    let insert = vec![mk_event(
        Op::Create,
        json!({"id": 1, "amount": "10.00"}),
        json!(null),
        10,
    )];
    sink.send_batch(&insert).await.unwrap();

    // Replay the same batch — external version 10 == 10, ES returns 409, which
    // the sink treats as success (no DLQ, no change).
    let res = sink.send_batch(&insert).await.unwrap();
    assert!(res.dlq_failures.is_empty(), "replay must not DLQ");

    // A stale (out-of-order) update at version 5 must NOT overwrite.
    let stale = vec![mk_event(
        Op::Update,
        json!({"id": 1, "amount": "99.99"}),
        json!(null),
        5,
    )];
    sink.send_batch(&stale).await.unwrap();

    // A newer update at version 20 applies.
    let newer = vec![mk_event(
        Op::Update,
        json!({"id": 1, "amount": "20.50"}),
        json!(null),
        20,
    )];
    sink.send_batch(&newer).await.unwrap();
    refresh(&base, index).await;

    let (_s, doc) = es_get(&base, &format!("/{index}/_doc/1")).await;
    let d: Value = serde_json::from_str(&doc).unwrap();
    let amt = as_f64_loose(&d["_source"]["amount"]);
    assert!(
        (amt - 20.50).abs() < 1e-9,
        "stale update must be ignored, newer applied — got {doc}"
    );

    // Exactly one document exists for id=1.
    let (_s, count) = es_get(&base, &format!("/{index}/_count")).await;
    let c: Value = serde_json::from_str(&count).unwrap();
    assert_eq!(c["count"], 1, "one current-state doc: {count}");
}

#[tokio::test]
#[ignore]
async fn per_document_error_routes_to_dlq() {
    let (_c, base) = start_elasticsearch().await;
    let index = "orders_dlq";
    // Pre-create the index with `amount` as an integer so a non-numeric string
    // is a per-document mapping error. auto_create off → use this mapping.
    reqwest::Client::new()
        .put(format!("{base}/{index}"))
        .json(&json!({
            "mappings": {"properties": {
                "id": {"type": "long"},
                "amount": {"type": "integer"}
            }}
        }))
        .send()
        .await
        .expect("pre-create index");

    let sink = build_elasticsearch_sink(
        &cfg(&base, index, false),
        CancellationToken::new(),
        "p",
        Some(resolver()),
    )
    .unwrap();

    // Row 0: amount "abc" cannot map to integer -> per-doc failure -> DLQ.
    // Row 1: amount 5 is fine -> lands.
    let batch = vec![
        mk_event(
            Op::Create,
            json!({"id": 1, "amount": "abc"}),
            json!(null),
            1,
        ),
        mk_event(Op::Create, json!({"id": 2, "amount": 5}), json!(null), 2),
    ];
    let res = sink.send_batch(&batch).await.unwrap();
    assert_eq!(
        res.dlq_failures.len(),
        1,
        "one row to DLQ: {:?}",
        res.dlq_failures
    );
    assert_eq!(res.dlq_failures[0].0, 0, "row 0 failed");
    refresh(&base, index).await;

    let (status_good, _b) = es_get(&base, &format!("/{index}/_doc/2")).await;
    assert_eq!(
        status_good,
        reqwest::StatusCode::OK,
        "row 1 must have landed"
    );
    let (status_bad, _b) = es_get(&base, &format!("/{index}/_doc/1")).await;
    assert_eq!(
        status_bad,
        reqwest::StatusCode::NOT_FOUND,
        "row 0 must not have landed"
    );
}
