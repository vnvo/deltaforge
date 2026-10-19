//! Live Elasticsearch sink integration tests.
//!
//! Requires Docker; pulls `docker.elastic.co/elasticsearch/elasticsearch`. Run:
//!   cargo test -p sinks --test elasticsearch_sink_tests -- --ignored --test-threads=1

use std::sync::Arc;
use std::time::{Duration, Instant};

use deltaforge_config::{ElasticsearchSinkCfg, EsVersionSource};
use deltaforge_core::canonical::{
    CanonicalRow, CanonicalValue, DbValue, Decimal, IntWidth, Precision,
    TimestampValue, fixture,
};
use deltaforge_core::{
    Event, Op, Sink, SourceInfo, SourcePosition, TypeMapping,
};
use gate_ownership::GateOwned;
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

/// A typed `orders` row from a JSON literal: `id` is a BIGINT; an
/// `amount` string is a DECIMAL when it is exact decimal text (else TEXT,
/// for the per-document mapping-error test); other values are fixture
/// values.
fn orders_row(v: Value) -> Option<CanonicalRow> {
    let Value::Object(m) = v else {
        return None;
    };
    let cols = m
        .into_iter()
        .map(|(k, v)| {
            let value = match (k.as_str(), &v) {
                ("id", Value::Number(n)) => CanonicalValue::Db(
                    DbValue::int(n.as_i64().unwrap(), IntWidth::W64).unwrap(),
                ),
                ("amount", Value::String(s)) => match Decimal::parse(s) {
                    Ok(d) => CanonicalValue::Db(DbValue::Decimal(d)),
                    Err(_) => fixture::value(&v),
                },
                _ => fixture::value(&v),
            };
            (k, value)
        })
        .collect();
    Some(CanonicalRow::new(cols).unwrap())
}

fn mk_event(op: Op, after: Value, before: Value, ts: i64) -> Event {
    mk_row_event(op, orders_row(after), orders_row(before), ts)
}

fn mk_row_event(
    op: Op,
    after: Option<CanonicalRow>,
    before: Option<CanonicalRow>,
    ts: i64,
) -> Event {
    Event {
        before,
        after,
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

/// Ready once the cluster can take writes (status yellow or green), not just
/// once it answers HTTP: a red cluster accepts the request and then times out.
async fn wait_ready(base: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(r) = reqwest::get(format!(
            "{base}/_cluster/health?wait_for_status=yellow&timeout=5s"
        ))
        .await
            && r.status().is_success()
        {
            return;
        }
        if Instant::now() > deadline {
            panic!("elasticsearch not ready (status yellow) after 120s");
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
    // The host's free disk is not under test: without this, a host above
    // the disk watermarks (85% / 90% used) leaves shards unassigned and every
    // write times out.
    .with_env_var("cluster.routing.allocation.disk.threshold_enabled", "false")
    .with_env_var("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
    .gate_owned()
    .start()
    .await
    .expect("start elasticsearch container");
    let port = container.get_host_port_ipv4(ES_HTTP).await.unwrap();
    let base = format!("http://localhost:{port}");
    wait_ready(&base).await;
    // The deployment requirement for fenced deletes.
    set_auto_create(&base, "-df-w-*,+*").await;
    (container, base)
}

/// Set the cluster's `action.auto_create_index` (persistent).
async fn set_auto_create(base: &str, rule: &str) {
    let r = reqwest::Client::new()
        .put(format!("{base}/_cluster/settings"))
        .json(&json!({"persistent": {"action.auto_create_index": rule}}))
        .send()
        .await
        .expect("cluster settings");
    assert!(
        r.status().is_success(),
        "{}",
        r.text().await.unwrap_or_default()
    );
}

async fn es_json(
    method: reqwest::Method,
    base: &str,
    path: &str,
    body: Option<Value>,
) -> (u16, Value) {
    let mut req =
        reqwest::Client::new().request(method, format!("{base}{path}"));
    if let Some(b) = body {
        req = req.json(&b);
    }
    let r = req.send().await.expect("es request");
    let status = r.status().as_u16();
    (status, r.json().await.unwrap_or(json!({})))
}

/// The internal write aliases of `index`.
async fn internal_aliases(base: &str, index: &str) -> Vec<String> {
    let (_, v) = es_json(
        reqwest::Method::GET,
        base,
        &format!("/{index}/_alias"),
        None,
    )
    .await;
    v[index]["aliases"]
        .as_object()
        .map(|a| {
            a.keys()
                .filter(|k| k.starts_with("df-w-"))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

fn class(e: &deltaforge_core::SinkError) -> Option<String> {
    e.draft()
        .and_then(|d| d.discriminator.get("class").cloned())
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
        &sinks::SinkOwnership::ephemeral(),
        &sinks::ResolvedSinkCreds::default(),
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
    // Regression guard for the DLQ-everything bug: TEXT, BLOB and TIMESTAMP
    // columns must produce document values their generated text/binary/date
    // mappings accept. Values are typed by the source decoder (TEXT is text,
    // BLOB is bytes, TIMESTAMP is a microsecond instant), never guessed.
    let (_c, base) = start_elasticsearch().await;
    let index = "orders_shapes";
    let sink = build_elasticsearch_sink(
        &cfg(&base, index, true),
        CancellationToken::new(),
        "p",
        Some(resolver()),
        &sinks::SinkOwnership::ephemeral(),
        &sinks::ResolvedSinkCreds::default(),
    )
    .unwrap();

    let created_at =
        TimestampValue::instant(1_789_934_706_000_000, Precision::Fixed(6))
            .unwrap();
    let row = CanonicalRow::new(vec![
        (
            "id".into(),
            CanonicalValue::Db(DbValue::int(1, IntWidth::W64).unwrap()),
        ),
        (
            "amount".into(),
            CanonicalValue::Db(DbValue::Decimal(
                Decimal::parse("20.50").unwrap(),
            )),
        ),
        (
            "data".into(),
            CanonicalValue::Db(DbValue::Text("backlog-hello".into())),
        ),
        (
            "created_at".into(),
            CanonicalValue::Db(DbValue::Timestamp(created_at)),
        ),
        (
            "blobby".into(),
            CanonicalValue::Db(DbValue::Binary(b"\x00\x01raw".to_vec())),
        ),
    ])
    .unwrap();
    let batch = vec![mk_row_event(Op::Create, Some(row), None, 1)];
    let res = sink.send_batch(&batch).await.unwrap();
    assert!(
        res.dlq_failures.is_empty(),
        "must not DLQ: {:?}",
        res.dlq_failures
    );
    refresh(&base, index).await;

    // Document landed with the text, base64 bytes and the instant's text.
    let (status, doc) = es_get(&base, &format!("/{index}/_doc/1")).await;
    assert_eq!(status, reqwest::StatusCode::OK, "doc must land: {doc}");
    let d: Value = serde_json::from_str(&doc).unwrap();
    assert_eq!(d["_source"]["data"], json!("backlog-hello"), "text");
    assert_eq!(
        d["_source"]["blobby"],
        json!(b64("\u{0}\u{1}raw")),
        "blob b64"
    );
    assert_eq!(
        d["_source"]["created_at"],
        json!(deltaforge_core::canonical::render::timestamp_text(
            &created_at
        )),
        "timestamp as its exact text"
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
        &sinks::SinkOwnership::ephemeral(),
        &sinks::ResolvedSinkCreds::default(),
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
    // The sink creates the index (`amount` a scaled_float): a non-numeric
    // amount is a per-document failure.
    let sink = build_elasticsearch_sink(
        &cfg(&base, index, true),
        CancellationToken::new(),
        "p",
        Some(resolver()),
        &sinks::SinkOwnership::ephemeral(),
        &sinks::ResolvedSinkCreds::default(),
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

/// Ownership against a live cluster: a restart of the owner writes; a
/// restarted sink whose first event is another relation, another
/// deployment, an unmarked index and an alias are refused before any
/// document is written; a deleted index is neither recreated nor
/// auto-created; the refusals name no row data.
#[tokio::test]
#[ignore]
async fn ownership_holds_across_restarts_and_refuses_others() {
    let (_c, base) = start_elasticsearch().await;
    let index = "orders_owned";
    let ownership = sinks::SinkOwnership::ephemeral();
    let sink = |o: &sinks::SinkOwnership, index: &str| {
        build_elasticsearch_sink(
            &cfg(&base, index, true),
            CancellationToken::new(),
            "p",
            Some(resolver()),
            o,
            &sinks::ResolvedSinkCreds::default(),
        )
        .unwrap()
    };
    let row = |db: &str, id: i64| {
        let mut e = mk_event(
            Op::Create,
            json!({"id": id, "amount": "1.50"}),
            json!(null),
            id,
        );
        e.source.db = db.into();
        e
    };
    sink(&ownership, index)
        .send_batch(&[row("shop", 1)])
        .await
        .unwrap();
    // A restart of the same owner.
    sink(&ownership, index)
        .send_batch(&[row("shop", 2)])
        .await
        .unwrap();
    refresh(&base, index).await;
    let count = |index: String| {
        let base = base.clone();
        async move {
            let (_, body) = es_get(&base, &format!("/{index}/_count")).await;
            serde_json::from_str::<Value>(&body).unwrap()["count"].as_u64()
        }
    };
    assert_eq!(count(index.into()).await, Some(2));
    assert_eq!(internal_aliases(&base, index).await.len(), 1);

    let e = sink(&ownership, index)
        .send_batch(&[row("shop2", 3)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("bound_to_another_relation"));
    assert!(!format!("{e:?}").contains("1.50"), "no row data");
    let e = sink(&sinks::SinkOwnership::ephemeral(), index)
        .send_batch(&[row("shop", 3)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("owned_by_other"));
    refresh(&base, index).await;
    assert_eq!(count(index.into()).await, Some(2), "nothing written");

    // An index the sink did not create.
    es_json(
        reqwest::Method::PUT,
        &base,
        "/orders_unmarked",
        Some(json!({})),
    )
    .await;
    let e = sink(&ownership, "orders_unmarked")
        .send_batch(&[row("shop", 1)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("marker_missing"));
    // A name that is an alias of another index.
    es_json(
        reqwest::Method::PUT,
        &base,
        "/orders_real",
        Some(json!({"aliases": {"orders_via_alias": {}}})),
    )
    .await;
    let e = sink(&ownership, "orders_via_alias")
        .send_batch(&[row("shop", 1)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("alias_not_supported"));

    // Deleted: the verified session's next write fails at the server and
    // nothing is auto-created; a restart is refused, never recreating it.
    let live = sink(&ownership, index);
    live.send_batch(&[row("shop", 4)]).await.unwrap();
    es_json(reqwest::Method::DELETE, &base, &format!("/{index}"), None).await;
    let mut del = row("shop", 1);
    del.op = Op::Delete;
    del.before = del.after.take();
    let e = live.send_batch(&[del]).await.unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("fenced"));
    let (status, _) =
        es_json(reqwest::Method::GET, &base, &format!("/{index}"), None).await;
    assert_eq!(status, 404, "not auto-created");
    let (_, cat) = es_json(
        reqwest::Method::GET,
        &base,
        "/_cat/indices?format=json",
        None,
    )
    .await;
    assert!(
        !cat.as_array()
            .unwrap()
            .iter()
            .any(|i| i["index"].as_str().unwrap_or("").starts_with("df-w-")),
        "no index named after an alias: {cat}"
    );
    let e = sink(&ownership, index)
        .send_batch(&[row("shop", 5)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("destination_missing"));
    // Recreated by another deployment: ownership does not transfer.
    sink(&sinks::SinkOwnership::ephemeral(), index)
        .send_batch(&[row("shop", 6)])
        .await
        .unwrap();
    let e = sink(&ownership, index)
        .send_batch(&[row("shop", 7)])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("owned_by_other"));
}

/// A copy of the state store (the same deployment identity and records)
/// claiming the index fences the original: the original's very next write,
/// an index action and a delete alike, fails at the server, and so does its
/// next claim. Exactly one of them holds the fence.
#[tokio::test]
#[ignore]
async fn a_copied_store_fences_the_original_at_its_next_write() {
    let (_c, base) = start_elasticsearch().await;
    let index = "orders_clone";
    let store = std::sync::Arc::new(
        deltaforge_core::ownership::MemoryOwnershipStore::default(),
    );
    let original = sinks::SinkOwnership {
        deployment: deltaforge_core::ownership::random_token(),
        store: store.clone(),
    };
    let sink = |o: &sinks::SinkOwnership| {
        build_elasticsearch_sink(
            &cfg(&base, index, true),
            CancellationToken::new(),
            "p",
            Some(resolver()),
            o,
            &sinks::ResolvedSinkCreds::default(),
        )
        .unwrap()
    };
    let a = sink(&original);
    a.send_batch(&[mk_event(
        Op::Create,
        json!({"id": 1, "amount": "1.00"}),
        json!(null),
        1,
    )])
    .await
    .unwrap();
    let copy = sinks::SinkOwnership {
        deployment: original.deployment.clone(),
        store: std::sync::Arc::new(store.copy()),
    };
    let b = sink(&copy);
    b.send_batch(&[mk_event(
        Op::Create,
        json!({"id": 2, "amount": "2.00"}),
        json!(null),
        2,
    )])
    .await
    .unwrap();
    for ev in [
        mk_event(
            Op::Create,
            json!({"id": 3, "amount": "3.00"}),
            json!(null),
            3,
        ),
        mk_event(Op::Delete, json!(null), json!({"id": 2}), 4),
    ] {
        let e = a.send_batch(&[ev]).await.unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("fenced"), "{e:?}");
    }
    let e = sink(&original)
        .send_batch(&[mk_event(
            Op::Create,
            json!({"id": 5, "amount": "5.00"}),
            json!(null),
            5,
        )])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("fenced"));
    // The copy keeps writing.
    b.send_batch(&[mk_event(
        Op::Create,
        json!({"id": 6, "amount": "6.00"}),
        json!(null),
        6,
    )])
    .await
    .unwrap();
    refresh(&base, index).await;
    let (_, v) = es_json(
        reqwest::Method::GET,
        &base,
        &format!("/{index}/_search?size=20"),
        None,
    )
    .await;
    let mut ids: Vec<i64> = v["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["_source"]["id"].as_i64().unwrap())
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2, 6],
        "nothing of the fenced writer after the rotation"
    );
}

/// The allowed overlap: a bulk request the server already accepted may
/// complete while the fence rotates; a write submitted after the rotation
/// committed is rejected. Whatever the in-flight request did is idempotent
/// by `_id` and version.
#[tokio::test]
#[ignore]
async fn a_bulk_in_flight_during_rotation_may_complete_and_the_next_fails() {
    let (_c, base) = start_elasticsearch().await;
    let index = "orders_inflight";
    let store = std::sync::Arc::new(
        deltaforge_core::ownership::MemoryOwnershipStore::default(),
    );
    let original = sinks::SinkOwnership {
        deployment: deltaforge_core::ownership::random_token(),
        store: store.clone(),
    };
    let sink = |o: &sinks::SinkOwnership| {
        std::sync::Arc::new(
            build_elasticsearch_sink(
                &cfg(&base, index, true),
                CancellationToken::new(),
                "p",
                Some(resolver()),
                o,
                &sinks::ResolvedSinkCreds::default(),
            )
            .unwrap(),
        )
    };
    let a = sink(&original);
    a.send_batch(&[mk_event(
        Op::Create,
        json!({"id": 0, "amount": "0.00"}),
        json!(null),
        1,
    )])
    .await
    .unwrap();
    let big: Vec<Event> = (1..=5000)
        .map(|i| {
            mk_event(
                Op::Create,
                json!({"id": i, "amount": "1.00"}),
                json!(null),
                10 + i,
            )
        })
        .collect();
    let a2 = a.clone();
    let inflight = tokio::spawn(async move { a2.send_batch(&big).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let copy = sinks::SinkOwnership {
        deployment: original.deployment.clone(),
        store: std::sync::Arc::new(store.copy()),
    };
    sink(&copy)
        .send_batch(&[mk_event(
            Op::Create,
            json!({"id": 9001, "amount": "1.00"}),
            json!(null),
            9001,
        )])
        .await
        .unwrap();
    // Either outcome is allowed for the request already in flight.
    match inflight.await.unwrap() {
        Ok(_) => {
            eprintln!("in-flight bulk completed before or across the rotation")
        }
        Err(e) => assert_eq!(class(&e).as_deref(), Some("fenced"), "{e:?}"),
    }
    // A write submitted after the rotation committed is rejected.
    let e = a
        .send_batch(&[mk_event(
            Op::Create,
            json!({"id": 9002, "amount": "1.00"}),
            json!(null),
            9002,
        )])
        .await
        .unwrap_err();
    assert_eq!(class(&e).as_deref(), Some("fenced"));
}

/// Deletes are fenced only when the cluster forbids auto-creating `df-w-*`:
/// a claim refuses an index otherwise, and when the settings are
/// unreadable. Our evaluation of ordered rules agrees with the cluster's.
#[tokio::test]
#[ignore]
async fn the_auto_create_rule_is_proven_and_matches_the_cluster() {
    let (_c, base) = start_elasticsearch().await;
    let try_write = |index: &'static str| {
        let base = base.clone();
        async move {
            build_elasticsearch_sink(
                &cfg(&base, index, true),
                CancellationToken::new(),
                "p",
                Some(resolver()),
                &sinks::SinkOwnership::ephemeral(),
                &sinks::ResolvedSinkCreds::default(),
            )
            .unwrap()
            .send_batch(&[mk_event(
                Op::Create,
                json!({"id": 1, "amount": "1.00"}),
                json!(null),
                1,
            )])
            .await
        }
    };
    for rule in ["+*", "true", "+*,-df-w-*", "+df-w-1*,-df-w-*,+*"] {
        set_auto_create(&base, rule).await;
        let e = try_write("orders_rule").await.unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("fencing_unenforced"), "{rule}");
    }
    for rule in ["false", "-df-w-*,+*", "+logs-*,-df-*,+*", "+logs-*"] {
        set_auto_create(&base, rule).await;
        let index: &'static str =
            Box::leak(format!("orders_{}", rule.len()).into_boxed_str());
        assert!(try_write(index).await.is_ok(), "{rule}");
    }
    // The cluster's own verdict for a delete through a missing alias name,
    // against ours, rule by rule.
    for rule in [
        "-df-w-*,+*",
        "+*",
        "+logs-*",
        "+*,-df-w-*",
        "false",
        "-df-w-a*,+*",
        "+df-w-*",
    ] {
        set_auto_create(&base, rule).await;
        let name = format!("df-w-{}-{}", "a".repeat(32), "b".repeat(32));
        let body =
            format!("{{\"delete\":{{\"_index\":\"{name}\",\"_id\":\"1\"}}}}\n");
        let r = reqwest::Client::new()
            .post(format!("{base}/_bulk?require_alias=true"))
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .unwrap();
        let v: Value = r.json().await.unwrap();
        let refused = v["items"][0]["delete"]["error"]["type"]
            == "index_not_found_exception";
        let ours =
            sinks::elasticsearch::ownership::rules_forbid_prefix(rule, "df-w-")
                .is_ok();
        // Ours is conservative: it never claims fenced when the cluster
        // would auto-create.
        assert!(
            !ours || refused,
            "{rule}: ours={ours} cluster refused={refused}"
        );
        es_json(reqwest::Method::DELETE, &base, &format!("/{name}"), None)
            .await;
    }
    set_auto_create(&base, "-df-w-*,+*").await;
}

/// Claims never write `_meta`: unrelated `_meta` keys and user aliases
/// survive any number of claims.
#[tokio::test]
#[ignore]
async fn user_metadata_and_aliases_survive_claims() {
    let (_c, base) = start_elasticsearch().await;
    let index = "orders_meta";
    let ownership = sinks::SinkOwnership::ephemeral();
    let write = || async {
        build_elasticsearch_sink(
            &cfg(&base, index, true),
            CancellationToken::new(),
            "p",
            Some(resolver()),
            &ownership,
            &sinks::ResolvedSinkCreds::default(),
        )
        .unwrap()
        .send_batch(&[mk_event(
            Op::Create,
            json!({"id": 1, "amount": "1.00"}),
            json!(null),
            1,
        )])
        .await
        .unwrap();
    };
    write().await;
    // A careful user adds their own `_meta` key (keeping ours) and an alias.
    let (_, m) = es_json(
        reqwest::Method::GET,
        &base,
        &format!("/{index}/_mapping"),
        None,
    )
    .await;
    let mut meta = m[index]["mappings"]["_meta"].clone();
    meta["user_tool"] = json!({"owner": "team-x"});
    es_json(
        reqwest::Method::PUT,
        &base,
        &format!("/{index}/_mapping"),
        Some(json!({"_meta": meta})),
    )
    .await;
    es_json(
        reqwest::Method::POST,
        &base,
        "/_aliases",
        Some(json!({"actions": [{"add": {"index": index, "alias": "orders-read"}}]})),
    )
    .await;
    for _ in 0..3 {
        write().await;
    }
    let (_, m) = es_json(
        reqwest::Method::GET,
        &base,
        &format!("/{index}/_mapping"),
        None,
    )
    .await;
    assert_eq!(
        m[index]["mappings"]["_meta"]["user_tool"]["owner"],
        "team-x"
    );
    assert!(m[index]["mappings"]["_meta"]["deltaforge"].is_object());
    let (_, a) = es_json(
        reqwest::Method::GET,
        &base,
        &format!("/{index}/_alias"),
        None,
    )
    .await;
    assert!(a[index]["aliases"].get("orders-read").is_some());
    assert_eq!(internal_aliases(&base, index).await.len(), 1);
}

/// Claim and steady-state costs against a live cluster (printed; not a
/// pass/fail bound beyond "no extra remote call per batch").
#[tokio::test]
#[ignore]
async fn measure_claim_and_steady_state_costs() {
    let (_c, base) = start_elasticsearch().await;
    let index = "orders_measure";
    let ownership = sinks::SinkOwnership::ephemeral();
    let sink = || {
        build_elasticsearch_sink(
            &cfg(&base, index, true),
            CancellationToken::new(),
            "p",
            Some(resolver()),
            &ownership,
            &sinks::ResolvedSinkCreds::default(),
        )
        .unwrap()
    };
    let ev = |i: i64| {
        mk_event(
            Op::Create,
            json!({"id": i, "amount": "1.00"}),
            json!(null),
            i,
        )
    };
    let t = Instant::now();
    let s = sink();
    s.send_batch(&[ev(1)]).await.unwrap();
    let first = t.elapsed();
    let t = Instant::now();
    s.send_batch(&[ev(2)]).await.unwrap();
    let steady_one = t.elapsed();
    let t = Instant::now();
    sink().send_batch(&[ev(3)]).await.unwrap();
    let restart = t.elapsed();
    // Alias compare-and-swap alone.
    let alias = internal_aliases(&base, index).await.remove(0);
    let next = format!("{}{}", &alias[..alias.len() - 32], "c".repeat(32));
    let t = Instant::now();
    es_json(
        reqwest::Method::POST,
        &base,
        "/_aliases",
        Some(json!({"actions": [
            {"remove": {"index": index, "alias": alias, "must_exist": true}},
            {"add": {"index": index, "alias": next, "is_write_index": true}}
        ]})),
    )
    .await;
    let cas = t.elapsed();
    let s = sink();
    s.send_batch(&[ev(4)]).await.unwrap_err(); // fenced by the manual swap
    let s = {
        // A fresh owner for the throughput run.
        let o = sinks::SinkOwnership::ephemeral();
        build_elasticsearch_sink(
            &cfg(&base, "orders_measure2", true),
            CancellationToken::new(),
            "p",
            Some(resolver()),
            &o,
            &sinks::ResolvedSinkCreds::default(),
        )
        .unwrap()
    };
    s.send_batch(&[ev(1)]).await.unwrap();
    let batches = 50;
    let per = 1000;
    let t = Instant::now();
    for b in 0..batches {
        let batch: Vec<Event> =
            (0..per).map(|i| ev(10_000 + b * per + i)).collect();
        s.send_batch(&batch).await.unwrap();
    }
    let rate = (batches * per) as f64 / t.elapsed().as_secs_f64();
    println!(
        "ES claim costs: first_creation={first:?} steady_batch_of_1={steady_one:?} \
         restart_claim={restart:?} alias_cas={cas:?} steady_docs_per_s={rate:.0}"
    );
}
