//! JS Processor correctness tests.
//!
//! # Values at the JS boundary
//!
//! Scripts see the native rendering of each event (64-bit integers as exact
//! decimal strings). Outputs are re-typed against the input: an unchanged
//! value comes back as its exact original canonical value, a changed value
//! must parse as its column's declared type (from the batch's schema
//! context), and a field the schema does not declare is an untyped document.
//! Assertions read the native rendering through [`after`] / [`before`], or
//! the canonical values directly.

use deltaforge_core::canonical::schema::{
    ColumnType, ColumnTypes, ResolvedSchemas, SchemaContext,
    SchemaContextError, SchemaRef,
};
use deltaforge_core::canonical::{
    DbValue, Document, IntWidth, JsonText, Precision, TimestampValue, fixture,
};
use deltaforge_core::{
    BatchContext, CanonicalRow, CanonicalValue, Event, EventRouting,
    NativeOpts, Op, Processor, SourceInfo, SourcePosition, TypeMapping,
};
use pretty_assertions::assert_eq;
use processors::JsProcessor;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

/// The schema version every fixture event names.
const ORDERS_V1: &str = "orders-v1";

/// A schema context with one registered version, counting its lookups.
#[derive(Default)]
struct TestSchemas {
    lookups: AtomicUsize,
}

#[async_trait::async_trait]
impl SchemaContext for TestSchemas {
    async fn resolve(
        &self,
        r: &SchemaRef,
    ) -> Result<Arc<ColumnTypes>, SchemaContextError> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        if r.fingerprint != ORDERS_V1 {
            return Err(SchemaContextError::Conflict(format!(
                "{} is not registered",
                r.fingerprint
            )));
        }
        let int = |w| ColumnType::Int {
            width: w,
            unsigned: false,
        };
        Ok(Arc::new(ColumnTypes::new(
            ORDERS_V1.into(),
            Some(1),
            vec![
                ("id".into(), int(IntWidth::W32)),
                ("note".into(), ColumnType::Text),
                ("big".into(), int(IntWidth::W64)),
                ("n".into(), int(IntWidth::W32)),
                ("j".into(), ColumnType::Json),
                ("doc".into(), ColumnType::Text),
                (
                    "mood".into(),
                    ColumnType::Enum {
                        labels: vec!["happy".into(), "sad".into()],
                    },
                ),
                (
                    "tags".into(),
                    ColumnType::Set {
                        labels: vec!["a".into(), "b".into(), "c".into()],
                    },
                ),
                ("bin".into(), ColumnType::Binary { fixed: None }),
                (
                    "ts".into(),
                    ColumnType::Timestamp {
                        precision: Precision::Fixed(6),
                    },
                ),
            ],
        )))
    }
}

/// The batch context with the batch's schema versions resolved from the
/// test schema context (an unresolvable version is left out, as if the
/// pipeline could not resolve it).
async fn ctx_with(schemas: &TestSchemas, events: &[Event]) -> BatchContext {
    let mut versions = Vec::new();
    for r in events.iter().filter_map(SchemaRef::of) {
        if let Ok(t) = schemas.resolve(&r).await {
            versions.push((r, t));
        }
    }
    BatchContext::from_batch(events)
        .with_schemas(Arc::new(ResolvedSchemas::from_versions(versions)))
}

async fn ctx_for(events: &[Event]) -> BatchContext {
    ctx_with(&TestSchemas::default(), events).await
}

static TEST_ID: AtomicU32 = AtomicU32::new(1);
/// Distinct stable id per call (real events never share an id).
fn next_test_id() -> deltaforge_core::EventId {
    deltaforge_core::EventId::mysql_row_server(
        1,
        "t",
        TEST_ID.fetch_add(1, Ordering::Relaxed) as u64,
        0,
    )
}

/// The native rendering of an event's after image.
fn after(ev: &Event) -> serde_json::Value {
    ev.native_value(NativeOpts::default()).unwrap()["after"].clone()
}

/// The native rendering of an event's before image.
fn before(ev: &Event) -> serde_json::Value {
    ev.native_value(NativeOpts::default()).unwrap()["before"].clone()
}

fn new_event() -> Event {
    let mut ev = Event::new_row(
        next_test_id(),
        SourceInfo {
            version: "1.0.0".into(),
            connector: "mysql".into(),
            name: "test-db".into(),
            ts_ms: 1700000000000,
            db: "orders".into(),
            schema: None,
            table: "orders".into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Create,
        None,
        Some(fixture::row(json!({ "id": 1, "note": "original" }))),
        TypeMapping::V2,
        1700000000000,
        128,
    );
    ev.schema_version = Some(ORDERS_V1.into());
    ev.schema_sequence = Some(1);
    ev
}

fn new_update_event() -> Event {
    let mut ev = Event::new_row(
        next_test_id(),
        SourceInfo {
            version: "1.0.0".into(),
            connector: "mysql".into(),
            name: "test-db".into(),
            ts_ms: 1700000000000,
            db: "orders".into(),
            schema: None,
            table: "orders".into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Update,
        Some(fixture::row(json!({ "id": 1, "note": "before" }))),
        Some(fixture::row(json!({ "id": 1, "note": "after" }))),
        TypeMapping::V2,
        1700000000000,
        256,
    );
    ev.schema_version = Some(ORDERS_V1.into());
    ev.schema_sequence = Some(1);
    ev
}

fn new_delete_event() -> Event {
    let mut ev = Event::new_row(
        next_test_id(),
        SourceInfo {
            version: "1.0.0".into(),
            connector: "mysql".into(),
            name: "test-db".into(),
            ts_ms: 1700000000000,
            db: "orders".into(),
            schema: None,
            table: "orders".into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Delete,
        Some(fixture::row(json!({ "id": 1, "note": "deleted" }))),
        None,
        TypeMapping::V2,
        1700000000000,
        64,
    );
    ev.schema_version = Some(ORDERS_V1.into());
    ev.schema_sequence = Some(1);
    ev
}

// ============================================================================
// Basic Functionality
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn js_passthrough_returns_events_unchanged() {
    let js = r#"
        function processBatch(events) {
            return events;
        }
    "#;

    let proc = JsProcessor::new("passthrough".into(), js.into(), None)
        .expect("init ok");
    let ev = new_event();

    let events = vec![ev.clone()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].source.table, "orders");
    assert_eq!(out[0].op, Op::Create);
    // Event struct fields normalized correctly
    assert_eq!(out[0].ts_ms, 1700000000000_i64);
    // Row values come back exactly as they went in.
    assert_eq!(out[0].after, ev.after);
    assert_eq!(after(&out[0])["note"], "original");
}

#[tokio::test(flavor = "current_thread")]
async fn js_mutates_event_payload() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                if (ev.after) {
                    ev.after.note = "mutated";
                    ev.after.processed = true;
                }
            }
            return null; // use mutated input
        }
    "#;

    let proc =
        JsProcessor::new("mutate".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out.len(), 1);
    assert_eq!(after(&out[0])["note"], "mutated");
    assert_eq!(after(&out[0])["processed"], true);
}

#[tokio::test(flavor = "current_thread")]
async fn js_accesses_source_info_fields() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                ev.after.source_connector = ev.source.connector;
                ev.after.source_db = ev.source.db;
                ev.after.source_table = ev.source.table;
                ev.after.op_code = ev.op;
            }
            return null;
        }
    "#;

    let proc = JsProcessor::new("source_access".into(), js.into(), None)
        .expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    let after = after(&out[0]);
    assert_eq!(after["source_connector"], "mysql");
    assert_eq!(after["source_db"], "orders");
    assert_eq!(after["source_table"], "orders");
    assert_eq!(after["op_code"], "c"); // Op::Create serializes as "c"
}

// ============================================================================
// Operation Types
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn js_handles_update_with_before_and_after() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                if (ev.op === "u" && ev.before && ev.after) {
                    ev.after.had_before = true;
                }
            }
            return null;
        }
    "#;

    let proc =
        JsProcessor::new("update".into(), js.into(), None).expect("init ok");
    let events = vec![new_update_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out[0].op, Op::Update);
    assert!(out[0].before.is_some());
    assert_eq!(after(&out[0])["had_before"], true);
}

#[tokio::test(flavor = "current_thread")]
async fn js_handles_delete_operation() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                if (ev.op === "d" && ev.before) {
                    ev.before.tombstone = true;
                }
            }
            return null;
        }
    "#;

    let proc =
        JsProcessor::new("delete".into(), js.into(), None).expect("init ok");
    let events = vec![new_delete_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out[0].op, Op::Delete);
    assert!(out[0].after.is_none());
    assert_eq!(before(&out[0])["tombstone"], true);
}

#[tokio::test(flavor = "current_thread")]
async fn js_routes_by_op_type() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                const routing = {
                    "c": "inserts",
                    "u": "updates",
                    "d": "deletes",
                    "r": "snapshots"
                };
                if (ev.after) {
                    ev.after.routed_to = routing[ev.op] || "unknown";
                } else if (ev.before) {
                    ev.before.routed_to = routing[ev.op] || "unknown";
                }
            }
            return null;
        }
    "#;

    let proc =
        JsProcessor::new("router".into(), js.into(), None).expect("init ok");

    let create = new_event();
    let update = new_update_event();
    let delete = new_delete_event();

    let events = vec![create, update, delete];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(after(&out[0])["routed_to"], "inserts");
    assert_eq!(after(&out[1])["routed_to"], "updates");
    assert_eq!(before(&out[2])["routed_to"], "deletes");
}

// ============================================================================
// Batch Manipulation
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn js_can_add_events_to_batch() {
    let js = r#"
        function processBatch(events) {
            const out = [];
            for (const ev of events) {
                out.push(ev);
                // A new (audit) event must declare its parent via derive().
                const audit = JSON.parse(JSON.stringify(ev));
                audit.after = audit.after || {};
                audit.after.is_audit = true;
                out.push(derive(ev, audit));
            }
            return out;
        }
    "#;

    let proc =
        JsProcessor::new("expand".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let parent = events[0].event_id.unwrap();
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out.len(), 2);
    assert_eq!(after(&out[0])["note"], "original");
    assert_eq!(after(&out[1])["is_audit"], true);
    // 1:1 output keeps the parent id; the derived audit event is synthetic.
    assert_eq!(out[0].event_id, Some(parent));
    assert_eq!(
        out[1].event_id.unwrap().class(),
        deltaforge_core::EventClass::Syn
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_can_filter_events() {
    let js = r#"
        function processBatch(events) {
            return events.filter(ev => ev.op === "c");
        }
    "#;

    let proc =
        JsProcessor::new("filter".into(), js.into(), None).expect("init ok");
    let events = vec![new_event(), new_update_event(), new_delete_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].op, Op::Create);
}

#[tokio::test(flavor = "current_thread")]
async fn js_empty_return_drops_all() {
    let js = r#"
        function processBatch(events) {
            return [];
        }
    "#;

    let proc =
        JsProcessor::new("drop".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");
    assert_eq!(out.len(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn js_empty_input_batch() {
    let js = "function processBatch(events) { return events; }";
    let proc =
        JsProcessor::new("empty".into(), js.into(), None).expect("init ok");
    let events = vec![];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");
    assert_eq!(out.len(), 0);
}

// ============================================================================
// Return Value Handling
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn js_single_object_return_wrapped() {
    let js = r#"
        function processBatch(events) {
            const ev = events[0];
            ev.after.single = true;
            return ev; // not an array
        }
    "#;

    let proc =
        JsProcessor::new("single".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out.len(), 1);
    assert_eq!(after(&out[0])["single"], true);
}

#[tokio::test(flavor = "current_thread")]
async fn js_invalid_return_type_errors() {
    let js = r#"
        function processBatch(events) {
            return 42;
        }
    "#;

    let proc =
        JsProcessor::new("invalid".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let err = proc.process(events, &ctx).await.expect_err("should fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("unsupported type") || msg.contains("number"),
        "unexpected error: {msg}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_throw_propagates_error() {
    let js = r#"
        function processBatch(events) {
            throw new Error("intentional failure");
        }
    "#;

    let proc =
        JsProcessor::new("throw".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let err = proc.process(events, &ctx).await.expect_err("should fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("intentional") || msg.contains("JS processor threw"),
        "unexpected error: {msg}"
    );
}

// ============================================================================
// Runtime Behavior
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn js_runtime_persists_state_across_batches() {
    let js = r#"
        let batchCount = 0;
        function processBatch(events) {
            batchCount++;
            for (const ev of events) {
                ev.after = ev.after || {};
                ev.after.batch_number = batchCount;
            }
            return null;
        }
    "#;

    let proc =
        JsProcessor::new("stateful".into(), js.into(), None).expect("init ok");

    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out1 = proc.process(events, &ctx).await.expect("batch1 ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out2 = proc.process(events, &ctx).await.expect("batch2 ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out3 = proc.process(events, &ctx).await.expect("batch3 ok");

    // A new field is a document holding the number V8 handed back.
    assert_eq!(after(&out1[0])["batch_number"], 1.0);
    assert_eq!(after(&out2[0])["batch_number"], 2.0);
    assert_eq!(after(&out3[0])["batch_number"], 3.0);
}

#[tokio::test(flavor = "current_thread")]
async fn js_calls_rust_op_log() {
    let js = r#"
        function processBatch(events) {
            Deno.core.ops.op_log("hello-from-js");
            return events;
        }
    "#;

    let proc =
        JsProcessor::new("op_log".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");
    assert_eq!(out.len(), 1);
}

// ============================================================================
// Payload Transforms
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn js_transforms_payload_structure() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                const flat = {
                    _op: ev.op,
                    _source: ev.source.db + "." + ev.source.table,
                    ...ev.after
                };
                ev.after = flat;
            }
            return null;
        }
    "#;

    let proc =
        JsProcessor::new("transform".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    let after = after(&out[0]);
    assert_eq!(after["_op"], "c");
    assert_eq!(after["_source"], "orders.orders");
    // The spread original columns come back as their exact values.
    assert_eq!(after["id"], 1);
    assert_eq!(after["note"], "original");
}

// ============================================================================
// Number Type Behavior (documenting expected JS behavior)
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn js_payload_numbers_round_trip_exactly() {
    // An unmodified value comes back as its exact original canonical value:
    // integers do not become floats, 64-bit integers are not rounded.
    let js = r#"
        function processBatch(events) {
            return events; // no modification
        }
    "#;

    let proc = JsProcessor::new("float_test".into(), js.into(), None)
        .expect("init ok");

    let mut ev = new_event();
    ev.after = Some(fixture::row(json!({
        "int_field": 42,
        "float_field": 3.17,
        "big_int": 9007199254740991_i64  // JS MAX_SAFE_INTEGER, a 64-bit column
    })));
    let original = ev.after.clone();

    let events = vec![ev];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out[0].after, original, "canonical values are exact");
    let after = after(&out[0]);
    // A 32-bit integer renders as a JSON integer, not 42.0.
    assert_eq!(after["int_field"], json!(42));
    assert!(after["int_field"].is_i64());
    assert_eq!(after["float_field"], 3.17);
    // A 64-bit integer renders as its exact decimal string in v2.
    assert_eq!(after["big_int"], "9007199254740991");

    // Event struct fields are normalized back to i64
    assert_eq!(out[0].ts_ms, 1700000000000_i64);
}

// ============================================================================
// Initialization Errors
// ============================================================================

/// Wait until the processor's worker thread has exited (it crashes on an
/// invalid script), polling instead of assuming a fixed delay is enough.
fn worker_exits(proc: &JsProcessor) -> bool {
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if !proc.is_alive() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    false
}

#[test]
fn js_syntax_error_fails_initialization() {
    let js = "function processBatch(events { return events; }"; // missing )
    let result = JsProcessor::new("syntax".into(), js.into(), None);
    // May fail at init or when worker thread validates - either is acceptable
    // Worker thread crash makes is_alive() return false
    if let Ok(proc) = result {
        assert!(
            worker_exits(&proc),
            "processor should have crashed from syntax error"
        );
    }
}

#[test]
fn js_missing_process_batch_fails_initialization() {
    let js = "function wrongName(events) { return events; }";
    let result = JsProcessor::new("missing".into(), js.into(), None);
    if let Ok(proc) = result {
        assert!(
            worker_exits(&proc),
            "processor should have crashed from missing function"
        );
    }
}

// ============================================================================
// Dynamic Routing
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn js_sets_routing_topic() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                ev.route({ topic: "cdc." + ev.source.table });
            }
            return null;
        }
    "#;

    let proc =
        JsProcessor::new("route".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    let routing = out[0].routing.as_ref().expect("routing should be set");
    assert_eq!(routing.topic.as_deref(), Some("cdc.orders"));
    assert!(routing.key.is_none());
    assert!(routing.headers.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn js_sets_routing_key_and_headers() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                ev.route({
                    key: "k1",
                    headers: { "trace-id": "abc" }
                });
            }
            return null;
        }
    "#;

    let proc =
        JsProcessor::new("route_kh".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    let r = out[0].routing.as_ref().unwrap();
    assert_eq!(r.key.as_deref(), Some("k1"));
    assert_eq!(r.headers.as_ref().unwrap()["trace-id"], "abc");
}

#[tokio::test(flavor = "current_thread")]
async fn js_preserves_existing_routing() {
    let js = "function processBatch(events) { return events; }";

    let proc =
        JsProcessor::new("preserve".into(), js.into(), None).expect("init ok");
    let mut ev = new_event();
    ev.routing = Some(EventRouting {
        topic: Some("pre-existing".into()),
        ..Default::default()
    });

    let events = vec![ev];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");
    assert_eq!(
        out[0].routing.as_ref().unwrap().topic.as_deref(),
        Some("pre-existing")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_clone_gets_separate_routing() {
    let js = r#"
        function processBatch(events) {
            const out = [];
            for (const ev of events) {
                ev.route({ topic: "live" });
                out.push(ev);

                const clone = JSON.parse(JSON.stringify(ev));
                route(clone, { topic: "audit" });
                out.push(derive(ev, clone));
            }
            return out;
        }
    "#;

    let proc = JsProcessor::new("clone_route".into(), js.into(), None)
        .expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out.len(), 2);
    assert_eq!(
        out[0].routing.as_ref().unwrap().topic.as_deref(),
        Some("live")
    );
    assert_eq!(
        out[1].routing.as_ref().unwrap().topic.as_deref(),
        Some("audit")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_no_route_call_means_no_routing() {
    let js = "function processBatch(events) { return events; }";

    let proc =
        JsProcessor::new("no_route".into(), js.into(), None).expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");
    assert!(out[0].routing.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn js_filter_drops_routed_events() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                ev.route({ topic: "will-be-dropped" });
            }
            return []; // drop all
        }
    "#;

    let proc = JsProcessor::new("filter_drop".into(), js.into(), None)
        .expect("init ok");
    let events = vec![new_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");
    assert_eq!(out.len(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn js_route_overwrites_existing_routing() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                ev.route({ topic: "new-topic" });
            }
            return null;
        }
    "#;

    let proc =
        JsProcessor::new("overwrite".into(), js.into(), None).expect("init ok");
    let mut ev = new_event();
    ev.routing = Some(EventRouting {
        topic: Some("old-topic".into()),
        key: Some("old-key".into()),
        ..Default::default()
    });

    let events = vec![ev];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");
    let r = out[0].routing.as_ref().unwrap();
    assert_eq!(r.topic.as_deref(), Some("new-topic"));
    // ev.route() replaces the whole routing — key from old routing is gone
    assert!(r.key.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn js_conditional_routing_by_payload() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                if (ev.after && ev.after.id > 5) {
                    ev.route({ topic: "high-id" });
                } else {
                    ev.route({ topic: "low-id" });
                }
            }
            return null;
        }
    "#;

    let proc =
        JsProcessor::new("cond".into(), js.into(), None).expect("init ok");

    let mut low = new_event();
    low.after = Some(fixture::row(json!({"id": 2})));
    let mut high = new_event();
    high.after = Some(fixture::row(json!({"id": 10})));

    let events = vec![low, high];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(
        out[0].routing.as_ref().unwrap().topic.as_deref(),
        Some("low-id")
    );
    assert_eq!(
        out[1].routing.as_ref().unwrap().topic.as_deref(),
        Some("high-id")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_route_only_some_events() {
    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                if (ev.op === "d") {
                    ev.route({ topic: "deletes" });
                }
                // creates get no routing — sinks use their default
            }
            return null;
        }
    "#;

    let proc =
        JsProcessor::new("partial".into(), js.into(), None).expect("init ok");
    let events = vec![new_event(), new_delete_event()];
    let ctx = ctx_for(&events).await;
    let out = proc.process(events, &ctx).await.expect("ok");

    assert!(out[0].routing.is_none()); // create: no route() call
    assert_eq!(
        out[1].routing.as_ref().unwrap().topic.as_deref(),
        Some("deletes")
    );
}

// ============================================================================
// Explicit synthetic lineage (derive) contract
// ============================================================================

use deltaforge_core::{EventClass, EventId};

/// A row event carrying a specific provisional stable id.
fn event_with_id(id: EventId) -> Event {
    let mut ev = new_event();
    ev.event_id = Some(id);
    ev
}

fn parent_id(row: u32) -> EventId {
    EventId::mysql_row_server(1, "mysql-bin.000001", 100, row)
}

async fn run(js: &str, events: Vec<Event>) -> anyhow::Result<Vec<Event>> {
    let proc = JsProcessor::new("t".into(), js.into(), None).unwrap();
    let ctx = ctx_for(&events).await;
    proc.process(events, &ctx).await
}

#[tokio::test(flavor = "current_thread")]
async fn js_one_to_one_retains_parent_id() {
    let p = parent_id(0);
    let out = run(
        "function processBatch(e){ return e; }",
        vec![event_with_id(p)],
    )
    .await
    .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].event_id, Some(p));
}

#[tokio::test(flavor = "current_thread")]
async fn js_derive_mints_synthetic_id() {
    let p = parent_id(0);
    let js = r#"function processBatch(e){
        return [e[0], derive(e[0], { ...e[0], after: { audit: true } })];
    }"#;
    let out = run(js, vec![event_with_id(p)]).await.unwrap();
    assert_eq!(out.len(), 2);
    // 1:1 output keeps the parent id.
    assert_eq!(out[0].event_id, Some(p));
    // Derived output gets a distinct synthetic id.
    let syn = out[1].event_id.unwrap();
    assert_eq!(syn.class(), EventClass::Syn);
    assert_ne!(syn, p);
}

#[tokio::test(flavor = "current_thread")]
async fn js_new_event_without_lineage_is_rejected() {
    let js = r#"function processBatch(e){
        let o = { ...e[0] }; delete o.__df_id; o.after = { x: 1 };
        return [e[0], o];
    }"#;
    let err = run(js, vec![event_with_id(parent_id(0))])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("derive"),
        "error should guide to derive(): {err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_cross_batch_parent_is_rejected() {
    // A derive() parent id that is not in the input batch.
    let bogus = EventId::mysql_row_server(9, "other", 1, 0).to_string();
    let js = format!(
        r#"function processBatch(e){{ return [e[0], derive("{bogus}", {{...e[0]}})]; }}"#
    );
    let err = run(&js, vec![event_with_id(parent_id(0))])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not an input event"), "{err}");
}

#[tokio::test(flavor = "current_thread")]
async fn js_duplicate_retained_parent_is_rejected() {
    // Two outputs retain the same input id — fan-out must derive() extras.
    let js = r#"function processBatch(e){ return [e[0], e[0]]; }"#;
    let err = run(js, vec![event_with_id(parent_id(0))])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("more than one output"), "{err}");
}

#[tokio::test(flavor = "current_thread")]
async fn js_ordinals_are_deterministic_per_parent() {
    let p = parent_id(0);
    let js = r#"function processBatch(e){
        return [derive(e[0], {...e[0], n:1}), derive(e[0], {...e[0], n:2})];
    }"#;
    let a = run(js, vec![event_with_id(p)]).await.unwrap();
    let b = run(js, vec![event_with_id(p)]).await.unwrap();
    let a0 = a[0].event_id.unwrap();
    let a1 = a[1].event_id.unwrap();
    // Two derived outputs from the same parent get distinct (ordinal 0 vs 1) ids.
    assert_ne!(a0, a1);
    // Deterministic across runs.
    assert_eq!(a0, b[0].event_id.unwrap());
    assert_eq!(a1, b[1].event_id.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn js_interleaved_parents_attributed_correctly() {
    let (p0, p1) = (parent_id(0), parent_id(1));
    let js = r#"function processBatch(e){
        return [derive(e[1], {...e[1]}), derive(e[0], {...e[0]})];
    }"#;
    let out = run(js, vec![event_with_id(p0), event_with_id(p1)])
        .await
        .unwrap();
    let from_p1 = out[0].event_id.unwrap();
    let from_p0 = out[1].event_id.unwrap();
    // Each derived id is tied to its DECLARED parent (not output position):
    // recompute the exact synthetic id from that parent + the processor digest.
    let digest =
        processors::digest::js_digest(js, &None::<deltaforge_config::Limits>);
    assert_eq!(from_p0, EventId::synthetic(&p0, &digest, 0));
    assert_eq!(from_p1, EventId::synthetic(&p1, &digest, 0));
    assert_ne!(from_p0, from_p1);
}

#[tokio::test(flavor = "current_thread")]
async fn js_reserved_lineage_metadata_never_leaks() {
    // A 1:1 passthrough and a derived output: neither may leak __df_id/__df_parent
    // into the emitted event (payload or serialized form).
    let p = parent_id(0);
    let js = r#"function processBatch(e){
        return [e[0], derive(e[0], { ...e[0], after: { x: 1 } })];
    }"#;
    let out = run(js, vec![event_with_id(p)]).await.unwrap();
    assert_eq!(out.len(), 2);
    for ev in &out {
        let json = ev.native_value(NativeOpts::default()).unwrap().to_string();
        assert!(!json.contains("__df_id"), "__df_id leaked: {json}");
        assert!(!json.contains("__df_parent"), "__df_parent leaked: {json}");
        if let Some(after) = ev.after.as_ref() {
            assert!(after.get("__df_id").is_none());
            assert!(after.get("__df_parent").is_none());
        }
        // Every emitted event carries a stable EventId.
        assert!(ev.event_id.is_some(), "emitted event missing event_id");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn js_forged_out_of_batch_retained_id_is_rejected() {
    // A script cannot forge retention of an id that isn't in the input batch.
    let forged = EventId::mysql_row_server(7, "other", 9, 0).to_string();
    let js = format!(
        r#"function processBatch(e){{
            let o = {{ ...e[0] }}; o.__df_id = "{forged}";
            return [o];
        }}"#
    );
    let err = run(&js, vec![event_with_id(parent_id(0))])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not an input event"), "{err}");
}

// ============================================================================
// Typed boundary: re-typing script outputs against the input's columns
// ============================================================================

const IDENTITY: &str = "function processBatch(e){ return e; }";

fn event_with_after(columns: Vec<(&str, CanonicalValue)>) -> Event {
    let mut ev = new_event();
    ev.after = Some(
        CanonicalRow::new(
            columns
                .into_iter()
                .map(|(n, v)| (n.to_string(), v))
                .collect(),
        )
        .unwrap(),
    );
    ev
}

fn db(v: DbValue) -> CanonicalValue {
    CanonicalValue::Db(v)
}

#[tokio::test(flavor = "current_thread")]
async fn js_64_bit_integer_beyond_2_pow_53_passes_through_exactly() {
    // 2^53 + 1: a double would round it to 2^53.
    let big = db(DbValue::int(9_007_199_254_740_993, IntWidth::W64).unwrap());
    let ev = event_with_after(vec![("big", big.clone())]);
    let out = run(IDENTITY, vec![ev]).await.unwrap();
    assert_eq!(out[0].after.as_ref().unwrap().get("big"), Some(&big));
    assert_eq!(after(&out[0])["big"], "9007199254740993");
}

#[tokio::test(flavor = "current_thread")]
async fn js_changed_int_column_must_parse_as_its_type() {
    // A non-integer in a 32-bit int column fails the batch.
    let err = run(
        "function processBatch(e){ e[0].after.id = 1.5; return e; }",
        vec![new_event()],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("after.id"), "{err}");

    // Another integer is re-typed as the column's type, exactly.
    let out = run(
        "function processBatch(e){ e[0].after.id = 7; return e; }",
        vec![new_event()],
    )
    .await
    .unwrap();
    assert_eq!(
        out[0].after.as_ref().unwrap().get("id"),
        Some(&db(DbValue::int(7, IntWidth::W32).unwrap()))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_new_field_is_an_extension_never_a_column() {
    let out = run(
        "function processBatch(e){ e[0].after.meta = { a: { b: 'x' } }; return e; }",
        vec![new_event()],
    )
    .await
    .unwrap();
    let row = out[0].after.as_ref().unwrap();
    assert_eq!(row.get("meta"), None, "never a database column");
    assert_eq!(
        row.extensions()["meta"],
        Document::new(json!({ "a": { "b": "x" } }))
    );
    // The original columns are untouched, and the rendering merges both.
    assert_eq!(
        row.get("id"),
        Some(&db(DbValue::int(1, IntWidth::W32).unwrap()))
    );
    assert_eq!(after(&out[0])["meta"]["a"]["b"], "x");
}

#[tokio::test(flavor = "current_thread")]
async fn js_new_field_of_a_schema_less_event_is_a_document_column() {
    let mut ev = new_event();
    ev.schema_version = None;
    ev.after = Some(fixture::row(json!({"payload": {"k": 1}})));
    let out = run(
        "function processBatch(e){ e[0].after.extra = [1]; return e; }",
        vec![ev],
    )
    .await
    .unwrap();
    let row = out[0].after.as_ref().unwrap();
    assert_eq!(
        row.get("extra"),
        Some(&CanonicalValue::Document(Document::new(json!([1]))))
    );
    assert!(row.extensions().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn js_a_declared_column_the_image_lacks_is_retyped_not_an_extension() {
    // `note` is declared but absent from this (minimal) image.
    let ev = event_with_after(vec![(
        "id",
        db(DbValue::int(1, IntWidth::W32).unwrap()),
    )]);
    let out = run(
        "function processBatch(e){ e[0].after.note = 'n'; return e; }",
        vec![ev],
    )
    .await
    .unwrap();
    let row = out[0].after.as_ref().unwrap();
    assert_eq!(row.get("note"), Some(&db(DbValue::Text("n".into()))));
    assert!(row.extensions().is_empty());
    // A declared column cannot take an untyped value.
    let ev = event_with_after(vec![(
        "id",
        db(DbValue::int(1, IntWidth::W32).unwrap()),
    )]);
    assert!(
        run(
            "function processBatch(e){ e[0].after.n = {a: 1}; return e; }",
            vec![ev]
        )
        .await
        .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_extensions_survive_and_change_across_processor_chains() {
    let first = run("function processBatch(e){ e[0].after.x = {v: 1}; e[0].after.y = 2; return e; }", vec![new_event()])
        .await
        .unwrap();
    // A second chain sees the extensions, keeps one, changes one, adds one.
    let second = run(
        "function processBatch(e){ e[0].after.y = 3; e[0].after.z = 'new'; return e; }",
        first,
    )
    .await
    .unwrap();
    let ext = second[0].after.as_ref().unwrap().extensions().clone();
    assert_eq!(ext["x"], Document::new(json!({"v": 1})));
    assert_eq!(ext["y"], Document::new(json!(3)));
    assert_eq!(ext["z"], Document::new(json!("new")));
    // Removing an extension removes it.
    let third = run(
        "function processBatch(e){ delete e[0].after.x; return e; }",
        second,
    )
    .await
    .unwrap();
    assert!(
        third[0]
            .after
            .as_ref()
            .unwrap()
            .extensions()
            .get("x")
            .is_none()
    );
    // A derived event keeps its parent's typing: its extensions are extensions.
    let derived = run(
        "function processBatch(e){ const d = derive(e[0], JSON.parse(JSON.stringify(e[0]))); d.after.w = true; return [e[0], d]; }",
        vec![new_event()],
    )
    .await
    .unwrap();
    assert_eq!(
        derived[1].after.as_ref().unwrap().extensions()["w"],
        Document::new(json!(true))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_conflicting_extension_paths_fail() {
    let err = run(
        "function processBatch(e){ e[0].after['a'] = 1; e[0].after['a.b'] = 2; return e; }",
        vec![new_event()],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("conflicts"), "{err}");
}

#[tokio::test(flavor = "current_thread")]
async fn js_a_new_field_with_missing_schema_context_fails() {
    let mut ev = new_event();
    ev.schema_version = Some("orders-unknown".into());
    let err = run(
        "function processBatch(e){ e[0].after.q = 1; return e; }",
        vec![ev],
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("schema context is missing"),
        "{err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_a_null_column_takes_a_value_of_its_declared_type() {
    let ev = event_with_after(vec![("n", CanonicalValue::Null)]);
    let out = run(
        "function processBatch(e){ e[0].after.n = 5; return e; }",
        vec![ev],
    )
    .await
    .unwrap();
    assert_eq!(
        out[0].after.as_ref().unwrap().get("n"),
        Some(&db(DbValue::int(5, IntWidth::W32).unwrap()))
    );
    let ev = event_with_after(vec![("n", CanonicalValue::Null)]);
    let err = run(
        "function processBatch(e){ e[0].after.n = 'x'; return e; }",
        vec![ev],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("after.n"), "{err}");
}

#[tokio::test(flavor = "current_thread")]
async fn js_an_unchanged_toast_column_takes_a_value_of_its_declared_type() {
    let ev = event_with_after(vec![
        ("id", db(DbValue::int(1, IntWidth::W32).unwrap())),
        ("doc", CanonicalValue::Unchanged),
    ]);
    // Untouched, it stays unchanged.
    let out = run(IDENTITY, vec![ev]).await.unwrap();
    assert_eq!(
        out[0].after.as_ref().unwrap().get("doc"),
        Some(&CanonicalValue::Unchanged)
    );
    // Replaced, it is a value of the column's type.
    let ev = event_with_after(vec![("doc", CanonicalValue::Unchanged)]);
    let out = run(
        "function processBatch(e){ e[0].after.doc = 'new text'; return e; }",
        vec![ev],
    )
    .await
    .unwrap();
    assert_eq!(
        out[0].after.as_ref().unwrap().get("doc"),
        Some(&db(DbValue::Text("new text".into())))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_enum_and_set_changes_validate_against_their_definition() {
    use deltaforge_core::canonical::{EnumValue, SetValue};
    let def: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
    let ev = event_with_after(vec![
        (
            "mood",
            db(DbValue::Enum(EnumValue::mysql(1, "happy".into()).unwrap())),
        ),
        (
            "tags",
            db(DbValue::Set(SetValue::from_bitmask(1, &def).unwrap())),
        ),
    ]);
    let out = run(
        "function processBatch(e){ e[0].after.mood = 'sad'; e[0].after.tags = 'a,c'; return e; }",
        vec![ev],
    )
    .await
    .unwrap();
    let row = out[0].after.as_ref().unwrap();
    assert_eq!(
        row.get("mood"),
        Some(&db(DbValue::Enum(
            EnumValue::mysql(2, "sad".into()).unwrap()
        )))
    );
    assert_eq!(
        row.get("tags"),
        Some(&db(DbValue::Set(
            SetValue::from_bitmask(0b101, &def).unwrap()
        )))
    );
    for script in [
        "function processBatch(e){ e[0].after.mood = 'angry'; return e; }",
        "function processBatch(e){ e[0].after.tags = 'a,z'; return e; }",
    ] {
        let ev = event_with_after(vec![
            (
                "mood",
                db(DbValue::Enum(EnumValue::mysql(1, "happy".into()).unwrap())),
            ),
            (
                "tags",
                db(DbValue::Set(SetValue::from_bitmask(1, &def).unwrap())),
            ),
        ]);
        assert!(run(script, vec![ev]).await.is_err(), "{script}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn js_missing_or_conflicting_schema_context_fails_explicitly() {
    let script = "function processBatch(e){ e[0].after.id = 2; return e; }";
    // The event names a version that does not resolve.
    let mut ev = new_event();
    ev.schema_version = Some("orders-unknown".into());
    let err = run(script, vec![ev]).await.unwrap_err();
    assert!(err.to_string().contains("did not resolve"), "{err}");
    // The pipeline provides no schema context.
    let proc = JsProcessor::new("t".into(), script.into(), None).unwrap();
    let events = vec![new_event()];
    let ctx = BatchContext::from_batch(&events);
    let err = proc.process(events, &ctx).await.unwrap_err();
    assert!(err.to_string().contains("no schema context"), "{err}");
    // The event's value is not of the declared type.
    let ev = event_with_after(vec![("id", db(DbValue::Text("x".into())))]);
    let err = run(script, vec![ev]).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("conflicts with its schema context"),
        "{err}"
    );
    // A changed database column of an event naming no schema version.
    let mut ev = new_event();
    ev.schema_version = None;
    assert!(run(script, vec![ev]).await.is_err());
    // An undeclared column of a resolved version is a conflict, never a
    // document.
    let ev =
        event_with_after(vec![("undeclared", db(DbValue::Text("x".into())))]);
    let err = run(
        "function processBatch(e){ e[0].after.undeclared = 'y'; return e; }",
        vec![ev],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("does not declare"), "{err}");
}

#[tokio::test(flavor = "current_thread")]
async fn resolved_schemas_resolve_each_version_once_and_validate_values() {
    let schemas = TestSchemas::default();
    let events: Vec<Event> = (0..5).map(|_| new_event()).collect();
    let r = ResolvedSchemas::resolve(&schemas, &events).await.unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(schemas.lookups.load(Ordering::Relaxed), 1);
    // A missing version fails the batch before any processor runs.
    let mut ev = new_event();
    ev.schema_version = Some("orders-unknown".into());
    assert!(ResolvedSchemas::resolve(&schemas, &[ev]).await.is_err());
    // A value the version does not admit is a conflict.
    let ev = event_with_after(vec![("id", db(DbValue::Text("x".into())))]);
    assert!(ResolvedSchemas::resolve(&schemas, &[ev]).await.is_err());
    // So is a column it does not declare (a document is not a column).
    let ev = event_with_after(vec![("zzz", db(DbValue::Text("x".into())))]);
    assert!(ResolvedSchemas::resolve(&schemas, &[ev]).await.is_err());
    // Even a document: undeclared data belongs in the extensions.
    let ev = event_with_after(vec![(
        "zzz",
        CanonicalValue::Document(Document::new(json!(1))),
    )]);
    assert!(ResolvedSchemas::resolve(&schemas, &[ev]).await.is_err());
    let mut ev = new_event();
    ev.after
        .as_mut()
        .unwrap()
        .set_extension("zzz", Document::new(json!(1)))
        .unwrap();
    assert!(ResolvedSchemas::resolve(&schemas, &[ev]).await.is_ok());
}

/// Consecutive events of one version reuse its resolution, but every event
/// is still checked, and an event of another relation or version is
/// resolved on its own.
#[tokio::test]
async fn resolution_reuses_a_version_only_for_the_same_reference() {
    let schemas = TestSchemas::default();
    let mut other_table = new_event();
    other_table.source.table = "orders_archive".into();
    let events = vec![new_event(), new_event(), other_table, new_event()];
    let r = ResolvedSchemas::resolve(&schemas, &events).await.unwrap();
    assert_eq!(r.len(), 2, "a version per relation");
    // The fourth event is the first's version again: found, not resolved.
    assert_eq!(schemas.lookups.load(Ordering::Relaxed), 2);
    // A later event of the same version is still checked.
    let bad = event_with_after(vec![("id", db(DbValue::Text("x".into())))]);
    assert!(
        ResolvedSchemas::resolve(&schemas, &[new_event(), bad])
            .await
            .is_err()
    );
    // And one of another version is resolved (and refused) on its own.
    let mut unknown = new_event();
    unknown.schema_version = Some("orders-unknown".into());
    assert!(
        ResolvedSchemas::resolve(&schemas, &[new_event(), unknown])
            .await
            .is_err()
    );
    let mut other_sequence = new_event();
    other_sequence.schema_sequence = Some(2);
    let r = ResolvedSchemas::resolve(&schemas, &[new_event(), other_sequence])
        .await;
    assert!(r.is_err(), "sequence 2 conflicts with the registered 1");
}

#[tokio::test(flavor = "current_thread")]
async fn js_sql_null_and_json_null_stay_distinct() {
    let json_null = db(DbValue::Json(JsonText::new("null".into()).unwrap()));
    let ev = event_with_after(vec![
        ("j", json_null.clone()),
        ("n", CanonicalValue::Null),
    ]);
    let out = run(IDENTITY, vec![ev]).await.unwrap();
    let row = out[0].after.as_ref().unwrap();
    assert_eq!(row.get("j"), Some(&json_null));
    assert_eq!(row.get("n"), Some(&CanonicalValue::Null));
}

#[tokio::test(flavor = "current_thread")]
async fn js_binary_and_temporal_values_pass_through_unchanged() {
    let bin = db(DbValue::Binary(vec![0, 255]));
    let ts = db(DbValue::Timestamp(
        TimestampValue::instant(1_700_000_000_000_000, Precision::Fixed(6))
            .unwrap(),
    ));
    let ev = event_with_after(vec![("bin", bin.clone()), ("ts", ts.clone())]);
    let original = ev.after.clone();
    let out = run(IDENTITY, vec![ev]).await.unwrap();
    assert_eq!(out[0].after, original);
    let row = out[0].after.as_ref().unwrap();
    assert_eq!(row.get("bin"), Some(&bin));
    assert_eq!(row.get("ts"), Some(&ts));
}
