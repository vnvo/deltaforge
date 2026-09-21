//! JS Processor correctness tests.
//!
//! # Number Type Behavior
//!
//! JavaScript represents all numbers as f64. When events pass through the
//! JS processor, numeric values undergo conversion even without modification:
//!
//! - Event struct fields (ts_ms, size_bytes): Normalized back to i64
//! - Payload fields (before/after): Remain as floats after JS round-trip
//!
//! Tests marked with `// Note: payload integers become floats` demonstrate
//! this expected behavior.

use deltaforge_core::{
    BatchContext, Event, EventRouting, Op, Processor, SourceInfo,
    SourcePosition,
};
use pretty_assertions::assert_eq;
use processors::JsProcessor;
use serde_json::json;
use std::sync::atomic::{AtomicU32, Ordering};

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

fn new_event() -> Event {
    Event::new_row(
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
        Some(json!({ "id": 1, "note": "original" })),
        1700000000000,
        128,
    )
}

fn new_update_event() -> Event {
    Event::new_row(
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
        Some(json!({ "id": 1, "note": "before" })),
        Some(json!({ "id": 1, "note": "after" })),
        1700000000000,
        256,
    )
}

fn new_delete_event() -> Event {
    Event::new_row(
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
        Some(json!({ "id": 1, "note": "deleted" })),
        None,
        1700000000000,
        64,
    )
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
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].source.table, "orders");
    assert_eq!(out[0].op, Op::Create);
    // Event struct fields normalized correctly
    assert_eq!(out[0].ts_ms, 1700000000000_i64);
    // Note: payload integers become floats after JS round-trip
    assert_eq!(out[0].after.as_ref().unwrap()["note"], "original");
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
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].after.as_ref().unwrap()["note"], "mutated");
    assert_eq!(out[0].after.as_ref().unwrap()["processed"], true);
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
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");

    let after = out[0].after.as_ref().unwrap();
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
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out[0].op, Op::Update);
    assert!(out[0].before.is_some());
    assert_eq!(out[0].after.as_ref().unwrap()["had_before"], true);
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
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out[0].op, Op::Delete);
    assert!(out[0].after.is_none());
    assert_eq!(out[0].before.as_ref().unwrap()["tombstone"], true);
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
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out[0].after.as_ref().unwrap()["routed_to"], "inserts");
    assert_eq!(out[1].after.as_ref().unwrap()["routed_to"], "updates");
    assert_eq!(out[2].before.as_ref().unwrap()["routed_to"], "deletes");
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
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out.len(), 2);
    assert_eq!(out[0].after.as_ref().unwrap()["note"], "original");
    assert_eq!(out[1].after.as_ref().unwrap()["is_audit"], true);
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");
    assert_eq!(out.len(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn js_empty_input_batch() {
    let js = "function processBatch(events) { return events; }";
    let proc =
        JsProcessor::new("empty".into(), js.into(), None).expect("init ok");
    let events = vec![];
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].after.as_ref().unwrap()["single"], true);
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
    let out1 = proc.process(events, &ctx).await.expect("batch1 ok");
    let events = vec![new_event()];
    let ctx = BatchContext::from_batch(&events);
    let out2 = proc.process(events, &ctx).await.expect("batch2 ok");
    let events = vec![new_event()];
    let ctx = BatchContext::from_batch(&events);
    let out3 = proc.process(events, &ctx).await.expect("batch3 ok");

    // Note: batch_number is float due to JS number handling
    assert_eq!(out1[0].after.as_ref().unwrap()["batch_number"], 1.0);
    assert_eq!(out2[0].after.as_ref().unwrap()["batch_number"], 2.0);
    assert_eq!(out3[0].after.as_ref().unwrap()["batch_number"], 3.0);
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");

    let after = out[0].after.as_ref().unwrap();
    assert_eq!(after["_op"], "c");
    assert_eq!(after["_source"], "orders.orders");
}

// ============================================================================
// Number Type Behavior (documenting expected JS behavior)
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn js_payload_integers_become_floats() {
    // This test documents the expected behavior: payload integers become floats
    // after JS round-trip because V8 represents all numbers as f64.
    let js = r#"
        function processBatch(events) {
            return events; // no modification
        }
    "#;

    let proc = JsProcessor::new("float_test".into(), js.into(), None)
        .expect("init ok");

    let mut ev = new_event();
    ev.after = Some(json!({
        "int_field": 42,
        "float_field": 3.17,
        "big_int": 9007199254740991_i64  // JS MAX_SAFE_INTEGER
    }));

    let events = vec![ev];
    let ctx = BatchContext::from_batch(&events);
    let out = proc.process(events, &ctx).await.expect("ok");
    let after = out[0].after.as_ref().unwrap();

    // Integers become floats (42 -> 42.0)
    assert!(after["int_field"].is_number());
    // Original floats preserved
    assert_eq!(after["float_field"], 3.17);
    // Large integers within safe range preserved as floats
    assert!(after["big_int"].is_number());

    // Event struct fields are normalized back to i64
    assert_eq!(out[0].ts_ms, 1700000000000_i64);
}

// ============================================================================
// Initialization Errors
// ============================================================================

#[test]
fn js_syntax_error_fails_initialization() {
    let js = "function processBatch(events { return events; }"; // missing )
    let result = JsProcessor::new("syntax".into(), js.into(), None);
    // May fail at init or when worker thread validates - either is acceptable
    // Worker thread crash makes is_alive() return false
    if let Ok(proc) = result {
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !proc.is_alive(),
            "processor should have crashed from syntax error"
        );
    }
}

#[test]
fn js_missing_process_batch_fails_initialization() {
    let js = "function wrongName(events) { return events; }";
    let result = JsProcessor::new("missing".into(), js.into(), None);
    if let Ok(proc) = result {
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !proc.is_alive(),
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
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
    low.after = Some(json!({"id": 2}));
    let mut high = new_event();
    high.after = Some(json!({"id": 10}));

    let events = vec![low, high];
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
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
    let ctx = BatchContext::from_batch(&events);
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
        let json = serde_json::to_string(ev).unwrap();
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
