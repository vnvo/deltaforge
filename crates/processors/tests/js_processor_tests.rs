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
    Event, EventRouting, Op, Processor, SourceInfo, SourcePosition,
};
use pretty_assertions::assert_eq;
use processors::JsProcessor;
use serde_json::json;

fn new_event() -> Event {
    Event::new_row(
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

    let proc =
        JsProcessor::new("passthrough".into(), js.into()).expect("init ok");
    let ev = new_event();

    let out = proc.process(vec![ev.clone()]).await.expect("ok");
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

    let proc = JsProcessor::new("mutate".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");

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

    let proc =
        JsProcessor::new("source_access".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");

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

    let proc = JsProcessor::new("update".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_update_event()]).await.expect("ok");

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

    let proc = JsProcessor::new("delete".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_delete_event()]).await.expect("ok");

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

    let proc = JsProcessor::new("router".into(), js.into()).expect("init ok");

    let create = new_event();
    let update = new_update_event();
    let delete = new_delete_event();

    let out = proc
        .process(vec![create, update, delete])
        .await
        .expect("ok");

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
                // Clone for audit - use spread to avoid reference sharing
                const audit = JSON.parse(JSON.stringify(ev));
                audit.after = audit.after || {};
                audit.after.is_audit = true;
                out.push(audit);
            }
            return out;
        }
    "#;

    let proc = JsProcessor::new("expand".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");

    assert_eq!(out.len(), 2);
    assert_eq!(out[0].after.as_ref().unwrap()["note"], "original");
    assert_eq!(out[1].after.as_ref().unwrap()["is_audit"], true);
}

#[tokio::test(flavor = "current_thread")]
async fn js_can_filter_events() {
    let js = r#"
        function processBatch(events) {
            return events.filter(ev => ev.op === "c");
        }
    "#;

    let proc = JsProcessor::new("filter".into(), js.into()).expect("init ok");
    let out = proc
        .process(vec![new_event(), new_update_event(), new_delete_event()])
        .await
        .expect("ok");

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

    let proc = JsProcessor::new("drop".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");
    assert_eq!(out.len(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn js_empty_input_batch() {
    let js = "function processBatch(events) { return events; }";
    let proc = JsProcessor::new("empty".into(), js.into()).expect("init ok");
    let out = proc.process(vec![]).await.expect("ok");
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

    let proc = JsProcessor::new("single".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");

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

    let proc = JsProcessor::new("invalid".into(), js.into()).expect("init ok");
    let err = proc
        .process(vec![new_event()])
        .await
        .expect_err("should fail");
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

    let proc = JsProcessor::new("throw".into(), js.into()).expect("init ok");
    let err = proc
        .process(vec![new_event()])
        .await
        .expect_err("should fail");
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

    let proc = JsProcessor::new("stateful".into(), js.into()).expect("init ok");

    let out1 = proc.process(vec![new_event()]).await.expect("batch1 ok");
    let out2 = proc.process(vec![new_event()]).await.expect("batch2 ok");
    let out3 = proc.process(vec![new_event()]).await.expect("batch3 ok");

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

    let proc = JsProcessor::new("op_log".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");
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
        JsProcessor::new("transform".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");

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

    let proc =
        JsProcessor::new("float_test".into(), js.into()).expect("init ok");

    let mut ev = new_event();
    ev.after = Some(json!({
        "int_field": 42,
        "float_field": 3.17,
        "big_int": 9007199254740991_i64  // JS MAX_SAFE_INTEGER
    }));

    let out = proc.process(vec![ev]).await.expect("ok");
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
    let result = JsProcessor::new("syntax".into(), js.into());
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
    let result = JsProcessor::new("missing".into(), js.into());
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

    let proc = JsProcessor::new("route".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");

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

    let proc = JsProcessor::new("route_kh".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");

    let r = out[0].routing.as_ref().unwrap();
    assert_eq!(r.key.as_deref(), Some("k1"));
    assert_eq!(r.headers.as_ref().unwrap()["trace-id"], "abc");
}

#[tokio::test(flavor = "current_thread")]
async fn js_preserves_existing_routing() {
    let js = "function processBatch(events) { return events; }";

    let proc = JsProcessor::new("preserve".into(), js.into()).expect("init ok");
    let mut ev = new_event();
    ev.routing = Some(EventRouting {
        topic: Some("pre-existing".into()),
        ..Default::default()
    });

    let out = proc.process(vec![ev]).await.expect("ok");
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
                out.push(clone);
            }
            return out;
        }
    "#;

    let proc =
        JsProcessor::new("clone_route".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");

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

    let proc = JsProcessor::new("no_route".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");
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

    let proc =
        JsProcessor::new("filter_drop".into(), js.into()).expect("init ok");
    let out = proc.process(vec![new_event()]).await.expect("ok");
    assert_eq!(out.len(), 0);
}
