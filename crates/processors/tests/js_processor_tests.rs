use chrono::Utc;
use deltaforge_core::{Event, Op, Processor, SourceMeta};
use pretty_assertions::assert_eq;
use processors::JsProcessor;
use serde_json::json;
use uuid::Uuid;

fn new_event() -> Event {
    Event {
        event_id: Uuid::new_v4(),
        tenant_id: "t".into(),
        source: SourceMeta {
            kind: "mysql".into(),
            host: "localhost".into(),
            db: "orders".into(),
        },
        table: "public.orders".into(),
        op: Op::Insert,
        tx_id: Some("xid-1".into()),
        before: None,
        after: Some(json!({ "id": 1, "note": "original" })),
        schema_version: Some("v1".into()),
        schema_sequence: Some(0),
        ddl: None,
        timestamp: Utc::now(),
        trace_id: None,
        tags: None,
        checkpoint: None,
        size_bytes: 20 as usize,
        tx_end: true,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn js_returns_array_passthrough() {
    // JS returns the same batch (array of events) unchanged
    let js = r#"
        function processBatch(events) {
            return events;
        }
    "#
    .to_string();

    let proc = JsProcessor::new("p1".to_string(), js).expect("init ok");
    let ev = new_event();

    let out = proc.process(vec![ev.clone()]).await.expect("processing ok");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].table, "public.orders");
    assert_eq!(out[0].after.as_ref().unwrap()["note"], "original");
}

#[tokio::test(flavor = "current_thread")]
async fn js_can_mutate_events_in_place() {
    // JS mutates events in place and returns null -> mutated batch is used
    let js = r#"
        function processBatch(events) {
            for (const event of events) {
                event.tags = (event.tags || []).concat(["normalized"]);
                if (event.after) {
                    event.after.note = "mutated";
                }
            }
            // returning null/undefined means "use mutated input batch"
            return null;
        }
    "#
    .to_string();

    let proc = JsProcessor::new("p2".to_string(), js).expect("init ok");
    let ev = new_event();

    let out = proc.process(vec![ev]).await.expect("processing ok");
    assert_eq!(out.len(), 1);
    let e = &out[0];
    assert_eq!(e.after.as_ref().unwrap()["note"], "mutated");
    assert!(e.tags.as_ref().unwrap().contains(&"normalized".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn js_calls_rust_op_log() {
    // Call the Rust op registered via the extension macro.
    // In deno_core (>= 0.357), ops are available at Deno.core.ops
    let js = r#"
        function processBatch(events) {
            Deno.core.ops.op_log("hello-from-js");
            // pass through unchanged
            return events;
        }
    "#
    .to_string();

    let proc = JsProcessor::new("p3".to_string(), js).expect("init ok");
    let ev = new_event();

    let out = proc.process(vec![ev]).await.expect("processing ok");
    assert_eq!(out.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn js_returning_single_object_is_wrapped() {
    // JS returns a single object, not an array.
    // EXPECTATION: processor wraps it into a 1-length Vec<Event> preserving modifications.
    let js = r#"
        function processBatch(events) {
            const event = events[0];
            if (event.after) event.after.note = "single-object";
            event.tags = ["solo"];
            return event; // <- not an array
        }
    "#
    .to_string();

    let proc = JsProcessor::new("p4".to_string(), js).expect("init ok");
    let ev = new_event();

    let out = proc.process(vec![ev]).await.expect("processing ok");
    assert_eq!(out.len(), 1);
    let e = &out[0];
    assert_eq!(e.after.as_ref().unwrap()["note"], "single-object");
    assert!(e.tags.as_ref().unwrap().contains(&"solo".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn js_returning_invalid_shape_errors() {
    // returning a non-object/array (e.g., number) should error
    let js = r#"
        function processBatch(events) {
            return 42;
        }
    "#
    .to_string();

    let proc = JsProcessor::new("p5".to_string(), js).expect("init ok");
    let ev = new_event();

    let err = proc.process(vec![ev]).await.expect_err("should fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("unsupported type")
            || msg.contains("failed to decode")
            || msg.contains("invalid JS return"),
        "unexpected error: {msg}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_can_add_extra_events() {
    let js = r#"
        function processBatch(events) {
            const out = [];
            for (const ev of events) {
                // keep original
                out.push(ev);

                // add variant
                const clone = {
                    ...ev,
                    tags: (ev.tags || []).concat(["variant"])
                };
                out.push(clone);
            }
            return out;
        }
    "#
    .to_string();

    let proc = JsProcessor::new("extra".to_string(), js).expect("init ok");
    let ev = new_event();

    let out = proc.process(vec![ev]).await.expect("processing ok");
    assert_eq!(out.len(), 2);

    let original = &out[0];
    let variant = &out[1];

    // original unchanged
    assert_eq!(original.after.as_ref().unwrap()["note"], "original");
    // variant has extra tag
    assert!(
        variant
            .tags
            .as_ref()
            .unwrap()
            .contains(&"variant".to_string())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_can_drop_all_events_by_returning_empty_array() {
    let js = r#"
        function processBatch(events) {
            // drop everything
            return [];
        }
    "#
    .to_string();

    let proc = JsProcessor::new("drop".to_string(), js).expect("init ok");
    let ev = new_event();

    let out = proc.process(vec![ev]).await.expect("processing ok");
    assert_eq!(out.len(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn js_processes_multiple_events_in_batch() {
    let js = r#"
        function processBatch(events) {
            for (let i = 0; i < events.length; i++) {
                const ev = events[i];
                ev.tags = (ev.tags || []).concat([`idx-${i}`]);
            }
            // use mutated batch
            return null;
        }
    "#
    .to_string();

    let proc = JsProcessor::new("multi".to_string(), js).expect("init ok");
    let ev1 = new_event();
    let ev2 = new_event();

    let out = proc.process(vec![ev1, ev2]).await.expect("processing ok");
    assert_eq!(out.len(), 2);

    assert!(out[0].tags.as_ref().unwrap().contains(&"idx-0".to_string()));
    assert!(out[1].tags.as_ref().unwrap().contains(&"idx-1".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn js_throwing_error_is_propagated() {
    let js = r#"
        function processBatch(events) {
            throw new Error("boom!");
        }
    "#
    .to_string();

    let proc = JsProcessor::new("throw".to_string(), js).expect("init ok");
    let ev = new_event();

    let err = proc.process(vec![ev]).await.expect_err("should fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("boom") || msg.contains("JS processor threw"),
        "unexpected error: {msg}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn js_runtime_is_reused_across_batches() {
    // Mutates a counter on the global object so we can see reuse
    let js = r#"
        let count = 0;
        function processBatch(events) {
            count++;
            for (const ev of events) {
                ev.tags = (ev.tags || []).concat([`count-${count}`]);
            }
            return null;
        }
    "#
    .to_string();

    let proc = JsProcessor::new("reuse".to_string(), js).expect("init ok");

    let out1 = proc.process(vec![new_event()]).await.expect("batch1 ok");
    let out2 = proc.process(vec![new_event()]).await.expect("batch2 ok");

    assert!(
        out1[0]
            .tags
            .as_ref()
            .unwrap()
            .contains(&"count-1".to_string())
    );
    assert!(
        out2[0]
            .tags
            .as_ref()
            .unwrap()
            .contains(&"count-2".to_string())
    );
}
