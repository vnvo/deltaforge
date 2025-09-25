use chrono::Utc;
use deltaforge_core::{Event, SourceMeta, Op};
use deltaforge_core::Processor;
use deltaforge_processor_js::JsProcessor;
use pretty_assertions::assert_eq;
use serde_json::json;
use uuid::Uuid;

fn new_event() -> Event {
    Event {
        event_id: Uuid::new_v4(),
        tenant_id: "t".into(),
        source: SourceMeta {
            kind: "postgres".into(),
            host: "localhost".into(),
            db: "orders".into(),
        },
        table: "public.orders".into(),
        op: Op::Insert,
        tx_id: Some("xid-1".into()),
        before: None,
        after: Some(json!({"id": 1, "note": "original"})),
        schema_version: Some("v1".into()),
        ddl: None,
        timestamp: Utc::now(),
        trace_id: None,
        tags: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn js_returns_array_passthrough() {
    let js = r#"(event) => {return [event];}"#.to_string();
    let proc = JsProcessor::new(js);
    let ev = new_event();

    let out = proc.process(ev.clone()).await.expect("processing ok");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].table, "public.orders");
    assert_eq!(out[0].after.as_ref().unwrap()["note"], "original");
}

#[tokio::test(flavor = "current_thread")]
async fn js_can_mutate_event() {
    let js = r#"
    (event) => {
        event.tags = (event.tags || []).concat(["normalized"]);
        if (event.after) event.after.note = "mutated";
    }
    "#
    .to_string();

    let proc = JsProcessor::new(js);
    let ev = new_event();

    let out = proc.process(ev).await.expect("processing ok");
    assert_eq!(out.len(), 1);
    let e = &out[0];
    assert_eq!(e.after.as_ref().unwrap()["note"], "mutated");
    assert!(e.tags.as_ref().unwrap().contains(&"normalized".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn js_calls_rust_op_log() {
    // call the rust op we have registered via the extension macro.
    // In deno_core (>= 0.357), ops are available at Deno.core.ops
    let js = r#"
    (event) => {Deno.core.ops.op_log("hello-from-js");}
    "#
    .to_string();

    let proc = JsProcessor::new(js);
    let ev = new_event();

    let out = proc.process(ev).await.expect("processing ok");
    assert_eq!(out.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn js_returning_single_object_is_wrapped() {
    // JS returns a single object, not an array.
    // EXPECTATION: processor wraps it into a 1-length Vec<Event> preserving modifications.
    let js = r#"
        (event) => {
            if (event.after) event.after.note = "single-object";
            event.tags = ["solo"];
            return event; // <- not an array
        }
    "#
    .to_string();

    let proc = JsProcessor::new(js);
    let ev = new_event();

    let out = proc.process(ev).await.expect("processing ok");
    assert_eq!(out.len(), 1);
    let e = &out[0];
    assert_eq!(e.after.as_ref().unwrap()["note"], "single-object");
    assert!(e.tags.as_ref().unwrap().contains(&"solo".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn js_returning_invalid_shape_errors() {
    // returning a non-object/array (e.g., number) should error when converting to Event.
    let js = r#"(event) => {return 42;}"#.to_string();
    let proc = JsProcessor::new(js);
    let ev = new_event();

    let err = proc.process(ev).await.expect_err("should fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("invalid JS return") || msg.contains("expected value"),
        "unexpected error: {msg}"
    );
}
