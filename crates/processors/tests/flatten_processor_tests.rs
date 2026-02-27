//! Flatten processor tests.
//!
//! Covers: basic flattening, max_depth, empty_object policy, lists policy,
//! empty_list policy, collision policy, non-row events passthrough,
//! and outbox-style payloads.

use deltaforge_config::{
    CollisionPolicy, EmptyListPolicy, EmptyObjectPolicy, FlattenProcessorCfg,
    ListPolicy,
};
use deltaforge_core::{Event, Op, Processor, SourceInfo, SourcePosition};
use processors::FlattenProcessor;
use serde_json::json;

fn default_cfg() -> FlattenProcessorCfg {
    FlattenProcessorCfg::default()
}

fn make_event(after: serde_json::Value) -> Event {
    Event::new_row(
        SourceInfo {
            version: "1.0.0".into(),
            connector: "mysql".into(),
            name: "test-db".into(),
            ts_ms: 1700000000000,
            db: "shop".into(),
            schema: None,
            table: "orders".into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Create,
        None,
        Some(after),
        1700000000000,
        128,
    )
}

fn flatten(
    cfg: FlattenProcessorCfg,
    after: serde_json::Value,
) -> serde_json::Value {
    let proc = FlattenProcessor::new(cfg).expect("init ok");
    let ev = make_event(after);
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut out = rt.block_on(proc.process(vec![ev])).expect("process ok");
    out.remove(0).after.unwrap()
}

// ============================================================================
// Basic Flattening
// ============================================================================

#[test]
fn flattens_nested_object_with_default_separator() {
    let result = flatten(
        default_cfg(),
        json!({ "user": { "address": { "city": "Berlin" } }, "id": 1 }),
    );
    assert_eq!(result["user__address__city"], "Berlin");
    assert_eq!(result["id"], 1);
    assert!(result.get("user").is_none());
}

#[test]
fn already_flat_payload_is_idempotent() {
    let input = json!({ "id": 1, "name": "Alice", "score": 9.5 });
    let result = flatten(default_cfg(), input.clone());
    assert_eq!(result, input);
}

#[test]
fn custom_separator_used_in_keys() {
    let mut cfg = default_cfg();
    cfg.separator = ".".into();
    let result = flatten(cfg, json!({ "a": { "b": 1 } }));
    assert_eq!(result["a.b"], 1);
}

// ============================================================================
// max_depth
// ============================================================================

#[test]
fn max_depth_stops_recursion_and_keeps_remaining_as_leaf() {
    let mut cfg = default_cfg();
    cfg.max_depth = Some(1); // only one level deep
    let result =
        flatten(cfg, json!({ "user": { "address": { "city": "Berlin" } } }));
    // user__address is kept as a leaf object (not further flattened)
    assert_eq!(result["user__address"], json!({ "city": "Berlin" }));
    assert!(result.get("user__address__city").is_none());
}

#[test]
fn max_depth_zero_keeps_all_as_leaves() {
    let mut cfg = default_cfg();
    cfg.max_depth = Some(0);
    let input = json!({ "a": { "b": 1 }, "c": 2 });
    let result = flatten(cfg, input.clone());
    // Nothing should be flattened — all top-level fields kept as-is
    assert_eq!(result["a"], json!({ "b": 1 }));
    assert_eq!(result["c"], 2);
}

// ============================================================================
// empty_object policy
// ============================================================================

#[test]
fn empty_object_preserve_keeps_field() {
    let mut cfg = default_cfg();
    cfg.empty_object = EmptyObjectPolicy::Preserve;
    let result = flatten(cfg, json!({ "meta": {}, "id": 1 }));
    assert_eq!(result["meta"], json!({}));
}

#[test]
fn empty_object_drop_removes_field() {
    let mut cfg = default_cfg();
    cfg.empty_object = EmptyObjectPolicy::Drop;
    let result = flatten(cfg, json!({ "meta": {}, "id": 1 }));
    assert!(result.get("meta").is_none());
    assert_eq!(result["id"], 1);
}

#[test]
fn empty_object_null_converts_field() {
    let mut cfg = default_cfg();
    cfg.empty_object = EmptyObjectPolicy::Null;
    let result = flatten(cfg, json!({ "meta": {}, "id": 1 }));
    assert_eq!(result["meta"], json!(null));
}

#[test]
fn empty_object_at_max_depth_respects_policy() {
    let mut cfg = default_cfg();
    cfg.max_depth = Some(1);
    cfg.empty_object = EmptyObjectPolicy::Drop;
    // user__meta hits depth 1 (the boundary) and is empty — should be dropped
    let result =
        flatten(cfg, json!({ "user": { "meta": {} , "name": "Alice" } }));
    assert!(result.get("user__meta").is_none());
    assert_eq!(result["user__name"], "Alice");
}

// ============================================================================
// list policy
// ============================================================================

#[test]
fn lists_preserve_keeps_array_intact() {
    let mut cfg = default_cfg();
    cfg.lists = ListPolicy::Preserve;
    let result = flatten(cfg, json!({ "tags": [1, 2, 3] }));
    assert_eq!(result["tags"], json!([1, 2, 3]));
}

#[test]
fn lists_index_expands_to_keyed_entries() {
    let mut cfg = default_cfg();
    cfg.lists = ListPolicy::Index;
    let result = flatten(cfg, json!({ "tags": ["a", "b"] }));
    assert_eq!(result["tags__0"], "a");
    assert_eq!(result["tags__1"], "b");
}

// ============================================================================
// empty_list policy
// ============================================================================

#[test]
fn empty_list_preserve_keeps_field() {
    let mut cfg = default_cfg();
    cfg.empty_list = EmptyListPolicy::Preserve;
    let result = flatten(cfg, json!({ "tags": [], "id": 1 }));
    assert_eq!(result["tags"], json!([]));
}

#[test]
fn empty_list_drop_removes_field() {
    let mut cfg = default_cfg();
    cfg.empty_list = EmptyListPolicy::Drop;
    let result = flatten(cfg, json!({ "tags": [], "id": 1 }));
    assert!(result.get("tags").is_none());
    assert_eq!(result["id"], 1);
}

#[test]
fn empty_list_null_converts_field() {
    let mut cfg = default_cfg();
    cfg.empty_list = EmptyListPolicy::Null;
    let result = flatten(cfg, json!({ "tags": [], "id": 1 }));
    assert_eq!(result["tags"], json!(null));
}

// ============================================================================
// Collision policy
// ============================================================================

#[test]
fn collision_last_wins() {
    // Construct a case where two paths collide after flattening:
    // { "a__b": 1, "a": { "b": 2 } } -> both produce "a__b"
    let mut cfg = default_cfg();
    cfg.on_collision = CollisionPolicy::Last;
    // We simulate by providing a map where the object key would shadow a scalar
    let result = flatten(cfg, json!({ "a__b": 1, "a": { "b": 2 } }));
    assert_eq!(result["a__b"], 2); // object flatten wrote last
}

#[test]
fn collision_error_fails_batch() {
    let mut cfg = default_cfg();
    cfg.on_collision = CollisionPolicy::Error;
    let proc = FlattenProcessor::new(cfg).expect("init ok");
    let ev = make_event(json!({ "a": { "b": 1 }, "a__b": 99 }));
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let result = rt.block_on(proc.process(vec![ev]));
    assert!(result.is_err(), "expected collision error");
}

// ============================================================================
// before/after handled independently
// ============================================================================

#[test]
fn flattens_both_before_and_after() {
    let proc = FlattenProcessor::new(default_cfg()).expect("init ok");
    let mut ev = make_event(json!({}));
    ev.before = Some(json!({ "addr": { "city": "Old" } }));
    ev.after = Some(json!({ "addr": { "city": "New" } }));
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let out = rt.block_on(proc.process(vec![ev])).unwrap();
    assert_eq!(out[0].before.as_ref().unwrap()["addr__city"], "Old");
    assert_eq!(out[0].after.as_ref().unwrap()["addr__city"], "New");
}

// ============================================================================
// Outbox-style: no before/after, arbitrary payload
// ============================================================================

#[test]
fn outbox_style_event_with_payload_in_after() {
    // After the outbox processor runs, event.after IS the business payload
    let result = flatten(
        default_cfg(),
        json!({
            "order_id": "abc",
            "customer": { "id": 1, "address": { "city": "Berlin" } },
            "items": [1, 2]
        }),
    );
    assert_eq!(result["order_id"], "abc");
    assert_eq!(result["customer__id"], 1);
    assert_eq!(result["customer__address__city"], "Berlin");
    assert_eq!(result["items"], json!([1, 2])); // preserved by default
}

// ============================================================================
// Non-row events pass through
// ============================================================================

#[test]
fn event_with_no_payload_passes_through_unchanged() {
    let proc = FlattenProcessor::new(default_cfg()).expect("init ok");
    let mut ev = make_event(json!({}));
    ev.before = None;
    ev.after = None;
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let out = rt.block_on(proc.process(vec![ev])).unwrap();
    assert!(out[0].before.is_none());
    assert!(out[0].after.is_none());
}
