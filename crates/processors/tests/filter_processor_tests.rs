use common::TableFilter;
use deltaforge_config::{
    FieldOp, FieldPredicate, FilterProcessorCfg, MatchMode, OpFilter,
};
use deltaforge_core::{
    BatchContext, Event, Op, Processor, SourceInfo, SourcePosition,
};
use processors::FilterProcessor;
use serde_json::json;

// ============================================================================
// Helpers
// ============================================================================

fn source(db: &str, table: &str) -> SourceInfo {
    SourceInfo {
        version: "1.0.0".into(),
        connector: "mysql".into(),
        name: "test-db".into(),
        ts_ms: 1_700_000_000_000,
        db: db.into(),
        schema: None,
        table: table.into(),
        snapshot: None,
        position: SourcePosition::default(),
    }
}

fn make_event(
    db: &str,
    table: &str,
    op: Op,
    after: serde_json::Value,
) -> Event {
    Event::new_row(
        source(db, table),
        op,
        None,
        Some(after),
        1_700_000_000_000,
        64,
    )
}

fn pred(path: &str, op: FieldOp, value: serde_json::Value) -> FieldPredicate {
    FieldPredicate {
        path: path.into(),
        op,
        value: Some(value),
    }
}

fn pred_no_value(path: &str, op: FieldOp) -> FieldPredicate {
    FieldPredicate {
        path: path.into(),
        op,
        value: None,
    }
}

async fn run(cfg: FilterProcessorCfg, events: Vec<Event>) -> Vec<Event> {
    let proc = FilterProcessor::new(cfg).expect("init ok");
    let ctx = BatchContext::from_batch(&events);
    proc.process(events, &ctx).await.expect("ok")
}

// ============================================================================
// All three gates combined - the primary contract
// ============================================================================

#[tokio::test]
async fn all_gates_must_pass() {
    let mut cfg = FilterProcessorCfg::default();
    cfg.ops = vec![OpFilter::Create];
    cfg.tables = TableFilter::new(vec!["shop.orders".into()], vec![]);
    cfg.fields = vec![pred("status", FieldOp::Eq, json!("active"))];

    let events = vec![
        make_event("shop", "orders", Op::Create, json!({"status": "active"})), // pass
        make_event("shop", "orders", Op::Delete, json!({"status": "active"})), // fails op
        make_event("shop", "users", Op::Create, json!({"status": "active"})), // fails table
        make_event("shop", "orders", Op::Create, json!({"status": "inactive"})), // fails field
    ];
    let out = run(cfg, events).await;
    assert_eq!(out.len(), 1);
}

// ============================================================================
// Table gate - exclude priority is non-obvious
// ============================================================================

#[tokio::test]
async fn table_exclude_takes_priority_over_include() {
    let mut cfg = FilterProcessorCfg::default();
    cfg.tables =
        TableFilter::new(vec!["shop.*".into()], vec!["shop.tmp".into()]);

    let events = vec![
        make_event("shop", "orders", Op::Create, json!({})),
        make_event("shop", "tmp", Op::Create, json!({})),
    ];
    let out = run(cfg, events).await;
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].source.table, "orders");
}

// ============================================================================
// Field gate
// ============================================================================

#[tokio::test]
async fn field_eq_int_vs_float_normalised() {
    // 42 == 42.0 - critical when a JS processor upstream converts integers to floats.
    let mut cfg = FilterProcessorCfg::default();
    cfg.fields = vec![pred("score", FieldOp::Eq, json!(42))];

    let events = vec![
        make_event("db", "t", Op::Create, json!({"score": 42.0})),
        make_event("db", "t", Op::Create, json!({"score": 42.5})),
    ];
    let out = run(cfg, events).await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn field_dotted_path_traversal() {
    let mut cfg = FilterProcessorCfg::default();
    cfg.fields = vec![pred("order.status", FieldOp::Eq, json!("paid"))];

    let events = vec![
        make_event("db", "t", Op::Create, json!({"order": {"status": "paid"}})),
        make_event(
            "db",
            "t",
            Op::Create,
            json!({"order": {"status": "pending"}}),
        ),
        make_event("db", "t", Op::Create, json!({"order": {}})), // missing path → drop
    ];
    let out = run(cfg, events).await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn field_predicate_with_no_after_drops_event() {
    // Delete events have no after - field predicates must not panic.
    let mut cfg = FilterProcessorCfg::default();
    cfg.fields = vec![pred("status", FieldOp::Eq, json!("active"))];

    let mut ev = make_event("db", "t", Op::Delete, json!({}));
    ev.after = None;

    let out = run(cfg, vec![ev]).await;
    assert!(out.is_empty());
}

// ============================================================================
// match: any
// ============================================================================

#[tokio::test]
async fn match_any_passes_if_one_predicate_matches() {
    let mut cfg = FilterProcessorCfg::default();
    cfg.match_mode = MatchMode::Any;
    cfg.fields = vec![
        pred("status", FieldOp::Eq, json!("active")),
        pred("priority", FieldOp::Eq, json!("high")),
    ];

    let events = vec![
        make_event(
            "db",
            "t",
            Op::Create,
            json!({"status": "active",   "priority": "low"}),
        ),
        make_event(
            "db",
            "t",
            Op::Create,
            json!({"status": "inactive", "priority": "high"}),
        ),
        make_event(
            "db",
            "t",
            Op::Create,
            json!({"status": "inactive", "priority": "low"}),
        ),
    ];
    let out = run(cfg, events).await;
    assert_eq!(out.len(), 2);
}

// ============================================================================
// in / not_in
// ============================================================================

#[tokio::test]
async fn field_in_array() {
    let mut cfg = FilterProcessorCfg::default();
    cfg.fields = vec![FieldPredicate {
        path: "status".into(),
        op: FieldOp::In,
        value: Some(json!(["pending", "processing", "retry"])),
    }];

    let events = vec![
        make_event("db", "t", Op::Create, json!({"status": "pending"})),
        make_event("db", "t", Op::Create, json!({"status": "complete"})),
    ];
    let out = run(cfg, events).await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn field_not_in_missing_field_passes() {
    // A field absent from the event is not in any exclusion set - must pass.
    let mut cfg = FilterProcessorCfg::default();
    cfg.fields = vec![FieldPredicate {
        path: "region".into(),
        op: FieldOp::NotIn,
        value: Some(json!(["eu-west-1"])),
    }];

    let out = run(
        cfg,
        vec![make_event("db", "t", Op::Create, json!({"id": 1}))],
    )
    .await;
    assert_eq!(out.len(), 1);
}

// ============================================================================
// contains - polymorphic (string substring vs array element)
// ============================================================================

#[tokio::test]
async fn contains_works_on_strings_and_arrays() {
    let mut cfg = FilterProcessorCfg::default();
    cfg.match_mode = MatchMode::Any;
    cfg.fields = vec![
        FieldPredicate {
            path: "desc".into(),
            op: FieldOp::Contains,
            value: Some(json!("urgent")),
        },
        FieldPredicate {
            path: "tags".into(),
            op: FieldOp::Contains,
            value: Some(json!("vip")),
        },
    ];

    let events = vec![
        make_event(
            "db",
            "t",
            Op::Create,
            json!({"desc": "urgent shipment", "tags": ["retail"]}),
        ),
        make_event(
            "db",
            "t",
            Op::Create,
            json!({"desc": "routine",         "tags": ["vip"]}),
        ),
        make_event(
            "db",
            "t",
            Op::Create,
            json!({"desc": "routine",         "tags": ["retail"]}),
        ),
    ];
    let out = run(cfg, events).await;
    assert_eq!(out.len(), 2);
}

// ============================================================================
// changed
// ============================================================================

#[tokio::test]
async fn changed_only_passes_when_field_actually_differs() {
    let mut cfg = FilterProcessorCfg::default();
    cfg.fields = vec![pred_no_value("status", FieldOp::Changed)];

    let mut ev_changed =
        make_event("db", "t", Op::Update, json!({"status": "active"}));
    ev_changed.before = Some(json!({"status": "pending"}));

    // Same status, different unrelated field - must drop.
    let mut ev_same = make_event(
        "db",
        "t",
        Op::Update,
        json!({"status": "active", "name": "new"}),
    );
    ev_same.before = Some(json!({"status": "active", "name": "old"}));

    let out = run(cfg, vec![ev_changed, ev_same]).await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn changed_passes_creates_unconditionally() {
    // No before image on creates - always passes regardless of after content.
    let mut cfg = FilterProcessorCfg::default();
    cfg.fields = vec![pred_no_value("status", FieldOp::Changed)];

    let out = run(
        cfg,
        vec![make_event("db", "t", Op::Create, json!({"status": "new"}))],
    )
    .await;
    assert_eq!(out.len(), 1);
}

// ============================================================================
// regex
// ============================================================================

#[tokio::test]
async fn regex_matches_string_field() {
    let mut cfg = FilterProcessorCfg::default();
    cfg.fields = vec![FieldPredicate {
        path: "email".into(),
        op: FieldOp::Regex,
        value: Some(json!(r"@internal\.company\.com$")),
    }];

    let events = vec![
        make_event(
            "db",
            "t",
            Op::Create,
            json!({"email": "alice@internal.company.com"}),
        ),
        make_event("db", "t", Op::Create, json!({"email": "bob@external.com"})),
    ];
    let out = run(cfg, events).await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn regex_invalid_pattern_fails_construction() {
    let mut cfg = FilterProcessorCfg::default();
    cfg.fields = vec![FieldPredicate {
        path: "x".into(),
        op: FieldOp::Regex,
        value: Some(json!(r"[invalid")),
    }];
    assert!(FilterProcessor::new(cfg).is_err());
}
