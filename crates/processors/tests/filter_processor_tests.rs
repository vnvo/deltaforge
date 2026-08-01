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
    let cfg = FilterProcessorCfg {
        ops: vec![OpFilter::Create],
        tables: TableFilter::new(vec!["shop.orders".into()], vec![]),
        fields: vec![pred("status", FieldOp::Eq, json!("active"))],
        ..Default::default()
    };

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
    let cfg = FilterProcessorCfg {
        tables: TableFilter::new(
            vec!["shop.*".into()],
            vec!["shop.tmp".into()],
        ),
        ..Default::default()
    };

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
    let cfg = FilterProcessorCfg {
        fields: vec![pred("score", FieldOp::Eq, json!(42))],
        ..Default::default()
    };

    let events = vec![
        make_event("db", "t", Op::Create, json!({"score": 42.0})),
        make_event("db", "t", Op::Create, json!({"score": 42.5})),
    ];
    let out = run(cfg, events).await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn field_dotted_path_traversal() {
    let cfg = FilterProcessorCfg {
        fields: vec![pred("order.status", FieldOp::Eq, json!("paid"))],
        ..Default::default()
    };

    let events = vec![
        make_event("db", "t", Op::Create, json!({"order": {"status": "paid"}})),
        make_event(
            "db",
            "t",
            Op::Create,
            json!({"order": {"status": "pending"}}),
        ),
        make_event("db", "t", Op::Create, json!({"order": {}})), // missing path -> drop
    ];
    let out = run(cfg, events).await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn field_predicate_with_no_after_drops_event() {
    // Delete events have no after - field predicates must not panic.
    let cfg = FilterProcessorCfg {
        fields: vec![pred("status", FieldOp::Eq, json!("active"))],
        ..Default::default()
    };

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
    let cfg = FilterProcessorCfg {
        match_mode: MatchMode::Any,
        fields: vec![
            pred("status", FieldOp::Eq, json!("active")),
            pred("priority", FieldOp::Eq, json!("high")),
        ],
        ..Default::default()
    };

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
    let cfg = FilterProcessorCfg {
        fields: vec![FieldPredicate {
            path: "status".into(),
            op: FieldOp::In,
            value: Some(json!(["pending", "processing", "retry"])),
        }],
        ..Default::default()
    };

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
    let cfg = FilterProcessorCfg {
        fields: vec![FieldPredicate {
            path: "region".into(),
            op: FieldOp::NotIn,
            value: Some(json!(["eu-west-1"])),
        }],
        ..Default::default()
    };

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
    let cfg = FilterProcessorCfg {
        match_mode: MatchMode::Any,
        fields: vec![
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
        ],
        ..Default::default()
    };

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
            json!({"desc": "routine", "tags": ["vip"]}),
        ),
        make_event(
            "db",
            "t",
            Op::Create,
            json!({"desc": "routine", "tags": ["retail"]}),
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
    let cfg = FilterProcessorCfg {
        fields: vec![pred_no_value("status", FieldOp::Changed)],
        ..Default::default()
    };

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
    let cfg = FilterProcessorCfg {
        fields: vec![pred_no_value("status", FieldOp::Changed)],
        ..Default::default()
    };

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
    let cfg = FilterProcessorCfg {
        fields: vec![FieldPredicate {
            path: "email".into(),
            op: FieldOp::Regex,
            value: Some(json!(r"@internal\.company\.com$")),
        }],
        ..Default::default()
    };

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
    let cfg = FilterProcessorCfg {
        fields: vec![FieldPredicate {
            path: "x".into(),
            op: FieldOp::Regex,
            value: Some(json!(r"[invalid")),
        }],
        ..Default::default()
    };
    assert!(FilterProcessor::new(cfg).is_err());
}

// ============================================================================
// Predicate arms the existing count-based tests leave unpinned. Each uses a
// SINGLE event so a mutation that flips its pass/drop changes the total (a
// 2-event count assertion survives a swap of which event passes).
// ============================================================================

fn update_event(before: serde_json::Value, after: serde_json::Value) -> Event {
    Event::new_row(
        source("db", "t"),
        Op::Update,
        Some(before),
        Some(after),
        1_700_000_000_000,
        64,
    )
}

#[tokio::test]
async fn field_ne_passes_only_when_not_equal() {
    let mk = || FilterProcessorCfg {
        fields: vec![pred("status", FieldOp::Ne, json!("active"))],
        ..Default::default()
    };
    let out = run(
        mk(),
        vec![make_event(
            "db",
            "t",
            Op::Create,
            json!({"status": "other"}),
        )],
    )
    .await;
    assert_eq!(out.len(), 1, "not-equal passes Ne");
    let out = run(
        mk(),
        vec![make_event(
            "db",
            "t",
            Op::Create,
            json!({"status": "active"}),
        )],
    )
    .await;
    assert!(out.is_empty(), "equal fails Ne");
}

#[tokio::test]
async fn field_eq_int_vs_float_single_event() {
    // Single event: a json_eq `==`→`!=` flip drops it (0 vs 1).
    let cfg = FilterProcessorCfg {
        fields: vec![pred("score", FieldOp::Eq, json!(42))],
        ..Default::default()
    };
    let out = run(
        cfg,
        vec![make_event("db", "t", Op::Create, json!({"score": 42.0}))],
    )
    .await;
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn field_gt_orders_numbers_and_strings() {
    let num = |v| FilterProcessorCfg {
        fields: vec![pred("n", FieldOp::Gt, json!(v))],
        ..Default::default()
    };
    let out = run(
        num(3),
        vec![make_event("db", "t", Op::Create, json!({"n": 5}))],
    )
    .await;
    assert_eq!(out.len(), 1, "5 > 3 passes (numeric cmp_values)");
    let out = run(
        num(3),
        vec![make_event("db", "t", Op::Create, json!({"n": 2}))],
    )
    .await;
    assert!(out.is_empty(), "2 > 3 drops");
    // String ordering exercises the String arm of cmp_values.
    let scfg = FilterProcessorCfg {
        fields: vec![pred("s", FieldOp::Gt, json!("apple"))],
        ..Default::default()
    };
    let out = run(
        scfg,
        vec![make_event("db", "t", Op::Create, json!({"s": "banana"}))],
    )
    .await;
    assert_eq!(out.len(), 1, "banana > apple passes (string cmp_values)");
}

#[tokio::test]
async fn field_in_and_not_in_membership() {
    let out = run(
        FilterProcessorCfg {
            fields: vec![pred("c", FieldOp::In, json!(["a", "b", "c"]))],
            ..Default::default()
        },
        vec![make_event("db", "t", Op::Create, json!({"c": "b"}))],
    )
    .await;
    assert_eq!(out.len(), 1, "member passes In");

    let notin = || FilterProcessorCfg {
        fields: vec![pred("c", FieldOp::NotIn, json!(["a", "b"]))],
        ..Default::default()
    };
    let out = run(
        notin(),
        vec![make_event("db", "t", Op::Create, json!({"c": "z"}))],
    )
    .await;
    assert_eq!(out.len(), 1, "non-member passes NotIn");
    let out = run(
        notin(),
        vec![make_event("db", "t", Op::Create, json!({"c": "a"}))],
    )
    .await;
    assert!(out.is_empty(), "member fails NotIn");
}

#[tokio::test]
async fn field_changed_detects_value_change() {
    let mk = || FilterProcessorCfg {
        fields: vec![pred_no_value("status", FieldOp::Changed)],
        ..Default::default()
    };
    let out = run(
        mk(),
        vec![update_event(json!({"status": "a"}), json!({"status": "b"}))],
    )
    .await;
    assert_eq!(out.len(), 1, "a→b is a change");
    let out = run(
        mk(),
        vec![update_event(json!({"status": "a"}), json!({"status": "a"}))],
    )
    .await;
    assert!(out.is_empty(), "a→a is not a change");
    // Field absent in both images → not changed (pins the (None,None) arm).
    let out = run(
        mk(),
        vec![update_event(json!({"other": 1}), json!({"other": 2}))],
    )
    .await;
    assert!(out.is_empty(), "field absent in both → not changed");
}
