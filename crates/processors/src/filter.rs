//! Filter processor - drops events that do not match configured criteria.
//!
//! Evaluates three independent gate checks in order:
//! 1. **Op gate** - keep only specified operation types
//! 2. **Table gate** - include/exclude by `db.table` glob patterns
//! 3. **Field gate** - predicates on `event.after` values
//!
//! An event must pass all three gates to be forwarded. Omitting a gate
//! config skips that check entirely (pass-through).
//!
//! ## Field operators
//!
//! | Op | Description |
//! |----|-------------|
//! | `eq` / `ne` | Equality / inequality |
//! | `exists` / `not_exists` | Presence check (null = not exists) |
//! | `gt` / `gte` / `lt` / `lte` | Numeric and string ordering |
//! | `in` / `not_in` | Membership in a JSON array |
//! | `contains` | Substring (string) or element (array) |
//! | `changed` | Before ≠ after for the given path |
//! | `regex` | Pattern match against a string field |
//!
//! # Performance
//!
//! Pure Rust, zero allocations on the hot path for op and table checks.
//! Regex patterns are compiled once at construction time.
//! This is 2–3 orders of magnitude faster than the JS processor for
//! equivalent filtering logic.

use anyhow::Result;
use async_trait::async_trait;
use deltaforge_config::{
    FieldOp, FieldPredicate, FilterProcessorCfg, MatchMode, OpFilter,
};
use deltaforge_core::{BatchContext, Event, Op, Processor};
use metrics::counter;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use tracing::debug;

// ============================================================================
// Processor
// ============================================================================

pub struct FilterProcessor {
    id: String,
    cfg: FilterProcessorCfg,
    /// Pre-compiled regexes keyed by predicate index.
    regexes: HashMap<usize, Regex>,
}

impl FilterProcessor {
    pub fn new(cfg: FilterProcessorCfg) -> Result<Self> {
        let id = cfg.id.clone();

        // Compile all regex predicates upfront — fail fast on bad patterns.
        let mut regexes = HashMap::new();
        for (i, pred) in cfg.fields.iter().enumerate() {
            if pred.op == FieldOp::Regex {
                let pattern = pred
                    .value
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "filter predicate {i}: `regex` op requires a string `value`"
                        )
                    })?;
                let re = Regex::new(pattern).map_err(|e| {
                    anyhow::anyhow!(
                        "filter predicate {i}: invalid regex '{pattern}': {e}"
                    )
                })?;
                regexes.insert(i, re);
            }
        }

        Ok(Self { id, cfg, regexes })
    }

    // -----------------------------------------------------------------------
    // Gate: op
    // -----------------------------------------------------------------------

    fn op_allowed(&self, op: &Op) -> bool {
        if self.cfg.ops.is_empty() {
            return true;
        }
        let filter_op = match op {
            Op::Create => OpFilter::Create,
            Op::Update => OpFilter::Update,
            Op::Delete => OpFilter::Delete,
            Op::Read => OpFilter::Read,
            Op::Truncate => OpFilter::Truncate,
        };
        self.cfg.ops.contains(&filter_op)
    }

    // -----------------------------------------------------------------------
    // Gate: table
    // -----------------------------------------------------------------------

    fn table_allowed(&self, db: &str, table: &str) -> bool {
        self.cfg.tables.matches(db, table)
    }

    // -----------------------------------------------------------------------
    // Gate: fields
    // -----------------------------------------------------------------------

    fn fields_match(&self, event: &Event) -> bool {
        if self.cfg.fields.is_empty() {
            return true;
        }
        match self.cfg.match_mode {
            MatchMode::All => self
                .cfg
                .fields
                .iter()
                .enumerate()
                .all(|(i, p)| self.eval_predicate(i, p, event)),
            MatchMode::Any => self
                .cfg
                .fields
                .iter()
                .enumerate()
                .any(|(i, p)| self.eval_predicate(i, p, event)),
        }
    }

    fn eval_predicate(
        &self,
        idx: usize,
        pred: &FieldPredicate,
        event: &Event,
    ) -> bool {
        // `changed` compares before vs after — handled before resolving after.
        if pred.op == FieldOp::Changed {
            return eval_changed(event, &pred.path);
        }

        let field_val = event
            .after
            .as_ref()
            .and_then(|v| resolve_path(v, &pred.path));

        match pred.op {
            FieldOp::Exists => field_val.is_some(),
            FieldOp::NotExists => field_val.is_none(),
            FieldOp::Eq => match (field_val, pred.value.as_ref()) {
                (Some(fv), Some(pv)) => json_eq(fv, pv),
                _ => false,
            },
            FieldOp::Ne => match (field_val, pred.value.as_ref()) {
                (Some(fv), Some(pv)) => !json_eq(fv, pv),
                _ => false,
            },
            FieldOp::Gt => cmp_values(field_val, pred.value.as_ref())
                .map(|o| o.is_gt())
                .unwrap_or(false),
            FieldOp::Gte => cmp_values(field_val, pred.value.as_ref())
                .map(|o| o.is_ge())
                .unwrap_or(false),
            FieldOp::Lt => cmp_values(field_val, pred.value.as_ref())
                .map(|o| o.is_lt())
                .unwrap_or(false),
            FieldOp::Lte => cmp_values(field_val, pred.value.as_ref())
                .map(|o| o.is_le())
                .unwrap_or(false),
            FieldOp::In => match (field_val, pred.value.as_ref()) {
                (Some(fv), Some(Value::Array(arr))) => {
                    arr.iter().any(|item| json_eq(fv, item))
                }
                _ => false,
            },
            FieldOp::NotIn => match (field_val, pred.value.as_ref()) {
                (Some(fv), Some(Value::Array(arr))) => {
                    !arr.iter().any(|item| json_eq(fv, item))
                }
                // Missing field — not in any set.
                (None, _) => true,
                _ => false,
            },
            FieldOp::Contains => match field_val {
                Some(Value::String(s)) => pred
                    .value
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .map(|needle| s.contains(needle))
                    .unwrap_or(false),
                Some(Value::Array(arr)) => pred
                    .value
                    .as_ref()
                    .map(|needle| arr.iter().any(|item| json_eq(item, needle)))
                    .unwrap_or(false),
                _ => false,
            },
            FieldOp::Regex => match field_val {
                Some(Value::String(s)) => self
                    .regexes
                    .get(&idx)
                    .map(|re| re.is_match(s))
                    .unwrap_or(false),
                _ => false,
            },
            // Already handled above.
            FieldOp::Changed => unreachable!(),
        }
    }
}

// ============================================================================
// Trait impl
// ============================================================================

#[async_trait]
impl Processor for FilterProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    async fn process(
        &self,
        events: Vec<Event>,
        _ctx: &BatchContext,
    ) -> Result<Vec<Event>> {
        let in_len = events.len();
        let out: Vec<Event> = events
            .into_iter()
            .filter(|ev| {
                let pass = self.op_allowed(&ev.op)
                    && self.table_allowed(&ev.source.db, &ev.source.table)
                    && self.fields_match(ev);
                if !pass {
                    counter!("deltaforge_filter_dropped_total",
                        "processor" => self.id.clone()
                    )
                    .increment(1);
                }
                pass
            })
            .collect();

        let dropped = in_len - out.len();
        if dropped > 0 {
            debug!(
                processor = %self.id,
                input = in_len,
                output = out.len(),
                dropped,
                "filter processor dropped events"
            );
        }
        Ok(out)
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Resolve a dot-separated path into a JSON value, returning a reference
/// to the leaf (or `None` if the path does not exist or the leaf is null).
fn resolve_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = root;
    for segment in path.split('.') {
        cur = cur.get(segment)?;
    }
    if cur.is_null() { None } else { Some(cur) }
}

/// `changed` — passes if the field value differs between before and after,
/// or if either image is missing (Create has no before; Delete has no after).
fn eval_changed(event: &Event, path: &str) -> bool {
    match (&event.before, &event.after) {
        (Some(b), Some(a)) => {
            let bv = resolve_path(b, path);
            let av = resolve_path(a, path);
            match (bv, av) {
                (Some(b), Some(a)) => !json_eq(b, a),
                (None, None) => false,
                _ => true, // one side missing = field appeared or disappeared
            }
        }
        // Create (no before) or Delete (no after) — unconditionally pass.
        _ => true,
    }
}

/// Equality that handles JSON number comparison correctly (int vs float).
fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            // Compare via f64 to handle 42 == 42.0
            an.as_f64() == bn.as_f64()
        }
        _ => a == b,
    }
}

/// Numeric and string ordering comparison.
/// Returns `None` when types are incomparable.
fn cmp_values(
    field: Option<&Value>,
    predicate: Option<&Value>,
) -> Option<std::cmp::Ordering> {
    match (field?, predicate?) {
        (Value::Number(a), Value::Number(b)) => {
            let af = a.as_f64()?;
            let bf = b.as_f64()?;
            af.partial_cmp(&bf)
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}
