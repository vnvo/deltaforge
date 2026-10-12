//! Filter processor - drops events that do not match configured criteria.
//!
//! Evaluates three independent gate checks in order:
//! 1. **Op gate** - keep only specified operation types
//! 2. **Table gate** - include/exclude by `db.table` glob patterns
//! 3. **Field gate** - predicates on `event.after` values, read through the
//!    path view of the canonical values
//!    ([`deltaforge_core::canonical::view`]): integers are exact numbers
//!    (compared exactly, whatever their width), a path can descend into a
//!    JSON column, and every other value is its native rendering for the
//!    event's mapping version. A predicate on a PostgreSQL column whose
//!    value was not sent (unchanged TOAST) fails the batch: its value is
//!    not in the event. `changed` treats such a column as unchanged.
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
use deltaforge_core::canonical::view::row_view;
use deltaforge_core::{BatchContext, CanonicalValue, Event, Op, Processor};
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
    digest: String,
}

/// Bump when a code change alters this processor's output for identical config.
const FILTER_IMPL_VERSION: u32 = 1;

impl FilterProcessor {
    pub fn new(cfg: FilterProcessorCfg) -> Result<Self> {
        let id = cfg.id.clone();
        let digest =
            crate::digest::builtin_digest("filter", FILTER_IMPL_VERSION, &cfg);

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

        Ok(Self {
            id,
            cfg,
            regexes,
            digest,
        })
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

    fn fields_match(&self, event: &Event) -> Result<bool> {
        if self.cfg.fields.is_empty() {
            return Ok(true);
        }
        let view = event
            .after
            .as_ref()
            .map(|r| Value::Object(row_view(r, event.type_mapping)));
        let mut results = self
            .cfg
            .fields
            .iter()
            .enumerate()
            .map(|(i, p)| self.eval_predicate(i, p, event, view.as_ref()));
        Ok(match self.cfg.match_mode {
            MatchMode::All => {
                for r in results.by_ref() {
                    if !r? {
                        return Ok(false);
                    }
                }
                true
            }
            MatchMode::Any => {
                for r in results.by_ref() {
                    if r? {
                        return Ok(true);
                    }
                }
                false
            }
        })
    }

    fn eval_predicate(
        &self,
        idx: usize,
        pred: &FieldPredicate,
        event: &Event,
        after_view: Option<&Value>,
    ) -> Result<bool> {
        // `changed` compares before vs after — handled before resolving after.
        if pred.op == FieldOp::Changed {
            return Ok(eval_changed(event, &pred.path));
        }
        let column = pred.path.split('.').next().unwrap_or_default();
        if let Some(CanonicalValue::Unchanged) =
            event.after.as_ref().and_then(|r| r.get(column))
        {
            anyhow::bail!(
                "filter {}: column `{column}` of {} was not sent by \
                 PostgreSQL (unchanged TOAST value): predicate `{}` cannot be \
                 evaluated",
                self.id,
                event.source.full_table_name(),
                pred.path
            );
        }

        let field_val = after_view.and_then(|v| resolve_path(v, &pred.path));

        Ok(match pred.op {
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
        })
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

    fn identity_digest(&self) -> &str {
        &self.digest
    }

    async fn process(
        &self,
        events: Vec<Event>,
        _ctx: &BatchContext,
    ) -> Result<Vec<Event>> {
        let in_len = events.len();
        let mut out: Vec<Event> = Vec::with_capacity(in_len);
        for ev in events {
            let pass = self.op_allowed(&ev.op)
                && self.table_allowed(ev.source.namespace(), &ev.source.table)
                && self.fields_match(&ev)?;
            if pass {
                out.push(ev);
            } else {
                counter!("deltaforge_filter_dropped_total",
                    "processor" => self.id.clone()
                )
                .increment(1);
            }
        }

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
/// A column PostgreSQL did not send in the after image (unchanged TOAST) is
/// unchanged by definition.
fn eval_changed(event: &Event, path: &str) -> bool {
    match (&event.before, &event.after) {
        (Some(b), Some(a)) => {
            let column = path.split('.').next().unwrap_or_default();
            if let Some(CanonicalValue::Unchanged) = a.get(column) {
                return false;
            }
            let bv = Value::Object(row_view(b, event.type_mapping));
            let av = Value::Object(row_view(a, event.type_mapping));
            match (resolve_path(&bv, path), resolve_path(&av, path)) {
                (Some(b), Some(a)) => !json_eq(b, a),
                (None, None) => false,
                _ => true, // one side missing = field appeared or disappeared
            }
        }
        // Create (no before) or Delete (no after) — unconditionally pass.
        _ => true,
    }
}

/// Equality that compares integers exactly (whatever their width) and
/// other numbers numerically (42 == 42.0).
fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            match (as_integer(an), as_integer(bn)) {
                (Some(x), Some(y)) => x == y,
                _ => an.as_f64() == bn.as_f64(),
            }
        }
        _ => a == b,
    }
}

/// An integral JSON number as an i128 (exact for every i64 and u64).
fn as_integer(n: &serde_json::Number) -> Option<i128> {
    n.as_i64()
        .map(i128::from)
        .or_else(|| n.as_u64().map(i128::from))
}

/// Numeric and string ordering comparison.
/// Returns `None` when types are incomparable.
fn cmp_values(
    field: Option<&Value>,
    predicate: Option<&Value>,
) -> Option<std::cmp::Ordering> {
    match (field?, predicate?) {
        (Value::Number(a), Value::Number(b)) => {
            match (as_integer(a), as_integer(b)) {
                (Some(x), Some(y)) => Some(x.cmp(&y)),
                _ => a.as_f64()?.partial_cmp(&b.as_f64()?),
            }
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}
