use common::TableFilter;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ============================================================================
// Op filter
// ============================================================================

/// Operation types that can be included or excluded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpFilter {
    Create,
    Update,
    Delete,
    Read,
    Truncate,
}

// ============================================================================
// Field predicates
// ============================================================================

/// Comparison operator for a field predicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldOp {
    /// Field equals value.
    Eq,
    /// Field does not equal value.
    Ne,
    /// Field exists (is present and not null).
    Exists,
    /// Field does not exist (absent or null).
    NotExists,
    /// Field > value (numeric or string comparison).
    Gt,
    /// Field >= value.
    Gte,
    /// Field < value.
    Lt,
    /// Field <= value.
    Lte,
    /// Field value is one of the items in the array `value`.
    /// `value` must be a JSON array.
    In,
    /// Field value is not in the array `value`.
    /// `value` must be a JSON array.
    NotIn,
    /// String field contains the substring, OR array field contains the element.
    Contains,
    /// Field value differs between `event.before` and `event.after`.
    /// Passes Creates and Deletes unconditionally (no before/after to compare).
    /// `value` is ignored.
    Changed,
    /// Field string value matches the regex pattern in `value`.
    Regex,
}

/// A predicate evaluated against a field in `event.after`.
///
/// `path` is a dot-separated path into the JSON object, e.g. `"order.total"`.
/// For `exists` / `not_exists` / `changed`, `value` is ignored.
/// For `in` / `not_in`, `value` must be a JSON array.
/// For `regex`, `value` must be a JSON string containing a valid regex pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldPredicate {
    /// Dot-separated path into `event.after`, e.g. `"status"` or `"order.total"`.
    pub path: String,
    /// Comparison operator.
    pub op: FieldOp,
    /// Value to compare against (not used for `exists` / `not_exists` / `changed`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

// ============================================================================
// Match mode
// ============================================================================

/// How multiple field predicates are combined.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchMode {
    /// Event passes only if ALL predicates match (default).
    #[default]
    All,
    /// Event passes if ANY predicate matches.
    Any,
}

// ============================================================================
// Processor config
// ============================================================================

fn default_filter_id() -> String {
    "filter".into()
}

/// Filter processor config.
///
/// Drops events that do not satisfy all configured criteria. Each criterion
/// is optional; omit it to skip that check entirely.
///
/// ```yaml
/// processors:
///   - type: filter
///     id: only-active-orders
///     ops: [create, update]
///     tables:
///       include: ["shop.orders"]
///       exclude: ["*.tmp"]
///     fields:
///       - path: status
///         op: eq
///         value: "active"
///       - path: total
///         op: gte
///         value: 100
///     match: all
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FilterProcessorCfg {
    /// Processor identifier.
    #[serde(default = "default_filter_id")]
    pub id: String,

    /// Operations to keep. Empty = keep all.
    pub ops: Vec<OpFilter>,

    /// Table include/exclude patterns.
    pub tables: TableFilter,

    /// Field predicates evaluated against `event.after`.
    pub fields: Vec<FieldPredicate>,

    /// How to combine multiple `fields` predicates.
    #[serde(rename = "match")]
    pub match_mode: MatchMode,
}

impl Default for FilterProcessorCfg {
    fn default() -> Self {
        Self {
            id: default_filter_id(),
            ops: vec![],
            tables: TableFilter::default(),
            fields: vec![],
            match_mode: MatchMode::All,
        }
    }
}