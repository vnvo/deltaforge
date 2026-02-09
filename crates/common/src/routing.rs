//! Template-based event routing for sink destinations.
//!
//! Sink destination fields (topic, stream, subject) accept templates that resolve
//! against event fields at send time. Templates use `${path}` syntax with dotted
//! paths into the event JSON.
//!
//! # Examples
//!
//! ```text
//! "cdc.${source.table}"          → "cdc.orders"
//! "${tenant_id}.${source.table}" → "acme.orders"
//! "${after.customer_id}"         → "42"
//! "static-topic"                 → "static-topic" (no resolution needed)
//! ```
//!
//! # Performance
//!
//! - Static templates (no `${...}`) are detected at parse time and skip resolution entirely.
//! - Templates compile once at sink construction. Resolution is segment concatenation
//!   over pre-parsed segments — no regex or parsing in the hot path.
//! - `serde_json::to_value` is only called when a template has variables.

use std::collections::HashSet;
use std::fmt;
use std::sync::Mutex;

use serde_json::Value;
use tracing::warn;

// =============================================================================
// Errors
// =============================================================================

/// Template parsing error — caught at config load time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateError {
    /// Unclosed `${` in template string.
    UnclosedVariable { raw: String, pos: usize },
    /// Path has more than 3 segments (e.g., `${a.b.c.d}`).
    PathTooDeep { raw: String, path: String },
    /// Empty variable name `${}`.
    EmptyVariable { raw: String, pos: usize },
}

impl fmt::Display for TemplateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnclosedVariable { raw, pos } => {
                write!(
                    f,
                    "unclosed '${{' at position {} in template: {}",
                    pos, raw
                )
            }
            Self::PathTooDeep { raw, path } => {
                write!(
                    f,
                    "path '{}' exceeds max depth of 3 segments in template: {}",
                    path, raw
                )
            }
            Self::EmptyVariable { raw, pos } => {
                write!(
                    f,
                    "empty variable '${{}}' at position {} in template: {}",
                    pos, raw
                )
            }
        }
    }
}

impl std::error::Error for TemplateError {}

// =============================================================================
// Template Types
// =============================================================================

/// A single segment of a compiled template.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TemplateSegment {
    /// Literal text — copied verbatim to output.
    Literal(String),
    /// Variable path — resolved against event JSON.
    /// Stored as dotted path segments, max 3 deep.
    Variable(Vec<String>),
}

/// Compiled template for efficient per-event resolution.
///
/// Parse once at sink construction, resolve per-event via segment concatenation.
/// Static templates (no variables) are a zero-cost path.
#[derive(Debug)]
pub struct CompiledTemplate {
    segments: Vec<TemplateSegment>,
    /// True when template has no variables — skip resolution entirely.
    is_static: bool,
    /// Original template string for diagnostics.
    raw: String,
    /// Tracks which (variable_path) combos have already logged a warning
    /// to avoid log spam on repeated unresolvable fields.
    warned: Mutex<HashSet<String>>,
}

/// clone resets the warned set (clean slate, correct behavior since a cloned template is a new context)
impl Clone for CompiledTemplate {
    fn clone(&self) -> Self {
        Self {
            segments: self.segments.clone(),
            is_static: self.is_static,
            raw: self.raw.clone(),
            warned: Mutex::new(HashSet::new()),
        }
    }
}

impl CompiledTemplate {
    /// Parse a template string into a compiled template.
    ///
    /// Validates syntax at parse time:
    /// - Unclosed `${` → error
    /// - Empty `${}` → error
    /// - Paths deeper than 3 segments → error
    ///
    /// Static strings (no `${`) produce an `is_static` template with zero
    /// resolution overhead.
    pub fn parse(raw: &str) -> Result<Self, TemplateError> {
        let mut segments = Vec::new();
        let mut rest = raw;
        let mut offset = 0;

        while let Some(start) = rest.find("${") {
            // Literal before the variable
            if start > 0 {
                segments
                    .push(TemplateSegment::Literal(rest[..start].to_string()));
            }

            let after_open = start + 2;
            let Some(end) = rest[after_open..].find('}') else {
                return Err(TemplateError::UnclosedVariable {
                    raw: raw.to_string(),
                    pos: offset + start,
                });
            };
            let end_abs = after_open + end;

            let path_str = &rest[after_open..end_abs];
            if path_str.is_empty() {
                return Err(TemplateError::EmptyVariable {
                    raw: raw.to_string(),
                    pos: offset + start,
                });
            }

            let parts: Vec<String> =
                path_str.split('.').map(|s| s.to_string()).collect();
            if parts.len() > 3 {
                return Err(TemplateError::PathTooDeep {
                    raw: raw.to_string(),
                    path: path_str.to_string(),
                });
            }

            segments.push(TemplateSegment::Variable(parts));

            offset += end_abs + 1;
            rest = &rest[end_abs + 1..];
        }

        // Trailing literal
        if !rest.is_empty() {
            segments.push(TemplateSegment::Literal(rest.to_string()));
        }

        let is_static = segments
            .iter()
            .all(|s| matches!(s, TemplateSegment::Literal(_)));

        Ok(Self {
            segments,
            is_static,
            raw: raw.to_string(),
            warned: Mutex::new(HashSet::new()),
        })
    }

    /// Returns the original template string.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Returns true if the template has no variables (zero-cost resolution).
    pub fn is_static(&self) -> bool {
        self.is_static
    }

    /// Resolve the template strictly — returns `Err` if any variable is unresolvable.
    ///
    /// Use for topic/subject resolution where an empty segment would create
    /// a garbage destination.
    pub fn resolve_strict(
        &self,
        event_json: &Value,
    ) -> Result<String, UnresolvedVar> {
        if self.is_static {
            return Ok(self.static_value());
        }

        let mut result = String::with_capacity(self.raw.len());
        for seg in &self.segments {
            match seg {
                TemplateSegment::Literal(s) => result.push_str(s),
                TemplateSegment::Variable(path) => {
                    let val = resolve_path(event_json, path);
                    match val {
                        Some(s) => result.push_str(&s),
                        None => {
                            return Err(UnresolvedVar {
                                template: self.raw.clone(),
                                path: path.join("."),
                            });
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    /// Resolve the template leniently — unresolvable variables become empty string.
    ///
    /// Use for key resolution where empty = round-robin partitioning.
    /// Logs a warning once per unique unresolvable path.
    pub fn resolve_lenient(&self, event_json: &Value) -> String {
        if self.is_static {
            return self.static_value();
        }

        let mut result = String::with_capacity(self.raw.len());
        for seg in &self.segments {
            match seg {
                TemplateSegment::Literal(s) => result.push_str(s),
                TemplateSegment::Variable(path) => {
                    match resolve_path(event_json, path) {
                        Some(s) => result.push_str(&s),
                        None => self.warn_unresolved(path),
                    }
                }
            }
        }
        result
    }

    /// Returns the static value for templates with no variables.
    /// Panics if the template is not static (caller should check `is_static`).
    fn static_value(&self) -> String {
        debug_assert!(self.is_static);
        self.segments
            .iter()
            .map(|s| match s {
                TemplateSegment::Literal(l) => l.as_str(),
                TemplateSegment::Variable(_) => unreachable!(),
            })
            .collect()
    }

    /// Returns the static value as a borrowed str, if the template is a single literal.
    /// Useful for `Cow<str>` optimizations.
    pub fn as_static_str(&self) -> Option<&str> {
        if self.is_static && self.segments.len() == 1 {
            match &self.segments[0] {
                TemplateSegment::Literal(s) => Some(s),
                _ => None,
            }
        } else {
            None
        }
    }

    fn warn_unresolved(&self, path: &[String]) {
        let key = path.join(".");
        let mut warned = self.warned.lock().unwrap();
        if warned.insert(key.clone()) {
            warn!(
                template = %self.raw,
                path = %key,
                "unresolvable template variable — resolved to empty string"
            );
        }
    }
}

/// Error returned by strict template resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedVar {
    pub template: String,
    pub path: String,
}

impl fmt::Display for UnresolvedVar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "template '{}': variable '{}' could not be resolved",
            self.template, self.path
        )
    }
}

impl std::error::Error for UnresolvedVar {}

// =============================================================================
// Path Resolution
// =============================================================================

/// Resolve a dotted path against a JSON value.
///
/// Returns `None` if any segment is missing or the final value is null.
/// Non-string values (numbers, booleans) are converted via `to_string()`.
fn resolve_path(root: &Value, path: &[String]) -> Option<String> {
    let mut current = root;
    for segment in path {
        current = current.get(segment.as_str())?;
    }
    match current {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // =========================================================================
    // Parsing
    // =========================================================================

    #[test]
    fn parse_static_template() {
        let t = CompiledTemplate::parse("static-topic").unwrap();
        assert!(t.is_static());
        assert_eq!(t.as_static_str(), Some("static-topic"));
    }

    #[test]
    fn parse_single_variable() {
        let t = CompiledTemplate::parse("${source.table}").unwrap();
        assert!(!t.is_static());
        assert_eq!(t.segments.len(), 1);
    }

    #[test]
    fn parse_mixed_literal_and_variable() {
        let t = CompiledTemplate::parse("cdc.${source.db}.${source.table}")
            .unwrap();
        assert!(!t.is_static());
        // "cdc." + var + "." + var
        assert_eq!(t.segments.len(), 4);
    }

    #[test]
    fn parse_nested_path() {
        let t = CompiledTemplate::parse("${after.metadata.region}").unwrap();
        assert!(!t.is_static());
        match &t.segments[0] {
            TemplateSegment::Variable(parts) => {
                assert_eq!(parts, &["after", "metadata", "region"]);
            }
            _ => panic!("expected variable"),
        }
    }

    #[test]
    fn parse_rejects_unclosed_variable() {
        let err = CompiledTemplate::parse("cdc.${source.table").unwrap_err();
        assert!(matches!(err, TemplateError::UnclosedVariable { .. }));
    }

    #[test]
    fn parse_rejects_empty_variable() {
        let err = CompiledTemplate::parse("cdc.${}").unwrap_err();
        assert!(matches!(err, TemplateError::EmptyVariable { .. }));
    }

    #[test]
    fn parse_rejects_path_too_deep() {
        let err = CompiledTemplate::parse("${a.b.c.d}").unwrap_err();
        assert!(matches!(err, TemplateError::PathTooDeep { .. }));
    }

    #[test]
    fn parse_three_segment_path_ok() {
        CompiledTemplate::parse("${after.metadata.region}").unwrap();
    }

    // =========================================================================
    // Resolution
    // =========================================================================

    fn sample_event_json() -> Value {
        json!({
            "before": null,
            "after": {"id": 42, "customer_id": "cust-7", "metadata": {"region": "eu-west"}},
            "source": {"db": "shop", "table": "orders", "connector": "mysql", "schema": "public"},
            "op": "c",
            "tenant_id": "acme",
            "ts_ms": 1700000000000_i64
        })
    }

    #[test]
    fn resolve_source_table() {
        let t = CompiledTemplate::parse("cdc.${source.table}").unwrap();
        let ev = sample_event_json();
        assert_eq!(t.resolve_strict(&ev).unwrap(), "cdc.orders");
    }

    #[test]
    fn resolve_multi_variable() {
        let t = CompiledTemplate::parse("cdc.${source.db}.${source.table}")
            .unwrap();
        let ev = sample_event_json();
        assert_eq!(t.resolve_strict(&ev).unwrap(), "cdc.shop.orders");
    }

    #[test]
    fn resolve_after_field() {
        let t = CompiledTemplate::parse("${after.customer_id}").unwrap();
        let ev = sample_event_json();
        assert_eq!(t.resolve_strict(&ev).unwrap(), "cust-7");
    }

    #[test]
    fn resolve_nested_after_field() {
        let t = CompiledTemplate::parse("${after.metadata.region}").unwrap();
        let ev = sample_event_json();
        assert_eq!(t.resolve_strict(&ev).unwrap(), "eu-west");
    }

    #[test]
    fn resolve_numeric_value_coerced_to_string() {
        let t = CompiledTemplate::parse("${after.id}").unwrap();
        let ev = sample_event_json();
        assert_eq!(t.resolve_strict(&ev).unwrap(), "42");
    }

    #[test]
    fn resolve_op_field() {
        let t = CompiledTemplate::parse("cdc.${source.table}.${op}").unwrap();
        let ev = sample_event_json();
        assert_eq!(t.resolve_strict(&ev).unwrap(), "cdc.orders.c");
    }

    #[test]
    fn resolve_tenant_id() {
        let t =
            CompiledTemplate::parse("${tenant_id}.${source.table}").unwrap();
        let ev = sample_event_json();
        assert_eq!(t.resolve_strict(&ev).unwrap(), "acme.orders");
    }

    #[test]
    fn strict_fails_on_missing_field() {
        let t = CompiledTemplate::parse("cdc.${source.table}").unwrap();
        let ev = json!({"source": {"db": "shop"}}); // no table
        let err = t.resolve_strict(&ev).unwrap_err();
        assert_eq!(err.path, "source.table");
    }

    #[test]
    fn strict_fails_on_null_field() {
        let t = CompiledTemplate::parse("${after.customer_id}").unwrap();
        let ev = json!({"after": {"customer_id": null}});
        let err = t.resolve_strict(&ev).unwrap_err();
        assert_eq!(err.path, "after.customer_id");
    }

    #[test]
    fn lenient_returns_empty_on_missing_field() {
        let t = CompiledTemplate::parse("cdc.${source.table}").unwrap();
        let ev = json!({"source": {"db": "shop"}}); // no table
        assert_eq!(t.resolve_lenient(&ev), "cdc.");
    }

    #[test]
    fn lenient_on_delete_with_no_after() {
        let t = CompiledTemplate::parse("${after.customer_id}").unwrap();
        let ev = json!({"before": {"customer_id": "cust-7"}, "after": null});
        assert_eq!(t.resolve_lenient(&ev), "");
    }

    #[test]
    fn static_template_skips_resolution() {
        let t = CompiledTemplate::parse("my-fixed-topic").unwrap();
        let ev = json!({}); // doesn't matter
        assert_eq!(t.resolve_strict(&ev).unwrap(), "my-fixed-topic");
        assert_eq!(t.resolve_lenient(&ev), "my-fixed-topic");
    }

    #[test]
    fn resolve_before_field() {
        let t = CompiledTemplate::parse("${before.customer_id}").unwrap();
        let ev = json!({"before": {"customer_id": "cust-old"}, "after": null});
        assert_eq!(t.resolve_strict(&ev).unwrap(), "cust-old");
    }
}
