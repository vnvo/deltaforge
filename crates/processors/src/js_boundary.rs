//! The typed boundary between canonical events and JavaScript.
//!
//! Scripts receive each event's native rendering for its own mapping
//! version, with every value JavaScript cannot hold in a number (64-bit
//! integers, wide BIT values under v1) as its exact decimal string. Column
//! types never enter the script: each output is re-typed against the input
//! it retains (a 1:1 transform) or derives from (`derive(parent, output)`)
//! and that input's resolved schema context (the declared column types of
//! its schema version, resolved once per distinct version in the batch):
//!
//! - a column whose value equals its original rendering gets its original
//!   canonical value back, exactly;
//! - any other value of a database column (a changed value, a value for a
//!   NULL or unchanged TOAST column, a column the image did not carry)
//!   parses exactly as the column's declared type: ENUM labels and SET
//!   members against their declared symbols. A value that does not parse
//!   fails the batch;
//! - a removed column is removed;
//! - a column the schema does not declare, added by the script, is an
//!   untyped [`CanonicalValue::Document`] (typed sinks encode it only where
//!   they support documents, or refuse it).
//!
//! A database column whose schema context is missing (the event names a
//! version that does not resolve, or the pipeline has none) or conflicts
//! with the event's value fails the batch: it is never downgraded to a
//! document.

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use deltaforge_core::canonical::DbValue;
use deltaforge_core::canonical::Document;
use deltaforge_core::canonical::parse::parse_like;
use deltaforge_core::canonical::schema::{ColumnType, ColumnTypes};
use deltaforge_core::{
    CanonicalRow, CanonicalValue, Event, NativeOpts, TypeMapping,
};
use serde_json::{Map, Value};

/// An input event's schema context.
#[derive(Clone)]
pub(crate) enum SchemaCtx {
    /// The event names no schema version: its columns are not database
    /// columns (documents, logical-message bytes).
    NoReference,
    Resolved(Arc<ColumnTypes>),
    /// The event names a version that did not resolve: why.
    Missing(String),
}

/// One input event's typed rows, the renderings the script saw, and its
/// schema context.
pub(crate) struct Authority {
    pub mapping: TypeMapping,
    pub schema: SchemaCtx,
    pub before: Option<(CanonicalRow, Map<String, Value>)>,
    pub after: Option<(CanonicalRow, Map<String, Value>)>,
}

/// The script's view of `event`, and the authority to re-type its outputs.
pub(crate) fn to_js(
    event: &Event,
    schema: SchemaCtx,
) -> Result<(Value, Authority)> {
    let native = event
        .native(NativeOpts::default())
        .map_err(|e| anyhow!("render event for JS: {e}"))?;
    let before = native
        .before
        .zip(event.before.clone())
        .map(|(m, r)| (r.clone(), js_safe(&r, m.to_map())));
    let after = native
        .after
        .zip(event.after.clone())
        .map(|(m, r)| (r.clone(), js_safe(&r, m.to_map())));
    let mut value =
        serde_json::to_value(&native).expect("a native payload serializes");
    if let Value::Object(o) = &mut value {
        if let Some((_, m)) = &before {
            o.insert("before".into(), Value::Object(m.clone()));
        }
        if let Some((_, m)) = &after {
            o.insert("after".into(), Value::Object(m.clone()));
        }
    }
    Ok((
        value,
        Authority {
            mapping: event.type_mapping,
            schema,
            before,
            after,
        },
    ))
}

/// Replace every number JavaScript cannot hold exactly (a 64-bit integer, a
/// v1 BIT number) by its decimal string.
fn js_safe(
    row: &CanonicalRow,
    mut rendered: Map<String, Value>,
) -> Map<String, Value> {
    for (name, v) in row.iter() {
        let unsafe_number = matches!(
            v,
            CanonicalValue::Db(
                DbValue::Int { .. } | DbValue::UInt { .. } | DbValue::Bits(_)
            )
        ) && !fits_js(rendered.get(name));
        if unsafe_number && let Some(slot) = rendered.get_mut(name) {
            *slot = Value::String(slot.to_string());
        }
    }
    rendered
}

/// Whether a rendered value is not a number JavaScript would round.
fn fits_js(v: Option<&Value>) -> bool {
    const MAX_SAFE: u64 = 9_007_199_254_740_991;
    match v {
        Some(Value::Number(n)) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => i.unsigned_abs() <= MAX_SAFE,
            (_, Some(u)) => u <= MAX_SAFE,
            _ => true,
        },
        _ => true,
    }
}

/// Whether a value the script returned is the rendering it was given.
/// Numbers compare numerically: V8 hands integers back as floats.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| same(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same(v, w)))
        }
        _ => a == b,
    }
}

/// Re-type one row image the script returned, against the input's row, its
/// rendering and its schema context (`row`: `None` when the input had no
/// such image).
pub(crate) fn from_js_row(
    image: &str,
    row: Option<&(CanonicalRow, Map<String, Value>)>,
    schema: &SchemaCtx,
    mut returned: Map<String, Value>,
    mapping: TypeMapping,
) -> Result<CanonicalRow> {
    let mut columns = Vec::with_capacity(returned.len());
    let mut extensions: Vec<(String, Document)> = Vec::new();
    if let Some((row, rendered)) = row {
        for (name, original) in row.iter() {
            let Some(new) = returned.remove(name) else {
                if matches!(original, CanonicalValue::Unchanged) {
                    // Not sent by PostgreSQL, and not touched.
                    columns.push((name.to_string(), CanonicalValue::Unchanged));
                }
                continue;
            };
            let value = match rendered.get(name) {
                Some(seen) if same(&new, seen) => original.clone(),
                _ => match retype(
                    image,
                    name,
                    Some(original),
                    &new,
                    schema,
                    mapping,
                )? {
                    Slot::Column(v) => v,
                    Slot::Extension(_) => {
                        unreachable!("an existing column stays a column")
                    }
                },
            };
            columns.push((name.to_string(), value));
        }
        // The input's extensions: kept as they are, changed, or removed.
        for (name, doc) in row.extensions() {
            if let Some(new) = returned.remove(name) {
                let doc = if same(&new, doc.value()) {
                    doc.clone()
                } else {
                    Document::new(new)
                };
                extensions.push((name.clone(), doc));
            }
        }
    }
    // Fields the image did not carry: a declared column is typed by its
    // declaration; anything else is an extension (or, on an event naming no
    // schema version, a document column).
    for (name, new) in returned {
        match retype(image, &name, None, &new, schema, mapping)? {
            Slot::Column(v) => columns.push((name, v)),
            Slot::Extension(d) => extensions.push((name, d)),
        }
    }
    CanonicalRow::with_extensions(columns, extensions)
        .map_err(|e| anyhow!("JS processor {image}: {e}"))
}

/// Where a value the script returned belongs.
enum Slot {
    Column(CanonicalValue),
    Extension(Document),
}

/// Whether `v` is a value of a database column (as opposed to a document
/// or logical-message bytes): its type is the schema's to say.
fn is_database_value(v: &CanonicalValue) -> bool {
    matches!(
        v,
        CanonicalValue::Null
            | CanonicalValue::Unchanged
            | CanonicalValue::Db(_)
            | CanonicalValue::Opaque(_)
    )
}

/// Type a value the script gave column `name` (`original`: its value in
/// the input image, if it had one).
fn retype(
    image: &str,
    name: &str,
    original: Option<&CanonicalValue>,
    new: &Value,
    schema: &SchemaCtx,
    mapping: TypeMapping,
) -> Result<Slot> {
    let declared = match schema {
        SchemaCtx::Resolved(types) => types.get(name),
        _ => None,
    };
    let changed = |e| anyhow!("JS processor changed {image}.{name}: {e}");
    match (declared, original) {
        (Some(ty), original) => {
            if let Some(o) = original
                && !ty.admits(o)
            {
                bail!(
                    "JS processor: {image}.{name} conflicts with its schema \
                     context (the event's value is not of the declared type)"
                );
            }
            // An opaque value keeps the identity its source gave it.
            if let (
                ColumnType::Opaque(_),
                Some(o @ CanonicalValue::Opaque(_)),
            ) = (ty, original)
            {
                return parse_like(o, new, mapping)
                    .map(Slot::Column)
                    .map_err(changed);
            }
            ty.parse(new, mapping).map(Slot::Column).map_err(changed)
        }
        (None, Some(o)) if is_database_value(o) => match schema {
            SchemaCtx::Missing(why) => bail!(
                "JS processor changed {image}.{name}, but the event's schema \
                 context is missing ({why}): the value cannot be typed"
            ),
            SchemaCtx::Resolved(_) => bail!(
                "JS processor changed {image}.{name}, which the event's schema \
                 version does not declare (conflicting schema context)"
            ),
            SchemaCtx::NoReference => bail!(
                "JS processor changed {image}.{name}, a database column of an \
                 event that names no schema version: the value cannot be typed"
            ),
        },
        // A document or bytes column types itself.
        (None, Some(o)) => parse_like(o, new, mapping)
            .map(Slot::Column)
            .map_err(changed),
        // A field only the script knows.
        (None, None) => match schema {
            // The schema declares every column: an extension.
            SchemaCtx::Resolved(_) => {
                Ok(Slot::Extension(Document::new(new.clone())))
            }
            // A schema-less event's fields are its columns.
            SchemaCtx::NoReference => Ok(Slot::Column(
                CanonicalValue::Document(Document::new(new.clone())),
            )),
            // Without the schema the field may be a declared column.
            SchemaCtx::Missing(why) => bail!(
                "JS processor added {image}.{name}, but the event's schema \
                 context is missing ({why}): whether it is a declared column \
                 is unknown"
            ),
        },
    }
}
