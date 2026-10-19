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
use deltaforge_core::canonical::render::RowImage;
use deltaforge_core::canonical::schema::{ColumnType, ColumnTypes};
use deltaforge_core::{
    CanonicalRow, CanonicalValue, Event, NativeOpts, NativePayload, TypeMapping,
};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
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
    /// After-image columns PostgreSQL did not send.
    pub unchanged: Vec<String>,
}

/// The typed rows of `event` and their renderings for the script: the
/// authority to re-type its outputs.
pub(crate) fn to_js(event: &Event, schema: SchemaCtx) -> Result<Authority> {
    let native = event
        .native(NativeOpts::default())
        .map_err(|e| anyhow!("render event for JS: {e}"))?;
    let image = |rendered: Option<RowImage<'_>>, row: &Option<CanonicalRow>| {
        rendered
            .zip(row.as_ref())
            .map(|(m, r)| (r.clone(), js_safe(r, m.to_map())))
    };
    Ok(Authority {
        mapping: event.type_mapping,
        schema,
        before: image(native.before, &event.before),
        after: image(native.after, &event.after),
        unchanged: native.unchanged_columns,
    })
}

/// An input event as the script sees it: its native rendering with the
/// authority's renderings as its row images, and `__df_id` when it has a
/// stable id. Serialized straight into V8 (no intermediate JSON tree).
pub(crate) struct JsInput<'a> {
    pub event: &'a Event,
    pub authority: &'a Authority,
}

impl Serialize for JsInput<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let a = self.authority;
        let payload = NativePayload {
            event: self.event,
            before: a.before.as_ref().map(|(_, m)| m),
            after: a.after.as_ref().map(|(_, m)| m),
            unchanged_columns: a.unchanged.clone(),
        };
        let mut m = s.serialize_map(None)?;
        payload.serialize_entries(&mut m)?;
        if let Some(id) = self.event.event_id {
            m.serialize_entry(crate::js::DF_ID, &id.to_string())?;
        }
        m.end()
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::canonical::{IntWidth, fixture};
    use deltaforge_core::{EventId, Op, SourceInfo, SourcePosition};
    use serde_json::json;

    /// The object a script receives is the event's native rendering with
    /// the JavaScript-safe rows in place, then `__df_id`: the same keys, in
    /// the same order, with the same values, as the payload built through a
    /// JSON tree.
    #[test]
    fn the_script_sees_the_native_rendering_with_safe_rows_and_its_id() {
        let mut before = fixture::row(json!({"id": 1, "name": "a"}));
        before.set(
            "big",
            CanonicalValue::Db(DbValue::int(i64::MAX, IntWidth::W64).unwrap()),
        );
        let mut after = before.clone();
        after.set("doc", CanonicalValue::Unchanged);
        let mut event = Event::new_row(
            EventId::mysql_row_server(1, "f", 9, 0),
            SourceInfo {
                version: "v".into(),
                connector: "postgresql".into(),
                name: "src".into(),
                ts_ms: 5,
                db: "db".into(),
                schema: Some("public".into()),
                table: "t".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            Op::Update,
            Some(before),
            Some(after),
            TypeMapping::V2,
            7,
            64,
        );
        event.tenant_id = Some("acme".into());
        let authority = to_js(&event, SchemaCtx::NoReference).unwrap();
        let seen = serde_json::to_value(JsInput {
            event: &event,
            authority: &authority,
        })
        .unwrap();

        let mut expected = event.native_value(NativeOpts::default()).unwrap();
        let o = expected.as_object_mut().unwrap();
        o["before"] =
            Value::Object(authority.before.as_ref().unwrap().1.clone());
        o["after"] = Value::Object(authority.after.as_ref().unwrap().1.clone());
        o.insert(
            crate::js::DF_ID.into(),
            Value::String(event.event_id.unwrap().to_string()),
        );
        assert_eq!(seen, expected);
        let keys = |v: &Value| {
            v.as_object().unwrap().keys().cloned().collect::<Vec<_>>()
        };
        assert_eq!(keys(&seen), keys(&expected), "same key order");
        assert_eq!(seen["after"]["big"], json!(i64::MAX.to_string()));
        assert_eq!(seen["unchanged_columns"], json!(["doc"]));
    }
}
