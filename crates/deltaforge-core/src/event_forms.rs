//! An event's two serialized forms: the native rendering and the durable
//! form.

use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::canonical::render::{
    RenderError, RenderOpts, RowImage, WideIntegers, row_image,
};
use crate::canonical::{CanonicalRow, TypeMapping};
use crate::{Event, EventId, Op, SourceInfo, Transaction};

/// A sink's native-rendering settings. The mapping version is the event's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeOpts {
    pub wide_integers: WideIntegers,
}

impl NativeOpts {
    fn render(self, mapping: TypeMapping) -> RenderOpts {
        RenderOpts {
            mapping,
            wide_integers: self.wide_integers,
        }
    }
}

/// An event rendered natively: the Debezium payload structure, with the
/// row images rendered for the event's mapping version, `type_mapping` in
/// the `source` block, and (when PostgreSQL did not send some columns)
/// `unchanged_columns`. The row images are checked [`RowImage`]s, written
/// as they serialize; the path view uses JSON maps instead.
#[derive(Debug, Clone)]
pub struct NativePayload<'a, I = RowImage<'a>> {
    pub event: &'a Event,
    pub before: Option<I>,
    pub after: Option<I>,
    /// After-image columns PostgreSQL did not send (unchanged TOAST).
    pub unchanged_columns: Vec<String>,
}

impl Event {
    /// The native rendering of this event.
    pub fn native(
        &self,
        opts: NativeOpts,
    ) -> Result<NativePayload<'_>, RenderError> {
        let opts = opts.render(self.type_mapping);
        fn image<'r>(
            image: &str,
            row: &'r CanonicalRow,
            opts: RenderOpts,
        ) -> Result<(RowImage<'r>, Vec<String>), RenderError> {
            row_image(row, opts).map_err(|e| RenderError {
                column: format!("{image}.{}", e.column),
                failure: e.failure,
            })
        }
        let before = match &self.before {
            Some(r) => {
                let (row, unchanged) = image("before", r, opts)?;
                debug_assert!(
                    unchanged.is_empty(),
                    "before images are complete"
                );
                Some(row)
            }
            None => None,
        };
        let (after, unchanged_columns) = match &self.after {
            Some(r) => {
                let (row, unchanged) = image("after", r, opts)?;
                (Some(row), unchanged)
            }
            None => (None, Vec::new()),
        };
        Ok(NativePayload {
            event: self,
            before,
            after,
            unchanged_columns,
        })
    }

    /// The native rendering as a JSON value.
    pub fn native_value(&self, opts: NativeOpts) -> Result<Value, RenderError> {
        Ok(serde_json::to_value(self.native(opts)?)
            .expect("a native payload serializes"))
    }

    /// The path view of this event (filters, routing templates, schema
    /// sensing): its native structure with the row images as
    /// [`crate::canonical::view::row_view`]s.
    pub fn view_value(&self) -> Value {
        let mut payload: NativePayload<'_, Map<String, Value>> =
            NativePayload {
                event: self,
                before: None,
                after: None,
                unchanged_columns: Vec::new(),
            };
        payload.before = self
            .before
            .as_ref()
            .map(|r| crate::canonical::view::row_view(r, self.type_mapping));
        payload.after = self
            .after
            .as_ref()
            .map(|r| crate::canonical::view::row_view(r, self.type_mapping));
        payload.unchanged_columns = self
            .after
            .as_ref()
            .map(|r| {
                r.iter()
                    .filter(|(_, v)| {
                        matches!(v, crate::CanonicalValue::Unchanged)
                    })
                    .map(|(n, _)| n.to_string())
                    .collect()
            })
            .unwrap_or_default();
        serde_json::to_value(payload).expect("a view serializes")
    }

    /// The lossless durable form as a JSON value.
    pub fn to_durable(&self) -> Value {
        serde_json::to_value(DurableEvent(self))
            .expect("a durable event serializes")
    }

    /// Read an event's durable form.
    pub fn from_durable(v: Value) -> Result<Event, serde_json::Error> {
        let d: DurableOwned = serde_json::from_value(v)?;
        Ok(Event {
            before: d.before,
            after: d.after,
            source: d.source,
            op: d.op,
            ts_ms: d.ts_ms,
            transaction: d.transaction,
            type_mapping: d.type_mapping,
            event_id: d.event_id,
            tenant_id: d.tenant_id,
            schema_version: d.schema_version,
            schema_sequence: d.schema_sequence,
            ddl: d.ddl,
            trace_id: d.trace_id,
            tags: d.tags,
            synthetic: d.synthetic,
            routing: None,
            tx_end: d.tx_end,
            boundary: None,
            size_bytes: 0,
            received_at_ms: 0,
        })
    }
}

impl<'a> NativePayload<'a> {
    /// This payload with each present row image written as `{}`.
    pub(crate) fn row_placeholders(&self) -> NativePayload<'a> {
        NativePayload {
            event: self.event,
            before: self.before.map(RowImage::placeholder),
            after: self.after.map(RowImage::placeholder),
            unchanged_columns: self.unchanged_columns.clone(),
        }
    }
}

/// The `source` block of the native rendering: the source metadata and the
/// event's mapping version.
#[derive(Serialize)]
struct NativeSource<'a> {
    #[serde(flatten)]
    source: &'a SourceInfo,
    type_mapping: TypeMapping,
}

impl<I: Serialize> Serialize for NativePayload<'_, I> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let e = self.event;
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("before", &self.before)?;
        m.serialize_entry("after", &self.after)?;
        m.serialize_entry(
            "source",
            &NativeSource {
                source: &e.source,
                type_mapping: e.type_mapping,
            },
        )?;
        m.serialize_entry("op", &e.op)?;
        m.serialize_entry("ts_ms", &e.ts_ms)?;
        if let Some(t) = &e.transaction {
            m.serialize_entry("transaction", t)?;
        }
        if let Some(id) = &e.event_id {
            m.serialize_entry("event_id", id)?;
        }
        if let Some(v) = &e.tenant_id {
            m.serialize_entry("tenant_id", v)?;
        }
        if let Some(v) = &e.schema_version {
            m.serialize_entry("schema_version", v)?;
        }
        if let Some(v) = &e.schema_sequence {
            m.serialize_entry("schema_sequence", v)?;
        }
        if let Some(v) = &e.ddl {
            m.serialize_entry("ddl", v)?;
        }
        if let Some(v) = &e.trace_id {
            m.serialize_entry("trace_id", v)?;
        }
        if let Some(v) = &e.tags {
            m.serialize_entry("tags", v)?;
        }
        if let Some(v) = &e.synthetic {
            m.serialize_entry("synthetic", v)?;
        }
        if e.tx_end {
            m.serialize_entry("tx_end", &true)?;
        }
        if !self.unchanged_columns.is_empty() {
            m.serialize_entry("unchanged_columns", &self.unchanged_columns)?;
        }
        m.end()
    }
}

/// An event's lossless durable form (serialize with serde).
pub struct DurableEvent<'a>(pub &'a Event);

#[derive(Serialize)]
struct DurableRef<'a> {
    before: &'a Option<CanonicalRow>,
    after: &'a Option<CanonicalRow>,
    source: &'a SourceInfo,
    op: Op,
    ts_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction: &'a Option<Transaction>,
    type_mapping: TypeMapping,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_id: &'a Option<EventId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant_id: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_version: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_sequence: &'a Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ddl: &'a Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: &'a Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    synthetic: &'a Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    tx_end: bool,
}

/// The durable form read back. `type_mapping` is required: an event
/// without its version cannot be rendered.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableOwned {
    before: Option<CanonicalRow>,
    after: Option<CanonicalRow>,
    source: SourceInfo,
    op: Op,
    ts_ms: i64,
    #[serde(default)]
    transaction: Option<Transaction>,
    type_mapping: TypeMapping,
    #[serde(default)]
    event_id: Option<EventId>,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    schema_version: Option<String>,
    #[serde(default)]
    schema_sequence: Option<u64>,
    #[serde(default)]
    ddl: Option<Value>,
    #[serde(default)]
    trace_id: Option<String>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    synthetic: Option<String>,
    #[serde(default)]
    tx_end: bool,
}

impl Serialize for DurableEvent<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let e = self.0;
        DurableRef {
            before: &e.before,
            after: &e.after,
            source: &e.source,
            op: e.op,
            ts_ms: e.ts_ms,
            transaction: &e.transaction,
            type_mapping: e.type_mapping,
            event_id: &e.event_id,
            tenant_id: &e.tenant_id,
            schema_version: &e.schema_version,
            schema_sequence: &e.schema_sequence,
            ddl: &e.ddl,
            trace_id: &e.trace_id,
            tags: &e.tags,
            synthetic: &e.synthetic,
            tx_end: e.tx_end,
        }
        .serialize(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::{
        CanonicalValue, DbValue, Document, IntWidth, JsonText,
    };
    use crate::{SourcePosition, canonical_json_bytes};
    use serde_json::json;

    fn source(connector: &str) -> SourceInfo {
        SourceInfo {
            version: "test".into(),
            connector: connector.into(),
            name: "p".into(),
            ts_ms: 1,
            db: "d".into(),
            schema: None,
            table: "t".into(),
            snapshot: None,
            position: SourcePosition::default(),
        }
    }

    fn row(cols: Vec<(&str, CanonicalValue)>) -> CanonicalRow {
        CanonicalRow::new(
            cols.into_iter().map(|(n, v)| (n.to_string(), v)).collect(),
        )
        .unwrap()
    }

    fn big() -> CanonicalValue {
        CanonicalValue::Db(DbValue::uint(u64::MAX, IntWidth::W64).unwrap())
    }

    fn event(mapping: TypeMapping) -> Event {
        let mut e = Event::new_row(
            EventId::mysql_row_server(1, "t", 1, 0),
            source("mysql"),
            Op::Update,
            Some(row(vec![
                ("id", big()),
                (
                    "doc",
                    CanonicalValue::Db(DbValue::Json(
                        JsonText::new("null".into()).unwrap(),
                    )),
                ),
            ])),
            Some(row(vec![
                ("id", big()),
                ("doc", CanonicalValue::Unchanged),
                (
                    "extra",
                    CanonicalValue::Document(Document::new(
                        json!({"b": 1, "a": null}),
                    )),
                ),
                ("gone", CanonicalValue::Null),
            ])),
            mapping,
            5,
            10,
        );
        e.tenant_id = Some("acme".into());
        e
    }

    #[test]
    fn native_renders_the_event_version_and_lists_unchanged_columns() {
        let v = event(TypeMapping::V2)
            .native_value(NativeOpts::default())
            .unwrap();
        assert_eq!(v["source"]["type_mapping"], "v2");
        assert_eq!(v["source"]["connector"], "mysql");
        assert_eq!(v["after"]["id"], "18446744073709551615");
        assert_eq!(v["before"]["doc"], "null");
        assert!(
            v["after"].get("doc").is_none(),
            "an unchanged column is never a value"
        );
        assert_eq!(v["unchanged_columns"], json!(["doc"]));
        assert_eq!(v["after"]["extra"], json!({"a": null, "b": 1}));
        assert_eq!(v["after"]["gone"], Value::Null);
        assert_eq!(v["op"], "u");

        let v1 = event(TypeMapping::V1)
            .native_value(NativeOpts::default())
            .unwrap();
        assert_eq!(v1["source"]["type_mapping"], "v1");
        assert_eq!(v1["after"]["id"], json!(u64::MAX));
    }

    #[test]
    fn native_number_mode_fails_on_a_wide_unsigned_value() {
        let opts = NativeOpts {
            wide_integers: WideIntegers::Number,
        };
        let err = event(TypeMapping::V2).native(opts).unwrap_err();
        assert_eq!(
            err.failure,
            crate::canonical::render::RenderFailure::WideUnsignedNumber
        );
        assert_eq!(err.column, "before.id");
    }

    #[test]
    fn the_durable_form_reads_back_the_identical_event() {
        for mapping in TypeMapping::ALL {
            let e = event(mapping);
            let back = Event::from_durable(e.to_durable()).unwrap();
            assert_eq!(back.before, e.before);
            assert_eq!(back.after, e.after);
            assert_eq!(back.type_mapping, mapping);
            assert_eq!(back.tenant_id, e.tenant_id);
            assert_eq!(back.event_id, e.event_id);
            assert_eq!(
                canonical_json_bytes(&back.to_durable()),
                canonical_json_bytes(&e.to_durable())
            );
        }
    }

    #[test]
    fn a_durable_event_without_its_version_is_refused() {
        let mut v = event(TypeMapping::V2).to_durable();
        v.as_object_mut().unwrap().remove("type_mapping");
        assert!(Event::from_durable(v).is_err());
        let mut v = event(TypeMapping::V2).to_durable();
        v["type_mapping"] = json!("v9");
        assert!(Event::from_durable(v).is_err());
    }

    #[test]
    fn a_native_json_event_is_not_a_durable_event() {
        // An untyped (native) row image never reads back as canonical values.
        let native = event(TypeMapping::V2)
            .native_value(NativeOpts::default())
            .unwrap();
        assert!(Event::from_durable(native).is_err());
    }
}
