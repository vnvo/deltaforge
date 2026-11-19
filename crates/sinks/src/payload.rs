//! How the message sinks (Kafka, Redis, NATS, HTTP) turn an event into
//! bytes and routing input, from its canonical values.

use deltaforge_core::encoding::EncodingError;
use deltaforge_core::envelope::Envelope;
use deltaforge_core::{Event, NativeOpts, SinkError, SinkResult};
use serde_json::Value;

/// The JSON routing templates resolve against: the event's path view
/// (exact integers, JSON columns as documents).
pub(crate) fn route_view(event: &Event) -> Value {
    event.view_value()
}

/// [`route_view`] for template resolution, built only as far as a path
/// needs: `before.<column>...` and `after.<column>...` read that one
/// column; any other path resolves against the full view, built once.
pub(crate) struct RouteView<'a> {
    event: &'a Event,
    full: std::cell::OnceCell<Value>,
}

impl<'a> RouteView<'a> {
    pub(crate) fn new(event: &'a Event) -> Self {
        Self {
            event,
            full: std::cell::OnceCell::new(),
        }
    }
}

impl common::PathLookup for RouteView<'_> {
    fn resolve(&self, path: &[String]) -> Option<String> {
        if let [image, column, rest @ ..] = path {
            let row = match image.as_str() {
                "before" => Some(&self.event.before),
                "after" => Some(&self.event.after),
                _ => None,
            };
            if let Some(row) = row {
                let v = deltaforge_core::canonical::view::column_view(
                    row.as_ref()?,
                    column,
                    self.event.type_mapping,
                )?;
                return common::resolve_path(&v, rest);
            }
        }
        common::resolve_path(
            self.full.get_or_init(|| route_view(self.event)),
            path,
        )
    }
}

/// The smallest and largest capacity a payload buffer starts with.
const MIN_PAYLOAD: usize = 256;
const MAX_PAYLOAD_HINT: usize = 64 * 1024;

thread_local! {
    /// The size of this thread's last payload: the next one's starting
    /// capacity.
    static PAYLOAD_HINT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(MIN_PAYLOAD) };
}

/// The event's envelope, natively rendered for its mapping version and
/// encoded as JSON. A value the rendering cannot hold under `opts` is a
/// sink conversion failure (an incident, DLQ-eligible).
pub(crate) fn native_json(
    envelope: &dyn Envelope,
    event: &Event,
    opts: NativeOpts,
    sink_id: &str,
) -> SinkResult<Vec<u8>> {
    let data = envelope
        .wrap(event, opts)
        .map_err(|e| EncodingError::from(e).into_sink_error_for(sink_id))?;
    // Straight into the message buffer (no intermediate `Bytes` copy),
    // sized like this thread's previous payload so it rarely grows.
    let mut buf = Vec::with_capacity(PAYLOAD_HINT.get());
    deltaforge_core::json_out::to_writer(&mut buf, &data)
        .map_err(|e| EncodingError::from(e).into_sink_error_for(sink_id))?;
    PAYLOAD_HINT.set(buf.len().clamp(MIN_PAYLOAD, MAX_PAYLOAD_HINT));
    Ok(buf)
}

/// Outbox raw mode: the after row's native rendering (JSON `null` when the
/// event has none).
pub(crate) fn raw_payload(
    event: &Event,
    opts: NativeOpts,
    sink_id: &str,
) -> SinkResult<Vec<u8>> {
    let payload = event.native(opts).map_err(|e| {
        EncodingError::from(deltaforge_core::envelope::EnvelopeError::from(e))
            .into_sink_error_for(sink_id)
    })?;
    deltaforge_core::json_out::to_vec(&payload.after).map_err(|e| {
        SinkError::Serialization {
            details: e.to_string().into(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::canonical::render::WideIntegers;
    use deltaforge_core::canonical::{
        CanonicalRow, CanonicalValue, DbValue, IntWidth,
    };
    use deltaforge_core::envelope::{Debezium, Native};
    use deltaforge_core::incident::ReasonCode;
    use deltaforge_core::{
        EventId, Op, SourceInfo, SourcePosition, TypeMapping,
    };
    use serde_json::json;

    fn event(v: DbValue) -> Event {
        let row =
            CanonicalRow::new(vec![("big".into(), CanonicalValue::Db(v))])
                .unwrap();
        Event::new_row(
            EventId::mysql_row_server(1, "t", 1, 0),
            SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "db".into(),
                ts_ms: 0,
                db: "shop".into(),
                schema: None,
                table: "t".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            Op::Create,
            None,
            Some(row),
            TypeMapping::V2,
            0,
            64,
        )
    }

    fn opts(w: WideIntegers) -> NativeOpts {
        NativeOpts { wide_integers: w }
    }

    fn after_big(bytes: &[u8]) -> Value {
        let v: Value = serde_json::from_slice(bytes).unwrap();
        v["after"]["big"].clone()
    }

    #[test]
    fn wide_integers_default_to_exact_strings() {
        let i = event(DbValue::int(i64::MIN, IntWidth::W64).unwrap());
        let u = event(DbValue::uint(u64::MAX, IntWidth::W64).unwrap());
        let d = NativeOpts::default();
        assert_eq!(
            after_big(&native_json(&Native, &i, d, "s").unwrap()),
            json!(i64::MIN.to_string())
        );
        assert_eq!(
            after_big(&native_json(&Native, &u, d, "s").unwrap()),
            json!(u64::MAX.to_string())
        );
    }

    #[test]
    fn wide_integers_as_numbers_render_i64_as_a_json_number() {
        let n = opts(WideIntegers::Number);
        let i = event(DbValue::int(i64::MAX, IntWidth::W64).unwrap());
        assert_eq!(
            after_big(&native_json(&Native, &i, n, "s").unwrap()),
            json!(i64::MAX)
        );
        // An unsigned value up to i64::MAX is a number too.
        let u = event(DbValue::uint(i64::MAX as u64, IntWidth::W64).unwrap());
        assert_eq!(
            after_big(&native_json(&Native, &u, n, "s").unwrap()),
            json!(i64::MAX)
        );
    }

    #[test]
    fn wide_unsigned_above_i64_max_as_number_is_a_conversion_failure() {
        let n = opts(WideIntegers::Number);
        let u =
            event(DbValue::uint(i64::MAX as u64 + 1, IntWidth::W64).unwrap());
        for envelope in [&Native as &dyn Envelope, &Debezium] {
            let err = native_json(envelope, &u, n, "kafka-1").unwrap_err();
            let draft = err.draft().expect("an incident");
            assert_eq!(draft.reason_code, ReasonCode::SinkConversionFailed);
            assert!(
                matches!(err.root(), SinkError::Serialization { .. }),
                "{err:?}"
            );
            let text = err.to_string();
            assert!(text.contains("after.big"), "{text}");
            assert!(
                !text.contains(&(i64::MAX as u64 + 1).to_string()),
                "the value never leaks: {text}"
            );
        }
        // The outbox raw payload fails the same way.
        let err = raw_payload(&u, n, "kafka-1").unwrap_err();
        assert!(err.draft().is_some(), "{err:?}");
    }

    #[test]
    fn raw_payload_is_the_native_after_row() {
        let i = event(DbValue::int(7, IntWidth::W64).unwrap());
        let bytes = raw_payload(&i, NativeOpts::default(), "s").unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            json!({"big": "7"})
        );
        let mut none = i.clone();
        none.after = None;
        assert_eq!(
            raw_payload(&none, NativeOpts::default(), "s").unwrap(),
            b"null"
        );
    }

    #[test]
    fn route_view_keeps_wide_integers_exact() {
        let u = event(DbValue::uint(u64::MAX, IntWidth::W64).unwrap());
        let view = route_view(&u);
        assert_eq!(view["after"]["big"], json!(u64::MAX));
    }

    #[test]
    fn route_view_paths_resolve_as_the_full_view() {
        use common::PathLookup;
        use deltaforge_core::canonical::{Document, JsonText};
        let cols = |id: i64| {
            vec![
                (
                    "id".to_string(),
                    CanonicalValue::Db(
                        DbValue::int(id, IntWidth::W64).unwrap(),
                    ),
                ),
                (
                    "big".to_string(),
                    CanonicalValue::Db(
                        DbValue::uint(u64::MAX, IntWidth::W64).unwrap(),
                    ),
                ),
                (
                    "name".to_string(),
                    CanonicalValue::Db(DbValue::Text("a\"b".into())),
                ),
                ("flag".to_string(), CanonicalValue::Db(DbValue::Bool(true))),
                ("gone".to_string(), CanonicalValue::Null),
                (
                    "doc".to_string(),
                    CanonicalValue::Db(DbValue::Json(
                        JsonText::new(
                            r#"{"a": {"b": [1, "x"]}, "n": null}"#.into(),
                        )
                        .unwrap(),
                    )),
                ),
                ("toast".to_string(), CanonicalValue::Unchanged),
            ]
        };
        let ext =
            || vec![("extra".to_string(), Document::new(json!({"k": 3})))];
        let mut update = event(DbValue::int(0, IntWidth::W64).unwrap());
        update.before =
            Some(CanonicalRow::with_extensions(cols(1), ext()).unwrap());
        update.after =
            Some(CanonicalRow::with_extensions(cols(2), ext()).unwrap());
        update.tenant_id = Some("acme".into());
        let mut delete = update.clone();
        delete.after = None;
        let mut insert = update.clone();
        insert.before = None;

        let paths = [
            "after.id",
            "after.big",
            "after.name",
            "after.flag",
            "after.gone",
            "after.doc",
            "after.doc.a",
            "after.doc.a.b",
            "after.doc.n",
            "after.toast",
            "after.extra",
            "after.extra.k",
            "after.missing",
            "before.id",
            "before.toast",
            "before.missing",
            "after",
            "before",
            "source.table",
            "source.db",
            "op",
            "tenant_id",
            "nope.x",
            "after.id.x",
        ];
        for e in [&update, &delete, &insert] {
            let full = route_view(e);
            let lazy = RouteView::new(e);
            for p in paths {
                let path: Vec<String> =
                    p.split('.').map(String::from).collect();
                assert_eq!(
                    lazy.resolve(&path),
                    full.resolve(&path),
                    "{p} on {:?}",
                    e.op
                );
            }
        }
    }
}
