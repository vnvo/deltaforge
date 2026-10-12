//! How the message sinks (Kafka, Redis, NATS, HTTP) turn an event into
//! bytes and routing input, from its canonical values.

use deltaforge_core::encoding::{EncodingError, Json};
use deltaforge_core::envelope::Envelope;
use deltaforge_core::{Event, NativeOpts, SinkError, SinkResult};
use serde_json::Value;

/// The JSON routing templates resolve against: the event's path view
/// (exact integers, JSON columns as documents).
pub(crate) fn route_view(event: &Event) -> Value {
    event.view_value()
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
    Json.encode(&data)
        .map(|b| b.to_vec())
        .map_err(|e| e.into_sink_error_for(sink_id))
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
    serde_json::to_vec(&payload.after).map_err(|e| SinkError::Serialization {
        details: e.to_string().into(),
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
}
