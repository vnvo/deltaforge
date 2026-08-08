//! Derive the Elasticsearch document `_id` from an event.
//!
//! `_id` comes from the configured `id_fields`, else the source primary key,
//! joined by the separator. A table with neither is a hard error — never fall
//! back to ES auto-id, which would silently break upsert and delete.

use deltaforge_core::{Event, SinkError};

/// The row body carrying the id values: `after` for insert/update, `before`
/// for deletes (which have no `after`).
fn row(ev: &Event) -> Option<&serde_json::Value> {
    ev.after.as_ref().or(ev.before.as_ref())
}

/// Compute the `_id` for an event. `id_fields` overrides `pk` when non-empty.
pub fn derive_id(
    ev: &Event,
    id_fields: &[String],
    pk: &[String],
    sep: &str,
) -> Result<String, SinkError> {
    let fields = if id_fields.is_empty() { pk } else { id_fields };
    if fields.is_empty() {
        return Err(SinkError::Routing {
            details: "elasticsearch _id: source table has no primary key and \
                      no id_fields configured"
                .into(),
        });
    }
    let obj = row(ev).ok_or_else(|| SinkError::Serialization {
        details: "elasticsearch _id: event has no row body".into(),
    })?;
    let mut parts = Vec::with_capacity(fields.len());
    for f in fields {
        let v = obj.get(f).ok_or_else(|| SinkError::Serialization {
            details: format!("elasticsearch _id: field '{f}' missing from row")
                .into(),
        })?;
        parts.push(match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Null => {
                return Err(SinkError::Serialization {
                    details: format!("elasticsearch _id: field '{f}' is null")
                        .into(),
                });
            }
            other => other.to_string(),
        });
    }
    Ok(parts.join(sep))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elasticsearch::test_support::mk_event;
    use deltaforge_core::Op;
    use serde_json::json;

    fn ev(after: serde_json::Value) -> Event {
        mk_event(Op::Create, after, json!(null), "d", None, "t", 1, None)
    }

    #[test]
    fn single_pk_from_after() {
        let e = ev(json!({"id": 42, "name": "x"}));
        assert_eq!(derive_id(&e, &[], &["id".into()], "_").unwrap(), "42");
    }

    #[test]
    fn composite_pk_joined() {
        let e = ev(json!({"tenant": "acme", "id": 7}));
        let pk = vec!["tenant".to_string(), "id".to_string()];
        assert_eq!(derive_id(&e, &[], &pk, "_").unwrap(), "acme_7");
    }

    #[test]
    fn id_fields_override_pk() {
        let e = ev(json!({"id": 1, "uuid": "abc"}));
        assert_eq!(
            derive_id(&e, &["uuid".into()], &["id".into()], "_").unwrap(),
            "abc"
        );
    }

    #[test]
    fn delete_uses_before_body() {
        let e = mk_event(
            Op::Delete,
            json!(null),
            json!({"id": 9}),
            "d",
            None,
            "t",
            1,
            None,
        );
        assert_eq!(derive_id(&e, &[], &["id".into()], "_").unwrap(), "9");
    }

    #[test]
    fn no_pk_no_id_fields_errors() {
        let e = ev(json!({"id": 1}));
        assert!(derive_id(&e, &[], &[], "_").is_err());
    }

    #[test]
    fn missing_field_errors() {
        let e = ev(json!({"id": 1}));
        assert!(derive_id(&e, &["nope".into()], &[], "_").is_err());
    }
}
