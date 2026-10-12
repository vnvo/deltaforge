//! Declared column types of a source schema version, for consumers that
//! must type a value they did not decode (the JavaScript processor).
//!
//! Events carry no column metadata: each event names its schema version
//! (`schema_version` fingerprint, `schema_sequence`), and a
//! [`SchemaContext`] resolves that reference to an immutable, shared
//! [`ColumnTypes`]. Sources derive it from the registered schema through the
//! same type mapping their decoders use.

use std::sync::Arc;

use serde_json::Value;

use super::parse::{ParseError, parse_like};
use super::render::parse_base64_wrapper;
use super::{
    ArrayValue, Bits, CanonicalValue, DbValue, Decimal, EnumValue, IntWidth,
    Precision, SetValue, SourceType, TypeMapping,
};

/// A column's declared type, as its source decodes it.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int {
        width: IntWidth,
        unsigned: bool,
    },
    Decimal,
    Float32,
    Float64,
    Bool,
    Text,
    /// `fixed`: BINARY(n), zero-padded to n bytes.
    Binary {
        fixed: Option<usize>,
    },
    Bits {
        len: u8,
    },
    Enum {
        labels: Vec<String>,
    },
    Set {
        labels: Vec<String>,
    },
    Json,
    Date,
    Time {
        precision: Precision,
    },
    DateTime {
        precision: Precision,
    },
    Timestamp {
        precision: Precision,
    },
    Year,
    Uuid,
    Array {
        element: Box<ColumnType>,
        element_type: SourceType,
    },
    /// A type without a canonical mapping: values are its text.
    Opaque(SourceType),
    /// A type the source cannot decode (its values fail at the source).
    Unsupported(String),
}

/// The declared column types of one schema version. Immutable and shared.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnTypes {
    pub fingerprint: Arc<str>,
    pub sequence: Option<u64>,
    columns: Vec<(String, ColumnType)>,
}

impl ColumnTypes {
    pub fn new(
        fingerprint: Arc<str>,
        sequence: Option<u64>,
        columns: Vec<(String, ColumnType)>,
    ) -> ColumnTypes {
        ColumnTypes {
            fingerprint,
            sequence,
            columns,
        }
    }

    pub fn get(&self, name: &str) -> Option<&ColumnType> {
        self.columns.iter().find(|(n, _)| n == name).map(|(_, t)| t)
    }

    pub fn columns(&self) -> &[(String, ColumnType)] {
        &self.columns
    }
}

/// An event's schema reference: the table (database, namespace, table) and
/// the exact version.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaRef {
    pub connector: String,
    pub database: String,
    /// See [`crate::SourceInfo::namespace`].
    pub namespace: String,
    pub table: String,
    pub fingerprint: String,
    pub sequence: Option<u64>,
}

impl SchemaRef {
    /// The reference an event carries, if it carries one.
    pub fn of(event: &crate::Event) -> Option<SchemaRef> {
        Some(SchemaRef {
            connector: event.source.connector.clone(),
            database: event.source.db.clone(),
            namespace: event.source.namespace().to_string(),
            table: event.source.table.clone(),
            fingerprint: event.schema_version.clone()?,
            sequence: event.schema_sequence,
        })
    }
}

/// Why a schema reference did not resolve. Never silently downgraded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaContextError {
    /// The version cannot be read now (retry later).
    #[error("schema context unavailable: {0}")]
    Unavailable(String),
    /// The reference names no registered version (or another sequence).
    #[error("schema context conflict: {0}")]
    Conflict(String),
}

/// Resolves events' schema references to their declared column types.
/// Implementations cache per reference: the types of a version never
/// change.
#[async_trait::async_trait]
pub trait SchemaContext: Send + Sync {
    async fn resolve(
        &self,
        reference: &SchemaRef,
    ) -> Result<Arc<ColumnTypes>, SchemaContextError>;
}

/// The schema versions of one batch, resolved and validated before any
/// processor runs. Immutable.
#[derive(Debug, Clone, Default)]
pub struct ResolvedSchemas {
    versions: std::collections::HashMap<SchemaRef, Arc<ColumnTypes>>,
}

impl ResolvedSchemas {
    /// The declared types of `reference`, if the batch resolved it.
    pub fn get(&self, reference: &SchemaRef) -> Option<&Arc<ColumnTypes>> {
        self.versions.get(reference)
    }

    pub fn len(&self) -> usize {
        self.versions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// Resolve every distinct schema reference of `events` once. A missing
    /// version, a fingerprint or sequence conflict, or a resolved version
    /// that does not admit an event's values fails the whole batch.
    pub async fn resolve(
        context: &dyn SchemaContext,
        events: &[crate::Event],
    ) -> Result<ResolvedSchemas, SchemaContextError> {
        let mut versions = std::collections::HashMap::new();
        for ev in events {
            let Some(r) = SchemaRef::of(ev) else {
                continue;
            };
            let types = match versions.get(&r) {
                Some(t) => Arc::clone(t),
                None => {
                    let t = context.resolve(&r).await?;
                    if *t.fingerprint != *r.fingerprint
                        || (r.sequence.is_some()
                            && t.sequence.is_some()
                            && t.sequence != r.sequence)
                    {
                        return Err(SchemaContextError::Conflict(format!(
                            "{}.{} resolved {}@{:?} for the event's {}@{:?}",
                            r.namespace,
                            r.table,
                            t.fingerprint,
                            t.sequence,
                            r.fingerprint,
                            r.sequence
                        )));
                    }
                    versions.insert(r.clone(), Arc::clone(&t));
                    t
                }
            };
            for row in [&ev.before, &ev.after].into_iter().flatten() {
                for (name, v) in row.iter() {
                    match types.get(name) {
                        Some(ty) if ty.admits(v) => {}
                        Some(_) => {
                            return Err(SchemaContextError::Conflict(format!(
                                "{}.{} column `{name}`: the event's value is not of \
                                 the type schema version {} declares",
                                r.namespace, r.table, r.fingerprint
                            )));
                        }
                        // Undeclared data belongs in the row's extensions.
                        None => {
                            return Err(SchemaContextError::Conflict(format!(
                                "{}.{} column `{name}` is not declared by schema version {}",
                                r.namespace, r.table, r.fingerprint
                            )));
                        }
                    }
                }
            }
        }
        Ok(ResolvedSchemas { versions })
    }

    /// A batch's versions given directly (tests, and contexts resolved
    /// elsewhere).
    pub fn from_versions(
        versions: impl IntoIterator<Item = (SchemaRef, Arc<ColumnTypes>)>,
    ) -> ResolvedSchemas {
        ResolvedSchemas {
            versions: versions.into_iter().collect(),
        }
    }
}

fn invalid<T>(what: &'static str) -> Result<T, ParseError> {
    Err(ParseError::Invalid(what))
}

impl ColumnType {
    /// Whether `v` is a value of this type (a NULL or unchanged value is a
    /// value of any type). A mismatch means the schema context conflicts
    /// with the event.
    pub fn admits(&self, v: &CanonicalValue) -> bool {
        let db = match v {
            CanonicalValue::Null | CanonicalValue::Unchanged => return true,
            CanonicalValue::Opaque(_) => {
                return matches!(self, ColumnType::Opaque(_));
            }
            CanonicalValue::Document(_) | CanonicalValue::Bytes(_) => {
                return false;
            }
            CanonicalValue::Db(db) => db,
        };
        matches!(
            (self, db),
            (
                ColumnType::Int {
                    unsigned: false,
                    ..
                },
                DbValue::Int { .. }
            ) | (ColumnType::Int { unsigned: true, .. }, DbValue::UInt { .. })
                | (ColumnType::Decimal, DbValue::Decimal(_))
                | (ColumnType::Float32, DbValue::Float32(_))
                | (ColumnType::Float64, DbValue::Float64(_))
                | (ColumnType::Bool, DbValue::Bool(_))
                | (ColumnType::Text, DbValue::Text(_))
                | (ColumnType::Binary { .. }, DbValue::Binary(_))
                | (ColumnType::Bits { .. }, DbValue::Bits(_))
                | (ColumnType::Enum { .. }, DbValue::Enum(_))
                | (ColumnType::Set { .. }, DbValue::Set(_))
                | (ColumnType::Json, DbValue::Json(_))
                | (ColumnType::Date, DbValue::Date(_))
                | (ColumnType::Time { .. }, DbValue::Time(_))
                | (ColumnType::DateTime { .. }, DbValue::DateTime(_))
                | (ColumnType::Timestamp { .. }, DbValue::Timestamp(_))
                | (ColumnType::Year, DbValue::Year(_))
                | (ColumnType::Uuid, DbValue::Uuid(_))
                | (ColumnType::Array { .. }, DbValue::Array(_))
        )
    }

    /// A template value of this type, for the rendering parser (its own
    /// value never matters, only its type and declared attributes).
    fn template(&self) -> Result<CanonicalValue, ParseError> {
        use super::{
            CivilDate, DateTimeValue, DateValue, JsonText, TimeValue,
            TimestampValue,
        };
        let db = |v| Ok(CanonicalValue::Db(v));
        match self {
            ColumnType::Int {
                width,
                unsigned: false,
            } => db(DbValue::Int {
                value: 0,
                width: *width,
            }),
            ColumnType::Int {
                width,
                unsigned: true,
            } => db(DbValue::UInt {
                value: 0,
                width: *width,
            }),
            ColumnType::Decimal => {
                db(DbValue::Decimal(Decimal::Finite("0".into())))
            }
            ColumnType::Float32 => db(DbValue::Float32(0.0)),
            ColumnType::Float64 => db(DbValue::Float64(0.0)),
            ColumnType::Bool => db(DbValue::Bool(false)),
            ColumnType::Text => db(DbValue::Text(String::new())),
            ColumnType::Binary { .. } => db(DbValue::Binary(Vec::new())),
            ColumnType::Bits { len } => db(DbValue::Bits(
                Bits::new(*len, vec![0; (*len as usize).div_ceil(8)])
                    .map_err(|_| ParseError::Invalid("BIT length"))?,
            )),
            ColumnType::Json => {
                db(DbValue::Json(JsonText::new("null".into()).expect("valid")))
            }
            ColumnType::Date => db(DbValue::Date(DateValue::Civil(
                CivilDate::new(1970, 1, 1).expect("valid"),
            ))),
            ColumnType::Time { precision } => db(DbValue::Time(
                TimeValue::mysql(0, *precision).expect("valid"),
            )),
            ColumnType::DateTime { precision } => db(DbValue::DateTime(
                DateTimeValue::civil(
                    CivilDate::new(1970, 1, 1).expect("valid"),
                    0,
                    *precision,
                )
                .expect("valid"),
            )),
            ColumnType::Timestamp { precision } => {
                db(DbValue::Timestamp(TimestampValue::Instant {
                    micros: 0,
                    precision: *precision,
                }))
            }
            ColumnType::Year => db(DbValue::Year(0)),
            ColumnType::Uuid => db(DbValue::Uuid([0; 16])),
            ColumnType::Opaque(t) => {
                Ok(CanonicalValue::Opaque(super::OpaqueText {
                    source_type: t.clone(),
                    text: String::new(),
                }))
            }
            ColumnType::Enum { .. }
            | ColumnType::Set { .. }
            | ColumnType::Array { .. } => {
                unreachable!("parsed by declare_parse")
            }
            ColumnType::Unsupported(t) => {
                Err(ParseError::Unsupported(t.clone()))
            }
        }
    }

    /// Parse `v`, a rendering for `mapping`, as a value of this declared
    /// type. ENUM and SET values are checked against the declared labels.
    pub fn parse(
        &self,
        v: &Value,
        mapping: TypeMapping,
    ) -> Result<CanonicalValue, ParseError> {
        if v.is_null() {
            return Ok(CanonicalValue::Null);
        }
        match self {
            ColumnType::Enum { labels } => {
                let Value::String(s) = v else {
                    return invalid("ENUM label");
                };
                let i = labels.iter().position(|l| l == s).ok_or(
                    ParseError::Invalid("ENUM label (not a declared symbol)"),
                )?;
                let index =
                    u16::try_from(i + 1).or_else(|_| invalid("ENUM index"))?;
                Ok(CanonicalValue::Db(DbValue::Enum(
                    EnumValue::mysql(index, s.clone())
                        .or_else(|_| invalid("ENUM label"))?,
                )))
            }
            ColumnType::Set { labels } => {
                let Value::String(s) = v else {
                    return invalid("SET members");
                };
                let mut mask = 0u64;
                if !s.is_empty() {
                    for member in s.split(',') {
                        let i = labels.iter().position(|l| l == member).ok_or(
                            ParseError::Invalid(
                                "SET member (not a declared member)",
                            ),
                        )?;
                        if mask & (1 << i) != 0 {
                            return invalid(
                                "SET members (a member listed twice)",
                            );
                        }
                        mask |= 1 << i;
                    }
                }
                let set = SetValue::from_bitmask(mask, labels)
                    .or_else(|_| invalid("SET members"))?;
                if set.members().join(",") != *s {
                    return invalid("SET members (not in definition order)");
                }
                Ok(CanonicalValue::Db(DbValue::Set(set)))
            }
            ColumnType::Array {
                element,
                element_type,
            } => {
                let Value::Array(items) = v else {
                    return invalid("array");
                };
                let items = items
                    .iter()
                    .map(|i| element.parse(i, mapping))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(CanonicalValue::Db(DbValue::Array(
                    ArrayValue::new(element_type.clone(), items)
                        .or_else(|_| invalid("array"))?,
                )))
            }
            ColumnType::Binary { fixed: Some(n) } => {
                match parse_base64_wrapper(v) {
                    Some(b) if b.len() == *n => {
                        Ok(CanonicalValue::Db(DbValue::Binary(b)))
                    }
                    Some(_) => invalid("BINARY(n) value (not n bytes)"),
                    None => invalid("binary wrapper"),
                }
            }
            other => parse_like(&other.template()?, v, mapping),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn declared_types_parse_values_a_null_or_unchanged_value_could_not() {
        let e = ColumnType::Enum {
            labels: vec!["a".into(), "".into(), "c".into()],
        };
        assert_eq!(
            e.parse(&json!(""), TypeMapping::V2).unwrap(),
            CanonicalValue::Db(DbValue::Enum(
                EnumValue::mysql(2, String::new()).unwrap()
            ))
        );
        assert!(e.parse(&json!("z"), TypeMapping::V2).is_err());
        let s = ColumnType::Set {
            labels: vec!["x".into(), "y".into(), "z".into()],
        };
        let v = s.parse(&json!("x,z"), TypeMapping::V2).unwrap();
        assert_eq!(
            v,
            CanonicalValue::Db(DbValue::Set(
                SetValue::from_bitmask(
                    0b101,
                    &["x".into(), "y".into(), "z".into()]
                )
                .unwrap()
            ))
        );
        assert!(
            s.parse(&json!("z,x"), TypeMapping::V2).is_err(),
            "definition order"
        );
        assert!(s.parse(&json!("w"), TypeMapping::V2).is_err());
        assert_eq!(
            s.parse(&json!(""), TypeMapping::V2).unwrap(),
            CanonicalValue::Db(DbValue::Set(
                SetValue::from_bitmask(
                    0,
                    &["x".into(), "y".into(), "z".into()]
                )
                .unwrap()
            ))
        );
        let i = ColumnType::Int {
            width: IntWidth::W64,
            unsigned: true,
        };
        assert_eq!(
            i.parse(&json!("18446744073709551615"), TypeMapping::V2)
                .unwrap(),
            CanonicalValue::Db(DbValue::uint(u64::MAX, IntWidth::W64).unwrap())
        );
        assert!(i.parse(&json!(-1), TypeMapping::V2).is_err());
        let b = ColumnType::Binary { fixed: Some(2) };
        assert!(
            b.parse(&json!({"_base64": "AA=="}), TypeMapping::V2)
                .is_err()
        );
        let t = ColumnType::Timestamp {
            precision: Precision::Fixed(0),
        };
        assert!(
            t.parse(&json!("2024-01-01T00:00:00Z"), TypeMapping::V2)
                .is_ok()
        );
        assert_eq!(
            ColumnType::Text
                .parse(&Value::Null, TypeMapping::V2)
                .unwrap(),
            CanonicalValue::Null
        );
        assert!(
            ColumnType::Unsupported("vector".into())
                .parse(&json!(1), TypeMapping::V2)
                .is_err()
        );
    }

    #[test]
    fn admits_detects_a_conflicting_context() {
        let t = ColumnType::Int {
            width: IntWidth::W32,
            unsigned: false,
        };
        assert!(t.admits(&CanonicalValue::Db(
            DbValue::int(1, IntWidth::W32).unwrap()
        )));
        assert!(!t.admits(&CanonicalValue::Db(DbValue::Text("1".into()))));
        assert!(t.admits(&CanonicalValue::Null));
        assert!(t.admits(&CanonicalValue::Unchanged));
        assert!(!t.admits(&CanonicalValue::Document(
            super::super::Document::new(json!(1))
        )));
    }
}
