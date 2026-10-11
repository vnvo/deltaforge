//! Event encoding formats for wire serialization.
//!
//! Encodings control the serialization format (JSON, Avro, etc.)
//! independent of the envelope structure.
//!
//! # Available Encodings
//!
//! - [`Json`] — Standard JSON serialization (default)
//! - Avro — With Confluent Schema Registry support (see [`avro`] module)

pub mod arrow_schema;
pub mod arrow_types;
pub mod avro;
mod avro_infer;
pub mod avro_schema;
pub mod avro_types;
mod json;

pub use json::Json;

use bytes::Bytes;
use serde::Serialize;

/// Encoding error.
#[derive(Debug, thiserror::Error)]
pub enum EncodingError {
    #[error("JSON serialization failed: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Avro serialization failed: {0}")]
    Avro(String),

    /// The Schema Registry answered with an error that is neither an
    /// outage nor a compatibility rejection (for example authentication or
    /// an invalid schema). Not a property of the event: never dead-lettered.
    #[error("Schema Registry error: {0}")]
    SchemaRegistry(String),

    /// The Schema Registry is unreachable or temporarily failing (transport
    /// error, timeout, 5xx, 408, 429). Retryable infrastructure failure.
    #[error("Schema Registry unavailable: {0}")]
    SchemaRegistryUnavailable(String),

    /// The Schema Registry rejected a schema as incompatible with the
    /// subject's registered versions (HTTP 409): a schema or configuration
    /// failure, not bad event data.
    #[error(
        "Schema Registry rejected the schema for subject {subject}: {details}"
    )]
    SchemaRejected { subject: String, details: String },

    /// The source schema needed to encode is temporarily unavailable. Not a
    /// property of the event: callers must retry, never dead-letter it.
    #[error("source schema unavailable: {0}")]
    SchemaUnavailable(String),

    /// The event's source schema version conflicts with the registered
    /// one. Not a property of a single value: the sink must stop, never
    /// dead-letter the event or encode it with another schema.
    #[error("source schema version conflict: {0}")]
    SchemaConflict(String),

    #[error("encoding error: {0}")]
    Other(String),

    /// A value the target encoding cannot represent exactly. Never carries
    /// the value itself.
    #[error("{0}")]
    Conversion(ConversionFailure),
}

/// A conversion a sink encoding could not make exactly: the column, its
/// source type and the target type, and why (never the value).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversionFailure {
    /// The field path (`after.price`), or `""` at the top level.
    pub column: String,
    /// The column's source type when the schema records it
    /// (`bigint unsigned`), otherwise the JSON kind of the value.
    pub source_type: String,
    /// The target type (`Avro long`).
    pub target_type: String,
    /// Why the value cannot be represented, without the value.
    pub reason: &'static str,
}

impl std::fmt::Display for ConversionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "column {} ({}) cannot be represented exactly as {}: {}",
            if self.column.is_empty() {
                "<root>"
            } else {
                &self.column
            },
            self.source_type,
            self.target_type,
            self.reason
        )
    }
}

impl EncodingError {
    /// The sink error for a failed encode: a temporarily unavailable source
    /// schema is retryable backpressure (never routed to the DLQ as a poison
    /// event); every other encoding failure is a serialization error.
    pub fn into_sink_error(self) -> crate::SinkError {
        match self {
            EncodingError::SchemaUnavailable(details) => {
                crate::SinkError::Backpressure {
                    details: details.into(),
                }
            }
            EncodingError::SchemaRegistryUnavailable(details) => {
                crate::SinkError::Backpressure {
                    details: details.into(),
                }
            }
            e @ (EncodingError::SchemaConflict(_)
            | EncodingError::SchemaRegistry(_)
            | EncodingError::SchemaRejected { .. }) => {
                crate::SinkError::Fatal {
                    details: e.to_string().into(),
                }
            }
            other => crate::SinkError::Serialization {
                details: other.to_string().into(),
            },
        }
    }

    /// [`Self::into_sink_error`] for sink `sink_id`: a conversion failure
    /// becomes a `sink_conversion_failed` incident (column, source and target
    /// type as evidence, never the value) over a serialization error, so it
    /// stays DLQ-eligible; a registry compatibility rejection becomes a
    /// `sink_schema_rejected` incident over a fatal error (never
    /// dead-lettered).
    pub fn into_sink_error_for(self, sink_id: &str) -> crate::SinkError {
        use crate::incident::{
            CauseCode, Component, EvidenceKey as K, ReasonCode, Retryability,
            SafetyState,
        };
        match self {
            EncodingError::Conversion(f) => {
                let draft = crate::IncidentDraft::new(
                    ReasonCode::SinkConversionFailed,
                    Component::Sink {
                        id: sink_id.to_string(),
                    },
                    Retryability::OperatorAction,
                    SafetyState::HaltedSafe,
                    CauseCode::SinkSerialization,
                )
                .discriminate("column", f.column.clone())
                .discriminate("target_type", f.target_type.clone())
                .with_evidence(|e| {
                    e.text(K::SinkId, sink_id)
                        .text(K::Column, &f.column)
                        .text(K::SourceType, &evidence_token(&f.source_type))
                        .text(K::TargetType, &evidence_token(&f.target_type));
                });
                crate::SinkError::incident(
                    draft,
                    crate::SinkError::Serialization {
                        details: f.to_string().into(),
                    },
                )
            }
            EncodingError::SchemaRejected { subject, details } => {
                let draft = crate::IncidentDraft::new(
                    ReasonCode::SinkSchemaRejected,
                    Component::Sink {
                        id: sink_id.to_string(),
                    },
                    Retryability::OperatorAction,
                    SafetyState::HaltedSafe,
                    CauseCode::SinkFatal,
                )
                .discriminate("subject", subject.clone())
                .with_evidence(|e| {
                    e.text(K::SinkId, sink_id).text(K::Subject, &subject);
                });
                let cause = EncodingError::SchemaRejected { subject, details };
                crate::SinkError::incident(
                    draft,
                    crate::SinkError::Fatal {
                        details: cause.to_string().into(),
                    },
                )
            }
            other => other.into_sink_error(),
        }
    }
}

/// A type name as an incident evidence token: lowercase, every run of
/// characters outside `[a-z0-9_.,:+@/-]` replaced by `_` (`bigint unsigned`
/// becomes `bigint_unsigned`, `Avro long` becomes `avro_long`).
fn evidence_token(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.to_lowercase().chars() {
        if c.is_ascii_alphanumeric()
            || matches!(c, '.' | ',' | ':' | '+' | '@' | '/' | '-')
        {
            out.push(c);
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

/// Encoding type for sink configuration.
///
/// Uses enum dispatch rather than trait objects since encoding
/// implementations need generic serialize support.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EncodingType {
    #[default]
    Json,
    /// Avro encoding — actual serialization is handled by [`avro::AvroEncoder`]
    /// which needs async Schema Registry access. This variant is used for
    /// content_type/name identification only.
    Avro,
}

impl EncodingType {
    /// Encoding identifier for logging/metrics.
    pub const fn name(&self) -> &'static str {
        match self {
            EncodingType::Json => "json",
            EncodingType::Avro => "avro",
        }
    }

    /// MIME content type for this encoding.
    pub const fn content_type(&self) -> &'static str {
        match self {
            EncodingType::Json => "application/json",
            EncodingType::Avro => "application/avro",
        }
    }

    /// Serialize a value to bytes (JSON only).
    ///
    /// For Avro encoding, use [`avro::AvroEncoder`] directly — it requires
    /// async Schema Registry interaction.
    #[inline]
    pub fn encode<T: Serialize>(
        &self,
        value: &T,
    ) -> Result<Bytes, EncodingError> {
        match self {
            EncodingType::Json => Json.encode(value),
            EncodingType::Avro => Err(EncodingError::Other(
                "use AvroEncoder for Avro serialization".into(),
            )),
        }
    }
}

#[cfg(test)]
mod sink_error_tests {
    use super::*;

    /// Only a value the encoding cannot represent is an event failure
    /// (DLQ-eligible). An unavailable source schema or Schema Registry is
    /// retryable backpressure; every other registry or schema-version
    /// failure stops the sink.
    #[test]
    fn only_value_failures_are_event_failures() {
        use crate::SinkError as S;
        let map = |e: EncodingError| e.into_sink_error_for("k");
        assert!(matches!(
            map(EncodingError::SchemaUnavailable("x".into())),
            S::Backpressure { .. }
        ));
        assert!(matches!(
            map(EncodingError::SchemaRegistryUnavailable("down".into())),
            S::Backpressure { .. }
        ));
        for fatal in [
            EncodingError::SchemaRegistry("401".into()),
            EncodingError::SchemaConflict("seq".into()),
            EncodingError::SchemaRejected {
                subject: "s".into(),
                details: "409".into(),
            },
        ] {
            let e = map(fatal);
            assert!(matches!(e.root(), S::Fatal { .. }), "{e:?}");
            assert!(!e.is_dlq_eligible());
        }
        assert!(map(EncodingError::Avro("bad".into())).is_dlq_eligible());
    }
}
