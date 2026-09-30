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

    #[error("Schema Registry error: {0}")]
    SchemaRegistry(String),

    /// The source schema needed to encode is temporarily unavailable. Not a
    /// property of the event: callers must retry, never dead-letter it.
    #[error("source schema unavailable: {0}")]
    SchemaUnavailable(String),

    #[error("encoding error: {0}")]
    Other(String),
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
            other => crate::SinkError::Serialization {
                details: other.to_string().into(),
            },
        }
    }
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

    #[test]
    fn only_unavailable_schema_is_retryable_backpressure() {
        assert!(matches!(
            EncodingError::SchemaUnavailable("x".into()).into_sink_error(),
            crate::SinkError::Backpressure { .. }
        ));
        assert!(matches!(
            EncodingError::Avro("bad".into()).into_sink_error(),
            crate::SinkError::Serialization { .. }
        ));
        assert!(matches!(
            EncodingError::SchemaRegistry("down".into()).into_sink_error(),
            crate::SinkError::Serialization { .. }
        ));
    }
}
