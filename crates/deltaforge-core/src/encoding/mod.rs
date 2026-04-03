//! Event encoding formats for wire serialization.
//!
//! Encodings control the serialization format (JSON, Avro, Protobuf, etc.)
//! independent of the envelope structure.
//!
//! # Available Encodings
//!
//! - [`Json`] — Standard JSON serialization (default)
//! - Avro — With Confluent Schema Registry support (see [`avro`] module)

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

    #[error("encoding error: {0}")]
    Other(String),
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
