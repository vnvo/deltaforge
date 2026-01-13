//! Event encoding formats for wire serialization.
//!
//! Encodings control the serialization format (JSON, Avro, Protobuf, etc.)
//! independent of the envelope structure.
//!
//! # Available Encodings
//!
//! - [`Json`] — Standard JSON serialization (default)
//!
//! # Future Encodings
//!
//! - Avro — With Confluent Schema Registry support
//! - Protobuf — With schema management

mod json;

pub use json::Json;

use bytes::Bytes;
use serde::Serialize;

/// Encoding error.
#[derive(Debug, thiserror::Error)]
pub enum EncodingError {
    #[error("JSON serialization failed: {0}")]
    Json(#[from] serde_json::Error),

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
    // Future:
    // Avro,
    // Protobuf,
}

impl EncodingType {
    /// Encoding identifier for logging/metrics.
    pub const fn name(&self) -> &'static str {
        match self {
            EncodingType::Json => "json",
        }
    }

    /// MIME content type for this encoding.
    pub const fn content_type(&self) -> &'static str {
        match self {
            EncodingType::Json => "application/json",
        }
    }

    /// Serialize a value to bytes.
    #[inline]
    pub fn encode<T: Serialize>(
        &self,
        value: &T,
    ) -> Result<Bytes, EncodingError> {
        match self {
            EncodingType::Json => Json.encode(value),
        }
    }
}
