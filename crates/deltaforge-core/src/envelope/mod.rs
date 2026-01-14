//! Event envelope formats for sink serialization.
//!
//! Envelopes control the outer structure of serialized events without
//! affecting the core Event data. Since Event is already Debezium-compatible
//! at the payload level, envelopes are thin wrappers.
//!
//! # Available Envelopes
//!
//! - [`Native`] — Serializes Event directly (Debezium payload structure)
//! - [`Debezium`] — Wraps in `{"payload": <event>}` for full Debezium envelope
//! - [`CloudEvents`] — Restructures to CloudEvents 1.0 spec

use std::borrow::Cow;

mod native;
mod debezium;
mod cloudevents;

pub use native::Native;
pub use debezium::Debezium;
pub use cloudevents::CloudEvents;

use crate::Event;
use bytes::Bytes;

/// Envelope serialization trait.
///
/// Envelopes transform an Event into wire-ready bytes. The encoding
/// (JSON, Avro, etc.) is handled by the envelope implementation.
pub trait Envelope: Send + Sync {
    /// Envelope identifier for logging/metrics.
    fn name(&self) -> &'static str;

    /// Serialize an event to bytes.
    fn serialize(&self, event: &Event) -> Result<Bytes, EnvelopeError>;
}

/// Envelope serialization error.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("envelope error: {0}")]
    Other(String),
}

/// Envelope type selection for configuration.
#[derive(Debug, Clone, Default)]
pub enum EnvelopeType {
    /// Direct Event serialization (Debezium payload structure)
    #[default]
    Native,

    /// Full Debezium envelope: `{"payload": <event>}`
    Debezium,

    /// CloudEvents 1.0 specification
    CloudEvents {
        /// Type prefix for event type field (e.g., "com.example.cdc")
        type_prefix: String,
    },
}

impl EnvelopeType {
    /// Envelop identifier for logging/metrics.
    pub fn name(&self) -> Cow<'_, str> {
        match self {
            EnvelopeType::Native => Cow::Borrowed("native"),
            EnvelopeType::Debezium => Cow::Borrowed("debezium"),
            EnvelopeType::CloudEvents { type_prefix } => {
                Cow::Owned(format!("cloudevents-{}", type_prefix))
            }            
        }
    }

}


impl EnvelopeType {
    /// Create the envelope instance.
    pub fn build(&self) -> Box<dyn Envelope> {
        match self {
            EnvelopeType::Native => Box::new(Native),
            EnvelopeType::Debezium => Box::new(Debezium),
            EnvelopeType::CloudEvents { type_prefix } => {
                Box::new(CloudEvents::new(type_prefix.clone()))
            }
        }
    }
}