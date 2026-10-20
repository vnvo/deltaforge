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

mod cloudevents;
mod debezium;
mod native;

pub use cloudevents::CloudEvents;
pub use debezium::Debezium;
pub use native::Native;

use crate::canonical::render::RenderError;
use crate::{Event, NativeOpts, NativePayload};
use cloudevents::CloudEventsWrapper;
use debezium::DebeziumWrapper;
use serde::Serialize;

/// Envelope serialization trait.
///
/// Envelopes transform an Event into wire-ready bytes. The encoding
/// (JSON, Avro, etc.) is handled by the envelope implementation.
pub trait Envelope: Send + Sync {
    /// Envelope identifier for logging/metrics.
    fn name(&self) -> &'static str;

    /// Build an envelope-ready payload for encoding: the event's native
    /// rendering (its own mapping version, the sink's `opts`).
    fn wrap<'a>(
        &'a self,
        event: &'a Event,
        opts: NativeOpts,
    ) -> Result<EnvelopeData<'a>, EnvelopeError>;
}

/// Envelope serialization error.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    /// A value with no native rendering under the sink's settings: a sink
    /// conversion failure.
    #[error("{0}")]
    Render(#[from] RenderError),
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

/// Envelope payloads ready for encoding.
pub enum EnvelopeData<'a> {
    Native(NativePayload<'a>),
    Debezium(DebeziumWrapper<'a>),
    CloudEvents(CloudEventsWrapper<'a>),
}

impl<'a> EnvelopeData<'a> {
    /// This envelope with each present row image written as `{}`: for an
    /// encoder that converts (or rebuilds) the rows from their canonical
    /// values, so they are never rendered only to be discarded.
    pub(crate) fn row_placeholders(&self) -> EnvelopeData<'a> {
        match self {
            EnvelopeData::Native(p) => {
                EnvelopeData::Native(p.row_placeholders())
            }
            EnvelopeData::Debezium(w) => {
                EnvelopeData::Debezium(w.row_placeholders())
            }
            EnvelopeData::CloudEvents(w) => {
                EnvelopeData::CloudEvents(w.row_placeholders())
            }
        }
    }
}

impl Serialize for EnvelopeData<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            EnvelopeData::Native(payload) => payload.serialize(serializer),
            EnvelopeData::Debezium(wrapper) => wrapper.serialize(serializer),
            EnvelopeData::CloudEvents(wrapper) => wrapper.serialize(serializer),
        }
    }
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
