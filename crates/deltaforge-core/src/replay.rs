//! Event-replay journal envelope: the durable, deterministic unit of replay.
//!
//! An envelope captures one source COMMIT UNIT (a transaction, a standalone event, or a
//! data-less boundary) as the coordinator received it pre-processing: ordered raw
//! events, the source boundary, and schema provenance. The journal sequence identifies
//! an envelope; per-event `offset`s are subordinate. See
//! `docs/specs/event-replay-design.md` (contracts C1/C2).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{SourceBoundary, canonical_json};

pub const REPLAY_ENVELOPE_VERSION: u16 = 1;

/// Immutable, incarnation-scoped pipeline identity, bound into every envelope and its
/// capture id so a deleted-and-recreated pipeline can never inherit an old replay
/// stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineIdentity {
    pub pipeline: String,
    /// Durable id minted when the pipeline is created; new on recreate.
    pub incarnation: String,
    /// Stable source lineage (e.g. PG system_identifier / MySQL server-uuid lineage).
    pub source_identity: String,
}

/// The commit boundary carried verbatim from ingestion (checkpoint + optional durable
/// watermark), hex-encoded so the envelope is plain JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceBoundaryRecord {
    pub checkpoint_hex: String,
    pub watermark_hex: Option<String>,
}

impl SourceBoundaryRecord {
    pub fn from_boundary(b: &SourceBoundary) -> Self {
        Self {
            checkpoint_hex: hex(b.checkpoint.as_bytes()),
            watermark_hex: b.durable_watermark.as_ref().map(|w| hex(w)),
        }
    }
}

/// Provenance for the source schema that decoded an event. NOT consumed by replay
/// delivery today (events are stored already decoded); it is audit metadata and the
/// input to a future typed-reconstruction feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSchemaRef {
    pub table: String,
    pub schema_id: String,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaBinding {
    pub source_provenance: Vec<SourceSchemaRef>,
    /// Schema-registry sequence at capture time (audit + the AtCaptureSeq encoder option).
    pub registry_seq_at_capture: u64,
}

/// One raw (pre-processing) event within an envelope. `offset` is a subordinate
/// position for inspection; ordering is the array order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayEventRecord {
    pub offset: u32,
    pub event_id: String,
    pub tx_id: Option<String>,
    pub event: serde_json::Value,
}

/// The canonical, hashable payload - one captured commit unit. Excludes all
/// backend-assigned metadata (seq, stored_at_ms), which the storage layer adds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayEnvelopePayload {
    pub version: u16,
    pub pipeline_identity: PipelineIdentity,
    pub boundary: SourceBoundaryRecord,
    pub events: Vec<ReplayEventRecord>,
    pub schema_binding: SchemaBinding,
}

impl ReplayEnvelopePayload {
    /// Canonical bytes: JSON with recursively sorted object keys, so an embedded event
    /// object hashes the same regardless of key order. These are the exact bytes stored
    /// and the input to the backend's content digest.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let v =
            serde_json::to_value(self).expect("envelope serializes to JSON");
        canonical_json::canonical_json_bytes(&v)
    }

    /// Deterministic capture identity over the IDENTITY subset only - pipeline identity,
    /// boundary, and ordered event ids - domain-separated and length-prefixed so no two
    /// distinct inputs can concatenate to the same digest. Stable across process
    /// restarts and source retries; this is the idempotency key for
    /// `log_append_if_absent`.
    pub fn capture_id(&self) -> String {
        let mut h = Sha256::new();
        h.update(b"deltaforge:replay:capture:v1");
        feed(&mut h, self.pipeline_identity.pipeline.as_bytes());
        feed(&mut h, self.pipeline_identity.incarnation.as_bytes());
        feed(&mut h, self.pipeline_identity.source_identity.as_bytes());
        feed(&mut h, self.boundary.checkpoint_hex.as_bytes());
        // Watermark presence flag first, so None and Some("") cannot collide.
        h.update([self.boundary.watermark_hex.is_some() as u8]);
        feed(
            &mut h,
            self.boundary
                .watermark_hex
                .as_deref()
                .unwrap_or("")
                .as_bytes(),
        );
        h.update((self.events.len() as u64).to_be_bytes());
        for e in &self.events {
            feed(&mut h, e.event_id.as_bytes());
        }
        hex(&h.finalize())
    }
}

fn feed(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u64).to_be_bytes());
    h.update(bytes);
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CheckpointMeta;
    use serde_json::json;

    fn payload(events: Vec<ReplayEventRecord>) -> ReplayEnvelopePayload {
        ReplayEnvelopePayload {
            version: REPLAY_ENVELOPE_VERSION,
            pipeline_identity: PipelineIdentity {
                pipeline: "p".into(),
                incarnation: "inc-1".into(),
                source_identity: "src-1".into(),
            },
            boundary: SourceBoundaryRecord::from_boundary(
                &SourceBoundary::checkpoint_only(CheckpointMeta::from_vec(
                    b"cp".to_vec(),
                )),
            ),
            events,
            schema_binding: SchemaBinding {
                source_provenance: vec![],
                registry_seq_at_capture: 7,
            },
        }
    }

    fn ev(id: &str, obj: serde_json::Value) -> ReplayEventRecord {
        ReplayEventRecord {
            offset: 0,
            event_id: id.into(),
            tx_id: None,
            event: obj,
        }
    }

    #[test]
    fn canonical_bytes_stable_across_event_key_order() {
        let a = payload(vec![ev("e1", json!({"b": 1, "a": 2}))]);
        let b = payload(vec![ev("e1", json!({"a": 2, "b": 1}))]);
        assert_eq!(a.canonical_bytes(), b.canonical_bytes());
        assert_eq!(a.capture_id(), b.capture_id());
    }

    #[test]
    fn capture_id_changes_with_event_order_and_identity() {
        let base = payload(vec![ev("e1", json!({})), ev("e2", json!({}))]);
        let reordered = payload(vec![ev("e2", json!({})), ev("e1", json!({}))]);
        assert_ne!(base.capture_id(), reordered.capture_id());

        let mut other_incarnation = payload(vec![ev("e1", json!({}))]);
        other_incarnation.pipeline_identity.incarnation = "inc-2".into();
        assert_ne!(
            payload(vec![ev("e1", json!({}))]).capture_id(),
            other_incarnation.capture_id()
        );
    }

    #[test]
    fn capture_id_is_stable_and_roundtrips() {
        let p = payload(vec![ev("e1", json!({"x": 1}))]);
        let id1 = p.capture_id();
        let bytes = p.canonical_bytes();
        let back: ReplayEnvelopePayload =
            serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, p);
        assert_eq!(back.capture_id(), id1);
    }
}
