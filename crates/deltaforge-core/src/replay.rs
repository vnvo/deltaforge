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

/// Backend-computed content digest over the exact stored bytes, domain-separated so it
/// cannot collide with a hash taken for another purpose. Defined here (not in the
/// storage crate) so both the storage backend that writes it and the replay reader that
/// verifies it share one implementation.
pub fn content_digest(value: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(b"deltaforge:replay:content:v1");
    h.update(value);
    hex(&h.finalize())
}

/// Immutable, incarnation-scoped pipeline identity, bound into every envelope and its
/// capture id so a deleted-and-recreated pipeline can never inherit an old replay
/// stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineIdentity {
    pub pipeline: String,
    /// Durable id minted when the pipeline is created; new on recreate.
    pub incarnation: String,
    /// The stable source database lineage (e.g. PG system_identifier / MySQL
    /// server-uuid). `None` when the source does not yet expose it - explicitly
    /// unavailable rather than a misleading substitute. Lineage is part of this identity:
    /// it is bound into `capture_id` and into read-time identity equality. Changing it
    /// (None -> Some, or a different value) therefore requires minting a NEW incarnation;
    /// reusing the old incarnation with a changed lineage would make already-captured
    /// envelopes fail identity verification on read.
    pub source_lineage: Option<String>,
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

/// Schema provenance for an envelope. `source_tables` is honest, available metadata (the
/// distinct fully-qualified source tables in the unit). `registry_seq_at_capture` is the
/// schema-registry sequence at capture time when the registry handle is available,
/// otherwise `None` (explicitly not captured). Provenance only; not consumed by replay
/// delivery today.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaBinding {
    pub source_tables: Vec<String>,
    pub registry_seq_at_capture: Option<u64>,
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
/// backend-assigned metadata (seq, stored_at_ms, capture_id, content_hash), which the
/// storage layer adds and which [`StoredReplayEnvelope`] carries back on read.
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
    /// `log_append_if_absent`. It deliberately excludes fields that may vary between
    /// retries (e.g. a registry sequence), so a retry never conflicts on identical bytes.
    pub fn capture_id(&self) -> String {
        let mut h = Sha256::new();
        h.update(b"deltaforge:replay:capture:v1");
        feed(&mut h, self.pipeline_identity.pipeline.as_bytes());
        feed(&mut h, self.pipeline_identity.incarnation.as_bytes());
        h.update([self.pipeline_identity.source_lineage.is_some() as u8]);
        feed(
            &mut h,
            self.pipeline_identity
                .source_lineage
                .as_deref()
                .unwrap_or("")
                .as_bytes(),
        );
        feed(&mut h, self.boundary.checkpoint_hex.as_bytes());
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

/// A stored envelope as read back from the journal: the backend-assigned metadata plus
/// the decoded payload. Produced by a fail-closed load that verifies integrity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredReplayEnvelope {
    pub seq: u64,
    pub stored_at_ms: i64,
    pub capture_id: String,
    pub content_hash: String,
    pub payload: ReplayEnvelopePayload,
}

/// Fail-closed load errors: a stored envelope failed an integrity or contract check.
#[derive(Debug, thiserror::Error)]
pub enum ReplayLoadError {
    #[error("unsupported replay envelope version {got} (expected {expected})")]
    UnsupportedVersion { got: u16, expected: u16 },
    #[error(
        "replay envelope pipeline identity does not match the reading pipeline"
    )]
    IdentityMismatch,
    #[error(
        "replay envelope capture id mismatch (stored {stored}, recomputed {recomputed})"
    )]
    CaptureIdMismatch { stored: String, recomputed: String },
    #[error(
        "replay envelope content hash mismatch (tampered or corrupt bytes)"
    )]
    ContentHashMismatch,
    #[error("replay envelope is not valid JSON: {0}")]
    Decode(String),
}

impl StoredReplayEnvelope {
    /// Decode and verify one stored entry, fail-closed. Checks: valid JSON, supported
    /// version, matching pipeline identity, the stored `capture_id` equals the payload's
    /// recomputed capture id, and the stored `content_hash` equals the digest of the
    /// re-serialized canonical bytes (so tampering with either the bytes or the metadata
    /// is caught).
    pub fn decode_verified(
        seq: u64,
        stored_at_ms: i64,
        capture_id: &str,
        content_hash: &str,
        value: &[u8],
        expected_identity: &PipelineIdentity,
    ) -> Result<Self, ReplayLoadError> {
        let payload: ReplayEnvelopePayload = serde_json::from_slice(value)
            .map_err(|e| ReplayLoadError::Decode(e.to_string()))?;
        if payload.version != REPLAY_ENVELOPE_VERSION {
            return Err(ReplayLoadError::UnsupportedVersion {
                got: payload.version,
                expected: REPLAY_ENVELOPE_VERSION,
            });
        }
        if &payload.pipeline_identity != expected_identity {
            return Err(ReplayLoadError::IdentityMismatch);
        }
        // Recompute the digest over the canonical bytes we would have stored, and
        // compare to both the stored content_hash and the raw value's digest.
        let recomputed_hash = content_digest(&payload.canonical_bytes());
        if recomputed_hash != content_hash
            || content_digest(value) != content_hash
        {
            return Err(ReplayLoadError::ContentHashMismatch);
        }
        let recomputed_id = payload.capture_id();
        if recomputed_id != capture_id {
            return Err(ReplayLoadError::CaptureIdMismatch {
                stored: capture_id.to_string(),
                recomputed: recomputed_id,
            });
        }
        Ok(Self {
            seq,
            stored_at_ms,
            capture_id: capture_id.to_string(),
            content_hash: content_hash.to_string(),
            payload,
        })
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

    fn identity() -> PipelineIdentity {
        PipelineIdentity {
            pipeline: "p".into(),
            incarnation: "inc-1".into(),
            source_lineage: None,
        }
    }

    fn payload(events: Vec<ReplayEventRecord>) -> ReplayEnvelopePayload {
        ReplayEnvelopePayload {
            version: REPLAY_ENVELOPE_VERSION,
            pipeline_identity: identity(),
            boundary: SourceBoundaryRecord::from_boundary(
                &SourceBoundary::checkpoint_only(CheckpointMeta::from_vec(
                    b"cp".to_vec(),
                )),
            ),
            events,
            schema_binding: SchemaBinding {
                source_tables: vec!["db.t".into()],
                registry_seq_at_capture: None,
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

        let mut other = payload(vec![ev("e1", json!({}))]);
        other.pipeline_identity.incarnation = "inc-2".into();
        assert_ne!(
            payload(vec![ev("e1", json!({}))]).capture_id(),
            other.capture_id()
        );
    }

    #[test]
    fn source_lineage_is_identity_bound() {
        // Lineage is part of the identity: it changes the capture id and breaks read-time
        // identity equality, so binding lineage requires a new incarnation rather than
        // being a transparent change under the same one.
        let none = payload(vec![ev("e1", json!({}))]);
        let mut some = payload(vec![ev("e1", json!({}))]);
        some.pipeline_identity.source_lineage = Some("sys-42".into());
        assert_ne!(
            none.capture_id(),
            some.capture_id(),
            "lineage must change capture_id"
        );

        // An envelope captured with no lineage cannot be read back under an identity that
        // now claims a lineage (same incarnation): identity verification fails closed.
        let bytes = none.canonical_bytes();
        let cid = none.capture_id();
        let ch = content_digest(&bytes);
        let changed = PipelineIdentity {
            pipeline: "p".into(),
            incarnation: "inc-1".into(),
            source_lineage: Some("sys-42".into()),
        };
        assert!(matches!(
            StoredReplayEnvelope::decode_verified(
                1, 1, &cid, &ch, &bytes, &changed
            ),
            Err(ReplayLoadError::IdentityMismatch)
        ));
    }

    #[test]
    fn decode_verified_roundtrips_and_catches_tampering() {
        let p = payload(vec![ev("e1", json!({"x": 1}))]);
        let bytes = p.canonical_bytes();
        let cid = p.capture_id();
        let ch = content_digest(&bytes);

        let ok = StoredReplayEnvelope::decode_verified(
            5,
            123,
            &cid,
            &ch,
            &bytes,
            &identity(),
        )
        .unwrap();
        assert_eq!(ok.seq, 5);
        assert_eq!(ok.stored_at_ms, 123);
        assert_eq!(ok.payload, p);

        // Wrong identity.
        let other = PipelineIdentity {
            pipeline: "p".into(),
            incarnation: "inc-2".into(),
            source_lineage: None,
        };
        assert!(matches!(
            StoredReplayEnvelope::decode_verified(
                5, 123, &cid, &ch, &bytes, &other
            ),
            Err(ReplayLoadError::IdentityMismatch)
        ));
        // Tampered content hash.
        assert!(matches!(
            StoredReplayEnvelope::decode_verified(
                5,
                123,
                &cid,
                "deadbeef",
                &bytes,
                &identity()
            ),
            Err(ReplayLoadError::ContentHashMismatch)
        ));
        // Mismatched capture id.
        assert!(matches!(
            StoredReplayEnvelope::decode_verified(
                5,
                123,
                "wrong",
                &ch,
                &bytes,
                &identity()
            ),
            Err(ReplayLoadError::CaptureIdMismatch { .. })
        ));
    }
}
