//! The sink acknowledgement-uncertainty incident.

use deltaforge_core::SinkError;
use deltaforge_core::incident::{
    ActionCode, Component, EvidenceKey as K, IncidentDraft, ReasonCode,
    Retryability, SafetyState,
};
use sha2::{Digest, Sha256};

/// The `sink_ack_uncertain` incident around `cause`: a write that may already
/// be visible downstream was submitted, but its outcome is unknown and could
/// not be settled by an authoritative read. Identity is the sink and the
/// batch boundary (`boundary`: the batch's checkpoint bytes, exposed only as a
/// digest). The pipeline stops halted-uncertain; the checkpoint is not
/// advanced.
pub(crate) fn ack_uncertain(
    sink_id: &str,
    boundary: &[u8],
    attempts: u64,
    cause: SinkError,
) -> SinkError {
    let batch = hex::encode(Sha256::digest(boundary));
    let draft = IncidentDraft::new(
        ReasonCode::SinkAckUncertain,
        Component::Sink {
            id: sink_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedUncertain,
        cause.cause_code(),
    )
    .discriminate("batch", batch)
    .with_evidence(|e| {
        e.text(K::SinkId, sink_id)
            .digest(K::BatchBoundary, boundary, 1)
            .count(K::Attempts, attempts);
    })
    .with_actions(&[ActionCode::VerifySinkState]);
    SinkError::incident(draft, cause)
}
