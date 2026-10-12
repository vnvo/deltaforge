//! Durable bindings between sink destinations and source relations.
//!
//! A destination a sink writes rows to by key (a ClickHouse table, an
//! Elasticsearch index) holds exactly one source relation: rows of a second
//! relation would mix tables or overwrite each other's documents. The
//! binding is durable (it outlives restarts and in-memory caches), is made
//! once and atomically (concurrent first writers agree on one relation), and
//! records the target's own identity, so a target replaced since it was
//! bound is noticed.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::incident::{
    CauseCode, Component, EvidenceKey as K, IncidentDraft, ReasonCode,
    Retryability, SafetyState,
};
use crate::{SinkError, SourceRelationIdentity};

/// What a destination is bound to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationBinding {
    /// The relation's canonical identity.
    pub relation: String,
    /// The target's own identity when it was bound (an index UUID, a table
    /// UUID): a different one means the target was replaced.
    pub target_uid: String,
}

/// The durable store of destination bindings.
#[async_trait]
pub trait DestinationBindingStore: Send + Sync {
    /// The binding of `destination`, if it has one.
    async fn get(
        &self,
        destination: &str,
    ) -> anyhow::Result<Option<DestinationBinding>>;

    /// Atomically bind `destination` to `binding` when it has none. Returns
    /// the binding in force: `binding`, or the one a concurrent or earlier
    /// writer made.
    async fn bind(
        &self,
        destination: &str,
        binding: &DestinationBinding,
    ) -> anyhow::Result<DestinationBinding>;
}

/// An in-memory store (tests, and sinks built without a state store).
#[derive(Default)]
pub struct MemoryDestinationBindings {
    bound: Mutex<HashMap<String, DestinationBinding>>,
}

#[async_trait]
impl DestinationBindingStore for MemoryDestinationBindings {
    async fn get(
        &self,
        destination: &str,
    ) -> anyhow::Result<Option<DestinationBinding>> {
        Ok(self
            .bound
            .lock()
            .expect("bindings")
            .get(destination)
            .cloned())
    }

    async fn bind(
        &self,
        destination: &str,
        binding: &DestinationBinding,
    ) -> anyhow::Result<DestinationBinding> {
        Ok(self
            .bound
            .lock()
            .expect("bindings")
            .entry(destination.to_string())
            .or_insert_with(|| binding.clone())
            .clone())
    }
}

/// Why a destination refused a relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictClass {
    /// Bound to another relation.
    OtherRelation,
    /// Replaced (another target identity) since it was bound.
    TargetReplaced,
    /// Holds rows but has no binding (data from elsewhere, or a removed
    /// binding).
    UnboundRows,
}

impl ConflictClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OtherRelation => "bound_to_another_relation",
            Self::TargetReplaced => "target_replaced",
            Self::UnboundRows => "rows_without_binding",
        }
    }
}

/// The `sink_destination_conflict` error: halts the pipeline with the
/// sink's checkpoint held, naming the destination and both relations (never
/// row data).
pub fn destination_conflict(
    sink_id: &str,
    destination: &str,
    relation: &SourceRelationIdentity,
    bound: Option<&str>,
    class: ConflictClass,
) -> SinkError {
    let relation = relation.canonical();
    let bound = bound.unwrap_or("none").to_string();
    let draft = IncidentDraft::new(
        ReasonCode::SinkDestinationConflict,
        Component::Sink {
            id: sink_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SinkFatal,
    )
    .discriminate("destination", destination)
    .discriminate("class", class.as_str())
    .with_evidence(|e| {
        e.text(K::SinkId, sink_id)
            .text(K::Destination, destination)
            .text(K::Relation, &relation)
            .text(K::BoundRelation, &bound)
            .text(K::ReasonClass, class.as_str());
    });
    SinkError::incident(
        draft,
        SinkError::Fatal {
            details: format!(
                "sink {sink_id}: destination {destination} refused relation \
                 {relation} ({}; bound to {bound})",
                class.as_str()
            )
            .into(),
        },
    )
}
