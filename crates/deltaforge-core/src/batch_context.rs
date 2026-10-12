use std::collections::HashSet;
use std::sync::Arc;

use crate::canonical::schema::ResolvedSchemas;
use crate::{Event, EventId};

/// Immutable context passed through the entire processor chain for one batch.
///
/// Built once by the coordinator before any processor runs, so every
/// processor sees the same original event-ID set regardless of what
/// earlier processors added or removed.
#[derive(Clone)]
pub struct BatchContext {
    /// IDs of events that existed when this batch entered the chain.
    /// Used by `SyntheticMarkingProcessor` to detect newly created events.
    pub original_ids: HashSet<EventId>,
    /// The batch's schema versions, resolved and validated before any
    /// processor ran (`None`: the pipeline has no schema context, so a
    /// processor that needs one fails explicitly).
    pub schemas: Option<Arc<ResolvedSchemas>>,
}

impl std::fmt::Debug for BatchContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchContext")
            .field("original_ids", &self.original_ids)
            .field("schemas", &self.schemas.as_ref().map(|s| s.len()))
            .finish()
    }
}

impl BatchContext {
    /// Snapshot the event IDs from the batch as it enters the chain.
    pub fn from_batch(events: &[Event]) -> Self {
        Self {
            original_ids: events.iter().filter_map(|e| e.event_id).collect(),
            schemas: None,
        }
    }

    /// With the batch's resolved schema versions.
    pub fn with_schemas(mut self, schemas: Arc<ResolvedSchemas>) -> Self {
        self.schemas = Some(schemas);
        self
    }

    /// Returns true if this event existed before the processor chain ran.
    #[inline]
    pub fn is_original(&self, event: &Event) -> bool {
        event
            .event_id
            .map(|id| self.original_ids.contains(&id))
            .unwrap_or(false) // no ID → definitely new
    }
}
