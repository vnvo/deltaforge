use std::collections::HashSet;
use uuid::Uuid;

use crate::Event;

/// Immutable context passed through the entire processor chain for one batch.
///
/// Built once by the coordinator before any processor runs, so every
/// processor sees the same original event-ID set regardless of what
/// earlier processors added or removed.
#[derive(Debug, Clone)]
pub struct BatchContext {
    /// IDs of events that existed when this batch entered the chain.
    /// Used by `SyntheticMarkingProcessor` to detect newly created events.
    pub original_ids: HashSet<Uuid>,
}

impl BatchContext {
    /// Snapshot the event IDs from the batch as it enters the chain.
    pub fn from_batch(events: &[Event]) -> Self {
        Self {
            original_ids: events.iter().filter_map(|e| e.event_id).collect(),
        }
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
