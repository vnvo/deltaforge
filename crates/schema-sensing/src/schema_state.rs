use chrono::{DateTime, Utc};
use schema_analysis::{InferredSchema, Schema};
use serde::{Deserialize, Serialize};

use crate::fingerprint::compute_fingerprint;

/// State for a single table's inferred schema.
pub struct TableSchemaState {
    /// The table name
    pub table: String,

    /// The inferred schema (mutable for incremental updates)
    pub inferred: InferredSchema,

    /// Current schema fingerprint
    pub fingerprint: String,

    /// Monotonically increasing sequence number (increments on schema change)
    pub sequence: u64,

    /// Number of events observed
    pub event_count: u64,

    /// First observation timestamp
    pub first_seen: DateTime<Utc>,

    /// Most recent observation timestamp
    pub last_seen: DateTime<Utc>,

    /// Whether schema has stabilized (stopped sampling)
    pub stabilized: bool,
}

impl TableSchemaState {
    pub fn new(table: String, inferred: InferredSchema) -> Self {
        let fingerprint = compute_fingerprint(&inferred.schema);
        let now = Utc::now();

        Self {
            table,
            inferred,
            fingerprint,
            sequence: 1,
            event_count: 1,
            first_seen: now,
            last_seen: now,
            stabilized: false,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.inferred.schema
    }

    pub fn update_fingerprint(&mut self) {
        let new_fp = compute_fingerprint(&self.inferred.schema);
        if new_fp != self.fingerprint {
            self.fingerprint = new_fp;
            self.sequence += 1;
        }
    }

    pub fn record_observation(&mut self) {
        self.event_count += 1;
        self.last_seen = Utc::now();
    }

    pub fn mark_stabilized(&mut self) {
        self.stabilized = true;
    }

    pub fn version(&self) -> SchemaVersion {
        SchemaVersion {
            fingerprint: self.fingerprint.clone(),
            sequence: self.sequence,
        }
    }
}

/// Schema version information for event enrichment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaVersion {
    /// SHA-256 fingerprint of the schema
    pub fingerprint: String,

    /// Monotonic sequence number
    pub sequence: u64,
}

impl Default for SchemaVersion {
    fn default() -> Self {
        Self {
            fingerprint: String::new(),
            sequence: 0,
        }
    }
}

/// Serializable schema snapshot for persistence/API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    /// Table name
    pub table: String,

    /// The inferred schema (serialized)
    pub schema: Schema,

    /// Current fingerprint
    pub fingerprint: String,

    /// Sequence number
    pub sequence: u64,

    /// Number of events observed
    pub event_count: u64,

    /// First seen timestamp
    pub first_seen: DateTime<Utc>,

    /// Last seen timestamp
    pub last_seen: DateTime<Utc>,

    /// Whether sampling has stopped
    pub stabilized: bool,
}

impl From<&TableSchemaState> for SchemaSnapshot {
    fn from(state: &TableSchemaState) -> Self {
        Self {
            table: state.table.clone(),
            schema: state.inferred.schema.clone(),
            fingerprint: state.fingerprint.clone(),
            sequence: state.sequence,
            event_count: state.event_count,
            first_seen: state.first_seen,
            last_seen: state.last_seen,
            stabilized: state.stabilized,
        }
    }
}