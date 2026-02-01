//! Schema Sensing - Universal schema inference from JSON payloads.
//!
//! This crate provides automatic schema discovery and evolution tracking
//! for any data source that produces JSON events. It uses the `schema_analysis`
//! crate for type inference and `flowstats` for high-cardinality key handling.
//!
//! # Features
//!
//! - **Automatic inference**: Discovers schema from JSON payloads
//! - **Deep inspection**: Analyzes nested JSON structures  
//! - **Evolution tracking**: Detects schema changes over time
//! - **Fingerprinting**: Generates stable schema versions
//! - **High-cardinality handling**: Distinguishes stable fields from dynamic map keys
//! - **JSON Schema export**: Converts inferred schemas to JSON Schema
//!
//! # High-Cardinality Key Handling
//!
//! JSON objects may contain dynamic keys (UUIDs, session IDs, etc.) that cause:
//! - Constant false "schema evolution" events
//! - 0% structure cache hit rate
//! - Unbounded memory growth tracking unique keys
//!
//! This crate uses probabilistic data structures to classify fields:
//! - **Stable fields**: Appear in most events (schema properties)
//! - **Dynamic fields**: Appear rarely, unique values (map keys)
//!
//! ```ignore
//! // Event 1: {"id": 1, "sessions": {"sess_abc": {...}}}
//! // Event 2: {"id": 2, "sessions": {"sess_xyz": {...}}}
//! //
//! // Classification:
//! //   root: stable=[id, sessions], dynamic=false
//! //   sessions: stable=[], dynamic=true (detected as map)
//! //
//! // Structure hash ignores dynamic key names → cache hit!
//! ```

mod adaptive_hash;
mod errors;
mod field_classifier;
mod fingerprint;
mod high_cardinality;
mod json_schema;
mod schema_state;
mod sensor;

pub use adaptive_hash::{compute_adaptive_hash, compute_structure_hash};
pub use errors::{SensorError, SensorResult};
pub use field_classifier::FieldClassifier;
pub use fingerprint::compute_fingerprint;
pub use high_cardinality::{
    HighCardinalityConfig, PathClassification, PathFieldStats, StableField,
};
pub use json_schema::{JsonSchema, JsonSchemaType, to_json_schema};
pub use schema_state::{SchemaSnapshot, SensedSchemaVersion, TableSchemaState};
pub use sensor::{CacheStatsEntry, ObserveResult, SchemaSensor};

pub use deltaforge_config::SchemaSensingConfig;
pub use schema_analysis::Schema as InferredSchemaType;
