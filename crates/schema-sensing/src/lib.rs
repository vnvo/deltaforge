//! Schema Sensing - Universal schema inference from JSON payloads.
//!
//! This crate provides automatic schema discovery and evolution tracking
//! for any data source that produces JSON events. It uses the `schema_analysis`
//! crate under the hood for robust type inference.
//!
//! # Features
//!
//! - **Automatic inference**: Discovers schema from JSON payloads
//! - **Deep inspection**: Analyzes nested JSON structures
//! - **Evolution tracking**: Detects schema changes over time
//! - **Fingerprinting**: Generates stable schema versions
//! - **JSON Schema export**: Converts inferred schemas to JSON Schema
//!
//! # Example
//!
//! ```ignore
//! use schema_sensing::{SchemaSensor, SchemaSensingConfig};
//!
//! let config = SchemaSensingConfig { enabled: true, ..Default::default() };
//! let mut sensor = SchemaSensor::new(config);
//!
//! // Feed events from any source
//! let json = r#"{"id": 1, "name": "Alice", "metadata": {"role": "admin"}}"#;
//! let result = sensor.observe("users", json.as_bytes())?;
//!
//! // Get schema info for event enrichment
//! if let Some(version) = sensor.get_version("users") {
//!     println!("Schema version: {}, sequence: {}", version.fingerprint, version.sequence);
//! }
//! ```

mod errors;
mod fingerprint;
mod json_schema;
mod schema_state;
mod sensor;

pub use errors::{SensorError, SensorResult};
pub use fingerprint::compute_fingerprint;
pub use json_schema::{JsonSchema, JsonSchemaType, to_json_schema};
pub use schema_state::{SchemaSnapshot, SensedSchemaVersion, TableSchemaState};
pub use sensor::{CacheStatsEntry, ObserveResult, SchemaSensor};

pub use deltaforge_config::SchemaSensingConfig;
pub use schema_analysis::Schema as InferredSchemaType;
