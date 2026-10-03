pub mod checkpoint;
pub mod incidents;
pub mod schema_key;
pub mod schema_migration;
pub mod schema_registry;
pub mod source_lineage;
pub mod store_gate;
#[cfg(any(test, feature = "testing"))]
pub mod test_util;

pub use checkpoint::BackendCheckpointStore;
pub use schema_key::{FORMAT_VERSION, LineageDescriptor, SchemaKey};
pub use schema_registry::{
    AdoptOutcome, DurableSchemaRegistry, HistoryCursor, HistoryPage,
    HistoryStream, LegacyCursor, LegacyPage, LegacyReadRefused, LegacyVersion,
    MAX_HISTORY_PAGE, MigrationMarkerInfo, MigrationProvenance, RegistryConfig,
    RegistryMetrics,
};
pub use source_lineage::{Established, LineageRef, SourceLineageRecord};

/// A durable record that exists but cannot be interpreted (malformed,
/// unsupported version, failed integrity check) - as opposed to a storage
/// failure. Downcast it from the error chain to tell the two apart.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct CorruptRecord(pub String);
