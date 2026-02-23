//! CDC source implementations for DeltaForge.
//!
//! This crate provides database CDC sources that implement the `Source` trait:
//!
//! - **MySQL**: Binlog-based CDC using `mysql_binlog_connector_rust`
//! - **PostgreSQL**: Logical replication using pgoutput protocol
//! - **Turso**: Experimental SQLite CDC (paused)
//!
//! Each source captures row-level changes and emits them as `Event`s
//! to be processed by the pipeline coordinator.

pub mod mysql;
pub mod postgres;
pub mod schema_loader;
#[cfg(feature = "turso")]
pub mod turso;

use anyhow::Result;
use deltaforge_config::{PipelineSpec, SourceCfg};
use deltaforge_core::ArcDynSource;
use schema_registry::InMemoryRegistry;
use std::sync::Arc;

// Re-export loader types
pub use schema_loader::{
    ArcSchemaLoader, LoadedSchema, SchemaListEntry, SourceSchemaLoader,
};

pub use mysql::{MySqlCheckpoint, MySqlSchemaLoader, MySqlSource};
pub use postgres::{PostgresCheckpoint, PostgresSource};
#[cfg(feature = "turso")]
pub use turso::{TursoCheckpoint, TursoSource};

/// Build a CDC source from pipeline configuration.
pub fn build_source(
    pipeline: &PipelineSpec,
    registry: Arc<InMemoryRegistry>,
) -> Result<ArcDynSource> {
    match &pipeline.spec.source {
        SourceCfg::Postgres(c) => Ok(Arc::new(postgres::PostgresSource {
            id: c.id.clone(),
            checkpoint_key: format!("pg-{}", c.id),
            dsn: c.dsn.clone(),
            slot: c.slot.clone(),
            publication: c.publication.clone(),
            tables: c.tables.clone(),
            pipeline: pipeline.metadata.name.clone(),
            tenant: pipeline.metadata.tenant.clone(),
            registry,
            outbox_prefixes: c
                .outbox
                .as_ref()
                .map(|o| o.allow_list())
                .unwrap_or_default(),
        })),

        SourceCfg::Mysql(c) => Ok(Arc::new(mysql::MySqlSource {
            id: c.id.clone(),
            checkpoint_key: format!("mysql-{}", c.id),
            dsn: c.dsn.clone(),
            tables: c.tables.clone(),
            tenant: pipeline.metadata.tenant.clone(),
            pipeline: pipeline.metadata.name.clone(),
            registry,
            outbox_tables: c
                .outbox
                .as_ref()
                .map(|o| o.allow_list())
                .unwrap_or_default(),
        })),
        #[cfg(feature = "turso")]
        SourceCfg::Turso(c) => Ok(Arc::new(turso::TursoSource::new(
            c.clone(),
            pipeline.metadata.tenant.clone(),
            pipeline.metadata.name.clone(),
            registry,
        ))),
    }
}

/// Build a schema loader from pipeline configuration.
///
/// Returns None for sources that handle schemas internally.
pub fn build_schema_loader(
    pipeline: &PipelineSpec,
    registry: Arc<InMemoryRegistry>,
) -> Option<ArcSchemaLoader> {
    match &pipeline.spec.source {
        SourceCfg::Postgres(c) => {
            Some(Arc::new(postgres::PostgresSchemaLoader::new(
                &c.dsn,
                registry,
                &pipeline.metadata.tenant,
            )))
        }

        SourceCfg::Mysql(c) => Some(Arc::new(mysql::MySqlSchemaLoader::new(
            &c.dsn,
            registry,
            &pipeline.metadata.tenant,
        ))),

        #[cfg(feature = "turso")]
        SourceCfg::Turso(_) => None, // Turso handles schemas internally
    }
}
