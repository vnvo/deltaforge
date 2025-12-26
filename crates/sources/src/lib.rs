pub(crate) mod conn_utils;
pub mod mysql;
pub mod schema_loader;
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

// Re-export source types
pub use mysql::{MySqlCheckpoint, MySqlSource};
pub use turso::{TursoCheckpoint, TursoSource};

/// Build a CDC source from pipeline configuration.
pub fn build_source(
    pipeline: &PipelineSpec,
    registry: Arc<InMemoryRegistry>,
) -> Result<ArcDynSource> {
    match &pipeline.spec.source {
        SourceCfg::Postgres(_) => {
            anyhow::bail!("postgres source not yet implemented")
        }

        SourceCfg::Mysql(c) => Ok(Arc::new(mysql::MySqlSource {
            id: c.id.clone(),
            checkpoint_key: format!("mysql-{}", c.id),
            dsn: c.dsn.clone(),
            tables: c.tables.clone(),
            tenant: pipeline.metadata.tenant.clone(),
            pipeline: pipeline.metadata.name.clone(),
            registry,
        })),

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
        SourceCfg::Mysql(c) => Some(Arc::new(mysql::MySqlSchemaLoader::new(
            &c.dsn,
            registry,
            &pipeline.metadata.tenant,
        ))),
        _ => None,
    }
}
