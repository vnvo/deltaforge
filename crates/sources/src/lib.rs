//! CDC source implementations for DeltaForge.
//!
//! This crate provides database CDC sources that implement the `Source` trait:
//!
//! - **MySQL**: Binlog-based CDC using `mysql_binlog_connector_rust`
//! - **PostgreSQL**: Logical replication using pgoutput protocol
//!
//! Each source captures row-level changes and emits them as `Event`s
//! to be processed by the pipeline coordinator.

pub mod credentials;
pub mod durable_checkpoint;
pub mod failover;
pub mod identity_resolution;
pub mod mysql;
pub mod postgres;
pub mod rotation;
pub mod schema_loader;
pub mod snapshot_event_id;
pub mod snapshot_frontier;
pub mod snapshot_generation;

use anyhow::{Context, Result};
use deltaforge_config::{PipelineSpec, SourceCfg};
use deltaforge_core::ArcDynSource;
use secrets::SecretResolver;
use std::sync::Arc;
use storage::{ArcStorageBackend, DurableSchemaRegistry};

// Re-export loader types
pub use schema_loader::{
    ArcSchemaLoader, LoadedSchema, SchemaListEntry, SourceSchemaLoader,
};

pub use credentials::{
    CredentialError, MysqlCredentials, PostgresCredentials, ProtectedDsn,
    default_secret_resolver, resolve_mysql_credentials,
    resolve_postgres_credentials,
};
pub use mysql::{MySqlCheckpoint, MySqlSchemaLoader, MySqlSource};
pub use postgres::{PostgresCheckpoint, PostgresSource};
pub use rotation::{
    ApplyOutcome, Candidate, DbKind, FieldSource, RetryConfig,
    RotationComposition, RotationCoordinator, RotationReject, apply_two_stage,
    compose, earliest_expiry,
};

/// Resolve the source's connection DSN from configuration (inline, whole-DSN
/// secret, or base DSN plus referenced credentials). This performs all secret
/// resolution up front, before any source takes long-lived ownership, so a
/// missing or invalid secret fails startup cleanly. The resulting [`ProtectedDsn`]
/// is shared into both the source and its schema loader.
pub async fn resolve_source_dsn(
    pipeline: &PipelineSpec,
    resolver: &dyn SecretResolver,
) -> Result<ProtectedDsn> {
    let dsn = match &pipeline.spec.source {
        SourceCfg::Postgres(c) => resolve_postgres_credentials(c, resolver)
            .await
            .and_then(|spec| spec.build_dsn()),
        SourceCfg::Mysql(c) => resolve_mysql_credentials(c, resolver)
            .await
            .and_then(|spec| spec.build_dsn()),
    }
    .context("resolve source credentials")?;
    Ok(dsn)
}

/// Build a CDC source from pipeline configuration and a pre-resolved DSN.
pub fn build_source(
    pipeline: &PipelineSpec,
    dsn: ProtectedDsn,
    registry: Arc<DurableSchemaRegistry>,
    backend: ArcStorageBackend,
) -> Result<ArcDynSource> {
    match &pipeline.spec.source {
        SourceCfg::Postgres(c) => Ok(Arc::new(postgres::PostgresSource {
            id: c.id.clone(),
            dsn,
            slot: c.slot.clone(),
            publication: c.publication.clone(),
            tables: c.tables.clone(),
            pipeline: pipeline.metadata.name.clone(),
            tenant: pipeline.metadata.tenant.clone(),
            registry,
            backend: Arc::clone(&backend),
            outbox_prefixes: c
                .outbox
                .as_ref()
                .map(|o| o.allow_list())
                .unwrap_or_default(),
            snapshot_cfg: c.snapshot.clone(),
            on_schema_drift: c.on_schema_drift.clone(),
            table_options: c.table_options.clone(),
        })),

        SourceCfg::Mysql(c) => Ok(Arc::new(mysql::MySqlSource {
            id: c.id.clone(),
            dsn,
            tables: c.tables.clone(),
            tenant: pipeline.metadata.tenant.clone(),
            pipeline: pipeline.metadata.name.clone(),
            registry,
            backend: Arc::clone(&backend),
            outbox_tables: c
                .outbox
                .as_ref()
                .map(|o| o.allow_list())
                .unwrap_or_default(),
            snapshot_cfg: c.snapshot.clone(),
            on_schema_drift: c.on_schema_drift.clone(),
            table_options: c.table_options.clone(),
        })),
    }
}

/// Build a schema loader from pipeline configuration and the pre-resolved DSN.
///
/// Returns None for sources that handle schemas internally.
pub fn build_schema_loader(
    pipeline: &PipelineSpec,
    dsn: &ProtectedDsn,
    registry: Arc<DurableSchemaRegistry>,
) -> Option<ArcSchemaLoader> {
    match &pipeline.spec.source {
        SourceCfg::Postgres(_) => {
            Some(Arc::new(postgres::PostgresSchemaLoader::new(
                dsn.clone(),
                registry,
                &pipeline.metadata.tenant,
            )))
        }

        SourceCfg::Mysql(_) => Some(Arc::new(mysql::MySqlSchemaLoader::new(
            dsn.clone(),
            registry,
            &pipeline.metadata.tenant,
        ))),
    }
}
