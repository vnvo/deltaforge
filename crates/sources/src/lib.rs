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
pub mod rotation_manager;
pub mod schema_loader;
pub mod snapshot_event_id;
pub mod snapshot_frontier;
pub mod snapshot_generation;
// Phase 2 lease-lifecycle machinery (models + durable store + pure state machine).
// Consumed by the Phase 2 integration slice (Vault lease ops/scheduler/recovery) and
// Phase 3; unused in production until then, so its API is allowed to be dead here.
#[cfg(feature = "vault")]
#[allow(dead_code)]
mod vault_lease;
#[cfg(feature = "vault")]
#[allow(dead_code)]
mod vault_lease_manager;
// Live Vault + PostgreSQL/MySQL lease lifecycle integration tests (ignored, env-gated).
#[cfg(all(test, feature = "vault"))]
mod vault_lease_e2e;

use anyhow::{Context, Result};
use deltaforge_config::{PipelineSpec, RotationTriggerCfg, SourceCfg};
use deltaforge_core::ArcDynSource;
use secrets::{
    CompositeResolver, EnvResolver, FileMode, FilePolicy, FileResolver,
    SecretResolver,
};
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

/// Build the secret resolver for a source's startup credential resolution and, for
/// a Vault-triggered rotation, for the rotation manager's polling.
///
/// Shared by the initial-DSN resolution and [`build_source`], so it is returned in
/// an `Arc`. The resolver depends on the configured rotation trigger:
/// - **No rotation:** the strict-symlink default.
/// - **File trigger:** the credential files are on a Kubernetes projected volume
///   whose keys are symlinks, so the file resolver runs in
///   [`FileMode::ProjectedVolume`] rooted at the configured `trusted_root`, or
///   startup resolution of the initial DSN would fail on the symlinks.
/// - **Vault trigger:** a Vault backend is connected and installed. Connection is
///   attempted here, so an invalid or unavailable Vault configuration fails **before
///   source construction** rather than after the source takes ownership.
pub async fn source_secret_resolver(
    pipeline: &PipelineSpec,
) -> Result<Arc<CompositeResolver>> {
    let rotation = match &pipeline.spec.source {
        SourceCfg::Postgres(c) => c.rotation.as_ref(),
        SourceCfg::Mysql(c) => c.rotation.as_ref(),
    };
    let Some(rot) = rotation else {
        return Ok(Arc::new(default_secret_resolver()));
    };
    match &rot.trigger {
        RotationTriggerCfg::File { trusted_root } => {
            Ok(Arc::new(CompositeResolver::new(
                EnvResolver::from_process(),
                FileResolver::new(FilePolicy {
                    max_size: rot.max_secret_bytes,
                    mode: FileMode::ProjectedVolume {
                        trusted_root: trusted_root.clone(),
                    },
                    trim_trailing_newline: false,
                }),
            )))
        }
        RotationTriggerCfg::Vault(vcfg) => {
            build_vault_resolver(vcfg, rot.max_secret_bytes).await
        }
    }
}

/// Connect a Vault backend and install it on a composite resolver. Fails closed on
/// an invalid config or an unreachable/unauthenticated Vault, before any source is
/// constructed.
#[cfg(feature = "vault")]
async fn build_vault_resolver(
    vcfg: &deltaforge_config::VaultRotationCfg,
    max_secret_bytes: usize,
) -> Result<Arc<CompositeResolver>> {
    let conn = vault_connection(vcfg, max_secret_bytes)
        .context("invalid Vault rotation configuration")?;
    let vault = secrets::VaultResolver::connect(conn)
        .await
        .map_err(|e| anyhow::anyhow!("connect to Vault: {e}"))
        .context("Vault credential rotation")?;
    Ok(Arc::new(
        CompositeResolver::new(
            EnvResolver::from_process(),
            FileResolver::new(FilePolicy::default()),
        )
        .with_vault(vault),
    ))
}

/// Without the `vault` feature, a Vault-triggered rotation fails closed at startup.
#[cfg(not(feature = "vault"))]
async fn build_vault_resolver(
    _vcfg: &deltaforge_config::VaultRotationCfg,
    _max_secret_bytes: usize,
) -> Result<Arc<CompositeResolver>> {
    Err(anyhow::anyhow!(
        "Vault-triggered credential rotation is configured, but this binary was \
         built without the `vault` feature; rebuild with `--features vault` or use \
         a file trigger"
    ))
}

/// Map the serde Vault config onto a validated [`secrets::VaultConnection`]. Public so
/// other startup paths (e.g. the storage bootstrap resolver) reuse the same validated
/// mapping instead of duplicating it.
#[cfg(feature = "vault")]
pub fn vault_connection(
    vcfg: &deltaforge_config::VaultRotationCfg,
    max_secret_bytes: usize,
) -> Result<secrets::VaultConnection> {
    use deltaforge_config::VaultAuthCfg;
    use secrets::{
        RenewPolicy, VaultAuth, VaultAuthFile, VaultConnection, VaultTimeouts,
    };
    use std::time::Duration;

    fn auth_file(
        path: &std::path::Path,
        root: &Option<std::path::PathBuf>,
    ) -> VaultAuthFile {
        match root {
            Some(r) => VaultAuthFile::ProjectedVolume {
                path: path.to_path_buf(),
                trusted_root: r.clone(),
            },
            None => VaultAuthFile::Strict {
                path: path.to_path_buf(),
            },
        }
    }

    let auth = match &vcfg.auth {
        VaultAuthCfg::TokenFile {
            path,
            projected_volume_root,
        } => VaultAuth::TokenFile(auth_file(path, projected_volume_root)),
        VaultAuthCfg::Kubernetes {
            mount,
            role,
            jwt_path,
            projected_volume_root,
        } => VaultAuth::Kubernetes {
            mount: mount.clone(),
            role: role.clone(),
            jwt: auth_file(jwt_path, projected_volume_root),
        },
    };

    let mut renew = RenewPolicy::default();
    if let Some(ms) = vcfg.renew_safety_margin_ms {
        renew.safety_margin = Duration::from_millis(ms);
    }
    let mut timeouts = VaultTimeouts::default();
    if let Some(ms) = vcfg.connect_timeout_ms {
        timeouts.connect = Duration::from_millis(ms);
    }
    if let Some(ms) = vcfg.request_timeout_ms {
        timeouts.request = Duration::from_millis(ms);
    }

    VaultConnection::new(
        vcfg.address.clone(),
        vcfg.namespace.clone(),
        auth,
        max_secret_bytes,
        renew,
        timeouts,
        vcfg.allow_insecure_http,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))
}

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
///
/// `resolver` resolves fixed (env) credential fields for controlled rotation and,
/// for a Vault trigger, is retained in the rotation spec to poll for new record
/// versions. A misconfigured rotation fails closed here.
pub async fn build_source(
    pipeline: &PipelineSpec,
    dsn: ProtectedDsn,
    registry: Arc<DurableSchemaRegistry>,
    backend: ArcStorageBackend,
    resolver: Arc<CompositeResolver>,
) -> Result<ArcDynSource> {
    let resolver: Arc<dyn SecretResolver> = resolver;
    match &pipeline.spec.source {
        SourceCfg::Postgres(c) => {
            let rotation = postgres::postgres_rotation::build_spec(
                c,
                Arc::clone(&resolver),
            )
            .await?;
            Ok(Arc::new(postgres::PostgresSource {
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
                rotation,
            }))
        }

        SourceCfg::Mysql(c) => {
            let rotation =
                mysql::mysql_rotation::build_spec(c, Arc::clone(&resolver))
                    .await?;
            Ok(Arc::new(mysql::MySqlSource {
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
                rotation,
            }))
        }
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

#[cfg(test)]
mod vault_wiring_tests {
    use deltaforge_config::{VaultAuthCfg, VaultRotationCfg};

    fn cfg(address: &str, allow_insecure_http: bool) -> VaultRotationCfg {
        VaultRotationCfg {
            address: address.to_string(),
            namespace: None,
            auth: VaultAuthCfg::TokenFile {
                path: "/run/secrets/vault/token".into(),
                projected_volume_root: None,
            },
            allow_insecure_http,
            connect_timeout_ms: Some(500),
            request_timeout_ms: Some(500),
            renew_safety_margin_ms: None,
        }
    }

    /// Without the `vault` feature, a Vault-triggered rotation fails closed at
    /// startup rather than silently disabling rotation or constructing a source.
    #[cfg(not(feature = "vault"))]
    #[tokio::test]
    async fn vault_trigger_fails_closed_without_feature() {
        let result = super::build_vault_resolver(
            &cfg("https://vault:8200", false),
            4096,
        )
        .await;
        let err = result.err().expect("must fail closed without the feature");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("vault") && msg.contains("feature"),
            "expected a fail-closed 'missing vault feature' error, got: {msg}"
        );
    }

    /// With the `vault` feature, an invalid Vault configuration is rejected before
    /// any source is constructed (config validation, no network).
    #[cfg(feature = "vault")]
    #[test]
    fn invalid_vault_config_rejected_before_construction() {
        // http without the explicit dev opt-in is rejected.
        assert!(
            super::vault_connection(&cfg("http://vault:8200", false), 4096)
                .is_err()
        );
        // A zero size limit is rejected.
        assert!(
            super::vault_connection(&cfg("https://vault:8200", false), 0)
                .is_err()
        );
        // A valid https config maps successfully.
        assert!(
            super::vault_connection(&cfg("https://vault:8200", false), 4096)
                .is_ok()
        );
    }
}
