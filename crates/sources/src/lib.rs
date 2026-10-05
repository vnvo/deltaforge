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
pub(crate) mod incident_drafts;
pub mod mysql;
pub mod postgres;
pub mod registry_scope;
pub mod rotation;
pub mod rotation_manager;
pub mod schema_loader;
pub(crate) mod snapshot_discovery;
pub mod snapshot_event_id;
pub mod snapshot_frontier;
pub mod snapshot_generation;
pub mod snapshot_plan;
pub mod snapshot_position;
pub mod snapshot_probe;
pub mod snapshot_queue;
pub mod stream_probe;
mod table_patterns;
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

/// The effective secret providers for a pipeline: the projected-volume file root, the
/// Vault connection, and the size policy that the shared resolver must satisfy.
///
/// Pipeline-level `spec.secrets` is primary; the source's rotation configuration is a
/// backward-compatible fallback for each provider it did not set.
struct EffectiveSecretProviders {
    projected_root: Option<std::path::PathBuf>,
    vault: Option<deltaforge_config::VaultRotationCfg>,
    max_secret_bytes: usize,
}

fn effective_secret_providers(
    pipeline: &PipelineSpec,
) -> EffectiveSecretProviders {
    let providers = pipeline.spec.secrets.as_ref();
    let rotation = match &pipeline.spec.source {
        SourceCfg::Postgres(c) => c.rotation.as_ref(),
        SourceCfg::Mysql(c) => c.rotation.as_ref(),
    };

    // Projected-volume file root: pipeline-level first, else a source File trigger's
    // trusted root.
    let projected_root = providers
        .and_then(|p| p.projected_file_root.clone())
        .or_else(|| match rotation.map(|r| &r.trigger) {
            Some(RotationTriggerCfg::File { trusted_root }) => {
                Some(trusted_root.clone())
            }
            _ => None,
        });

    // Vault connection: pipeline-level first, else a source Vault trigger's config.
    let vault = providers.and_then(|p| p.vault.clone()).or_else(|| {
        match rotation.map(|r| &r.trigger) {
            Some(RotationTriggerCfg::Vault(v)) => Some(v.clone()),
            _ => None,
        }
    });

    // Size policy: pipeline-level when present (its own default applies), else the
    // source rotation's, else the crate default.
    let max_secret_bytes = match (providers, rotation) {
        (Some(p), _) => p.max_secret_bytes,
        (None, Some(r)) => r.max_secret_bytes,
        (None, None) => secrets::DEFAULT_MAX_SECRET_BYTES,
    };

    EffectiveSecretProviders {
        projected_root,
        vault,
        max_secret_bytes,
    }
}

/// Build the secret resolver shared by a pipeline's source **and** sink credential
/// resolution, plus (for a Vault-triggered source rotation) the rotation manager's
/// polling.
///
/// Returned in an `Arc` because it is shared by the initial-DSN resolution,
/// [`build_source`], and sink construction. The resolver's providers come from
/// pipeline-level `spec.secrets` first, then fall back to the source's rotation
/// configuration, so a `vault` or projected-volume `file` reference on any sink works
/// even when the source uses ordinary (non-rotating) credentials:
/// - **File references:** run in [`FileMode::ProjectedVolume`] when a trusted root is
///   configured (pipeline-level or a source File trigger), else strict-symlink mode.
/// - **Vault references:** a Vault backend is connected and installed when configured
///   (pipeline-level or a source Vault trigger). Connection is attempted here, so an
///   invalid or unavailable Vault configuration fails **before source construction**.
pub async fn source_secret_resolver(
    pipeline: &PipelineSpec,
) -> Result<Arc<CompositeResolver>> {
    build_secret_resolver(effective_secret_providers(pipeline)).await
}

/// Assemble a composite resolver (env + file, plus Vault when configured) from the
/// effective providers. Fails closed if Vault is configured but cannot be connected.
async fn build_secret_resolver(
    providers: EffectiveSecretProviders,
) -> Result<Arc<CompositeResolver>> {
    let mode = match &providers.projected_root {
        Some(root) => FileMode::ProjectedVolume {
            trusted_root: root.clone(),
        },
        None => FileMode::Strict,
    };
    let base = CompositeResolver::new(
        EnvResolver::from_process(),
        FileResolver::new(FilePolicy {
            max_size: providers.max_secret_bytes,
            mode,
            trim_trailing_newline: false,
        }),
    );
    match &providers.vault {
        None => Ok(Arc::new(base)),
        Some(vcfg) => {
            install_vault(base, vcfg, providers.max_secret_bytes).await
        }
    }
}

/// Connect a Vault backend and install it on an existing composite resolver. Fails
/// closed on an invalid config or an unreachable/unauthenticated Vault, before any
/// source is constructed.
#[cfg(feature = "vault")]
async fn install_vault(
    base: CompositeResolver,
    vcfg: &deltaforge_config::VaultRotationCfg,
    max_secret_bytes: usize,
) -> Result<Arc<CompositeResolver>> {
    let conn = vault_connection(vcfg, max_secret_bytes)
        .context("invalid Vault configuration")?;
    let vault = secrets::VaultResolver::connect(conn)
        .await
        .map_err(|e| anyhow::anyhow!("connect to Vault: {e}"))
        .context("Vault credential resolution")?;
    Ok(Arc::new(base.with_vault(vault)))
}

/// Without the `vault` feature, a Vault-configured pipeline fails closed at startup.
#[cfg(not(feature = "vault"))]
async fn install_vault(
    _base: CompositeResolver,
    _vcfg: &deltaforge_config::VaultRotationCfg,
    _max_secret_bytes: usize,
) -> Result<Arc<CompositeResolver>> {
    Err(anyhow::anyhow!(
        "a Vault secret provider is configured, but this binary was built without \
         the `vault` feature; rebuild with `--features vault` or use env/file \
         references"
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
/// `registry_scope` is the pipeline's shared registry scope: the source
/// establishes its verified lineage into it, and the pipeline's schema loader
/// (see [`build_schema_loader`]) reads the same handle.
///
/// `resolver` resolves fixed (env) credential fields for controlled rotation and,
/// for a Vault trigger, is retained in the rotation spec to poll for new record
/// versions. A misconfigured rotation fails closed here.
pub async fn build_source(
    pipeline: &PipelineSpec,
    dsn: ProtectedDsn,
    registry: Arc<DurableSchemaRegistry>,
    registry_scope: registry_scope::SharedRegistryScope,
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
                registry_scope,
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
                registry_scope,
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
/// The loader shares `registry_scope` with the pipeline's source; it can reach
/// the registry only once the source has established its verified lineage, and
/// fails closed before that.
///
/// Returns None for sources that handle schemas internally.
pub fn build_schema_loader(
    pipeline: &PipelineSpec,
    dsn: &ProtectedDsn,
    registry: Arc<DurableSchemaRegistry>,
    registry_scope: registry_scope::SharedRegistryScope,
) -> Option<ArcSchemaLoader> {
    match &pipeline.spec.source {
        SourceCfg::Postgres(_) => {
            Some(Arc::new(postgres::PostgresSchemaLoader::new(
                dsn.clone(),
                registry,
                &pipeline.metadata.tenant,
                registry_scope,
            )))
        }

        SourceCfg::Mysql(_) => Some(Arc::new(mysql::MySqlSchemaLoader::new(
            dsn.clone(),
            registry,
            &pipeline.metadata.tenant,
            registry_scope,
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

    /// Without the `vault` feature, a Vault secret provider fails closed at startup
    /// rather than silently disabling it or constructing a source.
    #[cfg(not(feature = "vault"))]
    #[tokio::test]
    async fn vault_provider_fails_closed_without_feature() {
        let providers = super::EffectiveSecretProviders {
            projected_root: None,
            vault: Some(cfg("https://vault:8200", false)),
            max_secret_bytes: 4096,
        };
        let result = super::build_secret_resolver(providers).await;
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

/// Regression tests for pipeline-level secret providers assembling the SHARED
/// source/sink resolver, independent of source rotation configuration.
#[cfg(test)]
mod resolver_assembly_tests {
    use super::effective_secret_providers;
    use deltaforge_config::PipelineSpec;

    /// A minimal ordinary (non-rotation) PostgreSQL pipeline, with the given
    /// `spec.secrets` block spliced in (or omitted when null).
    fn spec_with_secrets(secrets: serde_json::Value) -> PipelineSpec {
        let mut spec = serde_json::json!({
            "apiVersion": "deltaforge/v1",
            "kind": "Pipeline",
            "metadata": { "name": "t", "tenant": "t" },
            "spec": {
                "source": {
                    "type": "postgres",
                    "config": {
                        "id": "pg",
                        "dsn": "postgres://localhost:5432/db",
                        "publication": "p",
                        "slot": "s",
                        "tables": ["public.t"]
                    }
                },
                "processors": [],
                "sinks": []
            }
        });
        if !secrets.is_null() {
            spec["spec"]["secrets"] = secrets;
        }
        serde_json::from_value(spec).expect("valid pipeline spec")
    }

    #[test]
    fn pipeline_vault_selected_for_ordinary_source() {
        // Before the fix an ordinary source produced the default resolver with no Vault,
        // so a Vault-backed sink failed UnsupportedProvider. Pipeline-level Vault config
        // must now drive the shared resolver.
        let spec = spec_with_secrets(serde_json::json!({
            "vault": {
                "address": "https://vault:8200",
                "auth": { "method": "token_file", "path": "/run/secrets/vault/token" }
            }
        }));
        let providers = effective_secret_providers(&spec);
        assert!(
            providers.vault.is_some(),
            "pipeline-level Vault must be selected"
        );
        assert!(providers.projected_root.is_none());
    }

    #[test]
    fn pipeline_projected_root_selected_for_ordinary_source() {
        let spec = spec_with_secrets(serde_json::json!({
            "projected_file_root": "/var/run/secrets"
        }));
        let providers = effective_secret_providers(&spec);
        assert_eq!(
            providers.projected_root.as_deref(),
            Some(std::path::Path::new("/var/run/secrets"))
        );
        assert!(providers.vault.is_none());
    }

    #[test]
    fn no_providers_without_config() {
        let providers = effective_secret_providers(&spec_with_secrets(
            serde_json::Value::Null,
        ));
        assert!(providers.vault.is_none());
        assert!(providers.projected_root.is_none());
    }

    /// Without the `vault` feature (the CI default), an ordinary source configured with
    /// a pipeline-level Vault provider fails closed at startup rather than silently
    /// ignoring it - proving pipeline-level Vault config is honored, not dropped.
    #[cfg(not(feature = "vault"))]
    #[tokio::test]
    async fn ordinary_source_with_pipeline_vault_fails_closed_without_feature()
    {
        let spec = spec_with_secrets(serde_json::json!({
            "vault": {
                "address": "https://vault:8200",
                "auth": { "method": "token_file", "path": "/run/secrets/vault/token" }
            }
        }));
        let err = super::source_secret_resolver(&spec)
            .await
            .err()
            .expect("must fail closed without the vault feature");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("vault") && msg.contains("feature"),
            "expected a fail-closed 'missing vault feature' error, got: {msg}"
        );
    }
}
