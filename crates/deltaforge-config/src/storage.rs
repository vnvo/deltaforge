use serde::{Deserialize, Serialize};

use crate::CredentialRefsCfg;

/// Top-level storage configuration.
///
/// Postgres credentials may be supplied three additive, mutually-exclusive ways
/// (mirroring the source model):
/// - `dsn`: an inline connection string (**deprecated**; may embed a password - kept for
///   back-compat, redacted everywhere, prefer a reference);
/// - `dsn_secret`: a [`SecretReference`](secrets::SecretReference) resolving to the whole
///   connection string;
/// - `dsn` (password-less base) + `credentials`: username/password resolved from
///   references and injected into the base DSN.
///
/// ```yaml
/// storage:
///   backend: postgres
///   dsn: "host=localhost dbname=deltaforge"
///   credentials:
///     username: { provider: env, location: DF_STORE_USER }
///     password: { provider: file, location: /run/secrets/store/password }
/// ```
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct StorageConfig {
    #[serde(default)]
    pub backend: StorageBackendKind,

    /// Path for SQLite database file (sqlite backend only).
    #[serde(default = "default_sqlite_path")]
    pub path: String,

    /// PostgreSQL connection string (postgres backend only). **Deprecated** when it
    /// carries a password inline; prefer `dsn_secret` or `credentials`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dsn: Option<String>,

    /// A reference resolving to the whole PostgreSQL connection string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dsn_secret: Option<secrets::SecretReference>,

    /// Username/password references injected into the (password-less) base `dsn`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<CredentialRefsCfg>,

    /// Kubernetes projected-volume trusted root for file references (enables following
    /// projected symlinks); when unset, file references must be regular files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_trusted_root: Option<std::path::PathBuf>,

    /// Process-level Vault connection/auth for resolving storage credential references
    /// from Vault KV. Independent of any pipeline's Vault configuration; installed on the
    /// storage bootstrap resolver before the backend is opened.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<crate::VaultRotationCfg>,
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render an inline DSN password; references are non-secret metadata.
        f.debug_struct("StorageConfig")
            .field("backend", &self.backend)
            .field("path", &self.path)
            .field(
                "dsn",
                &self.dsn.as_ref().map(|d| common::dsn::redact_dsn(d)),
            )
            .field("dsn_secret", &self.dsn_secret)
            .field("credentials", &self.credentials)
            .field("secret_trusted_root", &self.secret_trusted_root)
            .field("vault", &self.vault)
            .finish()
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackendKind::Sqlite,
            path: default_sqlite_path(),
            dsn: None,
            dsn_secret: None,
            credentials: None,
            secret_trusted_root: None,
            vault: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackendKind {
    #[default]
    Sqlite,
    Memory,
    Postgres,
}

fn default_sqlite_path() -> String {
    "./data/deltaforge.db".to_string()
}
