//! Bootstrap credential resolution for the global storage backend.
//!
//! Per the secret-coverage design, storage has its OWN bootstrap resolver (env + file,
//! optionally Kubernetes projected-volume), assembled before the backend is opened - so a
//! missing/invalid/conflicting credential fails closed **before** any network access or
//! pipeline start, and pipeline-scoped Vault config is never forced into storage startup.

use anyhow::{Context, Result, bail};
use deltaforge_config::{StorageBackendKind, StorageConfig};
use secrets::{
    CompositeResolver, DEFAULT_MAX_SECRET_BYTES, EnvResolver, FileMode,
    FilePolicy, FileResolver, SecretReference, SecretResolver,
};
use zeroize::Zeroizing;

/// Build the storage bootstrap resolver: environment variables (covers Kubernetes
/// `secretKeyRef`) plus files (regular files, or projected-volume symlinks when a trusted
/// root is configured), plus **process-level Vault KV** when `cfg.vault` is set. This
/// Vault configuration is the storage backend's own - it never depends on any pipeline's
/// Vault configuration. Connecting Vault here (before storage opens) means a bad Vault
/// config fails closed before the backend is touched.
async fn bootstrap_resolver(cfg: &StorageConfig) -> Result<CompositeResolver> {
    let mode = match cfg.secret_trusted_root.as_deref() {
        Some(root) => FileMode::ProjectedVolume {
            trusted_root: root.to_path_buf(),
        },
        None => FileMode::Strict,
    };
    let base = CompositeResolver::new(
        EnvResolver::from_process(),
        FileResolver::new(FilePolicy {
            max_size: DEFAULT_MAX_SECRET_BYTES,
            mode,
            trim_trailing_newline: true,
        }),
    );
    match &cfg.vault {
        None => Ok(base),
        Some(vcfg) => install_vault(base, vcfg).await,
    }
}

/// Install a process-level Vault KV provider on the storage resolver.
#[cfg(feature = "vault")]
async fn install_vault(
    base: CompositeResolver,
    vcfg: &deltaforge_config::VaultRotationCfg,
) -> Result<CompositeResolver> {
    let conn = sources::vault_connection(vcfg, DEFAULT_MAX_SECRET_BYTES)
        .context("invalid storage Vault configuration")?;
    let vault = secrets::VaultResolver::connect(conn)
        .await
        .map_err(|e| anyhow::anyhow!("connect storage Vault: {e}"))?;
    Ok(base.with_vault(vault))
}

/// Without the `vault` feature, a Vault-configured storage backend fails closed.
#[cfg(not(feature = "vault"))]
async fn install_vault(
    _base: CompositeResolver,
    _vcfg: &deltaforge_config::VaultRotationCfg,
) -> Result<CompositeResolver> {
    bail!(
        "storage credentials are configured to use Vault, but this binary was built \
         without the `vault` feature; rebuild with `--features vault` or use env/file \
         references"
    )
}

async fn resolve_ref(
    resolver: &CompositeResolver,
    reference: &SecretReference,
) -> Result<Zeroizing<String>> {
    let resolved = resolver
        .resolve(reference)
        .await
        .map_err(|e| anyhow::anyhow!("resolve storage secret: {e}"))?;
    let value = resolved
        .material()
        .as_utf8()
        .context("storage secret is not valid UTF-8")?;
    if value.is_empty() {
        bail!("storage secret resolved to an empty value");
    }
    Ok(Zeroizing::new(value.to_string()))
}

/// Whether a base DSN already carries a password (so injecting credential references
/// would silently override it - rejected).
fn base_has_password(base: &str) -> bool {
    if let Some((_, rest)) = base.split_once("://") {
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        authority
            .rsplit_once('@')
            .is_some_and(|(userinfo, _)| userinfo.contains(':'))
    } else {
        base.to_ascii_lowercase().contains("password=")
    }
}

fn inject_credentials(base: &str, user: &str, pass: &str) -> Result<String> {
    let dsn = if base.contains("://") {
        common::dsn::inject_url_credentials(base, user, pass)
    } else {
        common::dsn::replace_libpq_credentials(base, user, pass)
    }
    .map_err(|_| {
        anyhow::anyhow!("storage: could not inject credentials into base dsn")
    })?;
    Ok(dsn)
}

/// Resolve the PostgreSQL storage DSN from config (returns `None` for non-postgres
/// backends). Validates the mutually-exclusive credential forms and rejects partial sets
/// and inline/reference conflicts **before** any resolution or connection. The result is
/// zeroized on drop.
pub async fn resolve_storage_dsn(
    cfg: &StorageConfig,
) -> Result<Option<Zeroizing<String>>> {
    if !matches!(cfg.backend, StorageBackendKind::Postgres) {
        return Ok(None);
    }
    let has_dsn = cfg.dsn.is_some();
    let has_secret = cfg.dsn_secret.is_some();
    let has_creds = cfg.credentials.is_some();

    match (has_dsn, has_secret, has_creds) {
        (_, true, true) => {
            bail!("storage: set either dsn_secret or credentials, not both")
        }
        (true, true, _) => {
            bail!("storage: set either an inline dsn or dsn_secret, not both")
        }
        (false, false, false) => bail!(
            "storage postgres backend requires one of: dsn, dsn_secret, or a \
             password-less base dsn plus credentials"
        ),
        (false, false, true) => {
            bail!("storage: credentials require a password-less base dsn")
        }
        _ => {}
    }

    if let Some(reference) = &cfg.dsn_secret {
        let resolver = bootstrap_resolver(cfg).await?;
        return Ok(Some(resolve_ref(&resolver, reference).await?));
    }

    let base = cfg
        .dsn
        .as_deref()
        .expect("dsn present per validation above");

    if let Some(creds) = &cfg.credentials {
        let (Some(user_ref), Some(pass_ref)) =
            (&creds.username, &creds.password)
        else {
            bail!(
                "storage credentials require both a username and a password \
                 reference (partial credential sets are rejected)"
            );
        };
        if base_has_password(base) {
            bail!(
                "storage: base dsn must not contain a password when credential \
                 references are set (that would silently override them)"
            );
        }
        let resolver = bootstrap_resolver(cfg).await?;
        let user = resolve_ref(&resolver, user_ref).await?;
        let pass = resolve_ref(&resolver, pass_ref).await?;
        return Ok(Some(Zeroizing::new(inject_credentials(
            base, &user, &pass,
        )?)));
    }

    // Inline DSN (deprecated; may embed a password). Used as-is.
    Ok(Some(Zeroizing::new(base.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_config::{CredentialRefsCfg, StorageBackendKind};
    use secrets::SecretProvider;

    fn pg(dsn: Option<&str>) -> StorageConfig {
        StorageConfig {
            backend: StorageBackendKind::Postgres,
            path: String::new(),
            dsn: dsn.map(String::from),
            dsn_secret: None,
            credentials: None,
            secret_trusted_root: None,
            vault: None,
        }
    }

    fn env_ref(var: &str) -> SecretReference {
        SecretReference::new(SecretProvider::Env, var)
    }

    fn file_ref(path: &std::path::Path) -> SecretReference {
        SecretReference::new(SecretProvider::File, path.to_str().unwrap())
    }

    fn write_secret(value: &str) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(value.as_bytes()).unwrap();
        f
    }

    #[tokio::test]
    async fn non_postgres_needs_no_dsn() {
        let mut c = pg(None);
        c.backend = StorageBackendKind::Memory;
        assert!(resolve_storage_dsn(&c).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn missing_all_forms_fails_closed() {
        assert!(resolve_storage_dsn(&pg(None)).await.is_err());
    }

    #[tokio::test]
    async fn inline_and_secret_conflict_rejected() {
        let mut c = pg(Some("host=h dbname=d"));
        c.dsn_secret = Some(env_ref("DF_STORE_DSN"));
        assert!(resolve_storage_dsn(&c).await.is_err());
    }

    #[tokio::test]
    async fn partial_credentials_rejected() {
        let mut c = pg(Some("host=h dbname=d"));
        c.credentials = Some(CredentialRefsCfg {
            username: Some(env_ref("DF_STORE_USER")),
            password: None,
        });
        assert!(resolve_storage_dsn(&c).await.is_err());
    }

    #[tokio::test]
    async fn base_with_password_plus_credentials_rejected() {
        let mut c = pg(Some("host=h dbname=d password=inline"));
        c.credentials = Some(CredentialRefsCfg {
            username: Some(env_ref("DF_STORE_USER")),
            password: Some(env_ref("DF_STORE_PASS")),
        });
        assert!(resolve_storage_dsn(&c).await.is_err());
    }

    #[tokio::test]
    async fn base_plus_credential_refs_inject_from_files() {
        let user_f = write_secret("df_user");
        let pass_f = write_secret("df_pass");
        let mut c = pg(Some("host=h dbname=orders"));
        c.credentials = Some(CredentialRefsCfg {
            username: Some(file_ref(user_f.path())),
            password: Some(file_ref(pass_f.path())),
        });
        let dsn = resolve_storage_dsn(&c).await.unwrap().unwrap();
        assert!(dsn.contains("user=df_user"), "{}", &*dsn);
        assert!(dsn.contains("password=df_pass"), "{}", &*dsn);
    }

    #[tokio::test]
    async fn whole_dsn_secret_from_file() {
        let dsn_f = write_secret("host=h dbname=d user=u password=p");
        let mut c = pg(None);
        c.dsn_secret = Some(file_ref(dsn_f.path()));
        let dsn = resolve_storage_dsn(&c).await.unwrap().unwrap();
        assert!(dsn.contains("password=p"));
    }
}
