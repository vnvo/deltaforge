//! Static credential resolution for PostgreSQL and MySQL sources.
//!
//! At source startup the connection DSN is produced from one of three additive
//! configuration forms (validated to be mutually exclusive):
//!
//! 1. an inline `dsn` (may already carry credentials),
//! 2. a `dsn_secret` reference resolving to the entire DSN (UTF-8), or
//! 3. a non-secret base `dsn` plus referenced `credentials` (username/password).
//!
//! The resolved DSN is held only in a [`ProtectedDsn`] (redacted `Debug`, no
//! `Serialize`/`Display`, zeroized on last drop). Resolution happens before the
//! source takes long-lived ownership, so a missing/invalid secret fails startup
//! without leaving a pipeline holding resources.
//!
//! Credential injection never concatenates manually: PostgreSQL uses a libpq
//! `key=value` DSN (libpq does not percent-decode, so values are passed literally
//! and quoted), while MySQL uses a URL with `url`-crate percent-encoding (its
//! client percent-decodes). This keeps a password with special characters correct
//! end to end.

use std::sync::Arc;

use common::dsn::DsnComponents;
use deltaforge_config::{MysqlSrcCfg, PostgresSrcCfg, SourceCredentialsCfg};
use secrets::{
    CompositeResolver, CredentialFieldRequest, CredentialSet, EnvResolver,
    FilePolicy, FileResolver, SecretError, SecretReference, SecretResolver,
};
use zeroize::Zeroizing;

/// Protected connection DSN. Holds the (possibly reconstructed) DSN string with a
/// redacted `Debug`, no `Serialize`/`Display`, and best-effort zeroization when
/// the last clone drops. `Clone` shares the underlying buffer via `Arc`.
#[derive(Clone)]
pub struct ProtectedDsn(Arc<Zeroizing<String>>);

impl ProtectedDsn {
    /// The DSN string, for handing to a connection library as late as possible.
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }

    /// Whether two protected DSNs carry the same value, compared internally
    /// without handing plaintext to the caller. Used to suppress a no-op rotation
    /// to identical credentials; not a constant-time comparison and not for
    /// authentication decisions.
    pub fn same_dsn(&self, other: &ProtectedDsn) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl From<String> for ProtectedDsn {
    fn from(dsn: String) -> Self {
        ProtectedDsn(Arc::new(Zeroizing::new(dsn)))
    }
}

impl From<&str> for ProtectedDsn {
    fn from(dsn: &str) -> Self {
        ProtectedDsn::from(dsn.to_string())
    }
}

impl From<&String> for ProtectedDsn {
    fn from(dsn: &String) -> Self {
        ProtectedDsn::from(dsn.clone())
    }
}

impl std::fmt::Debug for ProtectedDsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProtectedDsn(REDACTED)")
    }
}

/// Typed, redacted credential-resolution error. Never carries DSN content, a
/// resolved value, or provider free text (the wrapped [`SecretError`] is itself
/// redacted to a safe reference).
#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("source config sets both `dsn` and `dsn_secret`")]
    ConflictingDsnSources,
    #[error("source config sets neither `dsn` nor `dsn_secret`")]
    MissingDsnSource,
    #[error("`credentials` cannot be combined with `dsn_secret`")]
    CredentialsWithDsnSecret,
    #[error(
        "`credentials` given but the inline `dsn` already contains credentials"
    )]
    ConflictingInlineCredentials,
    #[error("`credentials` is missing required field: {0}")]
    MissingCredentialField(&'static str),
    #[error("resolved {0} is not valid UTF-8")]
    NotUtf8(&'static str),
    #[error("DSN could not be parsed")]
    DsnParse,
    #[error(transparent)]
    Resolve(#[from] SecretError),
}

/// A resolved PostgreSQL credential specification: the connection inputs, before
/// the final DSN string is assembled. Owns any resolved material so its lifetime
/// stays scoped; `Debug` is redacted.
pub enum PostgresCredentials {
    /// A complete DSN (inline value or resolved `dsn_secret`).
    Dsn(ProtectedDsn),
    /// A non-secret base DSN plus a resolved username/password credential set.
    UsernamePassword {
        base_dsn: String,
        credentials: CredentialSet,
    },
    // mTLS variants are intentionally omitted until implemented; no
    // silently-ignored placeholder is added.
}

impl std::fmt::Debug for PostgresCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dsn(_) => f.write_str("PostgresCredentials::Dsn(REDACTED)"),
            Self::UsernamePassword { .. } => {
                f.write_str("PostgresCredentials::UsernamePassword(REDACTED)")
            }
        }
    }
}

impl PostgresCredentials {
    /// Assemble the final protected DSN, replacing **only** username/password in
    /// the base DSN and preserving every other option. URL-form DSNs use URL-aware
    /// userinfo replacement (percent-encoded; `from_url`/clients decode it);
    /// libpq `key=value` DSNs re-emit all pairs with the credentials replaced
    /// (literal values). The base DSN is never reduced to components.
    pub fn build_dsn(&self) -> Result<ProtectedDsn, CredentialError> {
        match self {
            Self::Dsn(dsn) => Ok(dsn.clone()),
            Self::UsernamePassword {
                base_dsn,
                credentials,
            } => {
                let user = require_utf8(credentials, "username")?;
                let pass = require_utf8(credentials, "password")?;
                let dsn = if is_pg_url(base_dsn) {
                    common::dsn::inject_url_credentials(base_dsn, user, pass)
                        .map_err(|_| CredentialError::DsnParse)?
                } else {
                    common::dsn::replace_libpq_credentials(base_dsn, user, pass)
                        .map_err(|_| CredentialError::DsnParse)?
                };
                Ok(ProtectedDsn::from(dsn))
            }
        }
    }
}

/// A resolved MySQL credential specification. See [`PostgresCredentials`].
pub enum MysqlCredentials {
    Dsn(ProtectedDsn),
    UsernamePassword {
        base_dsn: String,
        credentials: CredentialSet,
    },
}

impl std::fmt::Debug for MysqlCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dsn(_) => f.write_str("MysqlCredentials::Dsn(REDACTED)"),
            Self::UsernamePassword { .. } => {
                f.write_str("MysqlCredentials::UsernamePassword(REDACTED)")
            }
        }
    }
}

impl MysqlCredentials {
    /// Assemble the final protected DSN. For the username/password form the base
    /// URL DSN gets the resolved credentials injected via percent-encoding.
    pub fn build_dsn(&self) -> Result<ProtectedDsn, CredentialError> {
        match self {
            Self::Dsn(dsn) => Ok(dsn.clone()),
            Self::UsernamePassword {
                base_dsn,
                credentials,
            } => {
                let user = require_utf8(credentials, "username")?;
                let pass = require_utf8(credentials, "password")?;
                let dsn =
                    common::dsn::inject_url_credentials(base_dsn, user, pass)
                        .map_err(|_| CredentialError::DsnParse)?;
                Ok(ProtectedDsn::from(dsn))
            }
        }
    }
}

/// The default composite resolver used at runtime: environment variables and
/// mounted files (strict-symlink policy). Vault and projected-volume/trusted-root
/// policy wiring are later slices.
pub fn default_secret_resolver() -> CompositeResolver {
    CompositeResolver::new(
        EnvResolver::from_process(),
        FileResolver::new(FilePolicy::default()),
    )
}

/// Resolve the PostgreSQL source's credential specification from config.
pub async fn resolve_postgres_credentials(
    cfg: &PostgresSrcCfg,
    resolver: &dyn SecretResolver,
) -> Result<PostgresCredentials, CredentialError> {
    validate_dsn_shape(
        cfg.dsn.as_deref(),
        cfg.dsn_secret.as_ref(),
        cfg.credentials.as_ref(),
    )?;

    if let Some(reference) = &cfg.dsn_secret {
        let dsn = resolve_whole_dsn(reference, resolver).await?;
        return Ok(PostgresCredentials::Dsn(dsn));
    }

    let base = cfg.dsn.as_deref().expect("validated: dsn present");
    if let Some(creds) = &cfg.credentials {
        let credentials = resolve_username_password(creds, resolver).await?;
        return Ok(PostgresCredentials::UsernamePassword {
            base_dsn: base.to_string(),
            credentials,
        });
    }
    Ok(PostgresCredentials::Dsn(ProtectedDsn::from(base)))
}

/// Resolve the MySQL source's credential specification from config.
pub async fn resolve_mysql_credentials(
    cfg: &MysqlSrcCfg,
    resolver: &dyn SecretResolver,
) -> Result<MysqlCredentials, CredentialError> {
    validate_dsn_shape(
        cfg.dsn.as_deref(),
        cfg.dsn_secret.as_ref(),
        cfg.credentials.as_ref(),
    )?;

    if let Some(reference) = &cfg.dsn_secret {
        let dsn = resolve_whole_dsn(reference, resolver).await?;
        return Ok(MysqlCredentials::Dsn(dsn));
    }

    let base = cfg.dsn.as_deref().expect("validated: dsn present");
    if let Some(creds) = &cfg.credentials {
        let credentials = resolve_username_password(creds, resolver).await?;
        return Ok(MysqlCredentials::UsernamePassword {
            base_dsn: base.to_string(),
            credentials,
        });
    }
    Ok(MysqlCredentials::Dsn(ProtectedDsn::from(base)))
}

// --- shared helpers ---

/// Validate the three additive DSN forms are used correctly.
fn validate_dsn_shape(
    dsn: Option<&str>,
    dsn_secret: Option<&SecretReference>,
    credentials: Option<&SourceCredentialsCfg>,
) -> Result<(), CredentialError> {
    match (dsn.is_some(), dsn_secret.is_some()) {
        (true, true) => return Err(CredentialError::ConflictingDsnSources),
        (false, false) => return Err(CredentialError::MissingDsnSource),
        _ => {}
    }
    if let Some(creds) = credentials {
        // Referenced credentials augment a non-secret base DSN only.
        if dsn_secret.is_some() {
            return Err(CredentialError::CredentialsWithDsnSecret);
        }
        // Both fields are required, and the base DSN must not already carry creds.
        if creds.username.is_none() {
            return Err(CredentialError::MissingCredentialField("username"));
        }
        if creds.password.is_none() {
            return Err(CredentialError::MissingCredentialField("password"));
        }
        if let Some(base) = dsn {
            if base_dsn_has_credentials(base) {
                return Err(CredentialError::ConflictingInlineCredentials);
            }
        }
    }
    Ok(())
}

/// Resolve a whole-DSN secret reference to a protected DSN, requiring UTF-8.
async fn resolve_whole_dsn(
    reference: &SecretReference,
    resolver: &dyn SecretResolver,
) -> Result<ProtectedDsn, CredentialError> {
    let resolved = resolver.resolve(reference).await?;
    let dsn = resolved
        .material()
        .as_utf8()
        .ok_or(CredentialError::NotUtf8("dsn"))?;
    Ok(ProtectedDsn::from(dsn))
}

/// Resolve username+password as one credential set (a single structured read when
/// both come from the same record).
async fn resolve_username_password(
    creds: &SourceCredentialsCfg,
    resolver: &dyn SecretResolver,
) -> Result<CredentialSet, CredentialError> {
    let username = creds
        .username
        .clone()
        .ok_or(CredentialError::MissingCredentialField("username"))?;
    let password = creds
        .password
        .clone()
        .ok_or(CredentialError::MissingCredentialField("password"))?;
    let requests = vec![
        CredentialFieldRequest::new("username", username),
        CredentialFieldRequest::new("password", password),
    ];
    Ok(resolver.resolve_set(&requests).await?)
}

fn require_utf8<'a>(
    set: &'a CredentialSet,
    field: &'static str,
) -> Result<&'a str, CredentialError> {
    set.require(field)?
        .material()
        .as_utf8()
        .ok_or(CredentialError::NotUtf8(field))
}

/// Whether a base DSN is a PostgreSQL URL (vs a libpq `key=value` string).
fn is_pg_url(base: &str) -> bool {
    base.starts_with("postgres://") || base.starts_with("postgresql://")
}

/// Whether a base DSN already carries a username or password.
fn base_dsn_has_credentials(base: &str) -> bool {
    if base.contains("://") {
        DsnComponents::from_url(base, 0)
            .map(|c| c.has_credentials())
            .unwrap_or(false)
    } else {
        DsnComponents::from_keyvalue(base, 0, "", "").has_credentials()
    }
}
