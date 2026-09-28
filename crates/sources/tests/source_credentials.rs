//! Static source credential resolution: inline DSN, whole-DSN secret, and
//! referenced username/password, for both PostgreSQL and MySQL.

use async_trait::async_trait;
use std::collections::HashMap;
use std::io::Write;

use common::dsn::DsnComponents;
use deltaforge_config::{MysqlSrcCfg, PostgresSrcCfg, SourceCredentialsCfg};
use secrets::{
    DEFAULT_MAX_SECRET_BYTES, FilePolicy, FileResolver, ResolvedSecret,
    SecretBytes, SecretError, SecretMaterial, SecretProvider, SecretReference,
    SecretResolver, SecretString,
};
use sources::{
    CredentialError, resolve_mysql_credentials, resolve_postgres_credentials,
};

// ----------------------------------------------------------------------------
// Test doubles / helpers
// ----------------------------------------------------------------------------

/// A mock resolver used for environment-style references (env injection is
/// `unsafe` on edition 2024 and racy). Keyed by provider+location+selector.
#[derive(Default)]
struct MockResolver {
    utf8: HashMap<String, String>,
    bytes: HashMap<String, Vec<u8>>,
}

impl MockResolver {
    fn with_utf8(mut self, reference: &SecretReference, value: &str) -> Self {
        self.utf8.insert(key(reference), value.to_string());
        self
    }
    fn with_bytes(
        mut self,
        reference: &SecretReference,
        value: Vec<u8>,
    ) -> Self {
        self.bytes.insert(key(reference), value);
        self
    }
}

fn key(r: &SecretReference) -> String {
    format!("{:?}|{}|{:?}", r.provider, r.location, r.selector)
}

#[async_trait]
impl SecretResolver for MockResolver {
    async fn resolve(
        &self,
        reference: &SecretReference,
    ) -> Result<ResolvedSecret, SecretError> {
        let k = key(reference);
        if let Some(v) = self.utf8.get(&k) {
            return Ok(ResolvedSecret::new(SecretMaterial::Utf8(
                SecretString::new(
                    v.clone(),
                    DEFAULT_MAX_SECRET_BYTES,
                    reference,
                )
                .unwrap(),
            )));
        }
        if let Some(b) = self.bytes.get(&k) {
            return Ok(ResolvedSecret::new(SecretMaterial::Bytes(
                SecretBytes::new(
                    b.clone(),
                    DEFAULT_MAX_SECRET_BYTES,
                    reference,
                )
                .unwrap(),
            )));
        }
        Err(SecretError::NotFound(reference.safe()))
    }
}

fn env_ref(name: &str) -> SecretReference {
    SecretReference::new(SecretProvider::Env, name)
}

fn file_ref(path: &std::path::Path) -> SecretReference {
    SecretReference::new(SecretProvider::File, path.to_str().unwrap())
}

fn pg_cfg(
    dsn: Option<&str>,
    dsn_secret: Option<SecretReference>,
    credentials: Option<SourceCredentialsCfg>,
) -> PostgresSrcCfg {
    PostgresSrcCfg {
        id: "pg".to_string(),
        dsn: dsn.map(str::to_string),
        dsn_secret,
        credentials,
        publication: "pub".to_string(),
        slot: "slot".to_string(),
        tables: vec![],
        table_options: Default::default(),
        start_position: Default::default(),
        outbox: None,
        snapshot: Default::default(),
        on_schema_drift: Default::default(),
        rotation: None,
    }
}

fn my_cfg(
    dsn: Option<&str>,
    dsn_secret: Option<SecretReference>,
    credentials: Option<SourceCredentialsCfg>,
) -> MysqlSrcCfg {
    MysqlSrcCfg {
        id: "my".to_string(),
        dsn: dsn.map(str::to_string),
        dsn_secret,
        credentials,
        tables: vec![],
        table_options: Default::default(),
        outbox: None,
        snapshot: Default::default(),
        on_schema_drift: Default::default(),
        rotation: None,
    }
}

fn creds(
    username: SecretReference,
    password: SecretReference,
) -> SourceCredentialsCfg {
    SourceCredentialsCfg {
        username: Some(username),
        password: Some(password),
    }
}

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "df-srccred-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn write(&self, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let p = self.0.join(name);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
        p
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn file_resolver() -> FileResolver {
    FileResolver::new(FilePolicy::default())
}

// ----------------------------------------------------------------------------
// Inline DSN (backward compatibility)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn pg_inline_dsn_is_used_as_is() {
    let cfg = pg_cfg(Some("postgres://u:p@localhost/orders"), None, None);
    let spec = resolve_postgres_credentials(&cfg, &MockResolver::default())
        .await
        .unwrap();
    assert_eq!(
        spec.build_dsn().unwrap().expose(),
        "postgres://u:p@localhost/orders"
    );
}

#[tokio::test]
async fn mysql_inline_dsn_is_used_as_is() {
    let cfg = my_cfg(Some("mysql://u:p@localhost/orders"), None, None);
    let spec = resolve_mysql_credentials(&cfg, &MockResolver::default())
        .await
        .unwrap();
    assert_eq!(
        spec.build_dsn().unwrap().expose(),
        "mysql://u:p@localhost/orders"
    );
}

// ----------------------------------------------------------------------------
// Whole DSN from a secret (env + file)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn pg_whole_dsn_from_env() {
    let r = env_ref("DF_PG_DSN");
    let resolver =
        MockResolver::default().with_utf8(&r, "postgres://u:p@db/orders");
    let cfg = pg_cfg(None, Some(r), None);
    let dsn = resolve_postgres_credentials(&cfg, &resolver)
        .await
        .unwrap()
        .build_dsn()
        .unwrap();
    assert_eq!(dsn.expose(), "postgres://u:p@db/orders");
}

#[tokio::test]
async fn mysql_whole_dsn_from_file() {
    let dir = TempDir::new();
    let p = dir.write("dsn", b"mysql://u:p@db/orders");
    let cfg = my_cfg(None, Some(file_ref(&p)), None);
    let dsn = resolve_mysql_credentials(&cfg, &file_resolver())
        .await
        .unwrap()
        .build_dsn()
        .unwrap();
    assert_eq!(dsn.expose(), "mysql://u:p@db/orders");
}

#[tokio::test]
async fn pg_missing_dsn_secret_fails_closed() {
    let dir = TempDir::new();
    let missing = dir.0.join("nope");
    let cfg = pg_cfg(None, Some(file_ref(&missing)), None);
    let err = resolve_postgres_credentials(&cfg, &file_resolver())
        .await
        .unwrap_err();
    assert!(matches!(err, CredentialError::Resolve(_)));
}

#[tokio::test]
async fn pg_empty_dsn_secret_fails_closed() {
    let dir = TempDir::new();
    let p = dir.write("empty", b"");
    let cfg = pg_cfg(None, Some(file_ref(&p)), None);
    let err = resolve_postgres_credentials(&cfg, &file_resolver())
        .await
        .unwrap_err();
    assert!(matches!(err, CredentialError::Resolve(_)));
}

#[tokio::test]
async fn pg_non_utf8_dsn_secret_fails_closed() {
    // Bytes material for a whole-DSN reference is rejected as not UTF-8.
    let r = env_ref("DF_PG_DSN");
    let resolver = MockResolver::default().with_bytes(&r, vec![0xff, 0xfe]);
    let cfg = pg_cfg(None, Some(r), None);
    let err = resolve_postgres_credentials(&cfg, &resolver)
        .await
        .unwrap_err();
    assert!(matches!(err, CredentialError::NotUtf8("dsn")));
}

// ----------------------------------------------------------------------------
// Username/password from references
// ----------------------------------------------------------------------------

#[tokio::test]
async fn pg_username_password_from_separate_env_refs() {
    let u = env_ref("DF_PG_USER");
    let p = env_ref("DF_PG_PASS");
    let resolver = MockResolver::default()
        .with_utf8(&u, "svc")
        .with_utf8(&p, "secret");
    let cfg = pg_cfg(
        Some("postgres://db.internal:5432/orders"),
        None,
        Some(creds(u, p)),
    );
    let dsn = resolve_postgres_credentials(&cfg, &resolver)
        .await
        .unwrap()
        .build_dsn()
        .unwrap();
    // URL base: credentials injected into the URL (options preserved); parses back.
    let comp = DsnComponents::from_url(dsn.expose(), 5432).unwrap();
    assert_eq!(comp.host, "db.internal");
    assert_eq!(comp.user, "svc");
    assert_eq!(comp.password, "secret");
    assert_eq!(comp.database, "orders");
}

#[tokio::test]
async fn pg_keyvalue_base_preserves_options_and_injects_credentials() {
    let u = env_ref("U");
    let p = env_ref("P");
    let resolver = MockResolver::default()
        .with_utf8(&u, "svc")
        .with_utf8(&p, "p@ss w0rd"); // space exercises libpq quoting
    let base = "host=db.internal port=5432 dbname=orders \
                sslmode=require connect_timeout=10 \
                application_name='df worker'";
    let cfg = pg_cfg(Some(base), None, Some(creds(u, p)));
    let dsn = resolve_postgres_credentials(&cfg, &resolver)
        .await
        .unwrap()
        .build_dsn()
        .unwrap();
    let s = dsn.expose();
    // Every non-credential option preserved.
    assert!(s.contains("sslmode=require"), "{s}");
    assert!(s.contains("connect_timeout=10"), "{s}");
    assert!(s.contains("application_name='df worker'"), "{s}");
    // Credentials injected and parse back exactly (incl. the space).
    let comp = DsnComponents::from_keyvalue(s, 5432, "", "");
    assert_eq!(comp.user, "svc");
    assert_eq!(comp.password, "p@ss w0rd");
    assert_eq!(comp.host, "db.internal");
}

#[tokio::test]
async fn mysql_username_password_from_separate_env_refs() {
    let u = env_ref("DF_MY_USER");
    let p = env_ref("DF_MY_PASS");
    let resolver = MockResolver::default()
        .with_utf8(&u, "svc")
        .with_utf8(&p, "secret");
    let cfg = my_cfg(
        Some("mysql://db.internal:3306/orders"),
        None,
        Some(creds(u, p)),
    );
    let dsn = resolve_mysql_credentials(&cfg, &resolver)
        .await
        .unwrap()
        .build_dsn()
        .unwrap();
    let url = url::Url::parse(dsn.expose()).unwrap();
    assert_eq!(url.username(), "svc");
    assert_eq!(url.password(), Some("secret"));
    assert_eq!(url.host_str(), Some("db.internal"));
}

#[tokio::test]
async fn pg_structured_file_username_password_one_read() {
    let dir = TempDir::new();
    let p = dir.write("db.json", br#"{"username":"svc","password":"secret"}"#);
    let cfg = pg_cfg(
        Some("postgres://db.internal/orders"),
        None,
        Some(creds(
            file_ref(&p).with_selector("username"),
            file_ref(&p).with_selector("password"),
        )),
    );
    let dsn = resolve_postgres_credentials(&cfg, &file_resolver())
        .await
        .unwrap()
        .build_dsn()
        .unwrap();
    // URL base -> URL output; from_url decodes back to the exact values.
    let comp = DsnComponents::from_url(dsn.expose(), 5432).unwrap();
    assert_eq!(comp.user, "svc");
    assert_eq!(comp.password, "secret");
}

#[tokio::test]
async fn pg_password_requiring_percent_encoding_roundtrips() {
    let u = env_ref("U");
    let p = env_ref("P");
    let secret = "p@ss:w0rd/x!";
    let resolver = MockResolver::default()
        .with_utf8(&u, "svc")
        .with_utf8(&p, secret);
    let cfg = pg_cfg(Some("postgres://db/orders"), None, Some(creds(u, p)));
    let dsn = resolve_postgres_credentials(&cfg, &resolver)
        .await
        .unwrap()
        .build_dsn()
        .unwrap();
    // URL base: encoded on the wire, decodes back to the exact secret.
    assert!(!dsn.expose().contains(secret));
    let comp = DsnComponents::from_url(dsn.expose(), 5432).unwrap();
    assert_eq!(comp.password, secret);
}

#[tokio::test]
async fn mysql_password_requiring_percent_encoding_roundtrips() {
    let u = env_ref("U");
    let p = env_ref("P");
    let secret = "p@ss:w0rd/x";
    let resolver = MockResolver::default()
        .with_utf8(&u, "svc")
        .with_utf8(&p, secret);
    let cfg = my_cfg(Some("mysql://db/orders"), None, Some(creds(u, p)));
    let dsn = resolve_mysql_credentials(&cfg, &resolver)
        .await
        .unwrap()
        .build_dsn()
        .unwrap();
    // The serialized DSN is percent-encoded (raw sequence absent) but a
    // URL-decoding client recovers the exact password.
    assert!(!dsn.expose().contains(secret));
    let url = url::Url::parse(dsn.expose()).unwrap();
    let decoded = percent_decode(url.password().unwrap());
    assert_eq!(decoded, secret);
}

/// Minimal percent-decoder for asserting URL round-trips (avoids an extra dep).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
            out.push(u8::from_str_radix(hex, 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

// ----------------------------------------------------------------------------
// Shape validation (fail closed, before provider access)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn conflicting_dsn_and_dsn_secret_fails() {
    let cfg =
        pg_cfg(Some("postgres://u:p@db/orders"), Some(env_ref("X")), None);
    let err = resolve_postgres_credentials(&cfg, &MockResolver::default())
        .await
        .unwrap_err();
    assert!(matches!(err, CredentialError::ConflictingDsnSources));
}

#[tokio::test]
async fn neither_dsn_nor_dsn_secret_fails() {
    let cfg = my_cfg(None, None, None);
    let err = resolve_mysql_credentials(&cfg, &MockResolver::default())
        .await
        .unwrap_err();
    assert!(matches!(err, CredentialError::MissingDsnSource));
}

#[tokio::test]
async fn credentials_with_inline_credentials_conflict() {
    // Base DSN already carries credentials AND a credentials block is given.
    let cfg = pg_cfg(
        Some("postgres://inlineu:inlinep@db/orders"),
        None,
        Some(creds(env_ref("U"), env_ref("P"))),
    );
    let err = resolve_postgres_credentials(&cfg, &MockResolver::default())
        .await
        .unwrap_err();
    assert!(matches!(err, CredentialError::ConflictingInlineCredentials));
}

#[tokio::test]
async fn credentials_with_dsn_secret_conflict() {
    let cfg = my_cfg(
        None,
        Some(env_ref("DSN")),
        Some(creds(env_ref("U"), env_ref("P"))),
    );
    let err = resolve_mysql_credentials(&cfg, &MockResolver::default())
        .await
        .unwrap_err();
    assert!(matches!(err, CredentialError::CredentialsWithDsnSecret));
}

// ----------------------------------------------------------------------------
// Redaction / leak prevention
// ----------------------------------------------------------------------------

#[tokio::test]
async fn resolver_errors_contain_no_values() {
    let dir = TempDir::new();
    // A non-UTF-8 DSN file whose bytes include a sentinel: must not leak.
    let p = dir.write("dsn", b"\xff\xfeS3NT1NEL-dsn");
    let cfg = pg_cfg(None, Some(file_ref(&p)), None);
    let err = resolve_postgres_credentials(&cfg, &file_resolver())
        .await
        .unwrap_err();
    assert!(!format!("{err}").contains("S3NT1NEL-dsn"));
    assert!(!format!("{err:?}").contains("S3NT1NEL-dsn"));
}

#[tokio::test]
async fn protected_dsn_debug_is_redacted() {
    // The runtime source structs hold their DSN in this type; its Debug is what
    // keeps a derived `Source` Debug from leaking credentials.
    let cfg = pg_cfg(Some("postgres://u:supersecret@db/orders"), None, None);
    let dsn = resolve_postgres_credentials(&cfg, &MockResolver::default())
        .await
        .unwrap()
        .build_dsn()
        .unwrap();
    let shown = format!("{dsn:?}");
    assert!(!shown.contains("supersecret"), "leaked: {shown}");
    assert!(shown.contains("REDACTED"));
}

// ----------------------------------------------------------------------------
// Identity / lifecycle guarantees
// ----------------------------------------------------------------------------

#[test]
fn source_id_is_independent_of_dsn_form() {
    use deltaforge_config::SourceCfg;
    let inline = SourceCfg::Postgres(pg_cfg(
        Some("postgres://u:p@db/orders"),
        None,
        None,
    ));
    let referenced =
        SourceCfg::Postgres(pg_cfg(None, Some(env_ref("DSN")), None));
    // Checkpoint identity is the source id; it does not depend on how the DSN is
    // sourced.
    assert_eq!(inline.source_id(), referenced.source_id());
    assert_eq!(inline.source_id(), "pg");
}

#[tokio::test]
async fn schema_loader_debug_does_not_reveal_dsn() {
    use storage::{DurableSchemaRegistry, MemoryStorageBackend};
    let registry = DurableSchemaRegistry::new(std::sync::Arc::new(
        MemoryStorageBackend::new(),
    ))
    .await
    .unwrap();
    let loader = sources::postgres::PostgresSchemaLoader::new(
        "postgres://u:S3NT1NEL-loader@db/orders",
        registry,
        "t",
    );
    let shown = format!("{loader:?}");
    assert!(!shown.contains("S3NT1NEL-loader"), "leaked: {shown}");
    assert!(shown.contains("REDACTED"));
}

#[tokio::test]
async fn resolution_failure_yields_error_before_construction() {
    // A missing secret makes credential resolution fail; the caller (spawn) must
    // therefore abort before constructing a source or touching checkpoints. Here
    // we assert the failure is surfaced as an error rather than a built DSN.
    let cfg = pg_cfg(None, Some(env_ref("ABSENT")), None);
    let result =
        resolve_postgres_credentials(&cfg, &MockResolver::default()).await;
    assert!(result.is_err());
}
