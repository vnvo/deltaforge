//! Deployment proof for the secret-coverage baseline.
//!
//! Proves a complete PostgreSQL -> sink -> durable-store deployment whose every
//! credential is a reference (source DSN, sink credentials, schema registry auth, and
//! the storage backend), resolved through the real production code paths, with no inline
//! secrets anywhere in the spec. Also proves the deployment fails closed when a reference
//! cannot be resolved, before any client is constructed.
//!
//! These run without external infrastructure: every reference is a `file` reference to a
//! temp file, resolved by the same resolver the runner assembles at startup.

use std::io::Write;

use anyhow::Result;
use deltaforge_config::{
    CredentialRefsCfg, PipelineSpec, SinkCfg, StorageBackendKind, StorageConfig,
};
use runner::storage_secrets::resolve_storage_dsn;
use secrets::{SecretProvider, SecretReference};
use tempfile::NamedTempFile;

/// Write a secret to a regular temp file (no trailing newline) and return the handle;
/// the file lives until the handle drops.
fn secret_file(value: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("create temp secret file");
    f.write_all(value.as_bytes()).expect("write secret");
    f.flush().expect("flush secret");
    f
}

fn path_of(f: &NamedTempFile) -> &str {
    f.path().to_str().expect("utf-8 temp path")
}

fn file_ref(f: &NamedTempFile) -> SecretReference {
    SecretReference::new(SecretProvider::File, path_of(f))
}

/// Sentinel secret values, distinct so we can assert both resolution and non-leakage.
const PG_USER: &str = "SENTINEL_pg_user";
const PG_PASS: &str = "SENTINEL_pg_pass";
const SASL_PASS: &str = "SENTINEL_sasl_pass";
const SR_PASS: &str = "SENTINEL_sr_pass";
const S3_ACCESS: &str = "SENTINEL_s3_access";
const S3_SECRET: &str = "SENTINEL_s3_secret";
const STORE_USER: &str = "SENTINEL_store_user";
const STORE_PASS: &str = "SENTINEL_store_pass";

/// Build a pipeline spec: PostgreSQL source (password-less base DSN + credential
/// references), a Kafka sink whose SASL password and Avro schema-registry password are
/// references, and an S3 durable sink whose access/secret keys are references. No inline
/// secret appears anywhere.
fn build_spec(
    pg_user: &NamedTempFile,
    pg_pass: &NamedTempFile,
    sasl_pass: &NamedTempFile,
    sr_pass: &NamedTempFile,
    s3_access: &NamedTempFile,
    s3_secret: &NamedTempFile,
) -> PipelineSpec {
    let yaml = format!(
        r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: secret-coverage-proof
  tenant: test
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://localhost:5432/orders
      publication: df_pub
      slot: df_slot
      tables: [public.orders]
      credentials:
        username: {{ provider: file, location: {pg_user} }}
        password: {{ provider: file, location: {pg_pass} }}
  processors: []
  sinks:
    - type: kafka
      config:
        id: kafka
        brokers: localhost:9092
        topic: orders.events
        secret_refs:
          sasl.password: {{ provider: file, location: {sasl_pass} }}
        encoding:
          type: avro
          schema_registry_url: http://schema-registry:8081
          username: sr-user
          password_ref: {{ provider: file, location: {sr_pass} }}
    - type: s3
      config:
        id: s3
        bucket: cdc-archive
        prefix: orders/
        access_key_id_ref: {{ provider: file, location: {s3_access} }}
        secret_access_key_ref: {{ provider: file, location: {s3_secret} }}
"#,
        pg_user = path_of(pg_user),
        pg_pass = path_of(pg_pass),
        sasl_pass = path_of(sasl_pass),
        sr_pass = path_of(sr_pass),
        s3_access = path_of(s3_access),
        s3_secret = path_of(s3_secret),
    );
    serde_yaml::from_str(&yaml).expect("parse pipeline spec")
}

fn storage_config(
    base_dsn: &str,
    user: Option<SecretReference>,
    pass: Option<SecretReference>,
) -> StorageConfig {
    StorageConfig {
        backend: StorageBackendKind::Postgres,
        dsn: Some(base_dsn.to_string()),
        credentials: Some(CredentialRefsCfg {
            username: user,
            password: pass,
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn full_deployment_resolves_every_credential_from_references()
-> Result<()> {
    let pg_user = secret_file(PG_USER);
    let pg_pass = secret_file(PG_PASS);
    let sasl_pass = secret_file(SASL_PASS);
    let sr_pass = secret_file(SR_PASS);
    let s3_access = secret_file(S3_ACCESS);
    let s3_secret = secret_file(S3_SECRET);
    let store_user = secret_file(STORE_USER);
    let store_pass = secret_file(STORE_PASS);

    let spec = build_spec(
        &pg_user, &pg_pass, &sasl_pass, &sr_pass, &s3_access, &s3_secret,
    );

    // No inline secret is present in the spec itself: only references (file paths).
    let serialized = serde_yaml::to_string(&spec)?;
    for sentinel in [PG_USER, PG_PASS, SASL_PASS, SR_PASS, S3_ACCESS, S3_SECRET]
    {
        assert!(
            !serialized.contains(sentinel),
            "secret value leaked into serialized spec: {sentinel}"
        );
    }

    // Assemble the pipeline resolver exactly as the runner does, then resolve the source
    // DSN through the production path.
    let resolver = sources::source_secret_resolver(&spec).await?;
    let dsn = sources::resolve_source_dsn(&spec, resolver.as_ref()).await?;
    assert!(
        dsn.expose().contains(PG_USER) && dsn.expose().contains(PG_PASS),
        "source DSN must carry the referenced credentials"
    );

    // Resolve every sink's credentials through the same resolver.
    let sink_secrets =
        sinks::resolve_sink_secrets(&spec, resolver.as_ref()).await?;

    let kafka = sink_secrets.for_sink("kafka");
    assert_eq!(kafka.get("sasl.password"), Some(SASL_PASS));
    assert_eq!(kafka.schema_registry_password(), Some(SR_PASS));

    let s3 = sink_secrets.for_sink("s3");
    assert_eq!(s3.get("access_key_id"), Some(S3_ACCESS));
    assert_eq!(s3.get("secret_access_key"), Some(S3_SECRET));

    // Resolve the storage backend DSN through its own bootstrap resolver.
    let store = storage_config(
        "postgres://localhost:5432/deltaforge",
        Some(file_ref(&store_user)),
        Some(file_ref(&store_pass)),
    );
    let store_dsn = resolve_storage_dsn(&store)
        .await?
        .expect("postgres storage resolves a dsn");
    assert!(
        store_dsn.contains(STORE_USER) && store_dsn.contains(STORE_PASS),
        "storage DSN must carry the referenced credentials"
    );

    // Confirm the referenced sink variants are what the spec declared.
    assert!(matches!(spec.spec.sinks[0], SinkCfg::Kafka(_)));
    assert!(matches!(spec.spec.sinks[1], SinkCfg::S3(_)));
    Ok(())
}

#[tokio::test]
async fn source_deployment_fails_closed_on_missing_reference() -> Result<()> {
    let pg_user = secret_file(PG_USER);
    let sasl_pass = secret_file(SASL_PASS);
    let sr_pass = secret_file(SR_PASS);
    let s3_access = secret_file(S3_ACCESS);
    let s3_secret = secret_file(S3_SECRET);

    // Password file is created then removed, so its reference cannot resolve.
    let pg_pass = secret_file(PG_PASS);
    let missing_pass_ref =
        SecretReference::new(SecretProvider::File, path_of(&pg_pass));
    drop(pg_pass); // delete the file

    let mut spec = build_spec(
        &pg_user,
        &pg_user, // placeholder; password ref is overridden below
        &sasl_pass, &sr_pass, &s3_access, &s3_secret,
    );
    // Point the source password at the now-missing file.
    if let deltaforge_config::SourceCfg::Postgres(pc) = &mut spec.spec.source {
        pc.credentials.as_mut().unwrap().password = Some(missing_pass_ref);
    } else {
        panic!("expected postgres source");
    }

    let resolver = sources::source_secret_resolver(&spec).await?;
    let err = sources::resolve_source_dsn(&spec, resolver.as_ref()).await;
    assert!(
        err.is_err(),
        "source DSN resolution must fail closed on a missing reference"
    );
    Ok(())
}

#[tokio::test]
async fn storage_deployment_fails_closed_on_partial_credentials() {
    // A username reference without a password reference is a partial set: rejected
    // before the backend is opened.
    let store_user = secret_file(STORE_USER);
    let store = storage_config(
        "postgres://localhost:5432/deltaforge",
        Some(file_ref(&store_user)),
        None,
    );
    let err = resolve_storage_dsn(&store).await;
    assert!(
        err.is_err(),
        "storage must reject a partial credential set before opening the backend"
    );
}

/// Build a spec with an ordinary (non-rotation) PostgreSQL source and a single
/// ClickHouse sink whose password is a `file` reference at `password_path`. When
/// `projected_root` is set, a pipeline-level `secrets.projected_file_root` is included.
fn spec_with_clickhouse_ref(
    password_path: &str,
    projected_root: Option<&str>,
) -> PipelineSpec {
    let secrets_block = match projected_root {
        Some(root) => format!("\n  secrets:\n    projected_file_root: {root}"),
        None => String::new(),
    };
    let yaml = format!(
        r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: sink-provider-independence
  tenant: test
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://localhost:5432/orders
      publication: df_pub
      slot: df_slot
      tables: [public.orders]
  processors: []
  sinks:
    - type: clickhouse
      config:
        id: ch
        url: http://clickhouse:8123
        database: db
        table: t
        mode: upsert
        password_ref: {{ provider: file, location: {password_path} }}{secrets_block}
"#,
    );
    serde_yaml::from_str(&yaml).expect("parse pipeline spec")
}

/// A Vault-backed or projected-volume sink credential must resolve even when the
/// source uses ordinary credentials. This exercises the file-provider case: a sink
/// credential behind a projected-volume symlink resolves only because pipeline-level
/// `secrets.projected_file_root` drives the shared resolver; the strict default (no
/// pipeline secrets) rejects the symlink. Guards blocker 1 for the file provider.
#[tokio::test]
async fn sink_projected_file_resolves_via_pipeline_secrets() -> Result<()> {
    let root = tempfile::tempdir()?;
    let actual = root.path().join("ch_password");
    std::fs::write(&actual, b"ch-secret")?;
    // A Kubernetes projected volume exposes each key as a symlink; model that.
    let link = root.path().join("ch_password_link");
    std::os::unix::fs::symlink(&actual, &link)?;
    let link_path = link.to_str().expect("utf-8 path");
    let root_path = root.path().to_str().expect("utf-8 path");

    // Without pipeline-level secrets, the shared resolver is strict-symlink mode and a
    // non-rotation source contributes no projected root: the sink reference is rejected.
    let strict = spec_with_clickhouse_ref(link_path, None);
    let resolver = sources::source_secret_resolver(&strict).await?;
    assert!(
        sinks::resolve_sink_secrets(&strict, resolver.as_ref())
            .await
            .is_err(),
        "a projected symlink must be rejected under the strict default"
    );

    // With pipeline-level projected_file_root, the same reference resolves.
    let projected = spec_with_clickhouse_ref(link_path, Some(root_path));
    let resolver = sources::source_secret_resolver(&projected).await?;
    let secrets =
        sinks::resolve_sink_secrets(&projected, resolver.as_ref()).await?;
    assert_eq!(secrets.for_sink("ch").get("password"), Some("ch-secret"));
    Ok(())
}
