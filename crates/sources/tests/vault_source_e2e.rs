//! Live Vault + PostgreSQL source integration (feature `vault`).
//!
//! Proves the complete production chain end to end, which the separate
//! `VaultResolver` and version-decision unit tests do not:
//!
//! Vault KV update -> grouped `resolve_set` -> version detection -> `Candidate`
//! -> `RotationManager` -> PostgreSQL boundary reconnect -> continued CDC.
//!
//! A disposable dev-mode Vault holds the source's credentials in a KV v2 record;
//! the PostgreSQL source is configured with a Vault-triggered rotation through the
//! genuine startup path (`source_secret_resolver` -> `resolve_source_dsn` ->
//! `build_source`). Rotating the KV record (and the database role) must reconnect
//! the walsender (its pid changes) and CDC must continue afterwards.
//!
//! Run with:
//! `cargo test -p sources --features vault --test vault_source_e2e -- --ignored --nocapture --test-threads=1`
#![cfg(feature = "vault")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::PipelineSpec;
use deltaforge_core::SourceItem;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::{
    sync::mpsc,
    time::{Duration, sleep, timeout},
};

use sources::{build_source, resolve_source_dsn, source_secret_resolver};

mod test_common;
use test_common::{
    PG_CDC_PASS, PG_CDC_USER, make_registry, make_storage_backend, pg_drop_db,
    pg_port, pg_setup,
};

static UNIQ: AtomicU64 = AtomicU64::new(0);
const ROOT_TOKEN: &str = "root";
const LIMIT: usize = 1 << 20;

// --- Vault dev container + admin ---

async fn start_vault() -> (ContainerAsync<GenericImage>, String) {
    let image = GenericImage::new("hashicorp/vault", "1.15")
        .with_wait_for(WaitFor::message_on_stdout("Vault server started!"))
        .with_env_var("VAULT_DEV_ROOT_TOKEN_ID", ROOT_TOKEN)
        .with_env_var("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200");
    let container = image.start().await.expect("start vault dev container");
    let port = container
        .get_host_port_ipv4(8200)
        .await
        .expect("vault host port");
    (container, format!("http://127.0.0.1:{port}"))
}

/// Write a KV v2 record at `secret/<path>` (creates a new version each call).
async fn put_kv(base: &str, path: &str, username: &str, password: &str) {
    reqwest::Client::new()
        .post(format!("{base}/v1/secret/data/{path}"))
        .header("X-Vault-Token", ROOT_TOKEN)
        .json(&serde_json::json!({
            "data": { "username": username, "password": password }
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

// --- slot / event helpers (mirror the file-rotation suite) ---

async fn slot_active_pid(
    admin: &tokio_postgres::Client,
    slot: &str,
) -> Result<Option<i32>> {
    let row = admin
        .query_opt(
            // Only a walsender counts: while `pg_create_logical_replication_slot`
            // runs during snapshot anchoring, the slot is held by that ordinary
            // backend, whose pid is not the stream's.
            "SELECT s.active_pid FROM pg_replication_slots s \
             JOIN pg_stat_activity a ON a.pid = s.active_pid \
             WHERE s.slot_name = $1 AND a.backend_type = 'walsender'",
            &[&slot],
        )
        .await?;
    Ok(row.and_then(|r| r.get::<_, Option<i32>>(0)))
}

async fn wait_for_active_pid(
    admin: &tokio_postgres::Client,
    slot: &str,
    dur: Duration,
) -> Result<i32> {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if let Some(pid) = slot_active_pid(admin, slot).await? {
            return Ok(pid);
        }
        sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("slot never became active within {dur:?}")
}

async fn wait_for_pid_change(
    admin: &tokio_postgres::Client,
    slot: &str,
    baseline: i32,
    dur: Duration,
) -> Result<i32> {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if let Some(pid) = slot_active_pid(admin, slot).await? {
            if pid != baseline {
                return Ok(pid);
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("slot walsender pid did not change within {dur:?}")
}

async fn wait_for_event(
    rx: &mut mpsc::Receiver<SourceItem>,
    dur: Duration,
) -> bool {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        match timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(SourceItem::Event(_))) => return true,
            Ok(Some(_)) => continue,
            Ok(None) => return false,
            Err(_) => continue,
        }
    }
    false
}

/// Build a pipeline spec with a Vault-triggered PostgreSQL rotation, through the
/// real config path (serde), so the whole production chain is exercised.
fn vault_pipeline_spec(
    db: &str,
    port: u16,
    vault_addr: &str,
    token_path: &str,
    publication: &str,
    slot: &str,
) -> PipelineSpec {
    let v = serde_json::json!({
        "metadata": { "name": "vault-rot", "tenant": "acme" },
        "spec": {
            "source": {
                "type": "postgres",
                "config": {
                    "id": "pg-vault-rot",
                    "dsn": format!("host=127.0.0.1 port={port} dbname={db}"),
                    "credentials": {
                        "username": { "provider": "vault", "location": "secret/db", "selector": "username" },
                        "password": { "provider": "vault", "location": "secret/db", "selector": "password" }
                    },
                    "publication": publication,
                    "slot": slot,
                    "tables": ["public.orders"],
                    "snapshot": { "mode": "initial" },
                    "rotation": {
                        "trigger": {
                            "type": "vault",
                            "address": vault_addr,
                            "allow_insecure_http": true,
                            "auth": { "method": "token_file", "path": token_path }
                        },
                        "poll_interval_ms": 500,
                        "max_secret_bytes": LIMIT,
                        "apply_timeout_ms": 30000
                    }
                }
            },
            "processors": [],
            "sinks": []
        }
    });
    serde_json::from_value(v).expect("valid pipeline spec")
}

#[tokio::test]
#[ignore = "requires docker"]
async fn vault_kv_rotation_reconnects_postgres_and_continues_cdc() -> Result<()>
{
    let suffix = format!("vault_{}", UNIQ.fetch_add(1, Ordering::Relaxed));
    let (db, admin) = pg_setup(&suffix).await?;
    let port = pg_port().await;

    // Baseline role password (the cluster-wide CDC role may have been rotated by a
    // prior test).
    admin
        .execute(
            &format!("ALTER ROLE {PG_CDC_USER} PASSWORD '{PG_CDC_PASS}'"),
            &[],
        )
        .await?;
    admin
        .execute(
            "CREATE TABLE orders (id SERIAL PRIMARY KEY, sku TEXT NOT NULL)",
            &[],
        )
        .await?;
    admin
        .execute(
            &format!("GRANT SELECT ON public.orders TO {PG_CDC_USER}"),
            &[],
        )
        .await?;
    let publication =
        format!("vrot_pub_{}", UNIQ.fetch_add(1, Ordering::Relaxed));
    let slot = format!("vrot_slot_{}", UNIQ.fetch_add(1, Ordering::Relaxed));
    admin
        .execute(
            &format!(
                "CREATE PUBLICATION {publication} FOR TABLE public.orders"
            ),
            &[],
        )
        .await?;

    // Disposable Vault holding the initial credentials.
    let (_vault, vault_addr) = start_vault().await;
    put_kv(&vault_addr, "db", PG_CDC_USER, PG_CDC_PASS).await;

    // Root token in a file for token-file auth.
    let token_file = tempfile::NamedTempFile::new()?;
    std::fs::write(token_file.path(), ROOT_TOKEN)?;
    let token_path = token_file.path().to_str().unwrap().to_string();

    let spec = vault_pipeline_spec(
        &db,
        port,
        &vault_addr,
        &token_path,
        &publication,
        &slot,
    );

    // The genuine production chain: connect+install Vault, resolve the initial DSN
    // from the KV record, and build the source with a Vault-triggered rotation.
    let resolver = source_secret_resolver(&spec).await?;
    let dsn = resolve_source_dsn(&spec, resolver.as_ref()).await?;
    let source = build_source(
        &spec,
        dsn,
        make_registry().await,
        sources::registry_scope::SharedRegistryScope::default(),
        make_storage_backend().await,
        resolver,
    )
    .await?;

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel(256);
    let handle = source.run(tx, ckpt).await;

    let pid_before =
        wait_for_active_pid(&admin, &slot, Duration::from_secs(30)).await?;
    admin
        .execute("INSERT INTO orders (sku) VALUES ('before')", &[])
        .await?;
    assert!(
        wait_for_event(&mut rx, Duration::from_secs(15)).await,
        "should stream a CDC event before rotation"
    );

    // Rotate: change the database role password and publish a new KV version.
    let new_pw = "vault_rotated_pw";
    admin
        .execute(
            &format!("ALTER ROLE {PG_CDC_USER} PASSWORD '{new_pw}'"),
            &[],
        )
        .await?;
    put_kv(&vault_addr, "db", PG_CDC_USER, new_pw).await;

    // The Vault poll observes the new record version -> candidate -> boundary
    // reconnect. A fresh walsender (new pid) proves a genuine reconnect with the
    // rotated credential.
    let pid_after =
        wait_for_pid_change(&admin, &slot, pid_before, Duration::from_secs(40))
            .await?;
    assert_ne!(pid_before, pid_after, "walsender must reconnect");

    // CDC continues after the reconnect.
    admin
        .execute("INSERT INTO orders (sku) VALUES ('after')", &[])
        .await?;
    assert!(
        wait_for_event(&mut rx, Duration::from_secs(15)).await,
        "CDC must continue after the Vault-triggered reconnect"
    );

    handle.stop();
    handle.join().await.ok();
    pg_drop_db(&db).await;
    Ok(())
}
