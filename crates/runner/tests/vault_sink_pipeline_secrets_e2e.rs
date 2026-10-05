//! Live regression for blocker 1: a pipeline whose source uses ordinary (non-rotation)
//! credentials but whose sink is Vault-backed resolves the sink credential through
//! pipeline-level Vault configuration. Before the fix the shared resolver had no Vault
//! provider (it was derived only from source rotation), so the sink failed
//! UnsupportedProvider.
//!
//! Requires docker and the `vault` feature; ignored by default. Only Vault is needed
//! (no database): the test drives `source_secret_resolver` + `resolve_sink_secrets`.
#![cfg(feature = "vault")]

use anyhow::Result;
use deltaforge_config::PipelineSpec;
use gate_ownership::GateOwned;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};

const ROOT_TOKEN: &str = "root";

async fn start_vault() -> (ContainerAsync<GenericImage>, String) {
    let image = GenericImage::new("hashicorp/vault", "1.15")
        .with_wait_for(WaitFor::message_on_stdout("Vault server started!"))
        .with_env_var("VAULT_DEV_ROOT_TOKEN_ID", ROOT_TOKEN)
        .with_env_var("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200");
    let container = image
        .gate_owned()
        .start()
        .await
        .expect("start vault dev container");
    let port = container
        .get_host_port_ipv4(8200)
        .await
        .expect("vault host port");
    (container, format!("http://127.0.0.1:{port}"))
}

/// Write a KV v2 record at `secret/<path>`.
async fn put_kv(base: &str, path: &str, key: &str, value: &str) {
    reqwest::Client::new()
        .post(format!("{base}/v1/secret/data/{path}"))
        .header("X-Vault-Token", ROOT_TOKEN)
        .json(&serde_json::json!({ "data": { key: value } }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

fn spec(vault_addr: &str, token_path: &str) -> PipelineSpec {
    let v = serde_json::json!({
        "metadata": { "name": "vault-sink", "tenant": "acme" },
        "spec": {
            "source": { "type": "postgres", "config": {
                "id": "pg",
                "dsn": "host=127.0.0.1 port=5432 dbname=db",
                "publication": "p",
                "slot": "s",
                "tables": ["public.t"]
            }},
            "processors": [],
            "sinks": [{ "type": "clickhouse", "config": {
                "id": "ch",
                "url": "http://clickhouse:8123",
                "database": "db",
                "table": "t",
                "mode": "upsert",
                "password_ref": {
                    "provider": "vault",
                    "location": "secret/ch",
                    "selector": "password"
                }
            }}],
            "secrets": {
                "vault": {
                    "address": vault_addr,
                    "allow_insecure_http": true,
                    "auth": { "method": "token_file", "path": token_path }
                }
            }
        }
    });
    serde_json::from_value(v).expect("valid pipeline spec")
}

#[tokio::test]
#[ignore = "requires docker"]
async fn non_vault_source_vault_sink_resolves_via_pipeline_secrets()
-> Result<()> {
    let (_vault, addr) = start_vault().await;
    put_kv(&addr, "ch", "password", "ch-secret").await;

    let token_file = tempfile::NamedTempFile::new()?;
    std::fs::write(token_file.path(), ROOT_TOKEN)?;
    let spec = spec(&addr, token_file.path().to_str().unwrap());

    // The source uses ordinary credentials (no rotation); the shared resolver gets Vault
    // ONLY from pipeline-level config. The Vault-backed sink credential must resolve.
    let resolver = sources::source_secret_resolver(&spec).await?;
    let secrets = sinks::resolve_sink_secrets(&spec, resolver.as_ref()).await?;
    assert_eq!(secrets.for_sink("ch").get("password"), Some("ch-secret"));
    Ok(())
}
