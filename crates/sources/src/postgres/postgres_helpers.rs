//! PostgreSQL source helper utilities.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use deltaforge_core::{CheckpointMeta, SourceError, SourceResult};
use pgwire_replication::{
    Lsn, ReplicationClient, ReplicationConfig, TlsConfig,
};
use tokio::select;
use tokio::sync::Notify;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use checkpoints::{CheckpointStore, CheckpointStoreExt};

use crate::conn_utils::{RetryPolicy, retry_async, retryable_connect};

use super::{PostgresCheckpoint, PostgresSourceError, PostgresSourceResult};

// ----------------------------- Configuration Constants -----------------------------

pub const STATUS_INTERVAL_SECS: u64 = 1;
pub const IDLE_WAKEUP_INTERVAL_SECS: u64 = 30;
pub const BUFFER_EVENTS: usize = 8192;

// ----------------------------- Public Helpers -----------------------------

/// Parse DSN and extract connection components.
pub(super) fn parse_dsn(dsn: &str) -> PostgresSourceResult<DsnComponents> {
    // Handle both URL-style and key=value style DSNs
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        parse_url_dsn(dsn)
    } else {
        parse_keyvalue_dsn(dsn)
    }
}

#[derive(Debug, Clone)]
pub struct DsnComponents {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

fn parse_url_dsn(dsn: &str) -> PostgresSourceResult<DsnComponents> {
    let url = Url::parse(dsn)
        .map_err(|e| PostgresSourceError::InvalidDsn(e.to_string()))?;

    let host = url.host_str().unwrap_or("localhost").to_string();
    let port = url.port().unwrap_or(5432);
    let user = url.username().to_string();
    let password = url.password().unwrap_or("").to_string();
    let database = url.path().trim_start_matches('/').to_string();

    Ok(DsnComponents {
        host,
        port,
        user,
        password,
        database,
    })
}

fn parse_keyvalue_dsn(dsn: &str) -> PostgresSourceResult<DsnComponents> {
    let mut host = "localhost".to_string();
    let mut port = 5432u16;
    let mut user = "postgres".to_string();
    let mut password = String::new();
    let mut database = "postgres".to_string();

    for part in dsn.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            match key.to_lowercase().as_str() {
                "host" => host = value.to_string(),
                "port" => port = value.parse().unwrap_or(5432),
                "user" => user = value.to_string(),
                "password" => password = value.to_string(),
                "dbname" | "database" => database = value.to_string(),
                _ => {}
            }
        }
    }

    Ok(DsnComponents {
        host,
        port,
        user,
        password,
        database,
    })
}

/// Prepare ReplicationClient from DSN + last checkpoint.
pub(super) async fn prepare_replication_client(
    dsn: &str,
    source_id: &str,
    slot: &str,
    publication: &str,
    ckpt_store: &Arc<dyn CheckpointStore>,
) -> PostgresSourceResult<(
    DsnComponents,
    ReplicationConfig,
    Option<PostgresCheckpoint>,
)> {
    let components = parse_dsn(dsn)?;

    // Previous checkpoint (if any)
    let last_checkpoint: Option<PostgresCheckpoint> = ckpt_store
        .get(source_id)
        .await
        .map_err(|e| PostgresSourceError::Checkpoint(e.to_string()))?;

    info!(
        source_id = %source_id,
        host = %components.host,
        checkpoint = ?last_checkpoint,
        "preparing replication client"
    );

    // Determine start LSN
    let start_lsn = if let Some(ref cp) = last_checkpoint {
        Lsn::parse(&cp.lsn).map_err(|e| {
            PostgresSourceError::LsnParse(format!(
                "invalid checkpoint LSN '{}': {}",
                cp.lsn, e
            ))
        })?
    } else {
        // No checkpoint, will get start position from slot
        Lsn::parse("0/0").unwrap()
    };

    let config = ReplicationConfig {
        host: components.host.clone(),
        port: components.port,
        user: components.user.clone(),
        password: components.password.clone(),
        database: components.database.clone(),
        tls: TlsConfig::disabled(), // TODO: make configurable
        slot: slot.to_string(),
        publication: publication.to_string(),
        start_lsn,
        stop_at_lsn: None,
        status_interval: Duration::from_secs(STATUS_INTERVAL_SECS),
        idle_wakeup_interval: Duration::from_secs(IDLE_WAKEUP_INTERVAL_SECS),
        buffer_events: BUFFER_EVENTS,
    };

    Ok((components, config, last_checkpoint))
}

/// Connect to replication with retries.
pub(super) async fn connect_replication_with_retries(
    source_id: &str,
    config: ReplicationConfig,
    cancel: &CancellationToken,
    retry_policy: RetryPolicy,
) -> SourceResult<ReplicationClient> {
    let cfg = config.clone();

    let client = retry_async(
        move |_| {
            let cfg = cfg.clone();
            async move { connect_replication(source_id, cfg, cancel).await }
        },
        |e| e,
        retryable_connect,
        Duration::from_secs(30),
        retry_policy,
        cancel,
        "replication_connect",
    )
    .await?;

    Ok(client)
}

/// Connect to PostgreSQL replication with cancellation and timeout.
pub(super) async fn connect_replication(
    source_id: &str,
    config: ReplicationConfig,
    cancel: &CancellationToken,
) -> SourceResult<ReplicationClient> {
    debug!("connecting to postgres replication");
    let t0 = Instant::now();

    let client = select! {
        _ = cancel.cancelled() => Err(SourceError::Cancelled),
        res = tokio::time::timeout(Duration::from_secs(30), ReplicationClient::connect(config)) => {
            match res {
                Ok(Ok(client)) => Ok(client),
                Ok(Err(e)) => {
                    let error_msg = e.to_string();
                    error!(source_id = %source_id, error = %error_msg, "replication connect failed");

                    if error_msg.contains("password") || error_msg.contains("authentication") {
                        error!("PostgreSQL auth issue hints:");
                        error!("  1) Ensure pg_hba.conf allows replication connections");
                        error!("  2) Verify user has REPLICATION role: ALTER ROLE user REPLICATION;");
                    } else if error_msg.contains("slot") {
                        error!("Replication slot issue hints:");
                        error!("  1) Create slot: SELECT pg_create_logical_replication_slot('slot','pgoutput');");
                    } else if error_msg.contains("publication") {
                        error!("Publication issue hints:");
                        error!("  1) Create publication: CREATE PUBLICATION pub FOR ALL TABLES;");
                    }

                    Err(SourceError::Other(e.into()))
                }
                Err(_) => Err(SourceError::Timeout { action: "replication_connect".into() }),
            }
        }
    }?;

    info!(
        source_id = %source_id,
        ms = t0.elapsed().as_millis() as u64,
        "connected to postgres replication"
    );

    Ok(client)
}

/// Ensure publication and slot exist, get start LSN.
pub(super) async fn ensure_slot_and_publication(
    dsn: &str,
    slot: &str,
    publication: &str,
    tables: &[String],
) -> SourceResult<Lsn> {
    let (client, conn) =
        tokio_postgres::connect(dsn, NoTls).await.map_err(|e| {
            SourceError::Connect {
                details: format!("control plane connect: {}", e).into(),
            }
        })?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!("control plane connection error: {}", e);
        }
    });

    // Check if publication exists
    let pub_exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .map_err(|e| SourceError::Other(e.into()))?
        .is_some();

    if !pub_exists {
        // Create publication for specified tables
        let table_list = if tables.is_empty() {
            "ALL TABLES".to_string()
        } else {
            format!("TABLE {}", tables.join(", "))
        };

        client
            .batch_execute(&format!(
                "CREATE PUBLICATION {} FOR {}",
                publication, table_list
            ))
            .await
            .map_err(|e| SourceError::Other(e.into()))?;

        info!(publication = %publication, tables = ?tables, "created publication");
    }

    // Check if slot exists
    let slot_row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text, restart_lsn::text 
             FROM pg_replication_slots 
             WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| SourceError::Other(e.into()))?;

    let start_lsn = if let Some(row) = slot_row {
        // Slot exists, get confirmed LSN
        let confirmed: Option<String> = row.get(0);
        let restart: Option<String> = row.get(1);

        if let Some(s) = confirmed {
            Lsn::parse(&s).map_err(|e| SourceError::Other(e.into()))?
        } else if let Some(s) = restart {
            Lsn::parse(&s).map_err(|e| SourceError::Other(e.into()))?
        } else {
            get_current_wal_lsn(&client).await?
        }
    } else {
        // Create slot
        client
            .batch_execute(&format!(
                "SELECT * FROM pg_create_logical_replication_slot('{}', 'pgoutput')",
                slot
            ))
            .await
            .map_err(|e| SourceError::Other(e.into()))?;

        info!(slot = %slot, "created replication slot");
        get_current_wal_lsn(&client).await?
    };

    Ok(start_lsn)
}

/// Get current WAL LSN.
async fn get_current_wal_lsn(
    client: &tokio_postgres::Client,
) -> SourceResult<Lsn> {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .map_err(|e| SourceError::Other(e.into()))?;

    let lsn_str: String = row.get(0);
    Lsn::parse(&lsn_str).map_err(|e| SourceError::Other(e.into()))
}

/// Pause gate: wait until resume or cancel. Returns false if cancelled, true otherwise.
pub(super) async fn pause_until_resumed(
    cancel: &CancellationToken,
    paused: &AtomicBool,
    notify: &Notify,
) -> bool {
    if !paused.load(Ordering::SeqCst) {
        return true;
    }
    debug!("paused");
    select! {
        _ = cancel.cancelled() => return false,
        _ = notify.notified() => {}
    }
    true
}

// ----------------------------- Utility Functions -----------------------------

/// PostgreSQL epoch (2000-01-01) to Unix epoch offset in microseconds.
pub const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Convert PostgreSQL timestamp to Unix timestamp in milliseconds.
pub(crate) fn pg_timestamp_to_unix_ms(pg_micros: i64) -> i64 {
    (pg_micros + PG_EPOCH_OFFSET_MICROS) / 1000
}

/// Redact password from DSN for logging.
pub(crate) fn redact_password(dsn: &str) -> String {
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        if let Ok(mut url) = Url::parse(dsn) {
            if url.password().is_some() {
                let _ = url.set_password(Some("***"));
            }
            return url.to_string();
        }
    }
    // For key=value style, simple regex-like replacement
    let parts: Vec<&str> = dsn.split_whitespace().collect();
    parts
        .iter()
        .map(|p| {
            if p.to_lowercase().starts_with("password=") {
                "password=***"
            } else {
                *p
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Build checkpoint metadata from LSN.
pub(crate) fn make_checkpoint_meta(
    lsn: &Lsn,
    tx_id: Option<u32>,
) -> CheckpointMeta {
    let cp = PostgresCheckpoint {
        lsn: lsn.to_string(),
        tx_id,
    };

    let bytes = serde_json::to_vec(&cp).unwrap_or_else(|e| {
        tracing::error!(error=%e, "failed to serialize checkpoint");
        Vec::new()
    });
    CheckpointMeta::from_vec(bytes)
}

/// Simple allow-list on "schema.table" patterns.
#[derive(Clone)]
pub(in crate::postgres) struct AllowList {
    items: Vec<(Option<String>, String)>,
}

impl AllowList {
    pub(super) fn new(list: &[String]) -> Self {
        let items = list
            .iter()
            .map(|s| {
                if let Some((schema, table)) = s.split_once('.') {
                    (Some(schema.to_string()), table.to_string())
                } else {
                    // No schema specified, match any schema
                    (None, s.to_string())
                }
            })
            .collect();
        Self { items }
    }

    pub(super) fn matches(&self, schema: &str, table: &str) -> bool {
        if self.items.is_empty() {
            return true;
        }
        self.items.iter().any(|(s_opt, t)| {
            let schema_match =
                s_opt.as_ref().map(|s| s == schema).unwrap_or(true);
            let table_match = t == table || t == "*" || t == "%";
            schema_match && table_match
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_url_dsn() {
        let dsn = "postgres://user:pass@localhost:5433/mydb";
        let comp = parse_dsn(dsn).unwrap();
        assert_eq!(comp.host, "localhost");
        assert_eq!(comp.port, 5433);
        assert_eq!(comp.user, "user");
        assert_eq!(comp.password, "pass");
        assert_eq!(comp.database, "mydb");
    }

    #[test]
    fn test_parse_keyvalue_dsn() {
        let dsn = "host=127.0.0.1 port=5432 user=postgres password=secret dbname=test";
        let comp = parse_dsn(dsn).unwrap();
        assert_eq!(comp.host, "127.0.0.1");
        assert_eq!(comp.port, 5432);
        assert_eq!(comp.user, "postgres");
        assert_eq!(comp.password, "secret");
        assert_eq!(comp.database, "test");
    }

    #[test]
    fn test_redact_password() {
        let dsn1 = "postgres://user:secret@localhost/db";
        assert!(redact_password(dsn1).contains("***"));
        assert!(!redact_password(dsn1).contains("secret"));

        let dsn2 = "host=localhost password=secret user=test";
        assert!(redact_password(dsn2).contains("***"));
        assert!(!redact_password(dsn2).contains("secret"));
    }

    #[test]
    fn test_allow_list() {
        let list =
            AllowList::new(&["public.users".to_string(), "orders".to_string()]);

        assert!(list.matches("public", "users"));
        assert!(list.matches("any_schema", "orders"));
        assert!(!list.matches("public", "other"));
    }

    #[test]
    fn test_allow_list_empty() {
        let list = AllowList::new(&[]);
        assert!(list.matches("any", "table"));
    }

    #[test]
    fn test_pg_timestamp_conversion() {
        // 2020-01-01 00:00:00 UTC in PostgreSQL epoch microseconds
        let pg_ts = 631_152_000_000_000i64;
        let unix_ms = pg_timestamp_to_unix_ms(pg_ts);
        // Should be 2020-01-01 00:00:00 UTC in Unix ms
        assert_eq!(unix_ms, 1_577_836_800_000);
    }
}
