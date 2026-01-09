//! PostgreSQL source helper utilities.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use deltaforge_core::{CheckpointMeta, SourceError, SourceResult};
use pgwire_replication::{
    Lsn, PgWireError, ReplicationClient, ReplicationConfig, TlsConfig,
};
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use checkpoints::{CheckpointStore, CheckpointStoreExt};
use common::{DsnComponents, RetryOutcome, RetryPolicy, retry_async};

use super::{PostgresCheckpoint, PostgresSourceError, PostgresSourceResult};

// ----------------------------- Configuration Constants -----------------------------

pub const STATUS_INTERVAL_SECS: u64 = 1;
pub const IDLE_WAKEUP_INTERVAL_SECS: u64 = 30;
pub const BUFFER_EVENTS: usize = 8192;

// ----------------------------- Public Helpers -----------------------------

/// Parse DSN and extract connection components.
pub(super) fn parse_dsn(dsn: &str) -> PostgresSourceResult<DsnComponents> {
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        DsnComponents::from_url(dsn, 5432)
            .map_err(PostgresSourceError::InvalidDsn)
    } else {
        Ok(DsnComponents::from_keyvalue(
            dsn, 5432, "postgres", "postgres",
        ))
    }
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

    let start_lsn = if let Some(ref cp) = last_checkpoint {
        Lsn::parse(&cp.lsn).map_err(|e| {
            PostgresSourceError::LsnParse(format!(
                "invalid checkpoint LSN '{}': {}",
                cp.lsn, e
            ))
        })?
    } else {
        Lsn::parse("0/0").unwrap()
    };

    let config = ReplicationConfig {
        host: components.host.clone(),
        port: components.port,
        user: components.user.clone(),
        password: components.password.clone(),
        database: components.database.clone(),
        tls: TlsConfig::disabled(),
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
    let source_id = source_id.to_string();
    let cfg = config.clone();

    retry_async(
        move |_| {
            let cfg = cfg.clone();
            let source_id = source_id.clone();
            async move { connect_replication(&source_id, cfg).await }
        },
        is_retryable_source_error,
        Duration::from_secs(30),
        retry_policy,
        cancel,
        "replication_connect",
    )
    .await
    .map_err(|outcome| match outcome {
        RetryOutcome::Cancelled => SourceError::Cancelled,
        RetryOutcome::Timeout { action } => SourceError::Timeout { action },
        RetryOutcome::Exhausted {
            last_error,
            attempts,
        } => {
            warn!(
                attempts = attempts,
                "replication connection exhausted retries"
            );
            last_error
        }
        RetryOutcome::Failed(e) => e,
    })
}

/// Determine if a SourceError is worth retrying.
fn is_retryable_source_error(e: &SourceError) -> bool {
    match e {
        SourceError::Timeout { .. }
        | SourceError::Connect { .. }
        | SourceError::Io(_) => true,
        SourceError::Cancelled | SourceError::Auth { .. } => false,
        SourceError::Other(inner) => {
            let msg = inner.to_string().to_lowercase();
            msg.contains("connection")
                || msg.contains("timeout")
                || msg.contains("reset")
                || msg.contains("refused")
                || msg.contains("already active")
        }
        _ => false,
    }
}

/// Connect to PostgreSQL replication with timeout.
async fn connect_replication(
    source_id: &str,
    config: ReplicationConfig,
) -> SourceResult<ReplicationClient> {
    debug!(source_id = %source_id, host = %config.host, "connecting to postgres replication");
    let t0 = Instant::now();

    match tokio::time::timeout(
        Duration::from_secs(30),
        ReplicationClient::connect(config),
    )
    .await
    {
        Ok(Ok(client)) => {
            info!(source_id = %source_id, ms = t0.elapsed().as_millis() as u64, "connected to postgres replication");
            Ok(client)
        }
        Ok(Err(e)) => {
            error!(source_id = %source_id, error = %e, "replication connect failed");
            print_connection_hints(&e);
            Err(pgwire_error_to_source_error(e))
        }
        Err(_) => Err(SourceError::Timeout {
            action: "replication_connect".into(),
        }),
    }
}

/// Convert PgWireError to SourceError for connection phase.
/// Note: This is different from LoopControl which is used in the event loop.
fn pgwire_error_to_source_error(e: PgWireError) -> SourceError {
    use PgWireError::*;
    match e {
        Auth(msg) => SourceError::Auth {
            details: msg.into(),
        },
        Io(msg) | Task(msg) => SourceError::Connect {
            details: msg.into(),
        },
        Tls(msg) => SourceError::Incompatible {
            details: format!("TLS: {msg}").into(),
        },
        Server(msg) | Protocol(msg) => SourceError::Connect {
            details: msg.into(),
        },
        Internal(msg) => {
            SourceError::Other(anyhow::anyhow!("pgwire internal: {}", msg))
        }
    }
}

/// Print helpful hints for common connection errors.
fn print_connection_hints(err: &PgWireError) {
    let msg = err.to_string();

    if err.is_auth()
        || msg.contains("password")
        || msg.contains("authentication")
    {
        error!("PostgreSQL auth issue hints:");
        error!("  1) Ensure pg_hba.conf allows replication connections");
        error!(
            "  2) Verify user has REPLICATION role: ALTER ROLE user REPLICATION;"
        );
        error!("  3) Grant usage: GRANT USAGE ON SCHEMA public TO user;");
    } else if msg.contains("slot") && msg.contains("does not exist") {
        error!("Replication slot issue hints:");
        error!(
            "  1) Create slot: SELECT pg_create_logical_replication_slot('slot','pgoutput');"
        );
    } else if msg.contains("slot") && msg.contains("already active") {
        warn!("Slot is in use by another connection - will retry");
    } else if msg.contains("publication") {
        error!("Publication issue hints:");
        error!(
            "  1) Create publication: CREATE PUBLICATION pub FOR ALL TABLES;"
        );
    } else if msg.contains("wal_level") {
        error!("WAL level issue hints:");
        error!("  1) Set wal_level=logical in postgresql.conf");
        error!("  2) Restart PostgreSQL after changing");
    } else if err.is_tls() {
        error!("TLS issue hints:");
        error!("  1) Check SSL certificates are valid");
        error!("  2) Try disabling TLS if not required");
    }
}

/// Verify publication exists and ensure slot exists, get start LSN.
///
/// Publications must be pre-created by a DBA - this function will NOT auto-create them.
/// Slots will be auto-created if missing (safe, no ownership issues).
///
/// Returns LoopControl::Reconnect for missing publication (retryable - admin can create it).
/// Returns LoopControl::Fail for fatal errors (auth, incompatible config).
pub(super) async fn ensure_slot_and_publication(
    dsn: &str,
    slot: &str,
    publication: &str,
    tables: &[String],
) -> Result<Lsn, super::postgres_errors::LoopControl> {
    use super::postgres_errors::LoopControl;

    let (client, conn) =
        tokio_postgres::connect(dsn, NoTls).await.map_err(|e| {
            error!(error = %e, "control plane connect failed");
            LoopControl::from_tokio_postgres_error(&e)
        })?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!("control plane connection error: {}", e);
        }
    });

    // Check publication exists (do NOT auto-create)
    let pub_exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .map_err(|e| {
            error!(error = %e, "failed to check publication");
            LoopControl::from_tokio_postgres_error(&e)
        })?
        .is_some();

    if !pub_exists {
        let table_list = if tables.is_empty() {
            "ALL TABLES".to_string()
        } else {
            tables.join(", ")
        };

        error!(
            publication = %publication,
            tables = %table_list,
            "Publication does not exist. DeltaForge requires a pre-created publication."
        );
        error!("To create it, run as superuser or table owner:");
        if tables.is_empty() {
            error!("  CREATE PUBLICATION {} FOR ALL TABLES;", publication);
        } else {
            error!(
                "  CREATE PUBLICATION {} FOR TABLE {};",
                publication, table_list
            );
        }
        error!(
            "DeltaForge will retry and pick up the publication once created."
        );

        return Err(LoopControl::Reconnect);
    }

    // Check/create slot (auto-creation is safe - no ownership issues)
    let slot_row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text, restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| {
            error!(error = %e, slot = %slot, "failed to check replication slot");
            LoopControl::from_tokio_postgres_error(&e)
        })?;

    let start_lsn = if let Some(row) = slot_row {
        let confirmed: Option<String> = row.get(0);
        let restart: Option<String> = row.get(1);

        confirmed
            .or(restart)
            .map(|s| {
                Lsn::parse(&s).map_err(|e| {
                    error!(error = %e, lsn = %s, "failed to parse LSN");
                    LoopControl::Fail(SourceError::Other(e.into()))
                })
            })
            .transpose()?
            .unwrap_or(get_current_wal_lsn(&client).await?)
    } else {
        // Auto-create slot (safe - only requires REPLICATION privilege)
        client
            .batch_execute(&format!(
                "SELECT * FROM pg_create_logical_replication_slot('{}', 'pgoutput')",
                slot
            ))
            .await
            .map_err(|e| {
                error!(error = %e, slot = %slot, "failed to create replication slot");
                error!("Ensure user has REPLICATION privilege: ALTER ROLE username REPLICATION;");
                LoopControl::from_tokio_postgres_error(&e)
            })?;

        info!(slot = %slot, "created replication slot");
        get_current_wal_lsn(&client).await?
    };

    Ok(start_lsn)
}

/// Get current WAL LSN.
async fn get_current_wal_lsn(
    client: &tokio_postgres::Client,
) -> Result<Lsn, super::postgres_errors::LoopControl> {
    use super::postgres_errors::LoopControl;

    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .map_err(|e| {
            error!(error = %e, "failed to get current WAL LSN");
            LoopControl::from_tokio_postgres_error(&e)
        })?;

    let lsn_str: String = row.get(0);
    Lsn::parse(&lsn_str).map_err(|e| {
        error!(error = %e, lsn = %lsn_str, "failed to parse WAL LSN");
        LoopControl::Fail(SourceError::Other(e.into()))
    })
}

// ----------------------------- Utility Functions -----------------------------

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
    dsn.split_whitespace()
        .map(|p| {
            if p.to_lowercase().starts_with("password=") {
                "password=***"
            } else {
                p
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
        error!(error=%e, "failed to serialize checkpoint");
        Vec::new()
    });
    CheckpointMeta::from_vec(bytes)
}

/// Convert PostgreSQL epoch timestamp (microseconds since 2000-01-01) to Unix milliseconds.
pub(crate) fn pg_timestamp_to_unix_ms(pg_timestamp_us: i64) -> i64 {
    const PG_EPOCH_OFFSET_MS: i64 = 946_684_800_000;
    (pg_timestamp_us / 1000) + PG_EPOCH_OFFSET_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_timestamp_conversion() {
        let pg_ts = 631_152_000_000_000i64; // 2020-01-01 in PG epoch
        assert_eq!(pg_timestamp_to_unix_ms(pg_ts), 1_577_836_800_000);
    }

    #[test]
    fn test_is_retryable_source_error() {
        assert!(is_retryable_source_error(&SourceError::Timeout {
            action: "x".into()
        }));
        assert!(is_retryable_source_error(&SourceError::Connect {
            details: "failed".into()
        }));
        assert!(!is_retryable_source_error(&SourceError::Cancelled));
        assert!(!is_retryable_source_error(&SourceError::Auth {
            details: "denied".into()
        }));
    }

    #[test]
    fn test_pgwire_error_to_source_error() {
        assert!(matches!(
            pgwire_error_to_source_error(PgWireError::Auth("x".into())),
            SourceError::Auth { .. }
        ));
        assert!(matches!(
            pgwire_error_to_source_error(PgWireError::Io("x".into())),
            SourceError::Connect { .. }
        ));
        assert!(matches!(
            pgwire_error_to_source_error(PgWireError::Tls("x".into())),
            SourceError::Incompatible { .. }
        ));
    }
}
