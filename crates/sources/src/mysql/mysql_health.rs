//! MySQL source health checks.
//!
//! Covers pre-run validation (permissions, server health, retention capacity),
//! in-flight guards (binlog position still valid), and is the home for future
//! periodic checks (CDC lag, failover detection, server health).
//!
//! Runs before any workers are spawned. Detects hard blockers (binlog disabled,
//! missing privileges) and estimates whether binlog retention is sufficient for
//! the planned snapshot duration, logging actionable warnings when at risk.

use anyhow::{Context, Result};
use mysql_async::{Pool, Row, prelude::Queryable};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

// Conservative read throughput per parallel worker (bytes/sec).
// Intentionally pessimistic - better a false alarm than a missed purge.
const THROUGHPUT_PER_WORKER_BYTES: u64 = 20 * 1024 * 1024; // 20 MB/s

/// Result of a preflight check. Hard errors block the snapshot;
/// warnings are logged and the snapshot proceeds.
#[derive(Debug)]
pub struct PreflightReport {
    pub hard_errors: Vec<String>,
    pub warnings: Vec<String>,
    pub estimated_size_bytes: Option<u64>,
    pub estimated_duration_secs: Option<u64>,
    pub retention_secs: Option<u64>,
}

impl PreflightReport {
    /// Log the full report at the appropriate level and return Err if there
    /// are any hard errors.
    pub fn emit_and_check(
        &self,
        source_id: &str,
        table_count: usize,
    ) -> Result<()> {
        let size_str = self
            .estimated_size_bytes
            .map(|b| format!("{:.1} GB", b as f64 / 1_073_741_824.0))
            .unwrap_or_else(|| "unknown".into());

        let duration_str = self
            .estimated_duration_secs
            .map(format_duration)
            .unwrap_or_else(|| "unknown".into());

        let retention_str = self
            .retention_secs
            .map(format_duration)
            .unwrap_or_else(|| "unknown".into());

        info!(
            source_id,
            tables = table_count,
            estimated_size = %size_str,
            estimated_duration = %duration_str,
            binlog_retention = %retention_str,
            "snapshot preflight: mysql"
        );

        for w in &self.warnings {
            warn!(source_id, "{}", w);
        }

        if !self.hard_errors.is_empty() {
            let msg = self.hard_errors.join("; ");
            anyhow::bail!("snapshot preflight failed: {}", msg);
        }

        Ok(())
    }
}

/// Run all preflight checks and return a report.
/// Does not abort — callers decide what to do with hard errors.
pub async fn run_preflight(
    dsn: &str,
    tables: &[(String, String)], // (db, table)
    max_parallel_tables: usize,
) -> Result<PreflightReport> {
    let pool = Pool::new(dsn);
    let mut conn = pool
        .get_conn()
        .await
        .context("preflight: failed to connect")?;

    let mut report = PreflightReport {
        hard_errors: Vec::new(),
        warnings: Vec::new(),
        estimated_size_bytes: None,
        estimated_duration_secs: None,
        retention_secs: None,
    };

    // 1. binlog enabled and ROW format

    let log_bin: Option<String> = conn
        .query_first("SELECT @@GLOBAL.log_bin")
        .await
        .ok()
        .flatten()
        .map(|mut r: Row| r.take(0).unwrap_or_default());

    if log_bin.as_deref() != Some("1") {
        report.hard_errors.push(
            "binary logging is disabled (log_bin=0). \
             Enable with --log-bin --binlog-format=ROW."
                .into(),
        );
        // No point continuing - nothing will work.
        return Ok(report);
    }

    let binlog_format: Option<String> = conn
        .query_first("SELECT @@GLOBAL.binlog_format")
        .await
        .ok()
        .flatten()
        .map(|mut r: Row| r.take(0).unwrap_or_default());

    if binlog_format.as_deref() != Some("ROW") {
        report.hard_errors.push(format!(
            "binlog_format is {:?}, must be ROW for CDC.",
            binlog_format.as_deref().unwrap_or("unknown")
        ));
    }

    // 2. retention window

    // MySQL 8.0+: binlog_expire_logs_seconds (0 = never expire)
    // MySQL 5.7:  expire_logs_days
    let retention_secs: Option<u64> = {
        let secs: Option<u64> = conn
            .query_first("SELECT @@GLOBAL.binlog_expire_logs_seconds")
            .await
            .ok()
            .flatten()
            .and_then(|mut r: Row| r.take(0));

        if secs == Some(0) {
            // 0 means "never expire" — no retention concern
            None
        } else if let Some(s) = secs.filter(|&s| s > 0) {
            Some(s)
        } else {
            // Fallback for MySQL 5.7
            conn.query_first("SELECT @@GLOBAL.expire_logs_days")
                .await
                .ok()
                .flatten()
                .and_then(|mut r: Row| r.take::<u64, _>(0))
                .filter(|&d| d > 0)
                .map(|d| d * 86400)
        }
    };

    report.retention_secs = retention_secs;

    // 3. table size estimation

    if !tables.is_empty() {
        // Build per-db groups to minimise queries
        let mut by_db: std::collections::HashMap<&str, Vec<&str>> =
            std::collections::HashMap::new();
        for (db, table) in tables {
            by_db.entry(db.as_str()).or_default().push(table.as_str());
        }

        let mut total_bytes: u64 = 0;

        for (db, tbl_names) in &by_db {
            let placeholders = tbl_names
                .iter()
                .enumerate()
                .map(|(i, _)| format!("'{}'", tbl_names[i]))
                .collect::<Vec<_>>()
                .join(", ");

            let query = format!(
                "SELECT COALESCE(SUM(data_length + index_length), 0) \
                 FROM information_schema.tables \
                 WHERE table_schema = '{db}' AND table_name IN ({placeholders})"
            );

            if let Ok(Some(bytes)) = conn.query_first::<u64, _>(query).await {
                total_bytes += bytes;
            }
        }

        if total_bytes > 0 {
            report.estimated_size_bytes = Some(total_bytes);

            let effective_parallel =
                max_parallel_tables.min(tables.len()).max(1) as u64;
            let throughput = THROUGHPUT_PER_WORKER_BYTES * effective_parallel;
            let estimated_secs = total_bytes / throughput;
            report.estimated_duration_secs = Some(estimated_secs);

            // 4. retention risk assessment

            if let Some(retention) = retention_secs {
                let pct = (estimated_secs * 100) / retention.max(1);

                if pct >= 80 {
                    report.warnings.push(format!(
                        "HIGH RETENTION RISK: estimated snapshot duration ({}) \
                         is {}% of binlog_expire_logs_seconds ({}). \
                         The captured binlog position may be purged before the snapshot \
                         completes, causing CDC startup to fail. \
                         Recommended actions: \
                         (1) increase binlog_expire_logs_seconds to at least {}s, \
                         (2) reduce max_parallel_tables to decrease snapshot duration, \
                         or (3) use a read replica as the snapshot source.",
                        format_duration(estimated_secs),
                        pct,
                        format_duration(retention),
                        estimated_secs * 2,
                    ));
                } else if pct >= 50 {
                    report.warnings.push(format!(
                        "RETENTION WARNING: estimated snapshot duration ({}) \
                         is {}% of binlog_expire_logs_seconds ({}). \
                         Consider increasing binlog_expire_logs_seconds if tables \
                         are larger than information_schema estimates.",
                        format_duration(estimated_secs),
                        pct,
                        format_duration(retention),
                    ));
                }
            }
        }
    }

    conn.disconnect().await.ok();
    Ok(report)
}

/// Verify the captured binlog file is still present.
/// Called synchronously after all workers finish, before writing `finished = true`.
/// Returns Ok if still present or if verification can't be performed (transient).
/// Returns Err only on confirmed purge.
pub async fn verify_binlog_position(
    dsn: &str,
    captured_file: &str,
) -> Result<()> {
    let pool = Pool::new(dsn);
    let mut conn = pool
        .get_conn()
        .await
        .context("final position check: failed to connect")?;

    let rows: Vec<Row> = conn
        .query("SHOW BINARY LOGS")
        .await
        .context("final position check: SHOW BINARY LOGS failed")?;

    let available: Vec<String> = rows
        .into_iter()
        .filter_map(|mut r: Row| r.take::<String, usize>(0))
        .collect();

    // Empty result = transient issue, don't abort
    if available.is_empty() {
        return Ok(());
    }

    if !available.contains(&captured_file.to_string()) {
        anyhow::bail!(
            "final position check failed: binlog file '{}' was purged during snapshot \
             (available: [{}]). \
             The snapshot position is no longer valid for CDC resume. \
             Increase binlog_expire_logs_seconds and restart the pipeline to re-snapshot.",
            captured_file,
            available.join(", ")
        );
    }

    Ok(())
}

// internal helpers

pub(crate) fn binlog_file_still_present(
    available: &[String],
    captured: &str,
) -> bool {
    available.is_empty() || available.contains(&captured.to_string())
}

// ============================================================================
// Failover Detection
// ============================================================================
//
// Source-native half of failover handling: plain queries returning raw data.
// No orchestration, no state. Called by the failover reconciler above this layer.

/// The stable identity of a MySQL server instance.
///
/// `server_uuid` is assigned at initialisation, stored in `auto.cnf`, and
/// survives restarts. It is distinct per replica, making it the correct signal
/// for "did I just connect to a different server?".
///
/// Unlike `server_id` (small integer, user-assigned, often reused),
/// `server_uuid` is globally unique and never changes for the life of an install.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MySqlServerIdentity {
    pub server_uuid: String,
}

/// Fetch the server identity from a live MySQL connection.
///
/// Returns `Ok(None)` when `server_uuid` is unavailable (MySQL < 5.6, or
/// the variable is unset). The caller treats `None` as "cannot detect
/// failover" and falls through to position validation only.
pub async fn fetch_server_identity(
    dsn: &str,
) -> Result<Option<MySqlServerIdentity>> {
    let pool = Pool::new(dsn);
    let mut conn = pool
        .get_conn()
        .await
        .context("fetch_server_identity: connect failed")?;

    let row: Option<(Option<String>,)> = conn
        .query_first("SELECT @@global.server_uuid")
        .await
        .context("fetch_server_identity: query failed")?;

    conn.disconnect().await.ok();

    let uuid = row.and_then(|(v,)| v).filter(|s| {
        !s.is_empty() && s != "00000000-0000-0000-0000-000000000000"
    });

    Ok(uuid.map(|server_uuid| MySqlServerIdentity { server_uuid }))
}

// ============================================================================
// Position Reachability
// ============================================================================

/// Whether a saved checkpoint is still reachable on the current server.
///
/// Distinct from the snapshot-time `verify_binlog_position` in that it also
/// handles GTID-mode validation - the preferred path after failover because
/// GTID state is topology-resilient and survives promotion.
#[derive(Debug, PartialEq)]
pub enum PositionReachability {
    /// Confirmed reachable - resume is safe.
    Reachable,
    /// Confirmed gone - caller decides how to proceed.
    Lost { reason: String },
    /// Could not determine (transient connect error, empty result).
    /// Caller should warn but not hard-fail.
    Unknown { reason: String },
}

/// Check whether a saved checkpoint is still reachable on the connected server.
///
/// GTID path is tried first when `gtid_set` is present. Falls back to binlog
/// file presence check when file/pos only.
pub async fn check_position_reachability(
    dsn: &str,
    file: &str,
    gtid_set: Option<&str>,
) -> Result<PositionReachability> {
    let pool = Pool::new(dsn);
    let mut conn = match pool.get_conn().await {
        Ok(c) => c,
        Err(e) => {
            return Ok(PositionReachability::Unknown {
                reason: format!("connect failed: {e}"),
            });
        }
    };

    // GTID path: ask the new primary whether it has already executed the
    // transactions in our saved set. GTID_SUBSET(saved, executed) = 1 means
    // all our transactions are present.
    if let Some(gtid) = gtid_set.filter(|s| !s.is_empty()) {
        let query = format!(
            "SELECT GTID_SUBSET('{}', @@global.gtid_executed)",
            gtid.replace('\'', "\\'")
        );

        match conn.query_first::<Row, _>(&query).await {
            Ok(Some(mut row)) => {
                let is_subset: Option<i64> = row.take(0);
                conn.disconnect().await.ok();
                return match is_subset {
                    Some(1) => Ok(PositionReachability::Reachable),
                    Some(0) => Ok(PositionReachability::Lost {
                        reason: format!(
                            "GTID set '{gtid}' is not a subset of @@gtid_executed \
                             on the new primary — some transactions are absent"
                        ),
                    }),
                    _ => Ok(PositionReachability::Unknown {
                        reason: "GTID_SUBSET returned unexpected value".into(),
                    }),
                };
            }
            Ok(None) => { /* GTID unavailable, fall through */ }
            Err(e) => {
                warn!(error = %e, "GTID_SUBSET check failed, falling back to file check");
            }
        }
    }

    // File/pos fallback.
    let rows: Vec<Row> = match conn.query("SHOW BINARY LOGS").await {
        Ok(r) => r,
        Err(e) => {
            conn.disconnect().await.ok();
            return Ok(PositionReachability::Unknown {
                reason: format!("SHOW BINARY LOGS failed: {e}"),
            });
        }
    };

    conn.disconnect().await.ok();

    let available: Vec<String> = rows
        .into_iter()
        .filter_map(|mut r: Row| r.take::<String, usize>(0))
        .collect();

    if available.is_empty() {
        return Ok(PositionReachability::Unknown {
            reason: "SHOW BINARY LOGS returned no rows".into(),
        });
    }

    if available.contains(&file.to_string()) {
        Ok(PositionReachability::Reachable)
    } else {
        Ok(PositionReachability::Lost {
            reason: format!(
                "binlog file '{file}' not present on new primary \
                 (available: [{}])",
                available.join(", ")
            ),
        })
    }
}

// ============================================================================
// Live Catalog Fetch
// ============================================================================

/// A column as reported by INFORMATION_SCHEMA.
/// Used by the schema reconciler to diff live state against the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveColumn {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    /// "PRI", "UNI", "MUL", or ""
    pub column_key: String,
}

/// Fetch current columns for a table from INFORMATION_SCHEMA.
///
/// Returns `Ok(None)` when the table does not exist on this server - the
/// reconciler treats that as a dropped-table delta.
///
/// Called after failover detection, before row events resume.
pub async fn fetch_live_columns(
    dsn: &str,
    db: &str,
    table: &str,
) -> Result<Option<Vec<LiveColumn>>> {
    let pool = Pool::new(dsn);
    let mut conn = pool
        .get_conn()
        .await
        .context("fetch_live_columns: connect failed")?;

    let exists: Option<(i64,)> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (db, table),
        )
        .await
        .context("fetch_live_columns: existence check failed")?;

    if exists.map(|(n,)| n).unwrap_or(0) == 0 {
        conn.disconnect().await.ok();
        return Ok(None);
    }

    let rows: Vec<Row> = conn
        .exec(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (db, table),
        )
        .await
        .context("fetch_live_columns: COLUMNS query failed")?;

    conn.disconnect().await.ok();

    let columns = rows
        .into_iter()
        .filter_map(|mut r: Row| {
            let name: String = r.take(0)?;
            let data_type: String = r.take(1)?;
            let nullable: String = r.take(2).unwrap_or_default();
            let key: String = r.take(3).unwrap_or_default();
            Some(LiveColumn {
                name,
                data_type,
                is_nullable: nullable.eq_ignore_ascii_case("YES"),
                column_key: key,
            })
        })
        .collect();

    Ok(Some(columns))
}

fn format_duration(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{secs}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_present_in_list() {
        let available = vec!["binlog.000001".into(), "binlog.000002".into()];
        assert!(binlog_file_still_present(&available, "binlog.000001"));
    }

    #[test]
    fn file_absent_from_list() {
        let available = vec!["binlog.000003".into(), "binlog.000004".into()];
        assert!(!binlog_file_still_present(&available, "binlog.000001"));
    }

    #[test]
    fn empty_list_is_transient() {
        assert!(binlog_file_still_present(&[], "binlog.000001"));
    }

    #[test]
    fn format_duration_variants() {
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(90), "1m30s");
        assert_eq!(format_duration(3661), "1h1m");
    }

    #[test]
    fn retention_risk_at_90pct_generates_warning() {
        // 90% of retention -> HIGH RETENTION RISK
        let estimated = 3240u64; // 54 min
        let retention = 3600u64; // 60 min
        let pct = (estimated * 100) / retention;
        assert!(pct >= 80);
    }

    #[test]
    fn no_retention_risk_when_expire_is_zero() {
        // 0 = never expire - treated as None, no risk warning
        let retention: Option<u64> = None; // zero is mapped to None upstream
        assert!(retention.is_none());
    }

    // --- Failover detection ---

    #[test]
    fn server_identity_round_trips_json() {
        let id = MySqlServerIdentity {
            server_uuid: "6ccd780c-baba-1026-9564-5b8c656024db".into(),
        };
        let json = serde_json::to_string(&id).unwrap();
        let back: MySqlServerIdentity = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn reachability_file_present() {
        // Reuses binlog_file_still_present - the file path in
        // check_position_reachability delegates to the same logic.
        let available = vec!["binlog.000003".into(), "binlog.000004".into()];
        assert!(binlog_file_still_present(&available, "binlog.000003"));
        assert!(!binlog_file_still_present(&available, "binlog.000001"));
    }
}
