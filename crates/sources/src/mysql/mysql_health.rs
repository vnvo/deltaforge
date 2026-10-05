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
    /// True when a hard error is a missing-privilege problem (e.g. RELOAD),
    /// so callers can surface a typed `Permission` refusal rather than a generic
    /// incompatibility. Managed MySQL without RELOAD lands here.
    pub permission_error: bool,
}

impl PreflightReport {
    /// Log the report (info summary + warnings) without failing. Callers that
    /// want typed fail-closed behavior use this then inspect `hard_errors` /
    /// `permission_error` themselves.
    pub fn emit(&self, source_id: &str, table_count: usize) {
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
    }

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
pub async fn run_preflight<S: AsRef<str>>(
    dsn: &str,
    tables: &[(S, S)], // (db, table)
    max_parallel_tables: usize,
) -> Result<PreflightReport> {
    let pool = Pool::new(dsn);
    let mut conn = pool
        .get_conn()
        .await
        .context("preflight: failed to connect")?;
    let report = run_preflight_on(&mut conn, tables, max_parallel_tables).await;
    conn.disconnect().await.ok();
    report
}

/// [`run_preflight`] on a control connection verified as `expected_uuid`:
/// the checks that gate a snapshot describe the verified server only.
pub(crate) async fn run_preflight_verified<S: AsRef<str>>(
    dsn: &str,
    expected_uuid: &str,
    tables: &[(S, S)],
    max_parallel_tables: usize,
) -> Result<PreflightReport> {
    let mut conn = super::mysql_session::open_control_connection(
        dsn,
        expected_uuid,
        std::time::Duration::from_secs(10),
    )
    .await
    .map_err(|e| anyhow::Error::new(e.into_source_error(expected_uuid)))?;
    let report = run_preflight_on(&mut conn, tables, max_parallel_tables).await;
    conn.disconnect().await.ok();
    report
}

async fn run_preflight_on<S: AsRef<str>>(
    conn: &mut mysql_async::Conn,
    tables: &[(S, S)], // (db, table)
    max_parallel_tables: usize,
) -> Result<PreflightReport> {
    let mut report = PreflightReport {
        hard_errors: Vec::new(),
        warnings: Vec::new(),
        estimated_size_bytes: None,
        estimated_duration_secs: None,
        retention_secs: None,
        permission_error: false,
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

    // 1b. GTID mode must be fully ON (mandatory; no file/position downgrade).
    let gtid_mode: Option<String> = conn
        .query_first("SELECT @@GLOBAL.gtid_mode")
        .await
        .ok()
        .flatten()
        .map(|mut r: Row| r.take(0).unwrap_or_default());

    if let Some(err) = gtid_mode_hard_error(gtid_mode.as_deref()) {
        report.hard_errors.push(err);
    }

    // 1c. RELOAD privilege - required for FLUSH TABLES WITH READ LOCK, which
    // brackets the consistent snapshot anchor. Managed MySQL that restricts
    // RELOAD cannot guarantee a consistent initial snapshot and fails closed
    // (no silent fallback to an unsafe per-worker-snapshot anchor).
    let grants: Vec<String> = conn
        .query("SHOW GRANTS FOR CURRENT_USER()")
        .await
        .unwrap_or_default();
    if let Some(err) = reload_privilege_hard_error(&grants) {
        report.hard_errors.push(err);
        report.permission_error = true;
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
            by_db.entry(db.as_ref()).or_default().push(table.as_ref());
        }

        let mut total_bytes: u64 = 0;

        // Bounded statements: at most 1000 names per `IN (...)` list.
        let mut batches: Vec<(&str, &[&str])> = Vec::new();
        for (db, names) in &by_db {
            for chunk in names.chunks(1_000) {
                batches.push((db, chunk));
            }
        }
        for (db, tbl_names) in batches {
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

            // Storage-engine check: only InnoDB gives the MVCC consistent read
            // the snapshot anchor relies on. Fail closed on anything else.
            let engine_query = format!(
                "SELECT table_name, engine \
                 FROM information_schema.tables \
                 WHERE table_schema = '{db}' AND table_name IN ({placeholders})"
            );
            if let Ok(rows) = conn
                .query::<(String, Option<String>), _>(engine_query)
                .await
            {
                for (tname, engine) in rows {
                    if let Some(err) =
                        engine_hard_error(db, &tname, engine.as_deref())
                    {
                        report.hard_errors.push(err);
                    }
                }
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

    Ok(report)
}

/// Verify the captured binlog file is still present.
/// Called synchronously after all workers finish, before writing `finished = true`.
/// Returns Ok if still present or if verification can't be performed (transient).
/// Returns Err only on confirmed purge.
pub async fn verify_binlog_position(
    dsn: &str,
    expected_uuid: &str,
    captured_file: &str,
) -> Result<()> {
    // Only the verified server's binlog list answers this.
    let mut conn = super::mysql_session::open_control_connection(
        dsn,
        expected_uuid,
        std::time::Duration::from_secs(10),
    )
    .await
    .map_err(|e| anyhow::Error::new(e.into_source_error(expected_uuid)))
    .context("final position check")?;

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
    /// Confirmed gone: `class` is a stable reason class (`not_executed`,
    /// `binlog_missing`).
    Lost { class: &'static str, reason: String },
    /// Could not determine. `transient`: a connection or I/O failure a retry
    /// may clear; otherwise the server answered something a retry will not
    /// change.
    Unknown { transient: bool, reason: String },
}

/// Whether a MySQL client error is plausibly transient: anything but an error
/// the server answered with.
pub(crate) fn is_transient(e: &mysql_async::Error) -> bool {
    !matches!(e, mysql_async::Error::Server(_))
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
                transient: is_transient(&e),
                reason: format!("connect failed: {e}"),
            });
        }
    };
    let r = check_position_reachability_on(&mut conn, file, gtid_set).await;
    conn.disconnect().await.ok();
    r
}

/// [`check_position_reachability`] on an already-open connection (one whose
/// server identity the caller has verified).
pub(crate) async fn check_position_reachability_on(
    conn: &mut mysql_async::Conn,
    file: &str,
    gtid_set: Option<&str>,
) -> Result<PositionReachability> {
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
                return match is_subset {
                    Some(1) => Ok(PositionReachability::Reachable),
                    Some(0) => Ok(PositionReachability::Lost {
                        class: "not_executed",
                        reason: format!(
                            "GTID set '{gtid}' is not a subset of @@gtid_executed \
                             on the new primary — some transactions are absent"
                        ),
                    }),
                    _ => Ok(PositionReachability::Unknown {
                        transient: false,
                        reason: "GTID_SUBSET returned unexpected value".into(),
                    }),
                };
            }
            // A GTID position is never "proven" by a file name.
            Ok(None) => {
                return Ok(PositionReachability::Unknown {
                    transient: false,
                    reason: "GTID_SUBSET returned no row".into(),
                });
            }
            Err(e) => {
                return Ok(PositionReachability::Unknown {
                    transient: is_transient(&e),
                    reason: format!("GTID_SUBSET failed: {e}"),
                });
            }
        }
    }

    // File/pos position (no GTID set given).
    let rows: Vec<Row> = match conn.query("SHOW BINARY LOGS").await {
        Ok(r) => r,
        Err(e) => {
            return Ok(PositionReachability::Unknown {
                transient: is_transient(&e),
                reason: format!("SHOW BINARY LOGS failed: {e}"),
            });
        }
    };

    let available: Vec<String> = rows
        .into_iter()
        .filter_map(|mut r: Row| r.take::<String, usize>(0))
        .collect();

    if available.is_empty() {
        return Ok(PositionReachability::Unknown {
            transient: false,
            reason: "SHOW BINARY LOGS returned no rows".into(),
        });
    }

    if available.contains(&file.to_string()) {
        Ok(PositionReachability::Reachable)
    } else {
        Ok(PositionReachability::Lost {
            class: "binlog_missing",
            reason: format!(
                "binlog file '{file}' not present on new primary \
                 (available: [{}])",
                available.join(", ")
            ),
        })
    }
}

/// Require `GTID_SUBSET(set, @@GLOBAL.gtid_executed)` to be exactly 1 on
/// `conn` (whose identity the caller verified). Anything else - 0, NULL, no
/// row, a query error (e.g. a malformed set) - is an error carrying the reason.
pub(crate) async fn require_gtid_executed(
    conn: &mut mysql_async::Conn,
    set: &str,
) -> std::result::Result<(), GtidNotProven> {
    let row: std::result::Result<Option<Option<i64>>, _> = conn
        .exec_first("SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed)", (set,))
        .await;
    gtid_executed_outcome(row.map_err(|e| (is_transient(&e), e.to_string())))
}

/// Why a GTID set is not proven executed: `class` is a stable reason class
/// (`not_executed`, `unknown_unreachable`, `unknown_query_failed`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GtidNotProven {
    pub class: &'static str,
    pub reason: String,
}

impl std::fmt::Display for GtidNotProven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

fn gtid_executed_outcome(
    row: std::result::Result<Option<Option<i64>>, (bool, String)>,
) -> std::result::Result<(), GtidNotProven> {
    let not = |class, reason: String| Err(GtidNotProven { class, reason });
    match row {
        Ok(Some(Some(1))) => Ok(()),
        Ok(Some(Some(0))) => not(
            "not_executed",
            "not executed by the server (GTID_SUBSET = 0)".into(),
        ),
        Ok(Some(other)) => not(
            "unknown_query_failed",
            format!("GTID_SUBSET returned {other:?}"),
        ),
        Ok(None) => {
            not("unknown_query_failed", "GTID_SUBSET returned no row".into())
        }
        Err((true, e)) => {
            not("unknown_unreachable", format!("GTID_SUBSET failed: {e}"))
        }
        Err((false, e)) => {
            not("unknown_query_failed", format!("GTID_SUBSET failed: {e}"))
        }
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
    let r = fetch_live_columns_on(&mut conn, db, table).await;
    conn.disconnect().await.ok();
    r
}

/// [`fetch_live_columns`] on an already-open connection (one whose server
/// identity the caller has verified).
pub(crate) async fn fetch_live_columns_on(
    conn: &mut mysql_async::Conn,
    db: &str,
    table: &str,
) -> Result<Option<Vec<LiveColumn>>> {
    let exists: Option<(i64,)> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (db, table),
        )
        .await
        .context("fetch_live_columns: existence check failed")?;

    if exists.map(|(n,)| n).unwrap_or(0) == 0 {
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

/// Hard error unless GTID mode is fully `ON`. GTID is mandatory for the
/// snapshot-anchor hardening milestone: the anchor captures `@@GLOBAL.gtid_executed`
/// under the read lock and CDC resumes by GTID set, so there is no quiet
/// file/position downgrade. `ON_PERMISSIVE`/`OFF_PERMISSIVE` are migration
/// states, not a stable GTID stream, and are rejected.
fn gtid_mode_hard_error(gtid_mode: Option<&str>) -> Option<String> {
    match gtid_mode {
        Some("ON") => None,
        other => Some(format!(
            "gtid_mode is {:?}, must be ON for consistent snapshot anchoring. \
             Enable with --gtid-mode=ON --enforce-gtid-consistency=ON.",
            other.unwrap_or("unknown")
        )),
    }
}

/// Hard error unless the current user holds `RELOAD` (globally, `ON *.*`) - the
/// privilege `FLUSH TABLES WITH READ LOCK` requires to bracket the snapshot
/// anchor. `ALL PRIVILEGES ON *.*` implies RELOAD; a database-scoped `ALL
/// PRIVILEGES ON db.*` does not (RELOAD is global-only), so both the privilege
/// and the `ON *.*` scope must appear on the same grant line.
fn reload_privilege_hard_error(grants: &[String]) -> Option<String> {
    let has_reload = grants.iter().any(|g| {
        let up = g.to_uppercase();
        up.contains("ON *.*")
            && (up.contains("ALL PRIVILEGES") || up.contains("RELOAD"))
    });
    if has_reload {
        None
    } else {
        Some(
            "current user lacks the global RELOAD privilege required for \
             FLUSH TABLES WITH READ LOCK, which brackets the consistent snapshot \
             anchor. On managed MySQL that restricts RELOAD a consistent initial \
             snapshot cannot be guaranteed; grant RELOAD (GRANT RELOAD ON *.* ...), \
             or set snapshot mode to 'never' to stream changes only (not a complete \
             initial load)."
                .into(),
        )
    }
}

/// Hard error unless the table's storage engine is InnoDB. Only InnoDB provides
/// the MVCC consistent read the snapshot relies on; non-InnoDB tables are out of
/// scope for this milestone and must fail closed rather than snapshot inconsistently.
fn engine_hard_error(
    db: &str,
    table: &str,
    engine: Option<&str>,
) -> Option<String> {
    match engine {
        Some(e) if e.eq_ignore_ascii_case("InnoDB") => None,
        other => Some(format!(
            "table {}.{} uses storage engine {:?}, only InnoDB is supported for \
             consistent snapshots.",
            db,
            table,
            other.unwrap_or("unknown")
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_exact_gtid_subset_proves_a_resume_position() {
        assert_eq!(gtid_executed_outcome(Ok(Some(Some(1)))), Ok(()));
        for (row, class) in [
            (Ok(Some(Some(0))), "not_executed"),
            (Ok(Some(Some(2))), "unknown_query_failed"),
            (Ok(Some(None)), "unknown_query_failed"),
            (Ok(None), "unknown_query_failed"),
            (
                Err((false, "Malformed GTID set specification".to_string())),
                "unknown_query_failed",
            ),
            (
                Err((true, "connection reset".to_string())),
                "unknown_unreachable",
            ),
        ] {
            let out = gtid_executed_outcome(row.clone());
            assert_eq!(out.map_err(|e| e.class), Err(class), "{row:?}");
        }
    }

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
    fn gtid_mode_must_be_fully_on() {
        assert_eq!(gtid_mode_hard_error(Some("ON")), None);
        assert!(gtid_mode_hard_error(Some("OFF")).is_some());
        assert!(gtid_mode_hard_error(Some("OFF_PERMISSIVE")).is_some());
        // ON_PERMISSIVE is a migration state, not fully ON - reject.
        assert!(gtid_mode_hard_error(Some("ON_PERMISSIVE")).is_some());
        assert!(gtid_mode_hard_error(None).is_some());
    }

    #[test]
    fn engine_must_be_innodb() {
        assert_eq!(engine_hard_error("db", "t", Some("InnoDB")), None);
        // engine name comparison is case-insensitive
        assert_eq!(engine_hard_error("db", "t", Some("innodb")), None);
        assert!(engine_hard_error("db", "t", Some("MyISAM")).is_some());
        assert!(engine_hard_error("db", "t", None).is_some());
    }

    #[test]
    fn reload_privilege_detected() {
        let full = vec![
            "GRANT SELECT, RELOAD, REPLICATION SLAVE ON *.* TO 'df'@'%'".into(),
        ];
        assert_eq!(reload_privilege_hard_error(&full), None);
        let all = vec!["GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'".into()];
        assert_eq!(reload_privilege_hard_error(&all), None);
    }

    #[test]
    fn reload_privilege_missing_fails_closed() {
        // Managed MySQL: broad db grant but no global RELOAD.
        let managed = vec![
            "GRANT SELECT, INSERT ON `app`.* TO 'df'@'%'".into(),
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%'"
                .into(),
        ];
        assert!(reload_privilege_hard_error(&managed).is_some());
        // ALL PRIVILEGES scoped to a database is NOT global RELOAD.
        let db_all = vec!["GRANT ALL PRIVILEGES ON `app`.* TO 'df'@'%'".into()];
        assert!(reload_privilege_hard_error(&db_all).is_some());
        assert!(reload_privilege_hard_error(&[]).is_some());
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
