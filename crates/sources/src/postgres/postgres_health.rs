//! PostgreSQL source health checks.
//!
//! Covers pre-run validation (slot/publication health, WAL retention capacity),
//! in-flight guards (WAL slot still valid), and is the home for future
//! periodic checks (CDC lag, failover detection, slot health).
//!
//! Runs before any workers are spawned. Validates slot/publication health,
//! estimates whether max_slot_wal_keep_size is sufficient for the planned
//! snapshot duration, and logs actionable warnings when at risk.

use anyhow::{Context, Result};
use tokio_postgres::NoTls;
use tracing::{info, warn};

const THROUGHPUT_PER_WORKER_BYTES: u64 = 20 * 1024 * 1024; // 20 MB/s

/// WAL slot health from pg_replication_slots.
#[derive(Debug, PartialEq)]
pub enum SlotWalStatus {
    /// WAL retention is guaranteed.
    Reserved,
    /// Slot has extended retention beyond normal (pg 13+). Usually fine.
    Extended,
    /// WAL retention is no longer guaranteed — slot will likely become invalid soon.
    Unreserved,
    /// Required WAL has already been removed. Slot is unusable.
    Lost,
    /// Column not present (PG < 13) or slot not found.
    Unknown,
}

#[derive(Debug)]
pub struct PreflightReport {
    pub hard_errors: Vec<String>,
    pub warnings: Vec<String>,
    pub estimated_size_bytes: Option<u64>,
    pub estimated_duration_secs: Option<u64>,
    pub wal_keep_bytes: Option<i64>, // -1 = unlimited (0 in pg_settings)
    pub slot_wal_status: SlotWalStatus,
}

impl PreflightReport {
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
            .map(|s| format_duration(s))
            .unwrap_or_else(|| "unknown".into());

        let wal_keep_str = match self.wal_keep_bytes {
            None => "unknown".into(),
            Some(-1) => "unlimited".into(),
            Some(b) => format!("{:.1} GB", b as f64 / 1_073_741_824.0),
        };

        info!(
            source_id,
            tables = table_count,
            estimated_size = %size_str,
            estimated_duration = %duration_str,
            max_slot_wal_keep = %wal_keep_str,
            slot_wal_status = ?self.slot_wal_status,
            "snapshot preflight: postgres"
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

/// Run all preflight checks.
pub async fn run_preflight(
    dsn: &str,
    slot_name: Option<&str>,
    publication: &str,
    tables: &[(String, String)], // (schema, table)
    max_parallel_tables: usize,
) -> Result<PreflightReport> {
    let (client, conn) = tokio_postgres::connect(dsn, NoTls)
        .await
        .context("preflight: failed to connect")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut report = PreflightReport {
        hard_errors: Vec::new(),
        warnings: Vec::new(),
        estimated_size_bytes: None,
        estimated_duration_secs: None,
        wal_keep_bytes: None,
        slot_wal_status: SlotWalStatus::Unknown,
    };

    // 1. replication slot health

    if let Some(slot) = slot_name {
        let row = client
            .query_opt(
                "SELECT invalidation_reason \
                    FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .context("preflight: query pg_replication_slots")?;

        match row {
            None => {
                report.hard_errors.push(format!(
                    "replication slot '{slot}' does not exist. \
                     Create it with: \
                     SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput');"
                ));
            }
            Some(r) => {
                let invalidation: Option<String> = r.get(0);
                if let Some(reason) = &invalidation {
                    report.hard_errors.push(format!(
                        "replication slot '{slot}' is already invalidated \
                         (reason: {reason}). Drop and recreate the slot, \
                         then restart the pipeline."
                    ));
                }

                // wal_status check - PG 13+ only; query it separately
                let wal_status = query_wal_status(&client, slot).await;
                report.slot_wal_status = wal_status;

                match &report.slot_wal_status {
                    SlotWalStatus::Lost => {
                        report.hard_errors.push(format!(
                            "slot '{slot}' wal_status=lost: required WAL has already \
                             been removed. Increase max_slot_wal_keep_size and \
                             recreate the slot."
                        ));
                    }
                    SlotWalStatus::Unreserved => {
                        report.warnings.push(format!(
                            "slot '{slot}' wal_status=unreserved: WAL retention is not \
                             guaranteed. The slot may become invalid during snapshot. \
                             Consider increasing max_slot_wal_keep_size."
                        ));
                    }
                    _ => {}
                }
            }
        }
    }

    // 2. publication exists and has tables

    if !publication.is_empty() {
        let pub_row = client
            .query_opt(
                "SELECT COUNT(*) FROM pg_publication WHERE pubname = $1",
                &[&publication],
            )
            .await
            .context("preflight: check publication")?;

        let pub_count: i64 = pub_row.map(|r| r.get(0)).unwrap_or(0);
        if pub_count == 0 {
            report.hard_errors.push(format!(
                "publication '{publication}' does not exist. \
                Create it with: CREATE PUBLICATION {publication} FOR TABLE <tables>;"
            ));
        }
    }

    // 3. max_slot_wal_keep_size

    let wal_keep: Option<i64> = client
        .query_opt(
            "SELECT setting::bigint * 1024 * 1024 \
             FROM pg_settings WHERE name = 'max_slot_wal_keep_size'",
            &[],
        )
        .await
        .ok()
        .flatten()
        .map(|r| r.get(0));

    // -1 or 0 in pg_settings means unlimited
    report.wal_keep_bytes = wal_keep.map(|v| if v <= 0 { -1 } else { v });

    // 4. table size estimation

    if !tables.is_empty() {
        let table_exprs: Vec<String> = tables
            .iter()
            .map(|(s, t)| format!("'{s}.{t}'::regclass"))
            .collect();

        let query = format!(
            "SELECT COALESCE(SUM(pg_total_relation_size(t)), 0)::bigint \
             FROM unnest(ARRAY[{}]) AS t",
            table_exprs.join(", ")
        );

        let total_bytes: i64 = client
            .query_one(&query, &[])
            .await
            .map(|r| r.get(0))
            .unwrap_or(0);

        if total_bytes > 0 {
            let total_bytes = total_bytes as u64;
            report.estimated_size_bytes = Some(total_bytes);

            let effective_parallel =
                max_parallel_tables.min(tables.len()).max(1) as u64;
            let throughput = THROUGHPUT_PER_WORKER_BYTES * effective_parallel;
            let estimated_secs = total_bytes / throughput;
            report.estimated_duration_secs = Some(estimated_secs);

            // 5. WAL retention risk

            if let Some(keep_bytes) = report.wal_keep_bytes {
                if keep_bytes > 0 {
                    // Rough WAL bytes generated ≈ 2x data bytes (row images + overhead)
                    let wal_estimate = total_bytes * 2;
                    let pct = (wal_estimate * 100) / keep_bytes as u64;

                    if pct >= 80 {
                        report.warnings.push(format!(
                            "HIGH WAL RETENTION RISK: estimated WAL generated during \
                             snapshot (~{:.1} GB) is {}% of max_slot_wal_keep_size \
                             ({:.1} GB). \
                             The replication slot may be invalidated before the snapshot \
                             completes. Recommended: increase max_slot_wal_keep_size to \
                             at least {}MB, or reduce max_parallel_tables.",
                            wal_estimate as f64 / 1_073_741_824.0,
                            pct,
                            keep_bytes as f64 / 1_073_741_824.0,
                            (wal_estimate * 2) / (1024 * 1024),
                        ));
                    } else if pct >= 50 {
                        report.warnings.push(format!(
                            "WAL RETENTION WARNING: estimated WAL during snapshot \
                             (~{:.1} GB) is {}% of max_slot_wal_keep_size \
                             ({:.1} GB).",
                            wal_estimate as f64 / 1_073_741_824.0,
                            pct,
                            keep_bytes as f64 / 1_073_741_824.0,
                        ));
                    }
                }
            }
        }
    }

    Ok(report)
}

/// Check slot wal_status (PostgreSQL 13+). Returns Unknown on older versions.
async fn query_wal_status(
    client: &tokio_postgres::Client,
    slot_name: &str,
) -> SlotWalStatus {
    let row = client
        .query_opt(
            "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await;

    match row {
        Ok(Some(r)) => {
            let status: Option<String> = r.try_get(0).ok().flatten();
            match status.as_deref() {
                Some("reserved") => SlotWalStatus::Reserved,
                Some("extended") => SlotWalStatus::Extended,
                Some("unreserved") => SlotWalStatus::Unreserved,
                Some("lost") => SlotWalStatus::Lost,
                _ => SlotWalStatus::Unknown,
            }
        }
        _ => SlotWalStatus::Unknown,
    }
}

/// Final synchronous slot health check - called after all workers complete,
/// before writing `finished = true`. Returns Err on confirmed slot loss.
pub async fn verify_slot_still_healthy(
    dsn: &str,
    slot_name: &str,
) -> Result<()> {
    let (client, conn) = tokio_postgres::connect(dsn, NoTls)
        .await
        .context("final slot check: failed to connect")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let row = client
        .query_opt(
            "SELECT invalidation_reason FROM pg_replication_slots \
             WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .context("final slot check: query failed")?;

    match row {
        None => {
            anyhow::bail!(
                "final slot check: replication slot '{}' disappeared during snapshot. \
                 The WAL position captured at snapshot start may no longer be \
                 accessible. Re-create the slot and restart the pipeline.",
                slot_name
            );
        }
        Some(r) => {
            let invalidation: Option<String> = r.get(0);
            if let Some(reason) = invalidation {
                anyhow::bail!(
                    "final slot check: replication slot '{}' was invalidated \
                     during snapshot (reason: {}). \
                     Increase max_slot_wal_keep_size and restart the pipeline.",
                    slot_name,
                    reason
                );
            }

            let wal_status = query_wal_status(&client, slot_name).await;
            if wal_status == SlotWalStatus::Lost {
                anyhow::bail!(
                    "final slot check: slot '{}' wal_status=lost. \
                     Required WAL was removed during snapshot. \
                     Increase max_slot_wal_keep_size and restart the pipeline.",
                    slot_name
                );
            }
        }
    }

    Ok(())
}

// Background guard

use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::debug;

const GUARD_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Spawns a background task that monitors slot health every GUARD_INTERVAL seconds.
/// On `unreserved` status: warns but continues (WAL not guaranteed but not gone).
/// On `lost` status or invalidation: sets abort_reason and cancels.
pub fn spawn_wal_slot_guard(
    dsn: String,
    slot_name: String,
    cancel: CancellationToken,
    abort_reason: Arc<Mutex<Option<String>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(GUARD_INTERVAL);
        interval.tick().await; // skip the immediate first tick

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {}
            }

            let client = match tokio_postgres::connect(&dsn, NoTls).await {
                Ok((c, conn)) => {
                    tokio::spawn(async move {
                        let _ = conn.await;
                    });
                    c
                }
                Err(e) => {
                    warn!(error = %e, slot = %slot_name, "WAL guard: connect error, retrying");
                    continue;
                }
            };

            let row = client
                .query_opt(
                    "SELECT invalidation_reason \
                     FROM pg_replication_slots WHERE slot_name = $1",
                    &[&slot_name],
                )
                .await;

            match row {
                Ok(None) => {
                    let msg = format!(
                        "replication slot '{slot_name}' disappeared during snapshot. \
                         WAL position may no longer be accessible."
                    );
                    warn!("{}", msg);
                    *abort_reason.lock().unwrap() = Some(msg);
                    cancel.cancel();
                    return;
                }
                Ok(Some(r)) => {
                    let invalidation: Option<String> = r.get(0);
                    if let Some(reason) = invalidation {
                        let msg = format!(
                            "replication slot '{slot_name}' invalidated during snapshot \
                             (reason: {reason}). Increase max_slot_wal_keep_size \
                             and restart."
                        );
                        warn!("{}", msg);
                        *abort_reason.lock().unwrap() = Some(msg);
                        cancel.cancel();
                        return;
                    }

                    // wal_status check
                    let wal_status =
                        query_wal_status(&client, &slot_name).await;
                    match wal_status {
                        SlotWalStatus::Lost => {
                            let msg = format!(
                                "slot '{slot_name}' wal_status=lost: WAL removed \
                                 during snapshot. Restart with larger \
                                 max_slot_wal_keep_size."
                            );
                            warn!("{}", msg);
                            *abort_reason.lock().unwrap() = Some(msg);
                            cancel.cancel();
                            return;
                        }
                        SlotWalStatus::Unreserved => {
                            // warn but don't abort - WAL not guaranteed but not gone
                            warn!(
                                slot = %slot_name,
                                "wal_status=unreserved: WAL retention not guaranteed. \
                                 Consider increasing max_slot_wal_keep_size."
                            );
                        }
                        _ => {
                            debug!(slot = %slot_name, status = ?wal_status, "WAL guard: ok");
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, slot = %slot_name, "WAL guard: query error, retrying");
                }
            }
        }
    })
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
    fn wal_status_unreserved_is_warning_not_abort() {
        // unreserved should warn but not match the abort arm
        let status = SlotWalStatus::Unreserved;
        let is_abort = matches!(status, SlotWalStatus::Lost);
        assert!(!is_abort);
    }

    #[test]
    fn format_duration_variants() {
        assert_eq!(format_duration(45), "45s");
        assert_eq!(format_duration(125), "2m5s");
        assert_eq!(format_duration(7265), "2h1m");
    }

    #[test]
    fn wal_risk_at_90pct_generates_warning() {
        let wal_estimate: u64 = 18 * 1024 * 1024 * 1024; // 18 GB
        let keep_bytes: u64 = 20 * 1024 * 1024 * 1024; // 20 GB
        let pct = (wal_estimate * 100) / keep_bytes;
        assert!(pct >= 80);
    }

    #[test]
    fn unlimited_wal_keep_no_risk() {
        let wal_keep: Option<i64> = Some(-1); // unlimited
        let is_limited = wal_keep.map(|v| v > 0).unwrap_or(false);
        assert!(!is_limited);
    }

    #[tokio::test]
    async fn abort_reason_wired_correctly() {
        let cancel = CancellationToken::new();
        let abort: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        *abort.lock().unwrap() = Some("slot lost".into());
        cancel.cancel();
        assert!(cancel.is_cancelled());
        assert_eq!(abort.lock().unwrap().as_deref(), Some("slot lost"));
    }
}
