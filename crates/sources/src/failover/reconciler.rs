//! Schema reconciliation after failover.
//!
//! Diffs the registry's last-known schema against the live catalog on the
//! new primary, then records the result in durable storage before row
//! streaming resumes. No events are emitted - schema changes are reflected
//! in subsequent row events via `schema_version` as normal.
//!
//! # Hard stop vs auto-reconcile
//!
//! | Delta                  | Behaviour                                       |
//! |------------------------|-------------------------------------------------|
//! | Column added           | Recorded, schema loader invalidated, resume     |
//! | Column dropped         | Recorded, schema loader invalidated, resume     |
//! | Column type changed    | Recorded, schema loader invalidated, resume     |
//! | Table dropped          | Recorded, streaming resumes without that table  |
//! | Primary key changed    | Hard stop - requires operator action            |
//!
//! # Idempotency
//!
//! Before reconciling, the run loop calls `already_completed` to check whether
//! a record for the same identity transition already exists in storage. If it
//! does, reconciliation is skipped and the run loop resumes immediately.
//!
//! The record is written *before* returning to the caller so that a crash
//! between the `run()` call and schema loader invalidation re-runs only the
//! cheaper cache-invalidation path, not a full live catalog query.

use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::{ArcStorageBackend, DurableSchemaRegistry};
use tracing::info;

use crate::failover::identity::ServerIdentity;

const NS: &str = "failover";

// ============================================================================
// ColumnSnapshot
// ============================================================================

/// Canonical column representation for diffing, source-agnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSnapshot {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
}

impl From<crate::mysql::mysql_health::LiveColumn> for ColumnSnapshot {
    fn from(c: crate::mysql::mysql_health::LiveColumn) -> Self {
        Self {
            is_primary_key: c.column_key == "PRI",
            name: c.name,
            data_type: c.data_type,
            is_nullable: c.is_nullable,
        }
    }
}

impl From<crate::postgres::postgres_health::LiveColumn> for ColumnSnapshot {
    fn from(c: crate::postgres::postgres_health::LiveColumn) -> Self {
        Self {
            name: c.name,
            data_type: c.data_type,
            is_nullable: c.is_nullable,
            is_primary_key: c.is_primary_key,
        }
    }
}

// ============================================================================
// SchemaDelta
// ============================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SchemaDelta {
    ColumnAdded {
        column: ColumnSnapshot,
    },
    ColumnDropped {
        name: String,
    },
    ColumnTypeChanged {
        name: String,
        old_type: String,
        new_type: String,
    },
    TableDropped,
}

// ============================================================================
// ReconciliationRecord
// ============================================================================

/// Durable record of a completed reconciliation.
///
/// Stored at `KV failover/{source_id}/reconciliation` and appended to
/// `Log failover/{source_id}/history` for audit.
///
/// The `id` is derived from the identity pair so the same failover event
/// always produces the same ID regardless of timing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationRecord {
    /// Stable ID derived from (prev_identity, new_identity).
    pub id: String,
    pub source_id: String,
    pub previous_identity: ServerIdentity,
    pub new_identity: ServerIdentity,
    pub table_results: Vec<TableReconciliationResult>,
    pub reconciled_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableReconciliationResult {
    pub db: String,
    pub table: String,
    pub deltas: Vec<SchemaDelta>,
}

impl ReconciliationRecord {
    pub fn make_id(prev: &ServerIdentity, new: &ServerIdentity) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let input = format!(
            "{}:{}",
            serde_json::to_string(prev).unwrap_or_default(),
            serde_json::to_string(new).unwrap_or_default(),
        );
        let mut h = DefaultHasher::new();
        input.hash(&mut h);
        format!("{:016x}", h.finish())
    }

    fn kv_key(source_id: &str) -> String {
        format!("{source_id}/reconciliation")
    }
}

// ============================================================================
// Pure diff (no I/O)
// ============================================================================

#[derive(Debug)]
pub enum ReconcileOutcome {
    Reconcilable(Vec<SchemaDelta>),
    RequiresStop { reason: String },
}

/// Diff stored schema JSON against live columns for one table.
///
/// `stored`: `schema_json` from `DurableSchemaRegistry::get_latest`. `None` = noop.
/// `live`: `None` = table dropped on new primary.
pub fn reconcile_table(
    stored: Option<&Value>,
    live: Option<&[ColumnSnapshot]>,
) -> ReconcileOutcome {
    let stored_cols = match stored {
        None => return ReconcileOutcome::Reconcilable(vec![]),
        Some(v) => extract_stored_columns(v),
    };

    let live_cols = match live {
        None => {
            return ReconcileOutcome::Reconcilable(vec![
                SchemaDelta::TableDropped,
            ]);
        }
        Some(c) => c,
    };

    let stored_pk: Vec<&str> = stored_cols
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();
    let live_pk: Vec<&str> = live_cols
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();

    if stored_pk != live_pk {
        return ReconcileOutcome::RequiresStop {
            reason: format!(
                "primary key changed: was [{}], now [{}]",
                stored_pk.join(", "),
                live_pk.join(", "),
            ),
        };
    }

    let mut deltas = Vec::new();

    for sc in &stored_cols {
        match live_cols.iter().find(|lc| lc.name == sc.name) {
            None => deltas.push(SchemaDelta::ColumnDropped {
                name: sc.name.clone(),
            }),
            Some(lc) if lc.data_type != sc.data_type => {
                deltas.push(SchemaDelta::ColumnTypeChanged {
                    name: sc.name.clone(),
                    old_type: sc.data_type.clone(),
                    new_type: lc.data_type.clone(),
                })
            }
            _ => {}
        }
    }

    for lc in live_cols {
        if !stored_cols.iter().any(|sc| sc.name == lc.name) {
            deltas.push(SchemaDelta::ColumnAdded { column: lc.clone() });
        }
    }

    ReconcileOutcome::Reconcilable(deltas)
}

fn extract_stored_columns(v: &Value) -> Vec<ColumnSnapshot> {
    let pk_names: Vec<&str> = v
        .get("primary_key")
        .and_then(|pk| pk.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    v.get("columns")
        .and_then(|c| c.as_array())
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|col| {
            let name = col.get("name")?.as_str()?.to_string();
            let data_type = col
                .get("data_type")
                .or_else(|| col.get("column_type"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let is_nullable = col
                .get("is_nullable")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            Some(ColumnSnapshot {
                is_primary_key: pk_names.contains(&name.as_str()),
                name,
                data_type,
                is_nullable,
            })
        })
        .collect()
}

// ============================================================================
// SchemaReconciler
// ============================================================================

pub struct ReconcileInput {
    pub db: String,
    pub table: String,
    /// `None` = table doesn't exist on new primary.
    pub live_columns: Option<Vec<ColumnSnapshot>>,
}

pub struct SchemaReconciler {
    registry: Arc<DurableSchemaRegistry>,
    backend: ArcStorageBackend,
    tenant: String,
}

impl SchemaReconciler {
    pub fn new(
        registry: Arc<DurableSchemaRegistry>,
        backend: ArcStorageBackend,
        tenant: impl Into<String>,
    ) -> Self {
        Self {
            registry,
            backend,
            tenant: tenant.into(),
        }
    }

    /// Check whether this reconciliation already completed for this identity pair.
    ///
    /// Returns `Some(record)` if a matching record exists - the run loop can
    /// skip `run()` and proceed directly to schema loader invalidation + resume.
    pub async fn already_completed(
        &self,
        source_id: &str,
        prev: &ServerIdentity,
        new: &ServerIdentity,
    ) -> Result<Option<ReconciliationRecord>> {
        let expected_id = ReconciliationRecord::make_id(prev, new);
        let bytes = self
            .backend
            .kv_get(NS, &ReconciliationRecord::kv_key(source_id))
            .await
            .context("SchemaReconciler: check existing record")?;

        Ok(bytes
            .and_then(|b| {
                serde_json::from_slice::<ReconciliationRecord>(&b).ok()
            })
            .filter(|r| r.id == expected_id))
    }

    /// Run reconciliation for all tracked tables and persist the record.
    ///
    /// Returns `Err` on any hard stop (PK change).
    /// On success, the caller must call `reload_schema` on any table where
    /// `record.table_results[i].deltas` is non-empty.
    pub async fn run(
        &self,
        source_id: &str,
        prev: &ServerIdentity,
        new: &ServerIdentity,
        tables: &[ReconcileInput],
    ) -> Result<ReconciliationRecord> {
        let mut table_results = Vec::new();

        for input in tables {
            let stored = self
                .registry
                .get_latest(&self.tenant, &input.db, &input.table)
                .map(|sv| sv.schema_json);

            let deltas = match reconcile_table(
                stored.as_ref(),
                input.live_columns.as_deref(),
            ) {
                ReconcileOutcome::RequiresStop { reason } => {
                    anyhow::bail!(
                        "failover reconciliation hard stop for {}.{}: {}",
                        input.db,
                        input.table,
                        reason
                    );
                }
                ReconcileOutcome::Reconcilable(d) => d,
            };

            if !deltas.is_empty() {
                info!(
                    db = %input.db, table = %input.table,
                    deltas = deltas.len(),
                    "schema delta detected after failover"
                );
            }

            table_results.push(TableReconciliationResult {
                db: input.db.clone(),
                table: input.table.clone(),
                deltas,
            });
        }

        let record = ReconciliationRecord {
            id: ReconciliationRecord::make_id(prev, new),
            source_id: source_id.to_string(),
            previous_identity: prev.clone(),
            new_identity: new.clone(),
            table_results,
            reconciled_at: Utc::now(),
        };

        let bytes = serde_json::to_vec(&record)
            .context("SchemaReconciler: serialize record")?;

        // KV: idempotency anchor (overwritten each time for this source)
        self.backend
            .kv_put(NS, &ReconciliationRecord::kv_key(source_id), &bytes)
            .await
            .context("SchemaReconciler: persist record")?;

        // Log: append-only audit trail
        self.backend
            .log_append(NS, &format!("{source_id}/history"), &bytes)
            .await
            .context("SchemaReconciler: append audit log")?;

        Ok(record)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failover::identity::ServerIdentity;
    use crate::mysql::mysql_health::MySqlServerIdentity;
    use serde_json::json;
    use std::sync::Arc;
    use storage::MemoryStorageBackend;

    fn col(name: &str, dt: &str, pk: bool) -> ColumnSnapshot {
        ColumnSnapshot {
            name: name.into(),
            data_type: dt.into(),
            is_nullable: !pk,
            is_primary_key: pk,
        }
    }

    fn stored_schema(cols: &[(&str, &str, bool)]) -> Value {
        let pk: Vec<&str> = cols
            .iter()
            .filter(|(_, _, pk)| *pk)
            .map(|(n, _, _)| *n)
            .collect();
        let columns: Vec<Value> = cols
            .iter()
            .map(|(n, dt, nullable)| {
                json!({
                    "name": n, "data_type": dt, "is_nullable": nullable,
                })
            })
            .collect();
        json!({ "columns": columns, "primary_key": pk })
    }

    fn mysql_id(uuid: &str) -> ServerIdentity {
        ServerIdentity::MySql(MySqlServerIdentity {
            server_uuid: uuid.into(),
        })
    }

    async fn make_reconciler() -> (SchemaReconciler, Arc<DurableSchemaRegistry>)
    {
        let backend = Arc::new(MemoryStorageBackend::new());
        let registry = DurableSchemaRegistry::new(
            Arc::clone(&backend) as ArcStorageBackend
        )
        .await
        .unwrap();
        let reconciler = SchemaReconciler::new(
            Arc::clone(&registry),
            Arc::clone(&backend) as ArcStorageBackend,
            "t1",
        );
        (reconciler, registry)
    }

    // --- Pure diff ---

    #[test]
    fn no_delta_when_schema_unchanged() {
        let s =
            stored_schema(&[("id", "int", true), ("name", "varchar", false)]);
        let live = vec![col("id", "int", true), col("name", "varchar", false)];
        match reconcile_table(Some(&s), Some(&live)) {
            ReconcileOutcome::Reconcilable(d) => assert!(d.is_empty()),
            _ => panic!(),
        }
    }

    #[test]
    fn detects_all_three_column_delta_types() {
        let s = stored_schema(&[
            ("id", "int", true),
            ("score", "int", false),
            ("legacy", "text", false),
        ]);
        let live = vec![
            col("id", "int", true),
            col("score", "bigint", false), // type changed
            col("email", "varchar", false), // added
                                           // legacy dropped
        ];
        match reconcile_table(Some(&s), Some(&live)) {
            ReconcileOutcome::Reconcilable(d) => {
                assert_eq!(d.len(), 3);
                assert!(d.iter().any(|d| matches!(d, SchemaDelta::ColumnDropped { name } if name == "legacy")));
                assert!(d.iter().any(|d| matches!(d, SchemaDelta::ColumnTypeChanged { name, .. } if name == "score")));
                assert!(d.iter().any(|d| matches!(d, SchemaDelta::ColumnAdded { column } if column.name == "email")));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn pk_change_is_hard_stop() {
        let s = stored_schema(&[("id", "int", true)]);
        let live = vec![col("id", "int", false)];
        assert!(matches!(
            reconcile_table(Some(&s), Some(&live)),
            ReconcileOutcome::RequiresStop { .. }
        ));
    }

    #[test]
    fn table_dropped_is_reconcilable() {
        let s = stored_schema(&[("id", "int", true)]);
        match reconcile_table(Some(&s), None) {
            ReconcileOutcome::Reconcilable(d) => {
                assert_eq!(d, vec![SchemaDelta::TableDropped])
            }
            _ => panic!(),
        }
    }

    #[test]
    fn no_stored_schema_is_noop() {
        match reconcile_table(None, Some(&[col("id", "int", true)])) {
            ReconcileOutcome::Reconcilable(d) => assert!(d.is_empty()),
            _ => panic!(),
        }
    }

    // --- SchemaReconciler (storage) ---

    #[tokio::test]
    async fn run_persists_record_and_returns_deltas() {
        let (reconciler, registry) = make_reconciler().await;
        let schema = stored_schema(&[("id", "int", true)]);
        registry
            .register_with_checkpoint(
                "t1", "shop", "orders", "h1", &schema, None,
            )
            .await
            .unwrap();

        let record = reconciler
            .run(
                "src1",
                &mysql_id("server-a"),
                &mysql_id("server-b"),
                &[ReconcileInput {
                    db: "shop".into(),
                    table: "orders".into(),
                    live_columns: Some(vec![
                        col("id", "int", true),
                        col("email", "varchar", false),
                    ]),
                }],
            )
            .await
            .unwrap();

        assert_eq!(record.table_results[0].deltas.len(), 1);
        assert!(matches!(
            &record.table_results[0].deltas[0],
            SchemaDelta::ColumnAdded { column } if column.name == "email"
        ));
    }

    #[tokio::test]
    async fn already_completed_hits_on_same_pair_misses_on_different() {
        let (reconciler, _) = make_reconciler().await;
        let prev = mysql_id("server-a");
        let b = mysql_id("server-b");
        let c = mysql_id("server-c");

        reconciler.run("src1", &prev, &b, &[]).await.unwrap();

        assert!(
            reconciler
                .already_completed("src1", &prev, &b)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            reconciler
                .already_completed("src1", &prev, &c)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn run_errors_on_pk_change() {
        let (reconciler, registry) = make_reconciler().await;
        let schema = stored_schema(&[("id", "int", true)]);
        registry
            .register_with_checkpoint(
                "t1", "shop", "orders", "h1", &schema, None,
            )
            .await
            .unwrap();

        let result = reconciler
            .run(
                "src1",
                &mysql_id("a"),
                &mysql_id("b"),
                &[ReconcileInput {
                    db: "shop".into(),
                    table: "orders".into(),
                    live_columns: Some(vec![col("id", "int", false)]),
                }],
            )
            .await;
        assert!(result.is_err());
    }
}
