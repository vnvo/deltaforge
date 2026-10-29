//! Incident drafts and scoped resolvers shared by the sources.

use std::collections::HashSet;

use deltaforge_core::SourceError;
use deltaforge_core::incident::{
    ActionCode, CauseCode, Component, EvidenceKey as K, IncidentDraft,
    IncidentId, ReasonCode, Retryability, SafetyState, scope_key,
};
use storage::ArcStorageBackend;
use storage::adapters::incidents::{
    IncidentStore, SCHEMA_ACCEPTED, bind_epoch,
};
use tracing::{info, warn};

/// The `schema_drift_blocked` incident around `cause`: `on_schema_drift =
/// halt` stopped at a change of `qualified_table`. Its identity is the table
/// and the change (hashed, never exposed); it is resolved only when that table
/// is accepted at its first use in a later run.
pub(crate) fn schema_drift_blocked(
    source_id: &str,
    qualified_table: &str,
    change: &str,
    cause: SourceError,
) -> SourceError {
    let draft = IncidentDraft::new(
        ReasonCode::SchemaDriftBlocked,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceSchema,
    )
    .discriminate("table", qualified_table)
    .discriminate("change", change)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Table, qualified_table)
            .text(K::SchemaChange, change)
            .text(K::Policy, "halt");
    })
    .with_actions(&[
        ActionCode::ReviewSchemaChange,
        ActionCode::RestartWithAdapt,
    ])
    .resolved_by_scope(qualified_table);
    SourceError::incident(draft, cause)
}

/// The `source_decode_failed` incident: a value of `column` (declared
/// `source_type`) of `qualified_table` that its type cannot decode exactly.
/// `details` (never the value) explains why; the source stops before the
/// value's transaction.
pub(crate) fn source_decode_failed(
    source_id: &str,
    qualified_table: &str,
    column: &str,
    source_type: &str,
    details: &str,
) -> SourceError {
    let draft = IncidentDraft::new(
        ReasonCode::SourceDecodeFailed,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceSchema,
    )
    .discriminate("table", qualified_table)
    .discriminate("column", column)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Table, qualified_table)
            .text(K::Column, column)
            .text(K::SourceType, source_type)
            .text(K::ReasonClass, "value_not_decodable");
    })
    .with_actions(&[ActionCode::InspectLogs]);
    SourceError::incident(
        draft,
        SourceError::Schema {
            details: format!(
                "{qualified_table}: cannot decode column `{column}` \
                 ({source_type}): {details}"
            )
            .into(),
        },
    )
}

/// An activation window's batched capture could not lock its tables'
/// metadata within its attempts (`docs/design/mysql-proof-seek.md`,
/// "Capture contention fails closed"). Nothing of the window was
/// dispatched; the checkpoint stays at `at`, the boundary before it. A
/// restart (the normal recovery policy) retries once the schema change
/// holding the lock is done.
pub(crate) fn mysql_capture_contention(
    source_id: &str,
    tables: &[(String, String)],
    attempts: u32,
    code: u16,
    at: &crate::mysql::MySqlCheckpoint,
    details: &str,
) -> SourceError {
    let first = tables
        .first()
        .map(|(d, t)| format!("{d}.{t}"))
        .unwrap_or_default();
    let class = match code {
        1213 => "deadlock",
        _ => "lock_wait_timeout",
    };
    let position = match &at.gtid_set {
        Some(g) => g.clone(),
        None => format!("{}:{}", at.file, at.pos),
    };
    let draft = IncidentDraft::new(
        ReasonCode::MysqlCaptureContention,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::AutoRetry,
        SafetyState::HaltedSafe,
        CauseCode::SourceSchema,
    )
    .discriminate("boundary", &position)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Table, &first)
            .count(K::BlockingCount, tables.len() as u64)
            .count(K::Attempts, u64::from(attempts))
            .text(K::ReasonClass, class)
            .text(K::CheckpointPosition, &position);
    })
    .with_actions(&[ActionCode::InspectLogs]);
    SourceError::incident(
        draft,
        SourceError::Schema {
            details: format!(
                "activation window: the metadata of {} table(s) (first {first}) \
                 could not be locked for their schema capture after {attempts} \
                 attempt(s): {details}",
                tables.len()
            )
            .into(),
        },
    )
}

/// A schema capture that got no control-plane connection within its wait
/// (`mysql_control`): `scope` names the bound that stayed full (`runner` or
/// `source`). Nothing that needed the capture was delivered or recorded;
/// the checkpoint stays at `at`. A restart retries once connections free
/// up. No DSN or credential is part of it.
pub(crate) fn mysql_control_plane_saturated(
    source_id: &str,
    what: &str,
    tables: &[(String, String)],
    scope: &str,
    at: &crate::mysql::MySqlCheckpoint,
) -> SourceError {
    let first = tables
        .first()
        .map(|(d, t)| format!("{d}.{t}"))
        .unwrap_or_default();
    let position = window_position(at);
    let draft = IncidentDraft::new(
        ReasonCode::MysqlControlPlaneSaturated,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::AutoRetry,
        SafetyState::HaltedSafe,
        CauseCode::SourceBackpressure,
    )
    .discriminate("boundary", &position)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Table, &first)
            .count(K::BlockingCount, tables.len() as u64)
            .text(K::ReasonClass, scope)
            .text(K::CheckpointPosition, &position);
    })
    .with_actions(&[ActionCode::InspectLogs]);
    SourceError::incident(
        draft,
        SourceError::Connect {
            details: format!(
                "{what}: no MySQL control-plane connection for the schema \
                 capture of {} table(s) (first {first}): the {scope} bound \
                 stayed full",
                tables.len()
            )
            .into(),
        },
    )
}

/// A table whose activation timeline is over the per-timeline limit
/// (`mysql_history`): never loaded whole, so its rows cannot be decoded.
/// Nothing of them was dispatched; the checkpoint stays at `at`. An
/// operator must act (the timeline does not shrink by itself).
pub(crate) fn mysql_timeline_too_large(
    source_id: &str,
    e: &crate::mysql::TimelineTooLarge,
    at: &crate::mysql::MySqlCheckpoint,
) -> SourceError {
    let position = window_position(at);
    let digest =
        storage::state_key_digest(crate::mysql::ACTIVATION_NS_NAME, &e.stream);
    let draft = IncidentDraft::new(
        ReasonCode::MysqlTimelineTooLarge,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceSchema,
    )
    .discriminate("table", &e.table)
    .with_evidence(|ev| {
        ev.text(K::SourceId, source_id)
            .text(K::Table, &e.table)
            .count(K::StateLimit, crate::mysql::TIMELINE_MAX_BYTES)
            .text(K::KeyDigest, &digest)
            .text(K::CheckpointPosition, &position);
        if let Some((records, bytes)) = e.measured {
            ev.count(K::ReadItems, records).count(K::StateBytes, bytes);
        }
    })
    .with_actions(&[ActionCode::InspectLogs]);
    SourceError::incident(
        draft,
        SourceError::Schema {
            details: format!("{e}; no row of it was dispatched").into(),
        },
    )
}

/// The checkpoint position a window stays before, as evidence text.
fn window_position(at: &crate::mysql::MySqlCheckpoint) -> String {
    match &at.gtid_set {
        Some(g) => g.clone(),
        None => format!("{}:{}", at.file, at.pos),
    }
}

/// A table no activation window can take (`mysql_window_budget::Invalid`):
/// its state exceeds the per-table limit, or its registration needs more
/// reads than a plan allows. Nothing of the window was written or
/// dispatched; the checkpoint stays before it. An operator must act.
pub(crate) fn mysql_table_too_large(
    source_id: &str,
    db: &str,
    table: &str,
    invalid: &crate::mysql::WindowInvalid,
    at: &crate::mysql::MySqlCheckpoint,
) -> SourceError {
    use crate::mysql::WindowInvalid;
    let name = format!("{db}.{table}");
    let position = window_position(at);
    let (code, details) = match invalid {
        WindowInvalid::StateTooLarge { bytes, limit } => (
            ReasonCode::MysqlTableStateTooLarge,
            format!(
                "table {name}'s state is {bytes} bytes, over the {limit}-byte \
                 per-table limit"
            ),
        ),
        WindowInvalid::RegistryTooLarge { read_items, limit } => (
            ReasonCode::MysqlTableRegistryTooLarge,
            format!(
                "table {name}'s registration needs {read_items} reads, over \
                 one plan's {limit}"
            ),
        ),
    };
    let draft = IncidentDraft::new(
        code,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceSchema,
    )
    .discriminate("table", &name)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Table, &name)
            .text(K::CheckpointPosition, &position);
        match invalid {
            WindowInvalid::StateTooLarge { bytes, limit } => {
                e.count(K::StateBytes, *bytes).count(K::StateLimit, *limit);
            }
            WindowInvalid::RegistryTooLarge { read_items, limit } => {
                e.count(K::ReadItems, *read_items)
                    .count(K::StateLimit, *limit);
            }
        }
    })
    .with_actions(&[ActionCode::InspectLogs]);
    SourceError::incident(
        draft,
        SourceError::Schema {
            details: format!(
                "activation window: {details}; nothing of the window was \
                 admitted or dispatched and the checkpoint stays before it"
            )
            .into(),
        },
    )
}

/// A new state item of `db.table` over the store's item limit
/// (`storage::StateItemTooLarge`): refused before anything of its batch was
/// written. The checkpoint stays before the table's rows.
pub(crate) fn state_item_too_large(
    source_id: &str,
    db: &str,
    table: &str,
    too: &storage::StateItemTooLarge,
    at: &crate::mysql::MySqlCheckpoint,
) -> SourceError {
    let name = format!("{db}.{table}");
    let position = window_position(at);
    let draft = IncidentDraft::new(
        ReasonCode::StateItemTooLarge,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceSchema,
    )
    .discriminate("key_digest", &too.key_digest)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Table, &name)
            .count(K::StateBytes, too.size as u64)
            .count(K::StateLimit, storage::MAX_STATE_ITEM_BYTES as u64)
            .text(K::KeyDigest, &too.key_digest)
            .text(K::CheckpointPosition, &position);
    })
    .with_actions(&[ActionCode::InspectLogs]);
    SourceError::incident(
        draft,
        SourceError::Schema {
            details: format!("table {name}: {too}").into(),
        },
    )
}

/// A replication message over the source's configured limit. The stream is
/// abandoned without a reconnect: a reconnect with the same limit would meet
/// the same message again. Evidence is the sizes and the source only, never
/// the payload. `read_position` (the stream's read position before the
/// message) distinguishes the occurrence and is not exposed.
pub(crate) fn message_too_large(
    source_id: &str,
    read_position: &str,
    payload_bytes: usize,
    limit_bytes: usize,
) -> SourceError {
    let draft = IncidentDraft::new(
        ReasonCode::PgMessageTooLarge,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceIncompatible,
    )
    .discriminate("read_position", read_position)
    .discriminate("limit", limit_bytes.to_string())
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .count(K::MessageBytes, payload_bytes as u64)
            .count(K::MessageLimit, limit_bytes as u64);
    })
    .with_actions(&[ActionCode::RaiseMessageLimit]);
    SourceError::incident(
        draft,
        SourceError::Incompatible {
            details: format!(
                "replication message of {payload_bytes} bytes exceeds \
                 max_message_bytes {limit_bytes}"
            )
            .into(),
        },
    )
}

/// Record `draft`, a condition the source is retrying automatically, so the
/// pipeline reports it while the source retries. It is bound to the current
/// recovery epoch: the identity the supervisor gives the draft the source
/// stops on, so an exhausted retry reclassifies the same incident. Returns
/// its identity when recorded. Never fails the caller (the retry goes on;
/// the stop is recorded by the supervisor).
pub(crate) async fn record_retrying(
    store: &IncidentStore,
    draft: &IncidentDraft,
) -> Option<IncidentId> {
    let bound = match store.recovery_epoch().await {
        Ok(epoch) => bind_epoch(draft.clone(), epoch),
        Err(e) => {
            warn!(
                error = %format!("{e:#}"),
                "incident store unreadable; the retry is not reported"
            );
            return None;
        }
    };
    match store.raise(&bound, 1).await {
        Ok(raised) => Some(raised.record().incident_id.clone()),
        Err(e) => {
            warn!(
                error = %format!("{e:#}"),
                "could not record the retrying incident"
            );
            None
        }
    }
}

/// The source stopped on purpose while retrying: withdraw its auto-retry
/// incident (`operation_cancelled`), so an intentional stop leaves nothing
/// blocking. Never fails the caller.
pub(crate) async fn cancel_retrying(
    store: &IncidentStore,
    retrying: Option<IncidentId>,
) {
    let Some(id) = retrying else {
        return;
    };
    if let Err(e) = store.cancel_auto_retry(&id).await {
        warn!(
            error = %format!("{e:#}"),
            "could not withdraw the retrying incident; a verified start \
             resolves it"
        );
    }
}

/// Resolves a source's open `schema_drift_blocked` incidents when their table
/// is accepted at its first use. The open scopes are read once per run (one
/// bounded list), so accepting a table is an in-memory check unless it has an
/// open drift incident.
pub(crate) struct DriftResolver {
    store: IncidentStore,
    component: Component,
    open: Option<HashSet<String>>,
}

impl DriftResolver {
    pub(crate) fn new(
        backend: ArcStorageBackend,
        pipeline: &str,
        source_id: &str,
    ) -> Self {
        Self {
            store: IncidentStore::new(backend, pipeline),
            component: Component::Source {
                id: source_id.to_string(),
            },
            open: None,
        }
    }

    /// `qualified_table` was accepted (its first use matched its durable
    /// schema, or an adapt reload succeeded). Never fails the caller: a store
    /// error leaves the incident open for a later run.
    pub(crate) async fn accepted(&mut self, qualified_table: &str) {
        if self.open.is_none() {
            match self.store.list().await {
                Ok(records) => {
                    self.open = Some(
                        records
                            .into_iter()
                            .filter(|r| {
                                !r.status.is_resolved()
                                    && r.reason_code
                                        == ReasonCode::SchemaDriftBlocked
                                    && r.component == self.component
                            })
                            .filter_map(|r| r.resolve_scope)
                            .collect(),
                    );
                }
                Err(e) => {
                    warn!(
                        error = %format!("{e:#}"),
                        "incident store unreadable; drift incidents stay open"
                    );
                    return;
                }
            }
        }
        let key = scope_key(&self.component, qualified_table);
        if !self.open.as_mut().is_some_and(|open| open.remove(&key)) {
            return;
        }
        match self
            .store
            .resolve_matching(
                ReasonCode::SchemaDriftBlocked,
                &self.component,
                Some(&key),
                SCHEMA_ACCEPTED,
            )
            .await
        {
            Ok(n) if n > 0 => info!(
                table = %qualified_table,
                "schema accepted: drift incident resolved"
            ),
            Ok(_) => {}
            Err(e) => warn!(
                table = %qualified_table,
                error = %format!("{e:#}"),
                "could not resolve the drift incident; it stays open"
            ),
        }
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use storage::adapters::incidents::IncidentStore;

    /// Exactly one incident, the auto-retry one, withdrawn as
    /// `operation_cancelled` and no longer blocking.
    pub(crate) async fn assert_cancelled_withdrawn(incidents: &IncidentStore) {
        use storage::adapters::incidents::{IncidentStatus, Resolution};
        let all = incidents.list().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].retryability.as_str(), "auto_retry");
        assert!(matches!(
            all[0].status,
            IncidentStatus::Resolved {
                by: Resolution::OperationCancelled,
                ..
            }
        ));
        assert!(!all[0].is_blocking());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use storage::MemoryStorageBackend;
    use storage::adapters::incidents::{IncidentStatus, Resolution};

    /// A drift incident is resolved when its table is accepted, and only then
    /// (another table's acceptance leaves it open).
    #[tokio::test]
    async fn an_accepted_table_resolves_only_its_drift_incident() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let store = IncidentStore::new(Arc::clone(&backend), "p");
        let raise = |table: &str| {
            let store = store.clone();
            let table = table.to_string();
            async move {
                let err = schema_drift_blocked(
                    "src",
                    &table,
                    "columns [id] -> [id, x]",
                    SourceError::Schema {
                        details: "x".into(),
                    },
                );
                store
                    .raise(err.draft().unwrap(), 1)
                    .await
                    .unwrap()
                    .record()
                    .incident_id
                    .clone()
            }
        };
        let orders = raise("public.orders").await;
        let items = raise("public.items").await;

        let mut resolver = DriftResolver::new(Arc::clone(&backend), "p", "src");
        resolver.accepted("public.other").await;
        resolver.accepted("public.orders").await;
        match store.get(&orders).await.unwrap().unwrap().status {
            IncidentStatus::Resolved {
                by: Resolution::VerifiedRecovery { check },
                ..
            } => assert_eq!(check, SCHEMA_ACCEPTED),
            s => panic!("{s:?}"),
        }
        assert!(
            !store
                .get(&items)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved()
        );
        // Another source's resolver never touches it.
        let mut other = DriftResolver::new(backend, "p", "other-src");
        other.accepted("public.items").await;
        assert!(
            !store
                .get(&items)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved()
        );
    }
}

/// A catalog session could not be proven to reach the stream's node and
/// walsender at the target position (halted: nothing was used).
pub(crate) fn catalog_visibility_unproven(
    source_id: &str,
    slot: &str,
    target: &str,
    class: &str,
) -> SourceError {
    let draft = IncidentDraft::new(
        ReasonCode::PgCatalogVisibilityUnproven,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceLineage,
    )
    .discriminate("slot", slot)
    .discriminate("class", class)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Slot, slot)
            .text(K::TargetPosition, target)
            .text(K::ReasonClass, class);
    })
    .with_actions(&[ActionCode::VerifyEndpoint]);
    SourceError::incident(
        draft,
        SourceError::Lineage {
            details: format!(
                "catalog session not proven to reach the stream of slot \
                 {slot} at {target} ({class})"
            )
            .into(),
        },
    )
}

/// Tables a pipeline needing complete rows cannot capture without REPLICA
/// IDENTITY FULL (`pg_table_unsupported`, class
/// `toast_without_full_identity`). Evidence names the first table and counts
/// them all; `details` (the error) names every one. No row value is part of
/// either.
pub(crate) fn toast_without_full_identity(
    source_id: &str,
    class: &str,
    first_table: &str,
    tables: usize,
    details: String,
) -> SourceError {
    let draft = IncidentDraft::new(
        ReasonCode::PgTableUnsupported,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceIncompatible,
    )
    .discriminate("table", first_table)
    .discriminate("class", class)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Table, first_table)
            .text(K::ReasonClass, class)
            .count(K::BlockingCount, tables as u64);
    })
    .with_actions(&[ActionCode::ReviewSchemaChange]);
    SourceError::incident(
        draft,
        SourceError::Incompatible {
            details: details.into(),
        },
    )
}

/// A tracked table uses an unsupported feature (halted before its next row).
pub(crate) fn table_unsupported(
    source_id: &str,
    qualified_table: &str,
    class: &str,
    details: String,
) -> SourceError {
    let draft = IncidentDraft::new(
        ReasonCode::PgTableUnsupported,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceIncompatible,
    )
    .discriminate("table", qualified_table)
    .discriminate("class", class)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Table, qualified_table)
            .text(K::ReasonClass, class);
    })
    .with_actions(&[ActionCode::ReviewSchemaChange]);
    SourceError::incident(
        draft,
        SourceError::Incompatible {
            details: details.into(),
        },
    )
}

/// A capture-time annotation could not be recorded (running degraded).
pub(crate) fn annotation_unavailable(
    source_id: &str,
    qualified_table: &str,
    class: &str,
) -> IncidentDraft {
    IncidentDraft::new(
        ReasonCode::PgAnnotationUnavailable,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::AutoRetry,
        SafetyState::RunningDegraded,
        CauseCode::SourceOther,
    )
    .discriminate("table", qualified_table)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Table, qualified_table)
            .text(K::ReasonClass, class);
    })
    .resolved_by_scope(qualified_table)
}

/// The publication is unregistered or changed since the registration the
/// source accepted (halted before streaming).
pub(crate) fn publication_changed(
    source_id: &str,
    publication: &str,
    class: &str,
    details: String,
) -> SourceError {
    let draft = IncidentDraft::new(
        ReasonCode::PgPublicationChanged,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceSchema,
    )
    .discriminate("publication", publication)
    .discriminate("class", class)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Publication, publication)
            .text(K::ReasonClass, class);
    })
    .with_actions(&[ActionCode::PublicationMaintenance]);
    SourceError::incident(
        draft,
        SourceError::Schema {
            details: details.into(),
        },
    )
}

/// The database's publication enforcement is missing or tampered with
/// (halted before streaming).
pub(crate) fn publication_enforcement(
    source_id: &str,
    publication: &str,
    violations: &[String],
) -> SourceError {
    let list = violations.join("; ");
    let draft = IncidentDraft::new(
        ReasonCode::PgPublicationEnforcement,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceIncompatible,
    )
    .discriminate("publication", publication)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Publication, publication)
            .text(K::ReasonClass, &list);
    })
    .with_actions(&[
        ActionCode::PublicationMaintenance,
        ActionCode::InspectLogs,
    ]);
    SourceError::incident(
        draft,
        SourceError::Incompatible {
            details: format!(
                "publication enforcement of {publication}'s database: {list}"
            )
            .into(),
        },
    )
}
