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
