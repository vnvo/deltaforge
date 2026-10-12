//! One destination, one source relation, durably.
//!
//! A destination a sink writes rows to by key (a ClickHouse table, an
//! Elasticsearch index) holds one source relation. Before a sink session
//! first writes to a destination it verifies the destination's durable
//! binding ([`deltaforge_core::destination`]): the destination is bound to
//! this relation (binding it atomically when it has no binding and holds no
//! rows) and is the same target it was bound as. Anything else refuses the
//! batch before any of it is delivered, with the sink's checkpoint held.
//!
//! Verified destinations are remembered per session in a bounded set; one
//! evicted (or any after a restart) is verified against the durable binding
//! again.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};

use deltaforge_core::destination::{
    ConflictClass, DestinationBinding, DestinationBindingStore,
    destination_conflict,
};
use deltaforge_core::{SinkError, SourceRelationIdentity};

/// What a destination is now: its own identity, and whether it holds rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetState {
    /// The target's identity (an index UUID, a table UUID).
    pub uid: String,
    pub has_rows: bool,
}

/// How many verified destinations a session remembers.
pub(crate) const VERIFIED_CAPACITY: usize = 1024;

pub(crate) struct DestinationGuard {
    sink_id: String,
    store: Arc<dyn DestinationBindingStore>,
    verified: Mutex<Verified>,
}

/// Verified `(destination -> relation)` pairs, oldest evicted first.
struct Verified {
    capacity: usize,
    map: HashMap<String, String>,
    order: VecDeque<String>,
}

impl DestinationGuard {
    pub(crate) fn new(
        sink_id: &str,
        store: Arc<dyn DestinationBindingStore>,
    ) -> Self {
        Self::with_capacity(sink_id, store, VERIFIED_CAPACITY)
    }

    pub(crate) fn with_capacity(
        sink_id: &str,
        store: Arc<dyn DestinationBindingStore>,
        capacity: usize,
    ) -> Self {
        DestinationGuard {
            sink_id: sink_id.to_string(),
            store,
            verified: Mutex::new(Verified {
                capacity: capacity.max(1),
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
        }
    }

    /// Verify that `relation` may be written to `destination`; `target`
    /// reads the destination's current state (called only when the pair is
    /// not verified in this session).
    pub(crate) async fn verify<F, Fut>(
        &self,
        destination: &str,
        relation: &SourceRelationIdentity,
        target: F,
    ) -> Result<(), SinkError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<TargetState, SinkError>>,
    {
        let canonical = relation.canonical();
        if self
            .verified
            .lock()
            .expect("verified lock")
            .map
            .get(destination)
            == Some(&canonical)
        {
            return Ok(());
        }
        let state = target().await?;
        let unavailable = |e: anyhow::Error| SinkError::Backpressure {
            details: format!(
                "sink {}: destination binding store unavailable: {e:#}",
                self.sink_id
            )
            .into(),
        };
        let binding =
            match self.store.get(destination).await.map_err(unavailable)? {
                Some(b) => b,
                None if state.has_rows => {
                    return Err(destination_conflict(
                        &self.sink_id,
                        destination,
                        relation,
                        None,
                        ConflictClass::UnboundRows,
                    ));
                }
                None => self
                    .store
                    .bind(
                        destination,
                        &DestinationBinding {
                            relation: canonical.clone(),
                            target_uid: state.uid.clone(),
                        },
                    )
                    .await
                    .map_err(unavailable)?,
            };
        if binding.relation != canonical {
            return Err(destination_conflict(
                &self.sink_id,
                destination,
                relation,
                Some(&binding.relation),
                ConflictClass::OtherRelation,
            ));
        }
        if binding.target_uid != state.uid {
            return Err(destination_conflict(
                &self.sink_id,
                destination,
                relation,
                Some(&binding.relation),
                ConflictClass::TargetReplaced,
            ));
        }
        let mut v = self.verified.lock().expect("verified lock");
        if v.map.insert(destination.to_string(), canonical).is_none() {
            v.order.push_back(destination.to_string());
            while v.order.len() > v.capacity {
                if let Some(old) = v.order.pop_front() {
                    v.map.remove(&old);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::destination::MemoryDestinationBindings;
    use deltaforge_core::incident::ReasonCode;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn rel(ns: &str) -> SourceRelationIdentity {
        SourceRelationIdentity {
            connector: "postgresql".into(),
            source: "s".into(),
            database: "db".into(),
            namespace: ns.into(),
            table: "t".into(),
        }
    }

    fn state(uid: &str, has_rows: bool) -> TargetState {
        TargetState {
            uid: uid.into(),
            has_rows,
        }
    }

    fn class(e: &SinkError) -> Option<String> {
        let d = e.draft()?;
        assert_eq!(d.reason_code, ReasonCode::SinkDestinationConflict);
        d.discriminator.get("class").cloned()
    }

    #[tokio::test]
    async fn a_binding_survives_restart_whatever_comes_first() {
        let store: Arc<dyn DestinationBindingStore> =
            Arc::new(MemoryDestinationBindings::default());
        let first = DestinationGuard::new("es", Arc::clone(&store));
        first
            .verify("idx", &rel("a"), || async { Ok(state("u1", false)) })
            .await
            .unwrap();
        // A restart: a new session whose first event is the other relation.
        let restarted = DestinationGuard::new("es", Arc::clone(&store));
        let e = restarted
            .verify("idx", &rel("b"), || async { Ok(state("u1", true)) })
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("bound_to_another_relation"));
        let text = format!("{e}");
        assert!(text.contains(&rel("a").canonical()), "{text}");
        assert!(text.contains(&rel("b").canonical()), "{text}");
        // The bound relation still writes.
        restarted
            .verify("idx", &rel("a"), || async { Ok(state("u1", true)) })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn concurrent_first_writers_agree_on_one_relation() {
        let store: Arc<dyn DestinationBindingStore> =
            Arc::new(MemoryDestinationBindings::default());
        let mut tasks = Vec::new();
        for i in 0..16 {
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                let g = DestinationGuard::new("ch", store);
                let ns = if i % 2 == 0 { "a" } else { "b" };
                g.verify("db.t", &rel(ns), || async { Ok(state("u", false)) })
                    .await
                    .is_ok()
                    .then_some(ns)
            }));
        }
        let mut winners = std::collections::BTreeSet::new();
        for t in tasks {
            if let Some(ns) = t.await.unwrap() {
                winners.insert(ns);
            }
        }
        assert_eq!(winners.len(), 1, "{winners:?}");
    }

    #[tokio::test]
    async fn tampering_and_replacement_fail_closed() {
        let store = Arc::new(MemoryDestinationBindings::default());
        let g = DestinationGuard::new("es", store.clone());
        g.verify("idx", &rel("a"), || async { Ok(state("u1", false)) })
            .await
            .unwrap();
        // The index was deleted and recreated (another UUID).
        let s2 = DestinationGuard::new("es", store.clone());
        let e = s2
            .verify("idx", &rel("a"), || async { Ok(state("u2", false)) })
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("target_replaced"));
        // A target holding rows without a binding (a removed binding, or
        // data from elsewhere).
        let empty = Arc::new(MemoryDestinationBindings::default());
        let s3 = DestinationGuard::new("es", empty);
        let e = s3
            .verify("other", &rel("a"), || async { Ok(state("u", true)) })
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("rows_without_binding"));
        // A binding altered to another relation.
        store
            .bind(
                "tampered",
                &DestinationBinding {
                    relation: rel("z").canonical(),
                    target_uid: "u".into(),
                },
            )
            .await
            .unwrap();
        let e = s2
            .verify("tampered", &rel("a"), || async { Ok(state("u", true)) })
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("bound_to_another_relation"));
    }

    /// A verified destination is not read again; an evicted one is
    /// verified against the durable binding again.
    #[tokio::test]
    async fn eviction_reloads_and_verifies_the_binding() {
        let store = Arc::new(MemoryDestinationBindings::default());
        let g = DestinationGuard::with_capacity("es", store.clone(), 1);
        let reads = Arc::new(AtomicUsize::new(0));
        let read = |uid: &'static str| {
            let reads = Arc::clone(&reads);
            move || async move {
                reads.fetch_add(1, Ordering::SeqCst);
                Ok(state(uid, false))
            }
        };
        g.verify("i1", &rel("a"), read("u1")).await.unwrap();
        g.verify("i1", &rel("a"), read("u1")).await.unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 1, "cached");
        g.verify("i2", &rel("b"), read("u2")).await.unwrap(); // evicts i1
        // i1 was replaced meanwhile: the reload notices.
        let e = g.verify("i1", &rel("a"), read("u9")).await.unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("target_replaced"));
        assert_eq!(reads.load(Ordering::SeqCst), 3);
    }
}
