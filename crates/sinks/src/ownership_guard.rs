//! A sink session's view of the destinations it owns.
//!
//! Before a session first writes to a destination it claims it
//! ([`deltaforge_core::ownership::claim`]): the claim proves ownership and
//! rotates the fence the destination enforces, and every write then carries
//! that fence. A verified destination is remembered (bounded) with the fence
//! and the exact expectation it was verified for, so the steady-state write
//! path makes no ownership request. A reconnect, a client recreation or a
//! fencing rejection forgets verifications; the next write claims again.
//!
//! Claims of one destination are single-flight within the process: callers
//! wait for one claim and share its result only when they expect exactly
//! the same owner, relation, destination and encoding.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};

use deltaforge_core::SinkError;
use deltaforge_core::ownership::{
    Claimed, Expected, FencedDestination, OwnerIdentity, OwnershipStore, claim,
};

/// How many verified destinations a session remembers.
pub(crate) const VERIFIED_CAPACITY: usize = 1024;

pub(crate) struct OwnershipGuard {
    owner: OwnerIdentity,
    store: Arc<dyn OwnershipStore>,
    verified: Mutex<Verified>,
    /// Single-flight locks by destination key; entries whose lock no claim
    /// holds are pruned, so the map is bounded by claims in flight.
    locks: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
}

/// Verified destinations, oldest evicted first: key -> (expectation
/// digest, claim).
struct Verified {
    capacity: usize,
    map: HashMap<String, (String, Claimed)>,
    order: VecDeque<String>,
}

impl OwnershipGuard {
    pub(crate) fn new(
        owner: OwnerIdentity,
        store: Arc<dyn OwnershipStore>,
    ) -> Self {
        Self::with_capacity(owner, store, VERIFIED_CAPACITY)
    }

    pub(crate) fn with_capacity(
        owner: OwnerIdentity,
        store: Arc<dyn OwnershipStore>,
        capacity: usize,
    ) -> Self {
        OwnershipGuard {
            owner,
            store,
            verified: Mutex::new(Verified {
                capacity: capacity.max(1),
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
            locks: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn owner(&self) -> &OwnerIdentity {
        &self.owner
    }

    /// The state-store key of a destination: the owner's pipeline and sink
    /// (the deployment is the store itself) and the destination's identity.
    pub(crate) fn key(&self, expected: &Expected) -> String {
        format!(
            "{}/{}/{}",
            escape(&self.owner.pipeline),
            escape(&self.owner.sink),
            expected.destination.name()
        )
    }

    /// The fence writes to `expected.destination` must carry: remembered,
    /// or claimed now (once per destination at a time) on the destination
    /// `dest` builds (called only to claim).
    pub(crate) async fn fence<D, F>(
        &self,
        expected: &Expected,
        dest: F,
    ) -> Result<Claimed, SinkError>
    where
        D: FencedDestination,
        F: FnOnce() -> D,
    {
        let key = self.key(expected);
        let digest = expected.digest();
        if let Some(f) = self.cached(&key, &digest) {
            return Ok(f);
        }
        let lock = self.lock_for(&key);
        let _held = lock.lock().await;
        if let Some(f) = self.cached(&key, &digest) {
            return Ok(f);
        }
        let dest = dest();
        let result = claim(&dest, self.store.as_ref(), &key, expected).await;
        match result {
            Ok(claimed) => {
                self.remember(&key, digest, claimed.clone());
                Ok(claimed)
            }
            Err(e) => {
                self.invalidate_key(&key);
                Err(e.into_sink_error(
                    &self.owner.sink,
                    &expected.destination,
                    &expected.relation.identity(),
                ))
            }
        }
    }

    /// Forget every verification (a reconnect, a client recreation).
    pub(crate) fn invalidate_all(&self) {
        let mut v = self.verified.lock().expect("verified lock");
        v.map.clear();
        v.order.clear();
    }

    /// Forget one destination's verification (a fencing rejection).
    pub(crate) fn invalidate(&self, expected: &Expected) {
        self.invalidate_key(&self.key(expected));
    }

    fn invalidate_key(&self, key: &str) {
        let mut v = self.verified.lock().expect("verified lock");
        if v.map.remove(key).is_some() {
            v.order.retain(|k| k != key);
        }
    }

    fn cached(&self, key: &str, digest: &str) -> Option<Claimed> {
        let v = self.verified.lock().expect("verified lock");
        match v.map.get(key) {
            Some((d, claimed)) if d == digest => Some(claimed.clone()),
            _ => None,
        }
    }

    fn remember(&self, key: &str, digest: String, claimed: Claimed) {
        let mut v = self.verified.lock().expect("verified lock");
        if v.map.insert(key.to_string(), (digest, claimed)).is_none() {
            v.order.push_back(key.to_string());
            while v.order.len() > v.capacity {
                if let Some(old) = v.order.pop_front() {
                    v.map.remove(&old);
                }
            }
        }
    }

    fn lock_for(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().expect("locks");
        if let Some(l) = locks.get(key).and_then(Weak::upgrade) {
            return l;
        }
        // Prune locks no claim holds any more.
        locks.retain(|_, w| w.strong_count() > 0);
        let l = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(key.to_string(), Arc::downgrade(&l));
        l
    }

    #[cfg(test)]
    fn sizes(&self) -> (usize, usize) {
        let v = self.verified.lock().unwrap().map.len();
        let mut locks = self.locks.lock().unwrap();
        locks.retain(|_, w| w.strong_count() > 0);
        (v, locks.len())
    }
}

/// `%` and `/` escaped, so the key's parts never run together.
pub(crate) fn escape(s: &str) -> String {
    s.replace('%', "%25").replace('/', "%2F")
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use deltaforge_core::ownership::{
        ClaimFailure, DestinationMarker, EncodingMarker, Marker,
        MemoryOwnershipStore, Observation, RelationMarker, RemoteOutcome,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A destination counting its reads (one per claim).
    #[derive(Default)]
    struct Counting {
        reads: AtomicUsize,
        state: Mutex<Option<(serde_json::Value, String)>>,
    }

    #[async_trait]
    impl FencedDestination for Counting {
        async fn read(&self) -> Result<Observation, ClaimFailure> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok(match self.state.lock().unwrap().clone() {
                None => Observation::Absent { server: None },
                Some((m, f)) => Observation::Present {
                    marker: Some(m),
                    fence: Ok(f),
                    server: None,
                },
            })
        }
        async fn create(
            &self,
            marker: &Marker,
            fence: &str,
        ) -> Result<RemoteOutcome, ClaimFailure> {
            tokio::task::yield_now().await;
            *self.state.lock().unwrap() =
                Some((serde_json::to_value(marker).unwrap(), fence.into()));
            Ok(RemoteOutcome::Applied)
        }
        async fn swap_fence(
            &self,
            _: &str,
            from: &str,
            to: &str,
        ) -> Result<RemoteOutcome, ClaimFailure> {
            let mut s = self.state.lock().unwrap();
            match s.as_mut() {
                Some((_, f)) if f == from => {
                    *f = to.into();
                    Ok(RemoteOutcome::Applied)
                }
                _ => Ok(RemoteOutcome::Refused),
            }
        }
    }

    fn owner() -> OwnerIdentity {
        OwnerIdentity {
            deployment: "11111111111111111111111111111111".into(),
            pipeline: "p".into(),
            sink: "es".into(),
        }
    }

    fn expected(index: &str, table: &str) -> Expected {
        Expected {
            owner: owner(),
            relation: RelationMarker {
                connector: "postgresql".into(),
                source: "pg".into(),
                database: "db".into(),
                namespace: "public".into(),
                table: table.into(),
            },
            destination: DestinationMarker::elasticsearch("c1", index),
            encoding: EncodingMarker {
                layout: "elasticsearch.document.v1".into(),
                type_mapping: "v2".into(),
                mode: None,
                extensions_column: None,
            },
        }
    }

    fn guard(capacity: usize) -> OwnershipGuard {
        OwnershipGuard::with_capacity(
            owner(),
            Arc::new(MemoryOwnershipStore::default()),
            capacity,
        )
    }

    #[tokio::test]
    async fn concurrent_first_writes_share_one_claim() {
        let g = Arc::new(guard(16));
        let d = Arc::new(Counting::default());
        let e = expected("orders", "orders");
        let tasks: Vec<_> = (0..16)
            .map(|_| {
                let (g, d, e) = (g.clone(), d.clone(), e.clone());
                tokio::spawn(async move { g.fence(&e, || d.as_ref()).await })
            })
            .collect();
        let mut fences = std::collections::BTreeSet::new();
        for t in tasks {
            fences.insert(t.await.unwrap().unwrap().fence);
        }
        assert_eq!(fences.len(), 1, "one fence for every waiter");
        assert_eq!(d.reads.load(Ordering::SeqCst), 1, "one claim");
    }

    #[tokio::test]
    async fn a_different_expectation_never_shares_a_verification() {
        let g = guard(16);
        let d = Counting::default();
        g.fence(&expected("orders", "orders"), || &d).await.unwrap();
        // Same destination, another relation: claimed (and refused), not
        // served from the verification of the first.
        let e = g
            .fence(&expected("orders", "other"), || &d)
            .await
            .unwrap_err();
        assert_eq!(d.reads.load(Ordering::SeqCst), 2);
        let class = e
            .draft()
            .and_then(|d| d.discriminator.get("class").cloned());
        assert_eq!(class.as_deref(), Some("bound_to_another_relation"));
    }

    #[tokio::test]
    async fn the_steady_state_makes_no_ownership_request() {
        let g = guard(16);
        let d = Counting::default();
        let e = expected("orders", "orders");
        let f = g.fence(&e, || &d).await.unwrap();
        for _ in 0..100 {
            assert_eq!(g.fence(&e, || &d).await.unwrap(), f);
        }
        assert_eq!(d.reads.load(Ordering::SeqCst), 1);
        // A reconnect forgets it: the next write claims (and rotates).
        g.invalidate_all();
        let f2 = g.fence(&e, || &d).await.unwrap();
        assert_ne!(f, f2);
        assert_eq!(d.reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn verified_and_lock_maps_stay_bounded() {
        let g = guard(4);
        let dests: Vec<Counting> =
            (0..50).map(|_| Counting::default()).collect();
        for (i, d) in dests.iter().enumerate() {
            let e = expected(&format!("idx{i}"), &format!("t{i}"));
            g.fence(&e, || d).await.unwrap();
        }
        let (verified, locks) = g.sizes();
        assert_eq!(verified, 4, "bounded verified cache");
        assert_eq!(locks, 0, "no lock outlives its claims");
    }
}
