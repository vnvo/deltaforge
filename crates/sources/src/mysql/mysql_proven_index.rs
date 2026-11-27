//! The backend keys of the tables the selection cache holds as proven, kept
//! as the cache changes, so a speculative window takes a snapshot of them
//! without walking the cache.
//!
//! A shared base set and a delta of the keys that differ from it (each
//! entry present in the delta only while it differs). A snapshot shares
//! both; a change after it copies the delta (at most [`FOLD_AT_LEAST`]
//! keys or a sixteenth of the base, whichever is more), never the base.
//! When the delta passes that size it is folded into the base (copying the
//! base only if a snapshot still holds it), so a snapshot costs O(1) and a
//! change amortized O(1).
//!
//! The index is outside the cache's byte bound, like the signature memo:
//! one hash-set slot per proven table, its key shared with the cache's
//! entry (about 20 bytes a table), where a walk built a set of copied keys
//! per window.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The delta folds into the base once it holds more than this many keys
/// and more than a sixteenth of the base.
const FOLD_AT_LEAST: usize = 1024;

pub(crate) struct ProvenIndex {
    base: Arc<HashSet<Arc<str>>>,
    /// The keys that differ from the base: `true` proven, `false` not.
    delta: Arc<HashMap<Arc<str>, bool>>,
    /// [`FOLD_AT_LEAST`], lower in tests that need folds at small sizes.
    fold_at_least: usize,
    folds: u64,
}

impl Default for ProvenIndex {
    fn default() -> Self {
        Self::folding_at(FOLD_AT_LEAST)
    }
}

/// The proven keys at the moment it was taken; later changes to the index
/// never reach it.
#[derive(Clone, Default)]
pub(crate) struct ProvenSnapshot {
    base: Arc<HashSet<Arc<str>>>,
    delta: Arc<HashMap<Arc<str>, bool>>,
}

impl ProvenIndex {
    fn folding_at(fold_at_least: usize) -> Self {
        Self {
            base: Default::default(),
            delta: Default::default(),
            fold_at_least,
            folds: 0,
        }
    }

    /// An empty index that folds its delta past `n` keys (tests).
    #[cfg(test)]
    pub(crate) fn folding_past(n: usize) -> Self {
        Self::folding_at(n)
    }

    /// How many times the delta was folded into the base (tests).
    #[cfg(test)]
    pub(crate) fn folds(&self) -> u64 {
        self.folds
    }

    /// `key` is proven.
    pub(crate) fn insert(&mut self, key: &Arc<str>) {
        self.set(key, true);
    }

    /// `key` is not proven (or not cached).
    pub(crate) fn remove(&mut self, key: &str) {
        if let Some((k, _)) = self.delta.get_key_value(key) {
            let k = k.clone();
            self.set(&k, false);
        } else if let Some(k) = self.base.get(key) {
            let k = k.clone();
            self.set(&k, false);
        }
    }

    /// Nothing is proven.
    pub(crate) fn clear(&mut self) {
        self.base = Default::default();
        self.delta = Default::default();
    }

    pub(crate) fn snapshot(&self) -> ProvenSnapshot {
        ProvenSnapshot {
            base: Arc::clone(&self.base),
            delta: Arc::clone(&self.delta),
        }
    }

    fn set(&mut self, key: &Arc<str>, proven: bool) {
        let in_base = self.base.contains(&**key);
        match self.delta.get(&**key) {
            Some(&d) if d == proven => return,
            Some(_) => {
                // Back to the base's state.
                Arc::make_mut(&mut self.delta).remove(&**key);
                return;
            }
            None if in_base == proven => return,
            None => {}
        }
        Arc::make_mut(&mut self.delta).insert(key.clone(), proven);
        if self.delta.len() > self.fold_at_least.max(self.base.len() / 16) {
            self.fold();
        }
    }

    fn fold(&mut self) {
        self.folds += 1;
        let delta = std::mem::take(&mut self.delta);
        let base = Arc::make_mut(&mut self.base);
        for (k, proven) in delta.iter() {
            if *proven {
                base.insert(k.clone());
            } else {
                base.remove(&**k);
            }
        }
    }
}

impl ProvenSnapshot {
    pub(crate) fn contains(&self, key: &str) -> bool {
        match self.delta.get(key) {
            Some(&proven) => proven,
            None => self.base.contains(key),
        }
    }

    /// Every proven key (tests).
    #[cfg(test)]
    pub(crate) fn keys(&self) -> HashSet<String> {
        self.base
            .iter()
            .filter(|k| self.delta.get(&***k) != Some(&false))
            .map(|k| k.to_string())
            .chain(
                self.delta
                    .iter()
                    .filter(|(_, p)| **p)
                    .map(|(k, _)| k.to_string()),
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(i: usize) -> Arc<str> {
        format!("v1/t/s/l/db/t{i}").into()
    }

    /// Against a plain set, through every fold, with snapshots taken along
    /// the way: each snapshot keeps exactly the keys of its moment.
    #[test]
    fn the_index_equals_a_plain_set_and_snapshots_never_change() {
        let mut index = ProvenIndex::folding_past(64);
        let mut model: HashSet<String> = HashSet::new();
        let mut snaps: Vec<(ProvenSnapshot, HashSet<String>)> = Vec::new();
        let mut x: u64 = 0x9e3779b97f4a7c15;
        for step in 0..60_000usize {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Keys over a range that grows, so the base grows past the fold
            // minimum and folds happen at several base sizes.
            let key = k((x % (500 + step as u64 / 4)) as usize);
            match x % 10 {
                0..=5 => {
                    index.insert(&key);
                    model.insert(key.to_string());
                }
                6..=8 => {
                    index.remove(&key);
                    model.remove(&*key);
                }
                _ if step % 5_000 == 9 => {
                    index.clear();
                    model.clear();
                }
                _ => {}
            }
            let probe = k((x >> 20) as usize % 600);
            assert_eq!(
                index.snapshot().contains(&probe),
                model.contains(&*probe),
                "step {step}"
            );
            if step % 997 == 0 {
                assert_eq!(index.snapshot().keys(), model, "step {step}");
                snaps.push((index.snapshot(), model.clone()));
            }
        }
        for (snap, then) in &snaps {
            assert_eq!(&snap.keys(), then);
            for key in then {
                assert!(snap.contains(key));
            }
        }
        assert!(index.folds() > 20, "{} folds", index.folds());
    }

    /// A key removed, folded away and inserted again is proven; one inserted
    /// and removed before any fold is not, and leaves no delta entry.
    #[test]
    fn a_key_returns_to_the_base_state_without_a_delta_entry() {
        let mut index = ProvenIndex::default();
        let a = k(1);
        index.insert(&a);
        index.remove(&a);
        assert!(!index.snapshot().contains(&a));
        assert!(index.delta.is_empty());
        for i in 0..=FOLD_AT_LEAST {
            index.insert(&k(10 + i));
        }
        assert!(index.delta.is_empty(), "folded");
        index.remove(&k(10));
        assert!(!index.snapshot().contains(&k(10)));
        index.insert(&k(10));
        assert!(index.snapshot().contains(&k(10)));
        assert!(index.delta.is_empty());
    }
}
