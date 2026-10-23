//! The one MySQL GTID set type: exact per-server-UUID intervals.
//!
//! Every GTID set DeltaForge streams, checkpoints, proves, orders or
//! persists goes through [`GtidSet`]. A set holds exactly the transactions
//! added to it: intervals merge only when they overlap or are adjacent
//! (`5` and `6`), never across a gap. A set over-claiming a transaction it
//! never saw would make the server skip that transaction on resume, and a
//! proof treat it as already covered.
//!
//! Text form: `uuid:1-5:7,uuid2:3`. Parsing trims whitespace (MySQL prints
//! `@@gtid_executed` with newlines), lowercases UUIDs, and unions duplicate
//! entries. It refuses anything else that is not a plain interval list: a
//! zero or inverted interval, an empty UUID, or a tagged GTID
//! (`uuid:tag:1-5`, MySQL 8.3+), which DeltaForge does not support.
//! [`GtidSet`]'s `Display` is the one canonical rendering: UUIDs sorted,
//! intervals sorted and merged.

use std::collections::BTreeMap;
use std::fmt;

/// Sorted, disjoint, non-adjacent intervals `(lo, hi)`, inclusive.
pub(crate) type Intervals = Vec<(u64, u64)>;

/// An exact MySQL GTID set.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GtidSet {
    map: BTreeMap<String, Intervals>,
}

impl GtidSet {
    /// The empty set: nothing executed.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a GTID set's text. `None` on anything malformed.
    pub fn parse(s: &str) -> Option<Self> {
        let mut set = Self::new();
        let s = s.trim();
        if s.is_empty() {
            return Some(set);
        }
        for entry in s.split(',') {
            let mut parts = entry.trim().split(':');
            let uuid = parts.next()?.trim().to_ascii_lowercase();
            if uuid.is_empty() {
                return None;
            }
            let mut any = false;
            for iv in parts {
                any = true;
                let iv = iv.trim();
                let (lo, hi) = match iv.split_once('-') {
                    Some((a, b)) => (
                        a.trim().parse::<u64>().ok()?,
                        b.trim().parse::<u64>().ok()?,
                    ),
                    None => {
                        let n = iv.parse::<u64>().ok()?;
                        (n, n)
                    }
                };
                if lo == 0 || hi < lo {
                    return None;
                }
                set.map.entry(uuid.clone()).or_default().push((lo, hi));
            }
            if !any {
                return None;
            }
        }
        for ivs in set.map.values_mut() {
            normalize(ivs);
        }
        Some(set)
    }

    /// The set of one GTID (`uuid:n`). `None` when malformed.
    pub fn single(gtid: &str) -> Option<Self> {
        let (uuid, n) = split_gtid(gtid)?;
        let mut set = Self::new();
        set.map.insert(uuid, vec![(n, n)]);
        Some(set)
    }

    /// Add one transaction (`uuid:n`). Exact: it never claims another
    /// number. `false` (and no change) when the GTID is malformed.
    #[must_use]
    pub fn insert(&mut self, gtid: &str) -> bool {
        let Some((uuid, n)) = split_gtid(gtid) else {
            return false;
        };
        let ivs = self.map.entry(uuid).or_default();
        // The interval at or after n, and the one before it.
        let i = ivs.partition_point(|&(_, hi)| hi < n);
        if i < ivs.len() && ivs[i].0 <= n {
            return true; // Already in the set.
        }
        let joins_left = i > 0 && ivs[i - 1].1 + 1 == n;
        let joins_right = i < ivs.len() && n + 1 == ivs[i].0;
        match (joins_left, joins_right) {
            (true, true) => {
                ivs[i - 1].1 = ivs[i].1;
                ivs.remove(i);
            }
            (true, false) => ivs[i - 1].1 = n,
            (false, true) => ivs[i].0 = n,
            (false, false) => ivs.insert(i, (n, n)),
        }
        true
    }

    /// Every transaction of `other` added.
    pub fn union(&mut self, other: &GtidSet) {
        for (uuid, ivs) in &other.map {
            let e = self.map.entry(uuid.clone()).or_default();
            e.extend(ivs.iter().copied());
            normalize(e);
        }
    }

    /// Does the set hold this transaction (`uuid:n`)? `false` when
    /// malformed.
    pub fn contains(&self, gtid: &str) -> bool {
        split_gtid(gtid).is_some_and(|(uuid, n)| {
            self.map.get(&uuid).is_some_and(|ivs| {
                let i = ivs.partition_point(|&(_, hi)| hi < n);
                i < ivs.len() && ivs[i].0 <= n
            })
        })
    }

    /// Is every transaction of `self` in `other`?
    pub fn is_subset(&self, other: &GtidSet) -> bool {
        self.map.iter().all(|(uuid, ivs)| {
            ivs.is_empty()
                || other.map.get(uuid).is_some_and(|o| {
                    ivs.iter().all(|&(lo, hi)| {
                        let i = o.partition_point(|&(_, ohi)| ohi < lo);
                        i < o.len() && o[i].0 <= lo && hi <= o[i].1
                    })
                })
        })
    }

    pub fn is_empty(&self) -> bool {
        self.map.values().all(Vec::is_empty)
    }

    /// The number of intervals over all UUIDs.
    pub fn interval_count(&self) -> usize {
        self.map.values().map(Vec::len).sum()
    }

    /// The per-UUID intervals, in order.
    pub(crate) fn intervals(&self) -> impl Iterator<Item = (&str, &Intervals)> {
        self.map
            .iter()
            .filter(|(_, ivs)| !ivs.is_empty())
            .map(|(u, ivs)| (u.as_str(), ivs))
    }
}

impl fmt::Display for GtidSet {
    /// The canonical text: UUIDs sorted, each with its merged intervals.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (uuid, ivs) in self.intervals() {
            if !first {
                f.write_str(",")?;
            }
            first = false;
            f.write_str(uuid)?;
            for &(lo, hi) in ivs {
                if lo == hi {
                    write!(f, ":{lo}")?;
                } else {
                    write!(f, ":{lo}-{hi}")?;
                }
            }
        }
        Ok(())
    }
}

/// `uuid:n` split into its lowercase UUID and number, `n > 0`.
fn split_gtid(gtid: &str) -> Option<(String, u64)> {
    let (uuid, n) = gtid.trim().rsplit_once(':')?;
    let uuid = uuid.trim().to_ascii_lowercase();
    // A tagged GTID (`uuid:tag:n`) leaves a colon in the UUID part.
    if uuid.is_empty() || uuid.contains(':') {
        return None;
    }
    let n = n.trim().parse::<u64>().ok()?;
    (n > 0).then_some((uuid, n))
}

/// Sort and merge overlapping or adjacent intervals.
fn normalize(ivs: &mut Intervals) {
    ivs.sort_unstable();
    let mut out: Intervals = Vec::with_capacity(ivs.len());
    for &(lo, hi) in ivs.iter() {
        if let Some(last) = out.last_mut()
            && lo <= last.1.saturating_add(1)
        {
            last.1 = last.1.max(hi);
            continue;
        }
        out.push((lo, hi));
    }
    *ivs = out;
}

#[cfg(test)]
mod tests {
    use super::*;

    const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const V: &str = "8f2c0a1b-0000-11f1-8000-000000000001";

    fn set(s: &str) -> GtidSet {
        GtidSet::parse(s).unwrap_or_else(|| panic!("parses: {s}"))
    }

    fn ins(s: &mut GtidSet, g: &str) {
        assert!(s.insert(g), "inserts {g}");
    }

    #[test]
    fn an_isolated_gtid_keeps_its_gap() {
        // The defect: u:1-8 plus u:13 must not claim 9-12.
        let mut s = set(&format!("{U}:1-8"));
        ins(&mut s, &format!("{U}:13"));
        assert_eq!(s.to_string(), format!("{U}:1-8:13"));
        for n in 9..=12 {
            assert!(!s.contains(&format!("{U}:{n}")), "{n} is not claimed");
        }
        assert!(
            s.contains(&format!("{U}:13")) && s.contains(&format!("{U}:8"))
        );
    }

    #[test]
    fn adjacent_intervals_merge_and_separate_ones_never_do() {
        let mut s = set(&format!("{U}:1-5:7-9"));
        ins(&mut s, &format!("{U}:10"));
        assert_eq!(s.to_string(), format!("{U}:1-5:7-10"));
        ins(&mut s, &format!("{U}:6"));
        assert_eq!(s.to_string(), format!("{U}:1-10"), "6 joins both sides");
        ins(&mut s, &format!("{U}:12"));
        assert_eq!(s.to_string(), format!("{U}:1-10:12"));
        assert_eq!(
            set(&format!("{U}:1-5:6-10")).to_string(),
            format!("{U}:1-10")
        );
        assert_eq!(set(&format!("{U}:1-5:7-10")).interval_count(), 2);
    }

    #[test]
    fn out_of_order_insertion_never_shrinks_or_over_claims() {
        let mut s = set(&format!("{U}:1-10"));
        ins(&mut s, &format!("{U}:6")); // Already in it.
        assert_eq!(s.to_string(), format!("{U}:1-10"));
        let mut s = set(&format!("{U}:20-30"));
        ins(&mut s, &format!("{U}:5"));
        ins(&mut s, &format!("{U}:3"));
        ins(&mut s, &format!("{U}:4"));
        assert_eq!(s.to_string(), format!("{U}:3-5:20-30"));
        // Any insertion order gives the same set.
        let mut a = GtidSet::new();
        let mut b = GtidSet::new();
        for n in [9, 1, 5, 2, 8, 3] {
            ins(&mut a, &format!("{U}:{n}"));
        }
        for n in [1, 2, 3, 5, 8, 9] {
            ins(&mut b, &format!("{U}:{n}"));
        }
        assert_eq!(a, b);
        assert_eq!(a.to_string(), format!("{U}:1-3:5:8-9"));
    }

    #[test]
    fn several_uuids_are_kept_apart() {
        let mut s = set(&format!("{V}:1-3,{U}:1-8"));
        ins(&mut s, &format!("{U}:13"));
        ins(&mut s, &format!("{V}:4"));
        assert_eq!(
            s.to_string(),
            format!("{U}:1-8:13,{V}:1-4"),
            "sorted by UUID"
        );
        assert!(!s.contains(&format!("{V}:9")));
        assert!(!s.contains(&format!("{U}:9")));
    }

    #[test]
    fn duplicates_and_duplicate_entries_are_unioned() {
        let mut s = set(&format!("{U}:1-5,{U}:7,\n{U}:3-6"));
        assert_eq!(s.to_string(), format!("{U}:1-7"));
        ins(&mut s, &format!("{U}:7"));
        ins(&mut s, &format!("{U}:7"));
        assert_eq!(s.to_string(), format!("{U}:1-7"));
    }

    #[test]
    fn a_very_fragmented_set_stays_exact() {
        // Every odd number up to 20,000: 10,000 intervals.
        let mut s = GtidSet::new();
        for n in (1..20_000).step_by(2).rev() {
            ins(&mut s, &format!("{U}:{n}"));
        }
        assert_eq!(s.interval_count(), 10_000);
        assert!(
            !s.contains(&format!("{U}:2")) && s.contains(&format!("{U}:19999"))
        );
        // Filling every gap merges them all.
        for n in (2..20_000).step_by(2) {
            ins(&mut s, &format!("{U}:{n}"));
        }
        assert_eq!(s.to_string(), format!("{U}:1-19999"));
    }

    #[test]
    fn the_canonical_text_round_trips() {
        for text in [
            String::new(),
            format!("{U}:1"),
            format!("{U}:1-8:13"),
            format!("{U}:1-8:13:20-30,{V}:5"),
        ] {
            let s = set(&text);
            assert_eq!(s.to_string(), text, "canonical text is a fixed point");
            assert_eq!(set(&s.to_string()), s, "round trip");
        }
        // Whitespace, newlines, case and order are normalized.
        let upper = U.to_ascii_uppercase();
        let messy = format!(" {V}:5 ,\n {upper}:13:1-8 ");
        assert_eq!(set(&messy).to_string(), format!("{U}:1-8:13,{V}:5"));
    }

    #[test]
    fn malformed_text_is_refused() {
        for bad in [
            U.to_string(),
            format!("{U}:"),
            format!("{U}:0"),
            format!("{U}:5-3"),
            format!(":{U}:1"),
            format!("{U}:x"),
            format!("{U}:1-5,"),
            format!("{U}:tag:1-5"),
        ] {
            assert!(GtidSet::parse(&bad).is_none(), "refused: {bad:?}");
        }
        let mut s = GtidSet::new();
        for bad in ["", "u", "u:0", "u:x", &format!("{U}:tag:3")] {
            assert!(!s.insert(bad), "refused: {bad:?}");
        }
        assert!(s.is_empty());
    }

    #[test]
    fn membership_and_subsets_use_the_exact_set() {
        let gapped = set(&format!("{U}:1-8:13"));
        let full = set(&format!("{U}:1-13"));
        assert!(gapped.is_subset(&full));
        assert!(!full.is_subset(&gapped), "9-12 are not in the gapped set");
        assert!(set(&format!("{U}:13")).is_subset(&gapped));
        assert!(!set(&format!("{U}:12-13")).is_subset(&gapped));
        assert!(GtidSet::new().is_subset(&gapped));
        assert!(!set(&format!("{V}:1")).is_subset(&gapped));
        assert!(gapped.is_subset(&gapped));
        let mut u = gapped.clone();
        u.union(&set(&format!("{U}:9-12,{V}:1")));
        assert_eq!(u.to_string(), format!("{U}:1-13,{V}:1"));
    }
}
