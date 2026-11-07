//! The PostgreSQL source qualification's checks (release checklist G4),
//! over what was committed (the ledger), what was delivered and what the
//! source tables hold at the end. Pure: no I/O.
//!
//! - **Coverage and order:** every committed change of a row is delivered
//!   at least once, in the row's commit order.
//! - **Duplicates:** allowed only as the snapshot's copy of a row a CDC
//!   event also carries, or as a redelivery after the resume of a change
//!   first delivered before it; any other duplicate fails.
//! - **TOAST:** under `REPLICA IDENTITY FULL` an unchanged out-of-line value
//!   is delivered, exactly; under `DEFAULT` it is left out of `after` and
//!   listed in `unchanged_columns`. A changed or inserted value is always
//!   delivered exactly.
//! - **DDL:** rows of the altered table carry the added column exactly when
//!   they were committed after it was added.
//! - **Reconciliation:** applying the delivered events in order (an
//!   unchanged column keeps its previous value) gives every row's final
//!   state in the source table, in every column; a deleted row ends deleted.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use serde::Serialize;

/// A large value's identity: its length and a hash of its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct Fingerprint {
    pub len: usize,
    pub hash: u64,
}

impl Fingerprint {
    pub fn of(s: &str) -> Fingerprint {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        s.hash(&mut h);
        Fingerprint {
            len: s.len(),
            hash: h.finish(),
        }
    }
}

/// How a table's large column is stored and identified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Out-of-line values under `REPLICA IDENTITY FULL`.
    ToastFull,
    /// Out-of-line values under `REPLICA IDENTITY DEFAULT`.
    ToastDefault,
    /// The large column is always null.
    Plain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Insert,
    Update,
    Delete,
}

/// One committed change of a row (inserts of the fixture included).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Committed {
    pub table: u32,
    pub id: i64,
    pub op: Op,
    /// The row's version after the change (its version before, for a
    /// delete).
    pub v: i64,
    /// The large value after the change.
    pub big: Option<Fingerprint>,
    /// An update that left the large value unchanged.
    pub big_unchanged: bool,
    /// Committed after the added column existed.
    pub extra: bool,
}

/// The large column as delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum Big {
    /// Not in `after`.
    Absent,
    Null,
    Value(Fingerprint),
}

/// One delivered change event (a snapshot read is `snapshot: true`).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Delivered {
    pub offset: i64,
    /// Arrival, ms since the run started.
    pub at_ms: f64,
    pub table: u32,
    pub id: i64,
    pub op: Op,
    pub snapshot: bool,
    /// `after.v` (`before.v` for a delete, when the before image has it).
    pub v: Option<i64>,
    pub note: Option<String>,
    pub big: Big,
    pub unchanged: Vec<String>,
    /// `after.extra`: `None` when the column is not in the event.
    pub extra: Option<Option<i64>>,
}

/// A row as the source table holds it at the end.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FinalRow {
    pub v: i64,
    pub note: String,
    pub big: Option<Fingerprint>,
    /// `None` when the table has no added column.
    pub extra: Option<Option<i64>>,
}

/// What the checks need to know about the run.
pub struct Expect<'a> {
    pub kind: &'a dyn Fn(u32) -> Kind,
    /// The table that received `ADD COLUMN extra`.
    pub ddl_table: u32,
    /// When the pipeline was resumed (ms since the run started), if it was
    /// stopped.
    pub resumed_ms: Option<f64>,
}

/// The checks' results. Lists are capped at [`SAMPLE`] entries; their
/// counts are exact.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Report {
    pub committed: u64,
    pub delivered: u64,
    pub missing: u64,
    pub out_of_order: u64,
    pub duplicates_snapshot_overlap: u64,
    pub duplicates_restart_replay: u64,
    pub duplicates_unexplained: u64,
    pub unknown_versions: u64,
    pub toast_errors: u64,
    /// Delivered updates whose unchanged value was filled from the old
    /// row (under `FULL`).
    pub toast_reconstructed: u64,
    /// Delivered updates whose unchanged value was left out (under
    /// `DEFAULT`).
    pub toast_unavailable: u64,
    pub ddl_errors: u64,
    pub reconcile_mismatches: u64,
    pub rows_final: u64,
    pub samples: Vec<String>,
}

pub const SAMPLE: usize = 50;

impl Report {
    pub fn ok(&self) -> bool {
        self.committed > 0
            && self.missing == 0
            && self.out_of_order == 0
            && self.duplicates_unexplained == 0
            && self.unknown_versions == 0
            && self.toast_errors == 0
            && self.ddl_errors == 0
            && self.reconcile_mismatches == 0
    }

    fn note(&mut self, what: String) {
        if self.samples.len() < SAMPLE {
            self.samples.push(what);
        }
    }
}

type Key = (u32, i64);

/// A delivered event's place in its row's committed history: the index of
/// the committed change it carries, `None` if it matches none.
fn locate(history: &[Committed], d: &Delivered) -> Option<usize> {
    match d.op {
        // A delete is a row's last change (ids are never reused).
        Op::Delete => history.iter().rposition(|c| c.op == Op::Delete),
        Op::Insert | Op::Update => {
            let v = d.v?;
            history.iter().position(|c| c.op != Op::Delete && c.v == v)
        }
    }
}

/// Run every check.
pub fn evaluate(
    committed: &[Committed],
    delivered: &[Delivered],
    finals: &BTreeMap<Key, FinalRow>,
    expect: &Expect<'_>,
) -> Report {
    let mut r = Report {
        committed: committed.len() as u64,
        delivered: delivered.len() as u64,
        rows_final: finals.len() as u64,
        ..Report::default()
    };
    let mut history: HashMap<Key, Vec<Committed>> = HashMap::new();
    for c in committed {
        history.entry((c.table, c.id)).or_default().push(c.clone());
    }
    let mut events: HashMap<Key, Vec<&Delivered>> = HashMap::new();
    let mut ordered: Vec<&Delivered> = delivered.iter().collect();
    ordered.sort_by_key(|d| d.offset);
    for d in &ordered {
        events.entry((d.table, d.id)).or_default().push(d);
    }

    for (key, hist) in &history {
        let evs = events.get(key).map(Vec::as_slice).unwrap_or_default();
        let mut seen: HashMap<usize, &Delivered> = HashMap::new();
        let mut highest: Option<usize> = None;
        for d in evs {
            let Some(i) = locate(hist, d) else {
                r.unknown_versions += 1;
                r.note(format!("{key:?}: delivered {:?} v {:?} matches no committed change", d.op, d.v));
                continue;
            };
            if let Some(first) = seen.get(&i) {
                if d.snapshot != first.snapshot {
                    r.duplicates_snapshot_overlap += 1;
                } else if expect
                    .resumed_ms
                    .is_some_and(|t| first.at_ms < t && d.at_ms >= t)
                {
                    r.duplicates_restart_replay += 1;
                } else {
                    r.duplicates_unexplained += 1;
                    r.note(format!("{key:?}: change {i} delivered again at offset {} (first at {})", d.offset, first.offset));
                }
                continue;
            }
            if highest.is_some_and(|h| i < h) {
                r.out_of_order += 1;
                r.note(format!(
                    "{key:?}: change {i} after change {}",
                    highest.unwrap_or(0)
                ));
            }
            highest = Some(highest.map_or(i, |h| h.max(i)));
            seen.insert(i, d);
        }
        for (i, c) in hist.iter().enumerate() {
            if !seen.contains_key(&i) {
                r.missing += 1;
                r.note(format!(
                    "{key:?}: committed {:?} v {} never delivered",
                    c.op, c.v
                ));
            }
        }
        for d in evs {
            let Some(i) = locate(hist, d) else { continue };
            check_event(&mut r, (expect.kind)(key.0), key, &hist[i], d, expect);
        }
    }
    for key in events.keys() {
        if !history.contains_key(key) {
            r.unknown_versions += events[key].len() as u64;
            r.note(format!("{key:?}: delivered but never committed"));
        }
    }

    // Reconciliation: replay the delivered events per row, in order.
    let mut state: BTreeMap<Key, Option<FinalRow>> = BTreeMap::new();
    for d in &ordered {
        let key = (d.table, d.id);
        let prev = state.get(&key).cloned().flatten();
        let next = match d.op {
            Op::Delete => None,
            Op::Insert | Op::Update => {
                let big = match d.big {
                    Big::Value(f) => Some(f),
                    Big::Null => None,
                    Big::Absent if d.unchanged.iter().any(|c| c == "big") => {
                        prev.as_ref().and_then(|p| p.big)
                    }
                    Big::Absent => None,
                };
                Some(FinalRow {
                    v: d.v.unwrap_or(-1),
                    note: d.note.clone().unwrap_or_default(),
                    big,
                    extra: d.extra,
                })
            }
        };
        state.insert(key, next);
    }
    let keys: HashSet<Key> =
        state.keys().chain(finals.keys()).copied().collect();
    let mut keys: Vec<Key> = keys.into_iter().collect();
    keys.sort();
    for key in keys {
        let got = state.get(&key).cloned().flatten();
        let want = finals.get(&key).cloned();
        let same = match (&got, &want) {
            (None, None) => true,
            (Some(g), Some(w)) => {
                g.v == w.v
                    && g.note == w.note
                    && g.big == w.big
                    && (w.extra.is_none()
                        || g.extra.flatten() == w.extra.flatten())
            }
            _ => false,
        };
        if !same {
            r.reconcile_mismatches += 1;
            r.note(format!(
                "{key:?}: delivered state {got:?}, source {want:?}"
            ));
        }
    }
    r
}

fn check_event(
    r: &mut Report,
    kind: Kind,
    key: &Key,
    c: &Committed,
    d: &Delivered,
    expect: &Expect<'_>,
) {
    if d.op == Op::Delete {
        return;
    }
    let ok = match (kind, c.op, c.big_unchanged) {
        (Kind::Plain, _, _) => d.big == Big::Null,
        (Kind::ToastFull, Op::Update, true) => {
            r.toast_reconstructed += 1;
            c.big.is_some_and(|f| d.big == Big::Value(f))
        }
        (Kind::ToastDefault, Op::Update, true) => {
            r.toast_unavailable += 1;
            d.big == Big::Absent && d.unchanged.iter().any(|u| u == "big")
        }
        (Kind::ToastFull | Kind::ToastDefault, _, _) => {
            c.big.is_some_and(|f| d.big == Big::Value(f))
        }
    };
    if !ok {
        r.toast_errors += 1;
        r.note(format!("{key:?}: {kind:?} {:?} v {}: delivered large value {:?}, unchanged {:?}", c.op, c.v, d.big, d.unchanged));
    }
    if key.0 == expect.ddl_table {
        let carries = matches!(d.extra, Some(Some(_)));
        if carries != c.extra {
            r.ddl_errors += 1;
            r.note(format!(
                "{key:?}: v {} committed {} the added column, delivered {:?}",
                c.v,
                if c.extra { "after" } else { "before" },
                d.extra
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(s: &str) -> Fingerprint {
        Fingerprint::of(s)
    }

    fn kind(t: u32) -> Kind {
        match t {
            0 => Kind::ToastFull,
            1 => Kind::ToastDefault,
            _ => Kind::Plain,
        }
    }

    fn c(
        table: u32,
        id: i64,
        op: Op,
        v: i64,
        big: Option<&str>,
        unchanged: bool,
    ) -> Committed {
        Committed {
            table,
            id,
            op,
            v,
            big: big.map(fp),
            big_unchanged: unchanged,
            extra: false,
        }
    }

    fn d(
        offset: i64,
        table: u32,
        id: i64,
        op: Op,
        v: Option<i64>,
        big: Big,
    ) -> Delivered {
        Delivered {
            offset,
            at_ms: offset as f64,
            table,
            id,
            op,
            snapshot: false,
            v,
            note: Some(format!("n{}", v.unwrap_or(0))),
            big,
            unchanged: Vec::new(),
            extra: None,
        }
    }

    fn fin(v: i64, big: Option<&str>) -> FinalRow {
        FinalRow {
            v,
            note: format!("n{v}"),
            big: big.map(fp),
            extra: None,
        }
    }

    fn expect(resumed: Option<f64>) -> Expect<'static> {
        Expect {
            kind: &kind,
            ddl_table: 9,
            resumed_ms: resumed,
        }
    }

    /// A FULL table's unchanged value is delivered exactly; a DEFAULT
    /// table's is left out and listed; both reconcile to the source.
    #[test]
    fn unchanged_toast_values_follow_the_replica_identity() {
        let committed = vec![
            c(0, 1, Op::Insert, 0, Some("A"), false),
            c(0, 1, Op::Update, 1, Some("A"), true),
            c(1, 1, Op::Insert, 0, Some("B"), false),
            c(1, 1, Op::Update, 1, Some("B"), true),
            c(1, 1, Op::Update, 2, Some("C"), false),
        ];
        let mut unav = d(4, 1, 1, Op::Update, Some(1), Big::Absent);
        unav.unchanged = vec!["big".into()];
        let delivered = vec![
            d(1, 0, 1, Op::Insert, Some(0), Big::Value(fp("A"))),
            d(2, 0, 1, Op::Update, Some(1), Big::Value(fp("A"))),
            d(3, 1, 1, Op::Insert, Some(0), Big::Value(fp("B"))),
            unav,
            d(5, 1, 1, Op::Update, Some(2), Big::Value(fp("C"))),
        ];
        let finals = BTreeMap::from([
            ((0, 1), fin(1, Some("A"))),
            ((1, 1), fin(2, Some("C"))),
        ]);
        let r = evaluate(&committed, &delivered, &finals, &expect(None));
        assert!(r.ok(), "{r:?}");
        assert_eq!((r.toast_reconstructed, r.toast_unavailable), (1, 1));
    }

    /// A FULL table's unchanged value delivered missing, or a DEFAULT
    /// table's delivered without the unchanged list, fails.
    #[test]
    fn a_wrong_toast_delivery_fails() {
        let committed = vec![
            c(0, 1, Op::Insert, 0, Some("A"), false),
            c(0, 1, Op::Update, 1, Some("A"), true),
        ];
        let delivered = vec![
            d(1, 0, 1, Op::Insert, Some(0), Big::Value(fp("A"))),
            d(2, 0, 1, Op::Update, Some(1), Big::Absent),
        ];
        let finals = BTreeMap::from([((0, 1), fin(1, Some("A")))]);
        let r = evaluate(&committed, &delivered, &finals, &expect(None));
        assert_eq!(r.toast_errors, 1);
        assert_eq!(r.reconcile_mismatches, 1, "the value is lost downstream");
        assert!(!r.ok());
    }

    /// Missing, reordered and unexplained duplicate deliveries fail; a
    /// redelivery after the resume and a snapshot copy do not.
    #[test]
    fn coverage_order_and_duplicates() {
        let committed = vec![
            c(2, 1, Op::Insert, 0, None, false),
            c(2, 1, Op::Update, 1, None, false),
            c(2, 1, Op::Update, 2, None, false),
            c(2, 1, Op::Delete, 2, None, false),
        ];
        let mut snap = d(1, 2, 1, Op::Insert, Some(0), Big::Null);
        snap.snapshot = true;
        let base = vec![
            snap,
            d(2, 2, 1, Op::Insert, Some(0), Big::Null),
            d(3, 2, 1, Op::Update, Some(1), Big::Null),
            d(5, 2, 1, Op::Update, Some(1), Big::Null),
            d(6, 2, 1, Op::Update, Some(2), Big::Null),
            d(7, 2, 1, Op::Delete, None, Big::Absent),
        ];
        let finals = BTreeMap::new();
        let r = evaluate(&committed, &base, &finals, &expect(Some(4.0)));
        assert!(r.ok(), "{r:?}");
        assert_eq!(r.duplicates_snapshot_overlap, 1);
        assert_eq!(r.duplicates_restart_replay, 1);

        let r = evaluate(&committed, &base, &finals, &expect(None));
        assert_eq!(r.duplicates_unexplained, 1, "no restart: unexplained");

        let mut reordered = base.clone();
        reordered.retain(|e| e.offset != 3 && e.offset != 5);
        reordered.push(d(8, 2, 1, Op::Update, Some(1), Big::Null));
        let r = evaluate(&committed, &reordered, &finals, &expect(None));
        assert_eq!(r.out_of_order, 1);

        let mut lost = base.clone();
        lost.retain(|e| e.offset != 6);
        let r = evaluate(&committed, &lost, &finals, &expect(Some(4.0)));
        assert_eq!(r.missing, 1);
        assert!(!r.ok());
    }

    /// Rows of the altered table carry the added column exactly when
    /// committed after it; the final state must match the source.
    #[test]
    fn the_added_column_and_reconciliation() {
        let mut after = c(9, 1, Op::Update, 1, None, false);
        after.extra = true;
        let committed = vec![c(9, 1, Op::Insert, 0, None, false), after];
        let mut ev1 = d(2, 9, 1, Op::Update, Some(1), Big::Null);
        ev1.extra = Some(Some(1));
        let delivered = vec![d(1, 9, 1, Op::Insert, Some(0), Big::Null), ev1];
        let mut want = fin(1, None);
        want.extra = Some(Some(1));
        let finals = BTreeMap::from([((9, 1), want.clone())]);
        let r = evaluate(&committed, &delivered, &finals, &expect(None));
        assert!(r.ok(), "{r:?}");

        let mut early = delivered.clone();
        early[0].extra = Some(Some(0));
        let r = evaluate(&committed, &early, &finals, &expect(None));
        assert_eq!(r.ddl_errors, 1);

        let mut other = want;
        other.note = "changed outside".into();
        let finals = BTreeMap::from([((9, 1), other)]);
        let r = evaluate(&committed, &delivered, &finals, &expect(None));
        assert_eq!(r.reconcile_mismatches, 1);
    }

    /// A row delivered that was never committed, and a committed run with
    /// nothing delivered, both fail.
    #[test]
    fn foreign_rows_and_empty_runs_fail() {
        let delivered = vec![d(1, 2, 7, Op::Insert, Some(0), Big::Null)];
        let r = evaluate(&[], &delivered, &BTreeMap::new(), &expect(None));
        assert_eq!(r.unknown_versions, 1);
        assert!(!r.ok());
        let r = evaluate(&[], &[], &BTreeMap::new(), &expect(None));
        assert!(!r.ok(), "nothing committed proves nothing");
    }
}
