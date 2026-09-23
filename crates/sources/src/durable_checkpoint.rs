//! Source-aware durable watermarks and their ordering (P0.4).
//!
//! The durable S3 sink stores a watermark per published batch and must order a
//! proposed watermark against HEAD's using **structured source semantics**, never
//! a lexical comparison of bytes. Because a `Before`/`Equal` result lets the sink
//! skip a write, a false ordering would be silent data loss - so anything that
//! cannot be *proven* ordered returns [`CheckpointOrder::Incomparable`] and the
//! caller fails closed.
//!
//! A watermark always carries the source **lineage** alongside the position: a
//! reused `source_id` is not enough, since a rebuilt PostgreSQL cluster can reuse
//! an LSN and a rebuilt MySQL source can reuse binlog coordinates. Different
//! lineage is always `Incomparable`.

use std::collections::BTreeMap;

use deltaforge_core::{CheckpointComparator, CheckpointOrder};
use serde::{Deserialize, Serialize};

use crate::snapshot_generation::PersistedLineage;

/// A typed, order-preserving snapshot scan cursor. The kind is carried so two
/// watermarks whose cursor domains differ (a schema/PK-type change) are
/// `Incomparable` rather than silently mis-ordered. Never mix kinds within a
/// table.
///
/// Ordering: the derived `Ord` sorts by variant, then by the inner value. Within
/// one kind that is the correct numeric order (including negative signed PKs);
/// across kinds it is deterministic but meaningless, and callers must treat a
/// kind mismatch as `Incomparable` (see [`vector_order`]) rather than trusting
/// the cross-variant order. `u64`->`i64` casts are never used for ordering.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum SnapshotCursor {
    /// Signed integer PK (e.g. MySQL `BIGINT`, PG `bigint`): ordered as `i64`,
    /// so negative keys sort correctly.
    Signed(i64),
    /// Unsigned integer PK (e.g. MySQL `BIGINT UNSIGNED`) or a row-count cursor.
    Unsigned(u64),
    /// PostgreSQL `ctid` block number: a SCAN frontier only, never row identity.
    CtidBlock(u64),
}

/// The domain of a [`SnapshotCursor`], independent of its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorKind {
    Signed,
    Unsigned,
    CtidBlock,
}

impl CursorKind {
    /// The lowest cursor of this kind - a fresh frontier starts here, and the
    /// first chunk of a table abuts it (everything below the first scanned key is
    /// vacuously durable).
    pub fn min(self) -> SnapshotCursor {
        match self {
            CursorKind::Signed => SnapshotCursor::Signed(i64::MIN),
            CursorKind::Unsigned => SnapshotCursor::Unsigned(0),
            CursorKind::CtidBlock => SnapshotCursor::CtidBlock(0),
        }
    }

    /// The highest cursor of this kind - a fully-scanned (done) table's frontier
    /// when restoring from a table-level source checkpoint that records only
    /// completion, not a per-table cursor.
    pub fn max(self) -> SnapshotCursor {
        match self {
            CursorKind::Signed => SnapshotCursor::Signed(i64::MAX),
            CursorKind::Unsigned => SnapshotCursor::Unsigned(u64::MAX),
            CursorKind::CtidBlock => SnapshotCursor::CtidBlock(u64::MAX),
        }
    }
}

impl SnapshotCursor {
    pub fn kind(&self) -> CursorKind {
        match self {
            SnapshotCursor::Signed(_) => CursorKind::Signed,
            SnapshotCursor::Unsigned(_) => CursorKind::Unsigned,
            SnapshotCursor::CtidBlock(_) => CursorKind::CtidBlock,
        }
    }
}

/// Canonical durable-watermark version. Unknown versions are `Incomparable`.
pub const WATERMARK_VERSION: u16 = 1;

/// A durable watermark: source lineage plus a source-specific position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableWatermark {
    pub version: u16,
    pub lineage: PersistedLineage,
    pub pos: WmPos,
}

/// Source-specific position within a lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WmPos {
    /// PostgreSQL logical replication position. Only commit-boundary positions
    /// are comparable.
    PgLsn {
        lsn: u64,
        commit_boundary: bool,
        tx_id: Option<u32>,
    },
    /// MySQL GTID set (raw string; compared by set inclusion, not lexically).
    MysqlGtid { gtid_set: String },
    /// MySQL non-GTID binlog coordinate. `file_base` is the filename prefix
    /// (e.g. `"mysql-bin"`); it must match for two coordinates to be comparable,
    /// so a differently-named binlog on a reused server id is Incomparable. The
    /// numeric index (never the lexical filename) plus position gives the order.
    MysqlBinlog {
        file_base: String,
        file_index: u64,
        pos: u64,
    },
    /// Snapshot progress: a per-table cursor vector (a partial order), a
    /// generation, and `completed` - the explicit, durable completion marker that
    /// is the ONLY thing permitting a snapshot->CDC transition (see [`order`]).
    Snapshot {
        generation: u64,
        completed: bool,
        table_cursors: BTreeMap<String, SnapshotCursor>,
    },
}
// NOTE: there is deliberately no "standalone sequence" variant. DDL, logical
// messages and synthetic events inherit their real source/parent CDC coordinate
// (PgLsn/MysqlGtid/MysqlBinlog); order is never manufactured from a sink- or
// process-local counter that would reset on restart. An event with no durable
// source ordering has no DurableWatermark and cannot be published to the durable
// sink (fail closed at construction), rather than being given a fake order.

impl DurableWatermark {
    pub fn new(lineage: PersistedLineage, pos: WmPos) -> Self {
        Self {
            version: WATERMARK_VERSION,
            lineage,
            pos,
        }
    }

    /// PostgreSQL CDC commit watermark: the frozen cluster `system_identifier`
    /// and the COMMIT record's `end_lsn` (never a row/message LSN).
    pub fn pg_commit(
        system_identifier: u64,
        end_lsn: u64,
        tx_id: Option<u32>,
    ) -> Self {
        Self::new(
            PersistedLineage::Postgres { system_identifier },
            WmPos::PgLsn {
                lsn: end_lsn,
                commit_boundary: true,
                tx_id,
            },
        )
    }

    /// MySQL GTID CDC commit watermark: the frozen source lineage and the exact
    /// normalized accumulated GTID set represented by the commit checkpoint.
    pub fn mysql_gtid_commit(
        lineage: PersistedLineage,
        accumulated_gtid_set: String,
    ) -> Self {
        Self::new(
            lineage,
            WmPos::MysqlGtid {
                gtid_set: accumulated_gtid_set,
            },
        )
    }

    /// MySQL non-GTID CDC commit watermark: frozen server lineage plus the
    /// commit-record binlog file base + numeric index + position.
    pub fn mysql_binlog_commit(
        lineage: PersistedLineage,
        file: &str,
        pos: u64,
    ) -> Option<Self> {
        let (file_base, file_index) = binlog_file_parts(file)?;
        Some(Self::new(
            lineage,
            WmPos::MysqlBinlog {
                file_base,
                file_index,
                pos,
            },
        ))
    }

    /// Serialize for storage in HEAD.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("watermark serializes")
    }

    /// Parse, returning `None` on any malformed / unknown-version / missing-field
    /// input (including old formats without lineage) so the comparator can fail
    /// closed.
    pub fn parse(raw: &[u8]) -> Option<Self> {
        let w: DurableWatermark = serde_json::from_slice(raw).ok()?;
        if w.version != WATERMARK_VERSION {
            return None;
        }
        Some(w)
    }
}

/// Parse a PostgreSQL LSN string (`"HH/LL"`, hex/hex) to a u64.
pub fn parse_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
    Some((hi << 32) | lo)
}

/// Split a binlog filename (`"mysql-bin.000009"`) into its base prefix
/// (`"mysql-bin"`) and numeric index (`9`). Requires a `<base>.<digits>` shape;
/// never assumes filenames sort lexically. Returns `None` on any other shape.
pub fn binlog_file_parts(file: &str) -> Option<(String, u64)> {
    let (base, suffix) = file.rsplit_once('.')?;
    if base.is_empty() {
        return None;
    }
    let index = suffix.parse::<u64>().ok()?;
    Some((base.to_string(), index))
}

// ── GTID set parsing + inclusion ────────────────────────────────────────────

type Intervals = Vec<(u64, u64)>;

/// Parse a GTID set (`"uuid:1-5:10-12,uuid2:1-3"`) into per-UUID merged
/// intervals. Returns `None` on any malformed input.
fn parse_gtid_set(s: &str) -> Option<BTreeMap<String, Intervals>> {
    let mut map: BTreeMap<String, Intervals> = BTreeMap::new();
    let s = s.trim();
    if s.is_empty() {
        return Some(map);
    }
    for entry in s.split(',') {
        let entry = entry.trim();
        let mut parts = entry.split(':');
        let uuid = parts.next()?.trim().to_string();
        if uuid.is_empty() {
            return None;
        }
        let mut ivs: Intervals = Vec::new();
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
            ivs.push((lo, hi));
        }
        if !any {
            return None;
        }
        let e = map.entry(uuid).or_default();
        e.extend(ivs);
        merge_intervals(e);
    }
    Some(map)
}

fn merge_intervals(ivs: &mut Intervals) {
    ivs.sort_unstable();
    let mut out: Intervals = Vec::with_capacity(ivs.len());
    for &(lo, hi) in ivs.iter() {
        if let Some(last) = out.last_mut() {
            // Merge overlapping or contiguous (hi+1 == lo) ranges.
            if lo <= last.1.saturating_add(1) {
                last.1 = last.1.max(hi);
                continue;
            }
        }
        out.push((lo, hi));
    }
    *ivs = out;
}

/// Is every interval of `a` covered by `b` (for every UUID)?
fn gtid_subseteq(
    a: &BTreeMap<String, Intervals>,
    b: &BTreeMap<String, Intervals>,
) -> bool {
    for (uuid, a_ivs) in a {
        let Some(b_ivs) = b.get(uuid) else {
            return false;
        };
        for &(lo, hi) in a_ivs {
            if !b_ivs.iter().any(|&(blo, bhi)| blo <= lo && hi <= bhi) {
                return false;
            }
        }
    }
    true
}

fn order_from_subset(a_in_b: bool, b_in_a: bool) -> CheckpointOrder {
    match (a_in_b, b_in_a) {
        (true, true) => CheckpointOrder::Equal,
        (true, false) => CheckpointOrder::Before,
        (false, true) => CheckpointOrder::After,
        (false, false) => CheckpointOrder::Incomparable,
    }
}

fn cmp_scalar(a: u64, b: u64) -> CheckpointOrder {
    use std::cmp::Ordering::*;
    match a.cmp(&b) {
        Less => CheckpointOrder::Before,
        Equal => CheckpointOrder::Equal,
        Greater => CheckpointOrder::After,
    }
}

/// Order two parsed watermarks. Fail-closed: any doubt is `Incomparable`.
pub fn order(a: &DurableWatermark, b: &DurableWatermark) -> CheckpointOrder {
    // Lineage must match (validates PG system_identifier, MySQL server UUID /
    // server id). Different lineage is never ordered.
    if !a.lineage.stable_matches(&b.lineage) {
        return CheckpointOrder::Incomparable;
    }
    match (&a.pos, &b.pos) {
        (
            WmPos::PgLsn {
                lsn: la,
                commit_boundary: ca,
                tx_id: ta,
            },
            WmPos::PgLsn {
                lsn: lb,
                commit_boundary: cb,
                tx_id: tb,
            },
        ) => {
            // Only commit-boundary checkpoints are comparable.
            if !ca || !cb {
                return CheckpointOrder::Incomparable;
            }
            match la.cmp(lb) {
                std::cmp::Ordering::Equal => {
                    // Same LSN with conflicting transaction metadata is a
                    // contradiction: fail closed.
                    if ta == tb {
                        CheckpointOrder::Equal
                    } else {
                        CheckpointOrder::Incomparable
                    }
                }
                std::cmp::Ordering::Less => CheckpointOrder::Before,
                std::cmp::Ordering::Greater => CheckpointOrder::After,
            }
        }
        (
            WmPos::MysqlGtid { gtid_set: ga },
            WmPos::MysqlGtid { gtid_set: gb },
        ) => match (parse_gtid_set(ga), parse_gtid_set(gb)) {
            (Some(sa), Some(sb)) => order_from_subset(
                gtid_subseteq(&sa, &sb),
                gtid_subseteq(&sb, &sa),
            ),
            _ => CheckpointOrder::Incomparable,
        },
        (
            WmPos::MysqlBinlog {
                file_base: ba,
                file_index: fa,
                pos: pa,
            },
            WmPos::MysqlBinlog {
                file_base: bb,
                file_index: fb,
                pos: pb,
            },
        ) => {
            // Same lineage already checked; also require the same filename base
            // (a differently-named binlog on a reused server id is not the same
            // coordinate space). Then compare (index, pos) numerically.
            if ba != bb {
                return CheckpointOrder::Incomparable;
            }
            match fa.cmp(fb) {
                std::cmp::Ordering::Equal => cmp_scalar(*pa, *pb),
                std::cmp::Ordering::Less => CheckpointOrder::Before,
                std::cmp::Ordering::Greater => CheckpointOrder::After,
            }
        }
        (
            WmPos::Snapshot {
                generation: ga,
                completed: ca,
                table_cursors: ta,
            },
            WmPos::Snapshot {
                generation: gb,
                completed: cb,
                table_cursors: tb,
            },
        ) => snapshot_order(*ga, *ca, ta, *gb, *cb, tb),
        // The ONLY permitted cross-variant transition: a COMPLETED snapshot of
        // lineage L precedes CDC of lineage L. The `completed` flag is the
        // explicit durable completion marker; an incomplete snapshot or a
        // row-cursor snapshot is never ordered against CDC. (Lineage was already
        // checked above.)
        (WmPos::Snapshot { completed, .. }, cdc) if is_cdc(cdc) => {
            if *completed {
                CheckpointOrder::Before
            } else {
                CheckpointOrder::Incomparable
            }
        }
        (cdc, WmPos::Snapshot { completed, .. }) if is_cdc(cdc) => {
            if *completed {
                CheckpointOrder::After
            } else {
                CheckpointOrder::Incomparable
            }
        }
        // Any other cross-variant pairing cannot be ordered.
        _ => CheckpointOrder::Incomparable,
    }
}

fn is_cdc(p: &WmPos) -> bool {
    matches!(
        p,
        WmPos::PgLsn { .. }
            | WmPos::MysqlGtid { .. }
            | WmPos::MysqlBinlog { .. }
    )
}

/// Snapshot ordering. Within one generation the cursors form a partial order
/// (component-wise over the union of tables; a missing table is cursor 0):
/// all `<=` is Before/Equal, all `>=` is After, mixed is Incomparable. Across
/// generations, a higher generation only supersedes a **completed** lower one.
fn snapshot_order(
    ga: u64,
    ca: bool,
    ta: &BTreeMap<String, SnapshotCursor>,
    gb: u64,
    cb: bool,
    tb: &BTreeMap<String, SnapshotCursor>,
) -> CheckpointOrder {
    if ga == gb {
        return vector_order(ta, tb);
    }
    // Different generations: only ordered when the earlier one has completed.
    let (earlier_completed, dir) = if ga < gb {
        (ca, CheckpointOrder::Before)
    } else {
        (cb, CheckpointOrder::After)
    };
    if earlier_completed {
        dir
    } else {
        CheckpointOrder::Incomparable
    }
}

/// Component-wise dominance over table cursors.
///
/// The two vectors MUST describe the same table set. A missing component is
/// **not** treated as zero: a `SnapshotVector` is always built over the full
/// target table set (absent tables are explicit zeros), so differing key sets
/// mean a lifecycle change (tables added/removed) and are Incomparable rather
/// than risking a false Before that would let the sink skip a write.
fn vector_order(
    a: &BTreeMap<String, SnapshotCursor>,
    b: &BTreeMap<String, SnapshotCursor>,
) -> CheckpointOrder {
    if a.len() != b.len() || !a.keys().eq(b.keys()) {
        return CheckpointOrder::Incomparable;
    }
    let mut any_less = false;
    let mut any_greater = false;
    for (k, va) in a {
        let vb = b.get(k).expect("key sets equal");
        // A cursor-kind change for a table (schema/PK-type change) makes the two
        // vectors incomparable - never order across cursor domains.
        if va.kind() != vb.kind() {
            return CheckpointOrder::Incomparable;
        }
        match va.cmp(vb) {
            std::cmp::Ordering::Less => any_less = true,
            std::cmp::Ordering::Greater => any_greater = true,
            std::cmp::Ordering::Equal => {}
        }
    }
    match (any_less, any_greater) {
        (false, false) => CheckpointOrder::Equal,
        (true, false) => CheckpointOrder::Before,
        (false, true) => CheckpointOrder::After,
        (true, true) => CheckpointOrder::Incomparable,
    }
}

/// Owns and advances a snapshot's per-table cursor vector.
///
/// Ownership: the durable sink holds one `SnapshotVector` per active snapshot
/// generation. It is initialized with the **full target table set** (all cursors
/// at 0) so there are never "missing" components; per-table progress is merged
/// monotonically (max), so parallel-table updates join into a single monotonic
/// vector. Each emitted batch takes the **complete** current vector as its
/// proposed watermark (never inferred from just the last event), which therefore
/// dominates every position the batch delivered. On restart the vector is rebuilt
/// from HEAD's watermark via [`SnapshotVector::from_watermark`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVector {
    generation: u64,
    completed: bool,
    cursors: BTreeMap<String, SnapshotCursor>,
}

impl SnapshotVector {
    /// Start a generation over `tables`, each cursor at its kind's minimum, so the
    /// key set (and every cursor kind) is fixed from the first batch.
    pub fn new(generation: u64, tables: &[(String, CursorKind)]) -> Self {
        Self {
            generation,
            completed: false,
            cursors: tables.iter().map(|(t, k)| (t.clone(), k.min())).collect(),
        }
    }

    /// Merge per-table progress monotonically. A cursor never moves backward;
    /// an unknown table, or one whose cursor kind changed, is ignored (the target
    /// set and each kind are fixed at construction).
    pub fn observe(&mut self, table: &str, cursor: SnapshotCursor) {
        if let Some(c) = self.cursors.get_mut(table) {
            if c.kind() == cursor.kind() && cursor > *c {
                *c = cursor;
            }
        }
    }

    /// Mark the generation durably complete (permits the snapshot->CDC order).
    pub fn mark_completed(&mut self) {
        self.completed = true;
    }

    pub fn completed(&self) -> bool {
        self.completed
    }

    /// The complete current position, for use as a batch's proposed watermark.
    pub fn to_pos(&self) -> WmPos {
        WmPos::Snapshot {
            generation: self.generation,
            completed: self.completed,
            table_cursors: self.cursors.clone(),
        }
    }

    /// The full watermark (position + lineage) for a batch.
    pub fn watermark(&self, lineage: PersistedLineage) -> DurableWatermark {
        DurableWatermark::new(lineage, self.to_pos())
    }

    /// Rebuild from a stored watermark (restart survival).
    pub fn from_watermark(w: &DurableWatermark) -> Option<Self> {
        match &w.pos {
            WmPos::Snapshot {
                generation,
                completed,
                table_cursors,
            } => Some(Self {
                generation: *generation,
                completed: *completed,
                cursors: table_cursors.clone(),
            }),
            _ => None,
        }
    }
}

/// [`CheckpointComparator`] over serialized [`DurableWatermark`] bytes. Unparseable
/// input on either side is `Incomparable` (fail closed).
pub struct SourceCheckpointComparator;

impl CheckpointComparator for SourceCheckpointComparator {
    fn order(&self, proposed: &[u8], reference: &[u8]) -> CheckpointOrder {
        match (
            DurableWatermark::parse(proposed),
            DurableWatermark::parse(reference),
        ) {
            (Some(a), Some(b)) => order(&a, &b),
            _ => CheckpointOrder::Incomparable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::MySqlCheckpoint;
    use crate::postgres::PostgresCheckpoint;

    fn pg_lineage(id: u64) -> PersistedLineage {
        PersistedLineage::Postgres {
            system_identifier: id,
        }
    }
    fn gtid_lineage(uuid: [u8; 16]) -> PersistedLineage {
        PersistedLineage::MysqlGtid { source_uuid: uuid }
    }
    fn server_lineage(id: u32) -> PersistedLineage {
        PersistedLineage::MysqlServer {
            server_id: id,
            file: "mysql-bin.000001".into(),
        }
    }

    fn ord(a: &DurableWatermark, b: &DurableWatermark) -> CheckpointOrder {
        // Exercise the full serialize -> parse -> compare path (real HEAD bytes).
        SourceCheckpointComparator.order(&a.to_bytes(), &b.to_bytes())
    }

    // ── PostgreSQL, from real PostgresCheckpoint serialization ──────────────

    fn pg_wm(
        sysid: u64,
        lsn: &str,
        tx: Option<u32>,
        commit: bool,
    ) -> DurableWatermark {
        // Prove we can build from the production checkpoint format.
        let cp = PostgresCheckpoint {
            lsn: lsn.to_string(),
            tx_id: tx,
        };
        let raw = serde_json::to_vec(&cp).unwrap();
        let parsed: PostgresCheckpoint = serde_json::from_slice(&raw).unwrap();
        DurableWatermark::new(
            pg_lineage(sysid),
            WmPos::PgLsn {
                lsn: parse_lsn(&parsed.lsn).unwrap(),
                commit_boundary: commit,
                tx_id: parsed.tx_id,
            },
        )
    }

    #[test]
    fn pg_same_lineage_before_equal_after() {
        let a = pg_wm(1, "0/100", Some(5), true);
        let b = pg_wm(1, "0/200", Some(6), true);
        assert_eq!(ord(&a, &b), CheckpointOrder::Before);
        assert_eq!(ord(&b, &a), CheckpointOrder::After);
        assert_eq!(ord(&a, &a), CheckpointOrder::Equal);
    }

    #[test]
    fn pg_different_system_identifier_is_incomparable() {
        let a = pg_wm(1, "0/200", Some(5), true);
        let b = pg_wm(2, "0/100", Some(5), true);
        assert_eq!(ord(&a, &b), CheckpointOrder::Incomparable);
    }

    #[test]
    fn pg_same_lsn_conflicting_tx_is_incomparable() {
        let a = pg_wm(1, "0/100", Some(5), true);
        let b = pg_wm(1, "0/100", Some(6), true);
        assert_eq!(ord(&a, &b), CheckpointOrder::Incomparable);
    }

    #[test]
    fn pg_non_commit_boundary_is_incomparable() {
        let a = pg_wm(1, "0/100", Some(5), false);
        let b = pg_wm(1, "0/200", Some(6), true);
        assert_eq!(ord(&a, &b), CheckpointOrder::Incomparable);
    }

    #[test]
    fn pg_lsn_parses_high_and_low_words() {
        assert_eq!(parse_lsn("1/0"), Some(1u64 << 32));
        assert_eq!(parse_lsn("0/16B2C50"), Some(0x16B2C50));
        assert!(parse_lsn("garbage").is_none());
    }

    // ── MySQL GTID, from real MySqlCheckpoint serialization ─────────────────

    fn gtid_wm(uuid: [u8; 16], set: &str) -> DurableWatermark {
        let cp = MySqlCheckpoint {
            file: "mysql-bin.000001".into(),
            pos: 0,
            gtid_set: Some(set.to_string()),
        };
        let raw = serde_json::to_vec(&cp).unwrap();
        let parsed: MySqlCheckpoint = serde_json::from_slice(&raw).unwrap();
        DurableWatermark::new(
            gtid_lineage(uuid),
            WmPos::MysqlGtid {
                gtid_set: parsed.gtid_set.unwrap(),
            },
        )
    }

    #[test]
    fn mysql_gtid_subset_superset_divergent() {
        let u = [1u8; 16];
        let uuid = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
        let sub = gtid_wm(u, &format!("{uuid}:1-5"));
        let sup = gtid_wm(u, &format!("{uuid}:1-10"));
        assert_eq!(ord(&sub, &sup), CheckpointOrder::Before);
        assert_eq!(ord(&sup, &sub), CheckpointOrder::After);
        assert_eq!(ord(&sub, &sub), CheckpointOrder::Equal);

        // Divergent: neither is a subset of the other.
        let d1 = gtid_wm(u, &format!("{uuid}:1-5"));
        let d2 = gtid_wm(u, &format!("{uuid}:3-8"));
        // 1-5 vs 3-8: 1,2 not in d2; 6,7,8 not in d1 -> divergent.
        assert_eq!(ord(&d1, &d2), CheckpointOrder::Incomparable);
    }

    #[test]
    fn mysql_gtid_multi_source_inclusion() {
        let u = [2u8; 16];
        let a = "AAAAAAAA-0000-0000-0000-000000000000";
        let b = "BBBBBBBB-0000-0000-0000-000000000000";
        let small = gtid_wm(u, &format!("{a}:1-5,{b}:1-2"));
        let big = gtid_wm(u, &format!("{a}:1-5,{b}:1-4"));
        assert_eq!(ord(&small, &big), CheckpointOrder::Before);
    }

    #[test]
    fn mysql_gtid_contiguous_intervals_merge() {
        // "1-5:6-10" must be treated as 1-10.
        let m = parse_gtid_set("u:1-5:6-10").unwrap();
        assert_eq!(m.get("u").unwrap(), &vec![(1, 10)]);
    }

    // ── MySQL non-GTID binlog ───────────────────────────────────────────────

    fn binlog_wm(server: u32, file: &str, pos: u64) -> DurableWatermark {
        let (base, index) = binlog_file_parts(file).unwrap();
        DurableWatermark::new(
            server_lineage(server),
            WmPos::MysqlBinlog {
                file_base: base,
                file_index: index,
                pos,
            },
        )
    }

    #[test]
    fn binlog_rotation_9_to_10_is_after_not_lexical() {
        let f9 = binlog_wm(1, "mysql-bin.000009", 900);
        let f10 = binlog_wm(1, "mysql-bin.000010", 4);
        // Lexically "000009" > "000010" is false, but numerically 9 < 10, and a
        // later file outranks a higher position in an earlier file.
        assert_eq!(ord(&f9, &f10), CheckpointOrder::Before);
        assert_eq!(ord(&f10, &f9), CheckpointOrder::After);
    }

    #[test]
    fn binlog_same_file_compares_position() {
        let a = binlog_wm(1, "mysql-bin.000004", 100);
        let b = binlog_wm(1, "mysql-bin.000004", 200);
        assert_eq!(ord(&a, &b), CheckpointOrder::Before);
    }

    #[test]
    fn binlog_different_server_lineage_is_incomparable() {
        let a = binlog_wm(1, "mysql-bin.000010", 4);
        let b = binlog_wm(2, "mysql-bin.000001", 4);
        assert_eq!(ord(&a, &b), CheckpointOrder::Incomparable);
    }

    // ── Snapshot (vector) ───────────────────────────────────────────────────

    fn snap(
        generation: u64,
        completed: bool,
        cursors: &[(&str, u64)],
    ) -> DurableWatermark {
        snap_typed(
            generation,
            completed,
            &cursors
                .iter()
                .map(|(t, c)| (*t, SnapshotCursor::Unsigned(*c)))
                .collect::<Vec<_>>(),
        )
    }

    fn snap_typed(
        generation: u64,
        completed: bool,
        cursors: &[(&str, SnapshotCursor)],
    ) -> DurableWatermark {
        DurableWatermark::new(
            pg_lineage(1),
            WmPos::Snapshot {
                generation,
                completed,
                table_cursors: cursors
                    .iter()
                    .map(|(t, c)| (t.to_string(), *c))
                    .collect(),
            },
        )
    }

    #[test]
    fn snapshot_single_table_progress_orders() {
        let a = snap(1, false, &[("orders", 100)]);
        let b = snap(1, false, &[("orders", 200)]);
        assert_eq!(ord(&a, &b), CheckpointOrder::Before);
    }

    #[test]
    fn snapshot_parallel_tables_advancing_together_orders() {
        let a = snap(1, false, &[("orders", 100), ("users", 50)]);
        let b = snap(1, false, &[("orders", 200), ("users", 80)]);
        assert_eq!(ord(&a, &b), CheckpointOrder::Before);
    }

    #[test]
    fn snapshot_concurrent_cursors_are_incomparable() {
        // orders ahead in a, users ahead in b -> neither before nor after.
        let a = snap(1, false, &[("orders", 200), ("users", 50)]);
        let b = snap(1, false, &[("orders", 100), ("users", 80)]);
        assert_eq!(ord(&a, &b), CheckpointOrder::Incomparable);
    }

    #[test]
    fn snapshot_higher_generation_supersedes_only_when_prior_completed() {
        let g1_done = snap(1, true, &[("orders", 500)]);
        let g2 = snap(2, false, &[("orders", 10)]);
        assert_eq!(ord(&g1_done, &g2), CheckpointOrder::Before);
        assert_eq!(ord(&g2, &g1_done), CheckpointOrder::After);

        // Prior generation NOT completed -> not ordered.
        let g1_partial = snap(1, false, &[("orders", 500)]);
        assert_eq!(ord(&g1_partial, &g2), CheckpointOrder::Incomparable);
    }

    #[test]
    fn snapshot_completed_precedes_cdc_same_lineage() {
        let done = snap(1, true, &[("orders", 500)]);
        let cdc = pg_wm(1, "0/300", Some(9), true);
        assert_eq!(ord(&done, &cdc), CheckpointOrder::Before);
        assert_eq!(ord(&cdc, &done), CheckpointOrder::After);

        // Incomplete snapshot is not ordered against CDC.
        let partial = snap(1, false, &[("orders", 500)]);
        assert_eq!(ord(&partial, &cdc), CheckpointOrder::Incomparable);
    }

    // ── Malformed / version / lineage / cross-variant fail-closed ───────────

    #[test]
    fn malformed_or_unknown_version_is_incomparable() {
        let good = pg_wm(1, "0/100", Some(1), true);
        // Not JSON at all.
        assert_eq!(
            SourceCheckpointComparator.order(b"not json", &good.to_bytes()),
            CheckpointOrder::Incomparable
        );
        // Unknown version.
        let mut bumped = good.clone();
        bumped.version = 999;
        assert_eq!(
            SourceCheckpointComparator
                .order(&serde_json::to_vec(&bumped).unwrap(), &good.to_bytes()),
            CheckpointOrder::Incomparable
        );
    }

    #[test]
    fn old_format_without_lineage_is_incomparable() {
        // A legacy watermark that is just the raw source checkpoint (no lineage,
        // no version) must not parse as a DurableWatermark.
        let legacy = serde_json::to_vec(&PostgresCheckpoint {
            lsn: "0/100".into(),
            tx_id: Some(1),
        })
        .unwrap();
        let good = pg_wm(1, "0/200", Some(2), true);
        assert_eq!(
            SourceCheckpointComparator.order(&legacy, &good.to_bytes()),
            CheckpointOrder::Incomparable
        );
    }

    #[test]
    fn cross_variant_pg_vs_incomplete_snapshot_is_incomparable() {
        // A CDC position and an INCOMPLETE snapshot of the same lineage are not
        // ordered (only a completed snapshot precedes CDC).
        let cdc = DurableWatermark::new(
            pg_lineage(1),
            WmPos::PgLsn {
                lsn: 10,
                commit_boundary: true,
                tx_id: None,
            },
        );
        let partial = snap(1, false, &[("orders", 10)]);
        assert_eq!(ord(&cdc, &partial), CheckpointOrder::Incomparable);
        assert_eq!(ord(&partial, &cdc), CheckpointOrder::Incomparable);
    }

    #[test]
    fn binlog_different_file_base_is_incomparable() {
        // Same server id, same index, but a differently-named binlog: not the
        // same coordinate space.
        let (b1, i1) = binlog_file_parts("mysql-bin.000009").unwrap();
        let (b2, i2) = binlog_file_parts("binlog.000009").unwrap();
        let a = DurableWatermark::new(
            server_lineage(1),
            WmPos::MysqlBinlog {
                file_base: b1,
                file_index: i1,
                pos: 4,
            },
        );
        let b = DurableWatermark::new(
            server_lineage(1),
            WmPos::MysqlBinlog {
                file_base: b2,
                file_index: i2,
                pos: 4,
            },
        );
        assert_eq!(ord(&a, &b), CheckpointOrder::Incomparable);
    }

    // ── SnapshotVector construction / restart / dominance ───────────────────

    fn ucursor(v: u64) -> SnapshotCursor {
        SnapshotCursor::Unsigned(v)
    }
    fn utables(names: &[&str]) -> Vec<(String, CursorKind)> {
        names
            .iter()
            .map(|t| (t.to_string(), CursorKind::Unsigned))
            .collect()
    }

    #[test]
    fn snapshot_vector_merges_progress_monotonically() {
        let mut v = SnapshotVector::new(1, &utables(&["orders", "users"]));
        v.observe("orders", ucursor(100));
        v.observe("users", ucursor(50));
        // A late/out-of-order lower cursor must NOT move it backward.
        v.observe("orders", ucursor(30));
        v.observe("orders", ucursor(150));
        // Unknown table is ignored (fixed target set).
        v.observe("audit", ucursor(999));
        match v.to_pos() {
            WmPos::Snapshot { table_cursors, .. } => {
                assert_eq!(table_cursors.get("orders"), Some(&ucursor(150)));
                assert_eq!(table_cursors.get("users"), Some(&ucursor(50)));
                assert!(!table_cursors.contains_key("audit"));
            }
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn snapshot_vector_merges_signed_cursors_including_negatives() {
        // Signed PK cursors: a negative value is below a positive one, and the
        // monotonic merge keeps the greater (never a u64-cast reordering).
        let mut v = SnapshotVector::new(
            1,
            &[("orders".to_string(), CursorKind::Signed)],
        );
        v.observe("orders", SnapshotCursor::Signed(-100));
        v.observe("orders", SnapshotCursor::Signed(-5));
        // A lower (more negative) cursor must not move it backward.
        v.observe("orders", SnapshotCursor::Signed(-50));
        // A different-kind observation for the table is ignored (no domain mix).
        v.observe("orders", SnapshotCursor::Unsigned(1_000_000));
        match v.to_pos() {
            WmPos::Snapshot { table_cursors, .. } => {
                assert_eq!(
                    table_cursors.get("orders"),
                    Some(&SnapshotCursor::Signed(-5))
                );
            }
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn snapshot_vector_batches_are_ordered_and_dominate() {
        let mut v = SnapshotVector::new(1, &utables(&["orders", "users"]));
        v.observe("orders", ucursor(100));
        v.observe("users", ucursor(40));
        let earlier = v.watermark(pg_lineage(1));
        // Parallel-table updates join into one monotonic vector.
        v.observe("users", ucursor(90));
        v.observe("orders", ucursor(250));
        let later = v.watermark(pg_lineage(1));

        assert_eq!(ord(&earlier, &later), CheckpointOrder::Before);
        assert_eq!(ord(&later, &earlier), CheckpointOrder::After);
        // A dominated (earlier) batch's components are all <= the later vector.
        if let (
            WmPos::Snapshot {
                table_cursors: e, ..
            },
            WmPos::Snapshot {
                table_cursors: l, ..
            },
        ) = (earlier.pos, later.pos)
        {
            for (t, ec) in &e {
                assert!(ec <= l.get(t).unwrap());
            }
        }
    }

    #[test]
    fn snapshot_vector_survives_restart_via_head() {
        let mut v = SnapshotVector::new(3, &utables(&["orders", "users"]));
        v.observe("orders", ucursor(100));
        v.observe("users", ucursor(200));
        v.mark_completed();
        let stored = v.watermark(pg_lineage(1)).to_bytes();

        // Restart: rebuild from the stored watermark bytes.
        let parsed = DurableWatermark::parse(&stored).unwrap();
        let restored = SnapshotVector::from_watermark(&parsed).unwrap();
        assert_eq!(restored, v);
        assert!(restored.completed());
        // Same position compares Equal to the original.
        assert_eq!(
            ord(
                &restored.watermark(pg_lineage(1)),
                &v.watermark(pg_lineage(1))
            ),
            CheckpointOrder::Equal
        );
    }

    #[test]
    fn snapshot_vectors_over_different_table_sets_are_incomparable() {
        // A lifecycle change (table set differs) must not be forced into an
        // order by defaulting missing components to zero.
        let a = snap(1, false, &[("orders", 100)]);
        let b = snap(1, false, &[("orders", 100), ("users", 0)]);
        assert_eq!(ord(&a, &b), CheckpointOrder::Incomparable);
    }

    #[test]
    fn s3_head_ahead_source_replay_is_skippable_per_sink() {
        // Ownership boundary: the source replays from its OWN progress (re-emitting
        // a lower cursor); each sink recovers its own HEAD independently and its
        // comparator decides skip-vs-apply. The SAME source replay is Before one
        // sink's ahead HEAD (that sink skips - no regression, no duplicate durable
        // write) and After a behind sink's HEAD (that sink applies it). The source
        // is never fast-forwarded to any sink's HEAD.
        let replay = snap(1, false, &[("orders", 100)]);
        let s3_ahead = snap(1, false, &[("orders", 500)]);
        let other_behind = snap(1, false, &[("orders", 50)]);
        assert_eq!(ord(&replay, &s3_ahead), CheckpointOrder::Before);
        assert_eq!(ord(&replay, &other_behind), CheckpointOrder::After);
        // Equal HEAD is also skippable (Before/Equal both let the sink skip).
        let s3_equal = snap(1, false, &[("orders", 100)]);
        assert_eq!(ord(&replay, &s3_equal), CheckpointOrder::Equal);
    }

    #[test]
    fn snapshot_signed_cursors_order_negatives_correctly() {
        use SnapshotCursor::Signed;
        // -100 precedes -5 precedes 10: signed order, never a u64 cast.
        let a = snap_typed(1, false, &[("orders", Signed(-100))]);
        let b = snap_typed(1, false, &[("orders", Signed(-5))]);
        let c = snap_typed(1, false, &[("orders", Signed(10))]);
        assert_eq!(ord(&a, &b), CheckpointOrder::Before);
        assert_eq!(ord(&b, &c), CheckpointOrder::Before);
        assert_eq!(ord(&c, &a), CheckpointOrder::After);
        assert_eq!(ord(&a, &a), CheckpointOrder::Equal);
    }

    #[test]
    fn snapshot_cursor_kind_mismatch_is_incomparable() {
        // Same table, same numeric value, different cursor domain (a schema /
        // PK-type change) must never be ordered.
        let signed =
            snap_typed(1, false, &[("orders", SnapshotCursor::Signed(5))]);
        let unsigned =
            snap_typed(1, false, &[("orders", SnapshotCursor::Unsigned(5))]);
        assert_eq!(ord(&signed, &unsigned), CheckpointOrder::Incomparable);
        let ctid =
            snap_typed(1, false, &[("orders", SnapshotCursor::CtidBlock(5))]);
        assert_eq!(ord(&unsigned, &ctid), CheckpointOrder::Incomparable);
    }

    // ── CDC commit-watermark constructors ───────────────────────────────────

    #[test]
    fn pg_commit_watermark_uses_end_lsn_and_orders() {
        let earlier = DurableWatermark::pg_commit(42, 0x100, Some(5));
        let later = DurableWatermark::pg_commit(42, 0x200, Some(6));
        // Both are commit-boundary; earlier end_lsn precedes later.
        assert_eq!(ord(&earlier, &later), CheckpointOrder::Before);
        match earlier.pos {
            WmPos::PgLsn {
                lsn,
                commit_boundary,
                tx_id,
            } => {
                assert_eq!(lsn, 0x100);
                assert!(commit_boundary);
                assert_eq!(tx_id, Some(5));
            }
            _ => panic!("expected PgLsn"),
        }
    }

    #[test]
    fn mysql_gtid_commit_watermark_uses_accumulated_set() {
        let uuid = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
        let small = DurableWatermark::mysql_gtid_commit(
            gtid_lineage([1; 16]),
            format!("{uuid}:1-5"),
        );
        let big = DurableWatermark::mysql_gtid_commit(
            gtid_lineage([1; 16]),
            format!("{uuid}:1-9"),
        );
        assert_eq!(ord(&small, &big), CheckpointOrder::Before);
        match small.pos {
            WmPos::MysqlGtid { gtid_set } => {
                assert_eq!(gtid_set, format!("{uuid}:1-5"));
            }
            _ => panic!("expected MysqlGtid"),
        }
    }

    #[test]
    fn mysql_binlog_commit_watermark_rotation_orders_numerically() {
        let f9 = DurableWatermark::mysql_binlog_commit(
            server_lineage(1),
            "mysql-bin.000009",
            900,
        )
        .unwrap();
        let f10 = DurableWatermark::mysql_binlog_commit(
            server_lineage(1),
            "mysql-bin.000010",
            4,
        )
        .unwrap();
        assert_eq!(ord(&f9, &f10), CheckpointOrder::Before);
        // A malformed filename yields no watermark (fail closed upstream).
        assert!(
            DurableWatermark::mysql_binlog_commit(
                server_lineage(1),
                "nodot",
                1
            )
            .is_none()
        );
    }
}
