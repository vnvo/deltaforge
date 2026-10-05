//! Continuity proof before every START_REPLICATION (design spec 7.24).
//!
//! A checkpoint F belongs to one WAL history. Before a stream opens, the
//! authenticated replication session itself shows which server and timeline
//! it is on (IDENTIFY_SYSTEM, TIMELINE_HISTORY, the slot row), and the stream
//! resumes only when all of these hold; anything unknown fails closed:
//!
//! 1. same system identifier and database OID as the source's lineage, on a
//!    primary (a server in recovery can switch timeline while the session
//!    is open, a primary cannot);
//! 2. the server's timeline is the one F was written on, or descends from it
//!    with that timeline's switch point at or after F and at or after the
//!    position the stream reads from (changes already read beyond the switch
//!    point came from a history the new timeline does not contain);
//! 3. after a timeline switch, PostgreSQL 17+ only: the slot is a synchronized
//!    failover slot (a slot of the same name alone proves nothing);
//! 4. the slot is persistent, logical, not invalidated, its WAL not lost, and
//!    its `restart_lsn` and `confirmed_flush_lsn` are at or before F;
//! 5. the server's WAL flush position is at or after F.
//!
//! The proven continuity is recorded durably (the continuity record) when the
//! stream becomes authoritative, and every checkpoint it produces carries its
//! stamp: the record's chain id (random, created once), the transition
//! sequence within that chain and the timeline. Timeline numbers alone prove
//! no ancestry (timelines 2 and 3 can both fork from 1); only positions of one
//! chain are ordered. A source with a checkpoint but no record (every source
//! before this record existed) is adopted only on a server that has never
//! switched timeline; otherwise an operator decides.
//!
//! Unsupported (documented): a same-timeline filesystem rewind that later
//! forks; nothing on the server distinguishes it from the original history.

use std::sync::Arc;

use anyhow::{Context, Result};
use pgwire_replication::Lsn;
use serde::{Deserialize, Serialize};
use storage::StorageBackend;

/// PostgreSQL 17: synchronized failover slots.
pub(crate) const FAILOVER_SLOTS_VERSION: u32 = 170_000;

const RECORD_FORMAT: u32 = 1;
/// Kept beside the source's server identity (`failover` namespace).
const NS: &str = "failover";

fn record_key(source_id: &str) -> String {
    format!("pg_continuity:{source_id}")
}

/// The durable continuity record of a source: the server history its
/// checkpoints belong to, as last proven.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContinuityRecord {
    pub format: u32,
    /// Created once, with the record; kept across proven transitions.
    pub chain_id: String,
    pub system_identifier: u64,
    pub database_oid: u64,
    pub timeline: u32,
    /// Counts the proven timeline transitions (0: as first recorded).
    pub transition_id: u64,
    /// The durable checkpoint the record was proven at (`None`: none yet).
    pub proven_at: Option<String>,
}

pub(crate) async fn load_record(
    backend: &Arc<dyn StorageBackend>,
    source_id: &str,
) -> Result<Option<ContinuityRecord>> {
    let Some(bytes) = backend
        .kv_get(NS, &record_key(source_id))
        .await
        .context("read the continuity record")?
    else {
        return Ok(None);
    };
    let record: ContinuityRecord = serde_json::from_slice(&bytes)
        .context("decode the continuity record")?;
    anyhow::ensure!(
        record.format == RECORD_FORMAT,
        "unsupported continuity record format {}",
        record.format
    );
    Ok(Some(record))
}

pub(crate) async fn store_record(
    backend: &Arc<dyn StorageBackend>,
    source_id: &str,
    record: &ContinuityRecord,
) -> Result<()> {
    let bytes = serde_json::to_vec(record)?;
    backend
        .kv_put(NS, &record_key(source_id), &bytes)
        .await
        .context("persist the continuity record")
}

/// One line of a timeline history file: `timeline` ended at `switch_point`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoryEntry {
    pub timeline: u32,
    pub switch_point: Lsn,
}

/// Parse a timeline history file (`<parent tli>\t<switch point>\t<reason>`
/// per line; blank lines and `#` comments skipped). Malformed content is an
/// error, never a shorter history.
pub(crate) fn parse_history(
    content: &str,
) -> Result<Vec<HistoryEntry>, String> {
    let mut entries = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let (Some(tli), Some(point)) = (fields.next(), fields.next()) else {
            return Err(format!("malformed history line '{line}'"));
        };
        let timeline = tli
            .parse()
            .map_err(|_| format!("malformed timeline in '{line}'"))?;
        let switch_point = Lsn::parse(point)
            .map_err(|_| format!("malformed switch point in '{line}'"))?;
        entries.push(HistoryEntry {
            timeline,
            switch_point,
        });
    }
    Ok(entries)
}

/// The slot row as the session read it. Every field is optional: a column
/// this server version lacks, or a NULL, is unknown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SlotFacts {
    pub slot_type: Option<String>,
    pub temporary: Option<bool>,
    /// Why the slot was invalidated (`invalidation_reason`, or `conflicting`
    /// before PostgreSQL 17), `None` when it was not.
    pub invalidation: Option<String>,
    pub wal_status: Option<String>,
    pub restart: Option<Lsn>,
    pub confirmed: Option<Lsn>,
    pub failover: Option<bool>,
    pub synced: Option<bool>,
}

impl SlotFacts {
    /// From `to_jsonb(pg_replication_slots row)`, which carries exactly the
    /// columns this server version has.
    pub(crate) fn from_json(row: &serde_json::Value) -> Self {
        let text =
            |k: &str| row.get(k).and_then(|v| v.as_str()).map(String::from);
        let flag = |k: &str| row.get(k).and_then(|v| v.as_bool());
        let lsn = |k: &str| text(k).and_then(|s| Lsn::parse(&s).ok());
        let invalidation = text("invalidation_reason").or_else(|| {
            (flag("conflicting") == Some(true)).then(|| "conflicting".into())
        });
        Self {
            slot_type: text("slot_type"),
            temporary: flag("temporary"),
            invalidation,
            wal_status: text("wal_status"),
            restart: lsn("restart_lsn"),
            confirmed: lsn("confirmed_flush_lsn"),
            failover: flag("failover"),
            synced: flag("synced"),
        }
    }
}

/// What the replication session showed before START_REPLICATION.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionFacts {
    pub system_identifier: u64,
    pub database_oid: u64,
    pub timeline: u32,
    /// WAL flush position (IDENTIFY_SYSTEM `xlogpos`).
    pub flush: Lsn,
    pub server_version_num: u32,
    /// `pg_is_in_recovery()`.
    pub in_recovery: bool,
    /// `None`: no slot of that name.
    pub slot: Option<SlotFacts>,
}

/// Where in a continuity chain a checkpoint was read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChainPosition {
    pub chain_id: String,
    pub transition: u64,
    pub timeline: u32,
}

/// What the stream must continue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub lsn: Lsn,
    /// Absent in checkpoints written before continuity was recorded.
    pub chain: Option<ChainPosition>,
}

/// The continuity stamp of an authoritative stream: what its checkpoints
/// carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stamp {
    pub chain_id: String,
    pub transition: u64,
    pub timeline: u32,
}

impl Stamp {
    pub(crate) fn of(record: &ContinuityRecord) -> Self {
        Self {
            chain_id: record.chain_id.clone(),
            transition: record.transition_id,
            timeline: record.timeline,
        }
    }

    /// The JSON members a checkpoint carries (after `tx_id`).
    pub(crate) fn checkpoint_members(&self) -> String {
        format!(
            r#","timeline":{},"chain":"{}","transition":{}"#,
            self.timeline, self.chain_id, self.transition
        )
    }
}

/// What a snapshot anchor's own session shows (a primary only): the
/// server and the timeline its WAL is written on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AnchorFacts {
    pub system_identifier: u64,
    pub database_oid: u64,
    pub timeline: u32,
}

/// Read [`AnchorFacts`] on `client`; a server in recovery fails (a snapshot
/// anchors on a primary, whose timeline cannot change while it runs).
pub(crate) async fn anchor_facts(
    client: &tokio_postgres::Client,
) -> Result<AnchorFacts> {
    let row = client
        .query_one(
            "SELECT pg_is_in_recovery(), \
                    (SELECT system_identifier FROM pg_control_system())::text, \
                    (SELECT oid FROM pg_database \
                      WHERE datname = current_database())::bigint, \
                    CASE WHEN pg_is_in_recovery() THEN NULL \
                         ELSE pg_walfile_name(pg_current_wal_lsn()) END",
            &[],
        )
        .await
        .context("read the server and timeline of the snapshot anchor")?;
    let in_recovery: bool = row.get(0);
    anyhow::ensure!(
        !in_recovery,
        "the server is in recovery: a snapshot anchors on a primary"
    );
    let sysid: String = row.get(1);
    let walfile: String = row.get(3);
    let timeline = walfile
        .get(..8)
        .and_then(|t| u32::from_str_radix(t, 16).ok())
        .with_context(|| format!("unexpected WAL file name {walfile:?}"))?;
    Ok(AnchorFacts {
        system_identifier: sysid
            .parse()
            .context("parse the system identifier")?,
        database_oid: u64::try_from(row.get::<_, i64>(2))
            .context("database oid")?,
        timeline,
    })
}

/// The continuity stamp of a snapshot anchor taken between two reads of
/// [`AnchorFacts`] (`docs/design/snapshot-durable-queue.md`, section 3.4),
/// and the continuity record to store first when it changes:
/// - the reads differ: the server changed or switched timeline while the
///   anchor was taken; refused;
/// - a record of the same server and timeline: its stamp;
/// - otherwise, when no sink holds a stream position (nothing to order
///   against the anchor), a new chain on this timeline; a stream position
///   unstamped (written before continuity was recorded) still joins it on a
///   server that never switched timeline, as the stream proof adopts one.
///   Any other case needs the stream proof: refused.
pub(crate) fn anchor_stamp(
    record: Option<&ContinuityRecord>,
    before: AnchorFacts,
    after: AnchorFacts,
    stream_position: Option<bool>,
    new_chain_id: &str,
) -> std::result::Result<(Stamp, Option<ContinuityRecord>), String> {
    if before != after {
        return Err(format!(
            "the server changed or switched timeline while the snapshot \
             anchor was taken (timeline {} then {})",
            before.timeline, after.timeline
        ));
    }
    let f = after;
    if let Some(r) = record {
        if (r.system_identifier, r.database_oid)
            != (f.system_identifier, f.database_oid)
        {
            return Err(
                "the continuity record belongs to another cluster or database"
                    .into(),
            );
        }
        if r.timeline == f.timeline {
            return Ok((Stamp::of(r), None));
        }
    }
    // `Some(stamped)`: a sink holds a stream position.
    let fresh = match (record, stream_position) {
        (_, None) => true,
        (None, Some(false)) => f.timeline == 1,
        _ => false,
    };
    if !fresh {
        return Err(format!(
            "the server is on timeline {}, and stream positions of another \
             history are stored: a stream start must prove the transition \
             before a snapshot can anchor on it (start once with snapshot \
             mode 'initial')",
            f.timeline
        ));
    }
    let record = ContinuityRecord {
        format: RECORD_FORMAT,
        chain_id: new_chain_id.to_string(),
        system_identifier: f.system_identifier,
        database_oid: f.database_oid,
        timeline: f.timeline,
        transition_id: 0,
        proven_at: None,
    };
    Ok((Stamp::of(&record), Some(record)))
}

/// A new random chain id (128 bits, hex).
pub(crate) fn new_chain_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

/// The source's durable expectations.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Expected<'a> {
    /// `(system_identifier, database_oid)` of the source's lineage.
    pub lineage: (u64, u64),
    pub record: Option<&'a ContinuityRecord>,
    pub checkpoint: Option<&'a Checkpoint>,
    /// Where the stream starts reading (at or after the checkpoint).
    pub start: Lsn,
    /// The chain id a record created now gets.
    pub new_chain_id: &'a str,
}

impl Expected<'_> {
    /// The timeline the resume position belongs to: the checkpoint's own,
    /// else (a checkpoint from before continuity was recorded) the record's.
    fn base_timeline(&self) -> Option<u32> {
        self.checkpoint
            .and_then(|c| c.chain.as_ref())
            .map(|c| c.timeline)
            .or(self.record.map(|r| r.timeline))
    }
}

/// The timeline whose history the proof needs, if the session is on another
/// timeline than the resume position.
pub(crate) fn history_needed(
    expected: &Expected<'_>,
    facts: &SessionFacts,
) -> Option<u32> {
    match expected.base_timeline() {
        Some(base) if base != facts.timeline => Some(facts.timeline),
        _ => None,
    }
}

/// Whether continuation across a failover is available for this slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailoverSlot {
    Enabled,
    /// PostgreSQL 17+, but the slot is not a failover slot.
    Disabled,
    /// Before PostgreSQL 17.
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Proven {
    pub record: ContinuityRecord,
    /// The record must be persisted before the stream starts.
    pub record_changed: bool,
    pub failover_slot: FailoverSlot,
}

/// Safe-text facts behind a refusal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RefusalEvidence {
    pub recorded_timeline: Option<u32>,
    pub live_timeline: Option<u32>,
    pub switch_point: Option<Lsn>,
    pub restart: Option<Lsn>,
    pub confirmed: Option<Lsn>,
    pub flush: Option<Lsn>,
    pub start: Option<Lsn>,
    pub checkpoint_chain: Option<String>,
    pub checkpoint_transition: Option<u64>,
    pub recorded_chain: Option<String>,
    pub recorded_transition: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// Another cluster or a replaced database.
    DifferentCluster {
        expected: (u64, u64),
        live: (u64, u64),
    },
    /// `pg_continuity_unproven` with a stable class.
    Unproven {
        class: &'static str,
        evidence: Box<RefusalEvidence>,
    },
}

/// Decide whether the stream may resume. `history` is the parsed history of
/// the timeline [`history_needed`] named (fetched by the caller).
pub(crate) fn prove(
    expected: &Expected<'_>,
    facts: &SessionFacts,
    history: Option<&[HistoryEntry]>,
) -> Result<Proven, Refusal> {
    let live = (facts.system_identifier, facts.database_oid);
    let recorded = expected
        .record
        .map(|r| (r.system_identifier, r.database_oid));
    for expected_id in std::iter::once(expected.lineage).chain(recorded) {
        if expected_id != live {
            return Err(Refusal::DifferentCluster {
                expected: expected_id,
                live,
            });
        }
    }

    let f = expected.checkpoint.map(|c| c.lsn);
    let chain_pos = expected.checkpoint.and_then(|c| c.chain.as_ref());
    let mut evidence = RefusalEvidence {
        recorded_timeline: expected.base_timeline(),
        live_timeline: Some(facts.timeline),
        flush: Some(facts.flush),
        start: Some(expected.start),
        checkpoint_chain: chain_pos.map(|c| c.chain_id.clone()),
        checkpoint_transition: chain_pos.map(|c| c.transition),
        recorded_chain: expected.record.map(|r| r.chain_id.clone()),
        recorded_transition: expected.record.map(|r| r.transition_id),
        ..Default::default()
    };
    if let Some(slot) = &facts.slot {
        evidence.restart = slot.restart;
        evidence.confirmed = slot.confirmed;
    }
    let refuse = |class: &'static str, evidence: RefusalEvidence| {
        Err(Refusal::Unproven {
            class,
            evidence: Box::new(evidence),
        })
    };
    if facts.in_recovery {
        return refuse("server_in_recovery", evidence);
    }

    // A checkpoint stamped by a chain continues only that chain, at or before
    // its recorded transition (an earlier transition is the predecessor the
    // durable position still belongs to right after a promotion). An
    // unstamped checkpoint (written before continuity was recorded) after a
    // proven transition is the one that transition was proven at, or nothing.
    match (chain_pos, expected.record) {
        (Some(_), None) => return refuse("timeline_unrecorded", evidence),
        (Some(pos), Some(r))
            if r.chain_id != pos.chain_id
                || pos.transition > r.transition_id =>
        {
            return refuse("checkpoint_chain_mismatch", evidence);
        }
        (None, Some(r))
            if r.transition_id > 0
                && f.is_some_and(|f| {
                    r.proven_at.as_deref() != Some(f.to_string().as_str())
                }) =>
        {
            return refuse("checkpoint_chain_mismatch", evidence);
        }
        _ => {}
    }

    // The timeline the resume position belongs to, and the record to keep.
    let (timeline, transition_id) = match (expected.record, f) {
        // A fresh source: nothing to continue yet.
        (None, None) => (facts.timeline, 0),
        // A checkpoint without a record: adopt only a server that has never
        // switched timeline.
        (None, Some(_)) if facts.timeline == 1 => (1, 0),
        (None, Some(_)) => return refuse("timeline_unrecorded", evidence),
        (Some(record), _) => {
            let base = expected.base_timeline().expect("record present");
            if base == facts.timeline {
                (facts.timeline, record.transition_id)
            } else {
                if facts.server_version_num < FAILOVER_SLOTS_VERSION {
                    return refuse("failover_unsupported_version", evidence);
                }
                let switch = history
                    .and_then(|h| h.iter().find(|e| e.timeline == base))
                    .filter(|_| facts.timeline > base);
                let Some(switch) = switch else {
                    return refuse("timeline_not_descended", evidence);
                };
                evidence.switch_point = Some(switch.switch_point);
                if f.is_some_and(|f| switch.switch_point < f) {
                    return refuse("switch_before_checkpoint", evidence);
                }
                if switch.switch_point < expected.start {
                    return refuse("switch_before_read_position", evidence);
                }
                let synced = facts.slot.as_ref().is_some_and(|s| {
                    s.failover == Some(true) && s.synced == Some(true)
                });
                if !synced {
                    return refuse("slot_not_synced", evidence);
                }
                let transition = if record.timeline == facts.timeline {
                    record.transition_id
                } else {
                    record.transition_id + 1
                };
                (facts.timeline, transition)
            }
        }
    };

    if let Some(f) = f {
        let Some(slot) = &facts.slot else {
            return refuse("slot_missing", evidence);
        };
        if slot.invalidation.is_some() {
            return refuse("slot_invalidated", evidence);
        }
        match slot.wal_status.as_deref() {
            Some("lost") => return refuse("wal_lost", evidence),
            Some(_) => {}
            None => return refuse("unknown_slot_position", evidence),
        }
        if slot.slot_type.as_deref() != Some("logical")
            || slot.temporary != Some(false)
        {
            return refuse("slot_not_persistent_logical", evidence);
        }
        match (slot.restart, slot.confirmed) {
            (Some(r), Some(c)) if r <= f && c <= f => {}
            (Some(_), Some(_)) => {
                return refuse("slot_beyond_checkpoint", evidence);
            }
            _ => return refuse("unknown_slot_position", evidence),
        }
        if facts.flush < f {
            return refuse("wal_behind_checkpoint", evidence);
        }
    }

    let failover_slot = if facts.server_version_num < FAILOVER_SLOTS_VERSION {
        FailoverSlot::Unsupported
    } else if facts.slot.as_ref().and_then(|s| s.failover) == Some(true) {
        FailoverSlot::Enabled
    } else {
        FailoverSlot::Disabled
    };

    let unchanged = expected.record.is_some_and(|r| {
        r.timeline == timeline && r.transition_id == transition_id
    });
    let record = match expected.record {
        Some(r) if unchanged => r.clone(),
        _ => ContinuityRecord {
            format: RECORD_FORMAT,
            chain_id: expected
                .record
                .map_or(expected.new_chain_id.to_string(), |r| {
                    r.chain_id.clone()
                }),
            system_identifier: facts.system_identifier,
            database_oid: facts.database_oid,
            timeline,
            transition_id,
            proven_at: f.map(|f| f.to_string()),
        },
    };
    Ok(Proven {
        record,
        record_changed: !unchanged,
        failover_slot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const F: u64 = 0x3000;

    fn lsn(v: u64) -> Lsn {
        Lsn::from(v)
    }

    fn good_slot() -> SlotFacts {
        SlotFacts {
            slot_type: Some("logical".into()),
            temporary: Some(false),
            invalidation: None,
            wal_status: Some("reserved".into()),
            restart: Some(lsn(F - 0x100)),
            confirmed: Some(lsn(F)),
            failover: Some(true),
            synced: Some(false),
        }
    }

    fn facts(timeline: u32) -> SessionFacts {
        SessionFacts {
            system_identifier: 7,
            database_oid: 5,
            timeline,
            flush: lsn(F + 0x1000),
            server_version_num: 170_002,
            in_recovery: false,
            slot: Some(good_slot()),
        }
    }

    fn record(timeline: u32) -> ContinuityRecord {
        ContinuityRecord {
            format: RECORD_FORMAT,
            chain_id: "c".into(),
            system_identifier: 7,
            database_oid: 5,
            timeline,
            transition_id: 0,
            proven_at: None,
        }
    }

    static LEGACY_F: std::sync::LazyLock<Checkpoint> =
        std::sync::LazyLock::new(|| Checkpoint {
            lsn: Lsn::from(F),
            chain: None,
        });

    fn expected(record: Option<&ContinuityRecord>) -> Expected<'_> {
        Expected {
            lineage: (7, 5),
            record,
            checkpoint: Some(&LEGACY_F),
            start: lsn(F),
            new_chain_id: "new",
        }
    }

    fn stamped(transition: u64, timeline: u32, chain: &str) -> Checkpoint {
        Checkpoint {
            lsn: lsn(F),
            chain: Some(ChainPosition {
                chain_id: chain.into(),
                transition,
                timeline,
            }),
        }
    }

    fn class(r: Result<Proven, Refusal>) -> &'static str {
        match r {
            Err(Refusal::Unproven { class, .. }) => class,
            other => panic!("expected a continuity refusal, got {other:?}"),
        }
    }

    fn promoted(mut f: SessionFacts) -> SessionFacts {
        f.slot.as_mut().unwrap().synced = Some(true);
        f
    }

    fn history(switch: u64) -> Vec<HistoryEntry> {
        vec![HistoryEntry {
            timeline: 1,
            switch_point: lsn(switch),
        }]
    }

    #[test]
    fn same_timeline_resumes_without_rewriting_the_record() {
        let rec = record(1);
        let proven = prove(&expected(Some(&rec)), &facts(1), None).unwrap();
        assert!(!proven.record_changed);
        assert_eq!(proven.record, rec);
        assert_eq!(proven.failover_slot, FailoverSlot::Enabled);
    }

    #[test]
    fn another_cluster_or_database_is_refused_before_anything_else() {
        let rec = record(1);
        let mut f = facts(1);
        f.system_identifier = 8;
        f.slot = None;
        assert!(matches!(
            prove(&expected(Some(&rec)), &f, None),
            Err(Refusal::DifferentCluster { .. })
        ));
        let mut f = facts(1);
        f.database_oid = 6;
        assert!(matches!(
            prove(&expected(None), &f, None),
            Err(Refusal::DifferentCluster { .. })
        ));
        // The record must agree with the lineage too.
        let mut other = record(1);
        other.system_identifier = 9;
        assert!(matches!(
            prove(&expected(Some(&other)), &facts(1), None),
            Err(Refusal::DifferentCluster { .. })
        ));
    }

    #[test]
    fn a_proven_promotion_records_the_new_timeline() {
        let rec = record(1);
        let e = expected(Some(&rec));
        let f = promoted(facts(2));
        assert_eq!(history_needed(&e, &f), Some(2));
        let proven = prove(&e, &f, Some(&history(F))).unwrap();
        assert!(proven.record_changed);
        assert_eq!(proven.record.timeline, 2);
        assert_eq!(proven.record.transition_id, 1);
        assert_eq!(proven.record.chain_id, "c", "the same chain");
        assert_eq!(proven.record.proven_at, Some(lsn(F).to_string()));
    }

    /// The process dies after persisting a proven transition (record at
    /// transition 1, timeline 2) but before any checkpoint of the new stream:
    /// the durable checkpoint still belongs to transition 0 on timeline 1.
    /// The restart accepts it as the preceding transition of the same chain,
    /// proves the switch again from it and resumes on the recorded timeline
    /// without another transition.
    #[test]
    fn a_crash_after_recording_a_transition_resumes_on_it() {
        let rec = ContinuityRecord {
            transition_id: 1,
            proven_at: Some(lsn(F).to_string()),
            ..record(2)
        };
        let mut e = expected(Some(&rec));
        let predecessor = stamped(0, 1, "c");
        e.checkpoint = Some(&predecessor);
        let f = promoted(facts(2));
        assert_eq!(
            history_needed(&e, &f),
            Some(2),
            "the switch is proven again"
        );
        let proven = prove(&e, &f, Some(&history(F))).unwrap();
        assert!(!proven.record_changed);
        assert_eq!(Stamp::of(&proven.record).transition, 1);
        assert_eq!(Stamp::of(&proven.record).timeline, 2);
        // The same with a checkpoint from before continuity was recorded:
        // only the position the transition was proven at.
        e.checkpoint = Some(&LEGACY_F);
        assert!(prove(&e, &f, None).is_ok());
        // Immediately before or after it: no proof ties it to the chain.
        for near in [F - 1, F + 1] {
            let other = Checkpoint {
                lsn: lsn(near),
                chain: None,
            };
            let mut e = expected(Some(&rec));
            e.checkpoint = Some(&other);
            assert_eq!(
                class(prove(&e, &f, Some(&history(F + 0x10)))),
                "checkpoint_chain_mismatch",
                "unstamped position {near:#x}"
            );
        }
    }

    #[test]
    fn a_chain_refusal_carries_both_chains() {
        let rec = ContinuityRecord {
            transition_id: 1,
            ..record(2)
        };
        let mut e = expected(Some(&rec));
        let other_chain = stamped(1, 2, "other");
        e.checkpoint = Some(&other_chain);
        match prove(&e, &facts(2), None) {
            Err(Refusal::Unproven { class, evidence }) => {
                assert_eq!(class, "checkpoint_chain_mismatch");
                assert_eq!(evidence.checkpoint_chain.as_deref(), Some("other"));
                assert_eq!(evidence.checkpoint_transition, Some(1));
                assert_eq!(evidence.recorded_chain.as_deref(), Some("c"));
                assert_eq!(evidence.recorded_transition, Some(1));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_checkpoint_continues_only_its_own_chain() {
        let rec = ContinuityRecord {
            transition_id: 1,
            ..record(2)
        };
        let mut e = expected(Some(&rec));
        let other_chain = stamped(1, 2, "other");
        e.checkpoint = Some(&other_chain);
        assert_eq!(
            class(prove(&e, &facts(2), None)),
            "checkpoint_chain_mismatch"
        );
        let ahead = stamped(2, 2, "c");
        e.checkpoint = Some(&ahead);
        assert_eq!(
            class(prove(&e, &facts(2), None)),
            "checkpoint_chain_mismatch",
            "a transition the record never proved"
        );
        let mine = stamped(1, 2, "c");
        e.checkpoint = Some(&mine);
        assert!(prove(&e, &facts(2), None).is_ok());
        // A stamped checkpoint without its record is not legacy.
        let mut e = expected(None);
        e.checkpoint = Some(&mine);
        assert_eq!(class(prove(&e, &facts(1), None)), "timeline_unrecorded");
    }

    #[test]
    fn a_switch_before_the_checkpoint_is_refused() {
        let rec = record(1);
        let f = promoted(facts(2));
        assert_eq!(
            class(prove(&expected(Some(&rec)), &f, Some(&history(F - 1)))),
            "switch_before_checkpoint"
        );
    }

    #[test]
    fn changes_read_beyond_the_switch_are_refused() {
        // The old stream read past the switch point before the promotion: the
        // new timeline does not contain what was read there.
        let rec = record(1);
        let mut e = expected(Some(&rec));
        e.start = lsn(F + 0x20);
        let f = promoted(facts(2));
        assert_eq!(
            class(prove(&e, &f, Some(&history(F + 0x10)))),
            "switch_before_read_position"
        );
        assert!(prove(&e, &f, Some(&history(F + 0x20))).is_ok());
    }

    #[test]
    fn a_history_without_the_recorded_timeline_is_refused() {
        let rec = record(1);
        let f = promoted(facts(3));
        let unrelated = vec![HistoryEntry {
            timeline: 2,
            switch_point: lsn(F + 0x10),
        }];
        assert_eq!(
            class(prove(&expected(Some(&rec)), &f, Some(&unrelated))),
            "timeline_not_descended"
        );
        assert_eq!(
            class(prove(&expected(Some(&rec)), &f, None)),
            "timeline_not_descended",
            "a missing history fails closed"
        );
        // A server on an older timeline than the record never descends.
        let rec = record(3);
        assert_eq!(
            class(prove(
                &expected(Some(&rec)),
                &promoted(facts(2)),
                Some(&history(F))
            )),
            "timeline_not_descended"
        );
    }

    #[test]
    fn after_a_switch_only_a_synced_failover_slot_continues() {
        let rec = record(1);
        let e = expected(Some(&rec));
        let mut not_synced = facts(2);
        not_synced.slot.as_mut().unwrap().synced = Some(false);
        assert_eq!(
            class(prove(&e, &not_synced, Some(&history(F)))),
            "slot_not_synced"
        );
        let mut no_failover = promoted(facts(2));
        no_failover.slot.as_mut().unwrap().failover = Some(false);
        assert_eq!(
            class(prove(&e, &no_failover, Some(&history(F)))),
            "slot_not_synced"
        );
        let mut unknown = promoted(facts(2));
        unknown.slot.as_mut().unwrap().synced = None;
        assert_eq!(
            class(prove(&e, &unknown, Some(&history(F)))),
            "slot_not_synced"
        );
    }

    #[test]
    fn a_switch_before_postgresql_17_stops_explicitly() {
        let rec = record(1);
        let mut f = promoted(facts(2));
        f.server_version_num = 160_004;
        assert_eq!(
            class(prove(&expected(Some(&rec)), &f, Some(&history(F)))),
            "failover_unsupported_version"
        );
    }

    #[test]
    fn the_checkpoint_timeline_takes_precedence_over_the_record() {
        // The record moved to timeline 2, but the checkpoint was written on 1:
        // the switch point from 1 must still be proven.
        let rec = ContinuityRecord {
            transition_id: 1,
            ..record(2)
        };
        let mut e = expected(Some(&rec));
        let checkpoint = stamped(0, 1, "c");
        e.checkpoint = Some(&checkpoint);
        let f = promoted(facts(2));
        assert_eq!(history_needed(&e, &f), Some(2));
        assert_eq!(
            class(prove(&e, &f, Some(&history(F - 1)))),
            "switch_before_checkpoint"
        );
        let proven = prove(&e, &f, Some(&history(F))).unwrap();
        assert!(!proven.record_changed, "no new transition");
    }

    #[test]
    fn a_legacy_source_is_adopted_only_on_a_server_that_never_switched() {
        let proven = prove(&expected(None), &facts(1), None).unwrap();
        assert!(proven.record_changed);
        assert_eq!(proven.record.timeline, 1);
        assert_eq!(proven.record.chain_id, "new", "a new chain");
        assert_eq!(
            class(prove(&expected(None), &facts(2), None)),
            "timeline_unrecorded"
        );
    }

    #[test]
    fn a_fresh_source_records_the_current_timeline() {
        let mut e = expected(None);
        e.checkpoint = None;
        let mut f = facts(4);
        f.slot = None;
        let proven = prove(&e, &f, None).unwrap();
        assert_eq!(proven.record.timeline, 4);
        assert_eq!(proven.record.proven_at, None);
    }

    #[test]
    fn slot_conditions_fail_closed() {
        let rec = record(1);
        let e = expected(Some(&rec));
        let with = |change: fn(&mut SlotFacts)| {
            let mut f = facts(1);
            change(f.slot.as_mut().unwrap());
            class(prove(&e, &f, None))
        };
        assert_eq!(
            with(|s| s.confirmed = Some(lsn(F + 1))),
            "slot_beyond_checkpoint"
        );
        assert_eq!(
            with(|s| s.restart = Some(lsn(F + 1))),
            "slot_beyond_checkpoint"
        );
        assert_eq!(with(|s| s.confirmed = None), "unknown_slot_position");
        assert_eq!(with(|s| s.restart = None), "unknown_slot_position");
        assert_eq!(with(|s| s.wal_status = None), "unknown_slot_position");
        assert_eq!(with(|s| s.wal_status = Some("lost".into())), "wal_lost");
        assert_eq!(
            with(|s| s.invalidation = Some("rows_removed".into())),
            "slot_invalidated"
        );
        assert_eq!(
            with(|s| s.temporary = Some(true)),
            "slot_not_persistent_logical"
        );
        assert_eq!(with(|s| s.temporary = None), "slot_not_persistent_logical");
        assert_eq!(
            with(|s| s.slot_type = Some("physical".into())),
            "slot_not_persistent_logical"
        );
        let mut f = facts(1);
        f.slot = None;
        assert_eq!(class(prove(&e, &f, None)), "slot_missing");
    }

    #[test]
    fn a_server_in_recovery_is_refused() {
        let rec = record(1);
        let mut f = facts(1);
        f.in_recovery = true;
        assert_eq!(
            class(prove(&expected(Some(&rec)), &f, None)),
            "server_in_recovery"
        );
    }

    #[test]
    fn wal_flushed_before_the_checkpoint_is_refused() {
        let rec = record(1);
        let mut f = facts(1);
        f.flush = lsn(F - 1);
        assert_eq!(
            class(prove(&expected(Some(&rec)), &f, None)),
            "wal_behind_checkpoint"
        );
    }

    #[test]
    fn failover_slot_property_is_reported() {
        let rec = record(1);
        let mut f = facts(1);
        f.slot.as_mut().unwrap().failover = Some(false);
        let p = prove(&expected(Some(&rec)), &f, None).unwrap();
        assert_eq!(p.failover_slot, FailoverSlot::Disabled);
        f.server_version_num = 160_000;
        f.slot.as_mut().unwrap().failover = None;
        let p = prove(&expected(Some(&rec)), &f, None).unwrap();
        assert_eq!(p.failover_slot, FailoverSlot::Unsupported);
    }

    #[test]
    fn history_files_parse_strictly() {
        let content = "1\t0/3000000\tno recovery target specified\n\n\
                       # comment\n2\t0/5000060\tbefore 2026-01-01\n";
        let h = parse_history(content).unwrap();
        assert_eq!(
            h,
            vec![
                HistoryEntry {
                    timeline: 1,
                    switch_point: Lsn::parse("0/3000000").unwrap()
                },
                HistoryEntry {
                    timeline: 2,
                    switch_point: Lsn::parse("0/5000060").unwrap()
                },
            ]
        );
        assert!(parse_history("1\n").is_err());
        assert!(parse_history("x\t0/1\tr\n").is_err());
        assert!(parse_history("1\tnot-an-lsn\tr\n").is_err());
    }

    #[test]
    fn slot_facts_read_every_server_version_shape() {
        let v17 = serde_json::json!({
            "slot_type": "logical", "temporary": false,
            "invalidation_reason": null, "wal_status": "reserved",
            "restart_lsn": "0/16B3748", "confirmed_flush_lsn": "0/16B3780",
            "failover": true, "synced": true
        });
        let s = SlotFacts::from_json(&v17);
        assert_eq!(s.failover, Some(true));
        assert_eq!(s.synced, Some(true));
        assert_eq!(s.invalidation, None);
        assert_eq!(s.restart, Some(Lsn::parse("0/16B3748").unwrap()));
        // PostgreSQL 16: no failover columns; `conflicting` marks invalidation.
        let v16 = serde_json::json!({
            "slot_type": "logical", "temporary": false, "conflicting": true,
            "wal_status": "reserved", "restart_lsn": null,
            "confirmed_flush_lsn": "0/1"
        });
        let s = SlotFacts::from_json(&v16);
        assert_eq!(s.failover, None);
        assert_eq!(s.invalidation.as_deref(), Some("conflicting"));
        assert_eq!(s.restart, None);
    }
}

#[cfg(test)]
mod anchor_stamp_tests {
    use super::*;

    fn facts(timeline: u32) -> AnchorFacts {
        AnchorFacts {
            system_identifier: 7,
            database_oid: 5,
            timeline,
        }
    }

    fn record(timeline: u32, transition_id: u64) -> ContinuityRecord {
        ContinuityRecord {
            format: RECORD_FORMAT,
            chain_id: "c".into(),
            system_identifier: 7,
            database_oid: 5,
            timeline,
            transition_id,
            proven_at: Some("0/10".into()),
        }
    }

    #[test]
    fn an_anchor_takes_the_proven_stamp_of_its_timeline() {
        let r = record(3, 2);
        let (stamp, store) =
            anchor_stamp(Some(&r), facts(3), facts(3), Some(true), "n")
                .unwrap();
        assert_eq!(stamp, Stamp::of(&r));
        assert!(store.is_none(), "nothing to record");
    }

    #[test]
    fn a_timeline_switch_during_the_anchor_is_refused() {
        assert!(anchor_stamp(None, facts(1), facts(2), None, "n").is_err());
        let mut other = facts(1);
        other.system_identifier = 8;
        assert!(anchor_stamp(None, facts(1), other, None, "n").is_err());
    }

    #[test]
    fn a_new_chain_only_when_nothing_needs_the_old_history() {
        // A fresh source, or one whose sinks hold no stream position.
        for record in [None, Some(record(1, 0))] {
            let (stamp, store) =
                anchor_stamp(record.as_ref(), facts(2), facts(2), None, "n")
                    .unwrap();
            let store = store.expect("a new record");
            assert_eq!((store.chain_id.as_str(), store.timeline), ("n", 2));
            assert_eq!(stamp, Stamp::of(&store));
        }
        // Unstamped stream positions join a chain only on timeline 1.
        assert!(
            anchor_stamp(None, facts(1), facts(1), Some(false), "n").is_ok()
        );
        assert!(
            anchor_stamp(None, facts(2), facts(2), Some(false), "n").is_err()
        );
        // Stream positions of another proven history: the stream proves it.
        assert!(
            anchor_stamp(
                Some(&record(1, 0)),
                facts(2),
                facts(2),
                Some(true),
                "n"
            )
            .is_err()
        );
        let mut foreign = record(2, 0);
        foreign.database_oid = 9;
        assert!(
            anchor_stamp(Some(&foreign), facts(2), facts(2), None, "n")
                .is_err()
        );
    }
}
