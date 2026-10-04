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
//! The proven timeline is recorded durably (the continuity record) before the
//! stream starts. A source with a checkpoint but no record (every source
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

/// What the stream must continue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub lsn: Lsn,
    /// The timeline it was written on, when the checkpoint records one.
    pub timeline: Option<u32>,
}

/// The source's durable expectations.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Expected<'a> {
    /// `(system_identifier, database_oid)` of the source's lineage.
    pub lineage: (u64, u64),
    pub record: Option<&'a ContinuityRecord>,
    pub checkpoint: Option<Checkpoint>,
    /// Where the stream starts reading (at or after the checkpoint).
    pub start: Lsn,
}

impl Expected<'_> {
    /// The timeline the resume position belongs to.
    fn base_timeline(&self) -> Option<u32> {
        self.checkpoint
            .and_then(|c| c.timeline)
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
        evidence: RefusalEvidence,
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
    let mut evidence = RefusalEvidence {
        recorded_timeline: expected.base_timeline(),
        live_timeline: Some(facts.timeline),
        flush: Some(facts.flush),
        start: Some(expected.start),
        ..Default::default()
    };
    if let Some(slot) = &facts.slot {
        evidence.restart = slot.restart;
        evidence.confirmed = slot.confirmed;
    }
    let refuse = |class: &'static str, evidence: RefusalEvidence| {
        Err(Refusal::Unproven { class, evidence })
    };
    if facts.in_recovery {
        return refuse("server_in_recovery", evidence);
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
            system_identifier: 7,
            database_oid: 5,
            timeline,
            transition_id: 0,
            proven_at: None,
        }
    }

    fn expected(record: Option<&ContinuityRecord>) -> Expected<'_> {
        Expected {
            lineage: (7, 5),
            record,
            checkpoint: Some(Checkpoint {
                lsn: lsn(F),
                timeline: None,
            }),
            start: lsn(F),
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
        assert_eq!(proven.record.proven_at, Some(lsn(F).to_string()));
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
        e.checkpoint = Some(Checkpoint {
            lsn: lsn(F),
            timeline: Some(1),
        });
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
