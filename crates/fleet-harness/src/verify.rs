//! The verifying consumer (design section 4.4).
//!
//! Every event is decoded from DeltaForge's native envelope. Its logical row
//! comes from `after`, or from `before` for deletes, never from the Kafka
//! message key. In-stream it checks message keys (when primary-key keys are
//! expected), schema probes after DDL, end-to-end lag, and recovery marks;
//! every event is spilled as a [`Record`] for the offline completeness and
//! ordering checks ([`crate::ledger::compare`]).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use chrono::NaiveDateTime;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::Value;

use crate::config::ExpectedKey;
use crate::ledger::{Op, Record, Writer};
use crate::stats::Histogram;
use crate::topology::{Naming, RowKey, TableRef};

/// A decoded row event.
#[derive(Debug, Clone, PartialEq)]
pub struct Decoded {
    pub row: RowKey,
    pub op: Op,
    pub version: u64,
    /// The driver's `committed_at` (microseconds), for inserts and updates.
    pub committed_micros: Option<i64>,
    pub image: serde_json::Map<String, Value>,
}

/// What an event is.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Row(Decoded),
    /// Not a row event of a customer table (a DDL event, a table outside
    /// the topology): counted, not verified.
    Other,
}

fn as_u64(v: &Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// `committed_at` as written by the driver: a `DATETIME(6)` value, as a
/// string in either SQL or ISO form, or microseconds.
pub fn parse_micros(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    let s = v.as_str()?;
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.fZ",
    ] {
        if let Ok(t) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(t.and_utc().timestamp_micros());
        }
    }
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.timestamp_micros())
}

/// Decode one native-envelope event of server `server`.
pub fn decode(naming: &Naming, server: u16, payload: &[u8]) -> Result<Event> {
    let v: Value =
        serde_json::from_slice(payload).context("event is not JSON")?;
    let Some(op) = v.get("op").and_then(Value::as_str) else {
        return Ok(Event::Other);
    };
    let op = match op {
        "c" | "r" => Op::Insert,
        "u" => Op::Update,
        "d" => Op::Delete,
        _ => return Ok(Event::Other),
    };
    let source = v
        .get("source")
        .ok_or_else(|| anyhow!("event without source"))?;
    let (Some(db), Some(table)) = (
        source
            .get("db")
            .and_then(Value::as_str)
            .and_then(|d| naming.parse_database(d)),
        source
            .get("table")
            .and_then(Value::as_str)
            .and_then(|t| naming.parse_table(t)),
    ) else {
        return Ok(Event::Other);
    };
    let image_key = if op == Op::Delete { "before" } else { "after" };
    let image = v
        .get(image_key)
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("{image_key} image missing"))?
        .clone();
    let id = image
        .get("id")
        .and_then(as_u64)
        .ok_or_else(|| anyhow!("row without id"))?;
    let version = image
        .get("version")
        .and_then(as_u64)
        .ok_or_else(|| anyhow!("row without version"))?;
    let committed_micros = if op == Op::Delete {
        None
    } else {
        image.get("committed_at").and_then(parse_micros)
    };
    Ok(Event::Row(Decoded {
        row: RowKey {
            table: TableRef { server, db, table },
            id,
        },
        op,
        version,
        committed_micros,
        image,
    }))
}

/// Expected values written after a migration statement: (row, version) ->
/// (column, value).
#[derive(Debug, Default, Clone)]
pub struct Probes(Arc<Mutex<ProbeMap>>);

/// (row, version) -> (column, expected value).
type ProbeMap = HashMap<(RowKey, u64), (String, i64)>;

impl Probes {
    pub fn expect(
        &self,
        row: RowKey,
        version: u64,
        column: String,
        value: i64,
    ) {
        self.0.lock().insert((row, version), (column, value));
    }

    fn take(&self, row: RowKey, version: u64) -> Option<(String, i64)> {
        self.0.lock().remove(&(row, version))
    }

    pub fn pending(&self) -> usize {
        self.0.lock().len()
    }
}

/// Per-server recovery marks: after `mark`, the first event committed later
/// than the mark ends the server's recovery.
#[derive(Debug, Default, Clone)]
pub struct RecoveryMarks(Arc<Mutex<MarkMap>>);

/// server -> (mark, first later event received).
type MarkMap = HashMap<u16, (i64, Option<i64>)>;

impl RecoveryMarks {
    pub fn mark(&self, servers: &[u16], at_micros: i64) {
        let mut m = self.0.lock();
        for s in servers {
            m.insert(*s, (at_micros, None));
        }
    }

    fn observe(&self, server: u16, committed: i64, received: i64) {
        if let Some((mark, first)) = self.0.lock().get_mut(&server)
            && first.is_none()
            && committed > *mark
        {
            *first = Some(received);
        }
    }

    /// Marked servers without a later event yet.
    pub fn pending(&self) -> usize {
        self.0.lock().values().filter(|(_, f)| f.is_none()).count()
    }

    /// Seconds from the mark to the first later event, per server.
    pub fn take(&self) -> HashMap<u16, Option<f64>> {
        self.0
            .lock()
            .drain()
            .map(|(s, (mark, first))| {
                (s, first.map(|f| (f - mark) as f64 / 1e6))
            })
            .collect()
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct StreamReport {
    pub events: u64,
    pub row_events: u64,
    pub other_events: u64,
    pub decode_errors: u64,
    /// Events whose message key was not the row's primary key (only when
    /// primary-key keys are expected).
    pub key_mismatches: u64,
    pub key_mismatches_on_deletes: u64,
    pub key_examples: Vec<String>,
    pub probes_ok: u64,
    pub probes_failed: u64,
    pub probe_examples: Vec<String>,
    /// Row events of other runs on the same topics (row ids carry the run
    /// tag): skipped, not verified.
    pub foreign_events: u64,
    /// Row events per server index.
    pub per_server: std::collections::BTreeMap<u16, u64>,
}

/// In-stream verification state.
pub struct Verifier {
    naming: Naming,
    expected_key: ExpectedKey,
    probes: Probes,
    marks: RecoveryMarks,
    spill: Writer,
    /// Only rows of this run (`id >> 40`) are verified.
    run_tag: Option<u64>,
    pub report: StreamReport,
    /// End-to-end lag (ms) per server, inserts and updates.
    pub lag_ms: HashMap<u16, Histogram>,
}

impl Verifier {
    pub fn new(
        naming: Naming,
        expected_key: ExpectedKey,
        probes: Probes,
        marks: RecoveryMarks,
        spill_path: &Path,
    ) -> Result<Self> {
        Ok(Verifier {
            naming,
            expected_key,
            probes,
            marks,
            spill: Writer::create(spill_path)?,
            run_tag: None,
            report: StreamReport::default(),
            lag_ms: HashMap::new(),
        })
    }

    /// Verify only rows written by the run tagged `tag`.
    pub fn for_run(mut self, tag: u64) -> Self {
        self.run_tag = Some(tag);
        self
    }

    /// Verify one consumed message of `server`.
    pub fn observe(
        &mut self,
        server: u16,
        partition: i32,
        offset: i64,
        key: Option<&[u8]>,
        payload: &[u8],
        received_micros: i64,
    ) -> Result<()> {
        self.report.events += 1;
        let d = match decode(&self.naming, server, payload) {
            Ok(Event::Row(d)) => d,
            Ok(Event::Other) => {
                self.report.other_events += 1;
                return Ok(());
            }
            Err(_) => {
                self.report.decode_errors += 1;
                return Ok(());
            }
        };
        if self.run_tag.is_some_and(|t| d.row.id >> 40 != t) {
            self.report.foreign_events += 1;
            return Ok(());
        }
        self.report.row_events += 1;
        *self.report.per_server.entry(server).or_insert(0) += 1;
        if self.expected_key == ExpectedKey::PrimaryKey {
            let want = d.row.id.to_string();
            if key != Some(want.as_bytes()) {
                self.report.key_mismatches += 1;
                if d.op == Op::Delete {
                    self.report.key_mismatches_on_deletes += 1;
                }
                if self.report.key_examples.len() < 20 {
                    self.report.key_examples.push(format!(
                        "{:?} {:?}: key {:?}, expected {want:?}",
                        d.row,
                        d.op,
                        key.map(String::from_utf8_lossy)
                    ));
                }
            }
        }
        if d.op != Op::Delete
            && let Some((column, value)) = self.probes.take(d.row, d.version)
        {
            if d.image.get(&column).and_then(Value::as_i64) == Some(value) {
                self.report.probes_ok += 1;
            } else {
                self.report.probes_failed += 1;
                if self.report.probe_examples.len() < 20 {
                    self.report.probe_examples.push(format!(
                        "{:?} v{}: {column} = {:?}, expected {value}",
                        d.row,
                        d.version,
                        d.image.get(&column)
                    ));
                }
            }
        }
        if let Some(committed) = d.committed_micros {
            let lag = (received_micros - committed).max(0) as u64 / 1_000;
            self.lag_ms.entry(server).or_default().record(lag);
            self.marks.observe(server, committed, received_micros);
        }
        self.spill.append(&Record {
            row: d.row,
            version: d.version,
            op: d.op,
            at_micros: received_micros,
            partition,
            offset,
        })
    }

    /// Flush the spill; returns the records written.
    pub fn finish(
        self,
    ) -> Result<(StreamReport, HashMap<u16, Histogram>, u64)> {
        let n = self.spill.finish()?;
        Ok((self.report, self.lag_ms, n))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn naming() -> Naming {
        Naming {
            database_prefix: "cust_".into(),
        }
    }

    fn event(op: &str, image: Value) -> Vec<u8> {
        let (before, after) = if op == "d" {
            (image, Value::Null)
        } else {
            (Value::Null, image)
        };
        serde_json::to_vec(&json!({
            "before": before, "after": after, "op": op, "ts_ms": 1,
            "source": {"db": "cust_00002", "table": "t003", "connector": "mysql"}
        }))
        .unwrap()
    }

    fn verifier(dir: &Path, key: ExpectedKey, probes: Probes) -> Verifier {
        Verifier::new(
            naming(),
            key,
            probes,
            RecoveryMarks::default(),
            &dir.join("c.bin"),
        )
        .unwrap()
    }

    #[test]
    fn identity_comes_from_after_or_before() {
        let ins = decode(&naming(), 1, &event("c", json!({"id": 7, "version": 3, "committed_at": "2026-10-08 10:00:00.000001"}))).unwrap();
        let Event::Row(ins) = ins else { panic!() };
        assert_eq!(
            ins.row.table,
            TableRef {
                server: 1,
                db: 2,
                table: 3
            }
        );
        assert_eq!((ins.row.id, ins.version, ins.op), (7, 3, Op::Insert));
        assert!(ins.committed_micros.is_some());
        let del =
            decode(&naming(), 1, &event("d", json!({"id": 7, "version": 3})))
                .unwrap();
        let Event::Row(del) = del else { panic!() };
        assert_eq!((del.row.id, del.version, del.op), (7, 3, Op::Delete));
        assert_eq!(del.committed_micros, None);
        let other = serde_json::to_vec(&json!({"op": "c", "after": {"id": 1}, "source": {"db": "mysql", "table": "user"}})).unwrap();
        assert_eq!(decode(&naming(), 1, &other).unwrap(), Event::Other);
    }

    #[test]
    fn primary_key_keys_are_checked_on_every_operation_when_expected() {
        let dir = tempfile::tempdir().unwrap();
        let mut v =
            verifier(dir.path(), ExpectedKey::PrimaryKey, Probes::default());
        let now = 1_000_000;
        v.observe(
            1,
            0,
            0,
            Some(b"7"),
            &event("c", json!({"id": 7, "version": 1})),
            now,
        )
        .unwrap();
        // A delete keyed by a template on `after` arrives with an empty key.
        v.observe(
            1,
            0,
            1,
            Some(b""),
            &event("d", json!({"id": 7, "version": 1})),
            now,
        )
        .unwrap();
        assert_eq!(v.report.key_mismatches, 1);
        assert_eq!(v.report.key_mismatches_on_deletes, 1);
        let mut off =
            verifier(dir.path(), ExpectedKey::None, Probes::default());
        off.observe(
            1,
            0,
            1,
            Some(b""),
            &event("d", json!({"id": 7, "version": 1})),
            now,
        )
        .unwrap();
        assert_eq!(off.report.key_mismatches, 0);
    }

    #[test]
    fn probes_check_the_new_column_and_marks_measure_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let probes = Probes::default();
        let row = RowKey {
            table: TableRef {
                server: 1,
                db: 2,
                table: 3,
            },
            id: 9,
        };
        probes.expect(row, 5, "m0001_00".into(), 42);
        probes.expect(RowKey { id: 10, ..row }, 6, "m0001_00".into(), 43);
        let marks = RecoveryMarks::default();
        let mut v = Verifier::new(
            naming(),
            ExpectedKey::None,
            probes.clone(),
            marks.clone(),
            &dir.path().join("c.bin"),
        )
        .unwrap();
        marks.mark(&[1], 1_000_000);
        let at = |s: &str| json!(s);
        v.observe(1, 0, 0, None, &event("u", json!({"id": 9, "version": 5, "m0001_00": 42, "committed_at": at("1970-01-01 00:00:02.000000")})), 3_000_000).unwrap();
        v.observe(1, 0, 1, None, &event("u", json!({"id": 10, "version": 6, "committed_at": at("1970-01-01 00:00:02.500000")})), 3_500_000).unwrap();
        assert_eq!((v.report.probes_ok, v.report.probes_failed), (1, 1));
        assert_eq!(probes.pending(), 0);
        assert_eq!(marks.take().get(&1), Some(&Some(2.0)));
        let (report, lag, spilled) = v.finish().unwrap();
        assert_eq!((report.row_events, spilled), (2, 2));
        assert_eq!(lag[&1].quantile(1.0), Some(1_000));
    }
}
