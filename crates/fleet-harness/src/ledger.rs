//! Spilled operation records and the bounded-memory completeness check.
//!
//! The driver appends one [`Record`] per committed operation, and the
//! verifier one per consumed event, to files (a 6-hour run at fleet scale is
//! billions of operations, too many to hold in memory). The final check
//! sorts both sides externally by (row, version) and merges them.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::topology::RowKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[repr(u8)]
pub enum Op {
    Insert = 1,
    Update = 2,
    Delete = 3,
}

impl Op {
    fn from_u8(v: u8) -> Op {
        match v {
            1 => Op::Insert,
            2 => Op::Update,
            _ => Op::Delete,
        }
    }
}

/// One operation: written or consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record {
    pub row: RowKey,
    /// The driver's operation sequence: increasing per row.
    pub version: u64,
    pub op: Op,
    /// Microseconds: commit time (written) or receive time (consumed).
    pub at_micros: i64,
    /// Kafka partition (consumed), or -1 (written).
    pub partition: i32,
    /// Kafka offset within the partition (consumed), or -1 (written).
    pub offset: i64,
}

pub const RECORD_BYTES: usize = 48;
/// Bytes compared when sorting: row key, big-endian version, then the
/// operation. A delete carries the version of the row it removes (its before
/// image), so it sorts right after that version's insert or update.
const SORT_KEY: usize = RowKey::BYTES + 8 + 1;

impl Record {
    pub fn encode(&self) -> [u8; RECORD_BYTES] {
        let mut b = [0u8; RECORD_BYTES];
        b[..16].copy_from_slice(&self.row.to_bytes());
        b[16..24].copy_from_slice(&self.version.to_be_bytes());
        b[24] = self.op as u8;
        b[25..33].copy_from_slice(&self.at_micros.to_le_bytes());
        b[33..37].copy_from_slice(&self.partition.to_le_bytes());
        b[37..45].copy_from_slice(&self.offset.to_le_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> Record {
        Record {
            row: RowKey::from_bytes(&b[..16]),
            version: u64::from_be_bytes(b[16..24].try_into().expect("8")),
            op: Op::from_u8(b[24]),
            at_micros: i64::from_le_bytes(b[25..33].try_into().expect("8")),
            partition: i32::from_le_bytes(b[33..37].try_into().expect("4")),
            offset: i64::from_le_bytes(b[37..45].try_into().expect("8")),
        }
    }
}

/// An append-only record file.
pub struct Writer {
    out: BufWriter<File>,
    count: u64,
}

impl Writer {
    pub fn create(path: &Path) -> Result<Writer> {
        let f = File::create(path)
            .with_context(|| format!("create {}", path.display()))?;
        Ok(Writer {
            out: BufWriter::with_capacity(1 << 20, f),
            count: 0,
        })
    }

    pub fn append(&mut self, r: &Record) -> Result<()> {
        self.out.write_all(&r.encode())?;
        self.count += 1;
        Ok(())
    }

    pub fn finish(mut self) -> Result<u64> {
        self.out.flush()?;
        Ok(self.count)
    }
}

fn read_record(r: &mut impl Read) -> Result<Option<[u8; RECORD_BYTES]>> {
    let mut b = [0u8; RECORD_BYTES];
    match r.read_exact(&mut b) {
        Ok(()) => Ok(Some(b)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Sort the records of `inputs` by (row, version) into one file in `dir`,
/// holding at most `chunk_records` in memory.
pub fn sort_files(
    inputs: &[PathBuf],
    dir: &Path,
    chunk_records: usize,
) -> Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let mut runs = Vec::new();
    let mut chunk: Vec<[u8; RECORD_BYTES]> =
        Vec::with_capacity(chunk_records.min(1 << 20));
    let flush = |chunk: &mut Vec<[u8; RECORD_BYTES]>,
                 runs: &mut Vec<PathBuf>|
     -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        chunk.sort_unstable_by(|a, b| a[..SORT_KEY].cmp(&b[..SORT_KEY]));
        let path = dir.join(format!("run-{:05}.bin", runs.len()));
        let mut w = BufWriter::with_capacity(1 << 20, File::create(&path)?);
        for r in chunk.iter() {
            w.write_all(r)?;
        }
        w.flush()?;
        runs.push(path);
        chunk.clear();
        Ok(())
    };
    for input in inputs {
        let mut r = BufReader::with_capacity(1 << 20, File::open(input)?);
        while let Some(rec) = read_record(&mut r)? {
            chunk.push(rec);
            if chunk.len() >= chunk_records {
                flush(&mut chunk, &mut runs)?;
            }
        }
    }
    flush(&mut chunk, &mut runs)?;
    let out = dir.join("sorted.bin");
    merge_runs(&runs, &out)?;
    for run in runs {
        std::fs::remove_file(run).ok();
    }
    Ok(out)
}

struct Head {
    rec: [u8; RECORD_BYTES],
    src: usize,
}

impl PartialEq for Head {
    fn eq(&self, o: &Self) -> bool {
        self.rec[..SORT_KEY] == o.rec[..SORT_KEY]
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Head {
    fn cmp(&self, o: &Self) -> Ordering {
        // Min-heap on the sort key.
        o.rec[..SORT_KEY].cmp(&self.rec[..SORT_KEY])
    }
}

fn merge_runs(runs: &[PathBuf], out: &Path) -> Result<()> {
    let mut readers = runs
        .iter()
        .map(|p| Ok(BufReader::with_capacity(1 << 20, File::open(p)?)))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (src, r) in readers.iter_mut().enumerate() {
        if let Some(rec) = read_record(r)? {
            heap.push(Head { rec, src });
        }
    }
    let mut w = BufWriter::with_capacity(1 << 20, File::create(out)?);
    while let Some(Head { rec, src }) = heap.pop() {
        w.write_all(&rec)?;
        if let Some(next) = read_record(&mut readers[src])? {
            heap.push(Head { rec: next, src });
        }
    }
    w.flush()?;
    Ok(())
}

/// The completeness and final-state result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Completeness {
    pub written: u64,
    pub consumed: u64,
    /// Written operations never consumed.
    pub missing: u64,
    /// Consumed operations beyond the first copy (at-least-once).
    pub duplicates: u64,
    /// Consumed operations that were never written.
    pub unexpected: u64,
    /// Consumed operations of transactions whose commit outcome the driver
    /// could not know (neither missing nor unexpected).
    pub uncertain_delivered: u64,
    /// Rows whose last written version was never consumed.
    pub final_state_mismatches: u64,
    /// Operations of a row consumed out of order within one partition
    /// (a guarantee violation).
    pub partition_order_violations: u64,
    /// Rows whose events were spread over more than one partition (with a
    /// primary-key template: deletes keyed differently).
    pub cross_partition_rows: u64,
    /// Operations received before an earlier operation of the same row that
    /// sits in another partition (reported, not guaranteed: design 4.4).
    pub cross_partition_reorders: u64,
    /// Up to 20 examples of missing operations.
    pub missing_examples: Vec<String>,
}

impl Completeness {
    /// The guarantees: everything delivered, nothing invented, final state
    /// right, and order kept within each partition.
    pub fn ok(&self) -> bool {
        self.missing == 0
            && self.unexpected == 0
            && self.final_state_mismatches == 0
            && self.partition_order_violations == 0
    }
}

/// Compare a sorted written file with a sorted consumed file; `uncertain`
/// (sorted) lists operations whose commit outcome is unknown.
pub fn compare(
    written: &Path,
    consumed: &Path,
    uncertain: Option<&Path>,
) -> Result<Completeness> {
    let mut w = BufReader::with_capacity(1 << 20, File::open(written)?);
    let mut c = BufReader::with_capacity(1 << 20, File::open(consumed)?);
    let mut u = match uncertain {
        Some(p) => Some(BufReader::with_capacity(1 << 20, File::open(p)?)),
        None => None,
    };
    let mut next_u = match u.as_mut() {
        Some(r) => read_record(r)?,
        None => None,
    };
    let mut out = Completeness::default();
    let mut next_w = read_record(&mut w)?;
    let mut next_c = read_record(&mut c)?;
    // The last written record of the current row, and whether it was seen.
    let mut last_written: Option<([u8; RECORD_BYTES], bool)> = None;
    let close_row = |last: &mut Option<([u8; RECORD_BYTES], bool)>,
                     out: &mut Completeness| {
        if let Some((_, seen)) = last.take()
            && !seen
        {
            out.final_state_mismatches += 1;
        }
    };
    let mut prev_c: Option<[u8; RECORD_BYTES]> = None;
    let mut order = RowOrder::default();
    loop {
        match (next_w, next_c) {
            (None, None) => break,
            (Some(a), b) if b.is_none_or(|b| a[..SORT_KEY] < b[..SORT_KEY]) => {
                out.written += 1;
                out.missing += 1;
                if out.missing_examples.len() < 20 {
                    let r = Record::decode(&a);
                    out.missing_examples
                        .push(format!("{:?} v{} {:?}", r.row, r.version, r.op));
                }
                if last_written.is_some_and(|(l, _)| l[..16] != a[..16]) {
                    close_row(&mut last_written, &mut out);
                }
                last_written = Some((a, false));
                next_w = read_record(&mut w)?;
            }
            (a, Some(b)) if a.is_none_or(|a| b[..SORT_KEY] < a[..SORT_KEY]) => {
                out.consumed += 1;
                if prev_c.is_some_and(|p| p[..SORT_KEY] == b[..SORT_KEY]) {
                    out.duplicates += 1;
                } else {
                    // Advance the uncertain list to this key.
                    while let (Some(x), Some(r)) = (next_u, u.as_mut()) {
                        if x[..SORT_KEY] < b[..SORT_KEY] {
                            next_u = read_record(r)?;
                        } else {
                            break;
                        }
                    }
                    if next_u.is_some_and(|x| x[..SORT_KEY] == b[..SORT_KEY]) {
                        out.uncertain_delivered += 1;
                    } else {
                        out.unexpected += 1;
                    }
                    order.observe(&Record::decode(&b), &mut out);
                }
                prev_c = Some(b);
                next_c = read_record(&mut c)?;
            }
            (Some(a), Some(b)) => {
                // Equal keys: written and consumed.
                out.written += 1;
                out.consumed += 1;
                if last_written.is_some_and(|(l, _)| l[..16] != a[..16]) {
                    close_row(&mut last_written, &mut out);
                }
                last_written = Some((a, true));
                order.observe(&Record::decode(&b), &mut out);
                prev_c = Some(b);
                next_w = read_record(&mut w)?;
                next_c = read_record(&mut c)?;
            }
            _ => unreachable!("all cases covered"),
        }
    }
    close_row(&mut last_written, &mut out);
    order.finish(&mut out);
    Ok(out)
}

/// Per-row ordering over consumed records in (version, op) order: within a
/// partition offsets must increase; across partitions, receive times are
/// compared to report reorderings.
#[derive(Default)]
struct RowOrder {
    row: Option<RowKey>,
    /// (partition, highest offset) of the row's records so far.
    partitions: Vec<(i32, i64)>,
    /// The latest receive time among lower (version, op) records, and its
    /// partition.
    latest: Option<(i64, i32)>,
}

impl RowOrder {
    fn observe(&mut self, r: &Record, out: &mut Completeness) {
        if self.row != Some(r.row) {
            self.finish(out);
            self.row = Some(r.row);
        }
        match self.partitions.iter_mut().find(|(p, _)| *p == r.partition) {
            Some((_, last)) => {
                if r.offset < *last {
                    out.partition_order_violations += 1;
                }
                *last = (*last).max(r.offset);
            }
            None => self.partitions.push((r.partition, r.offset)),
        }
        if let Some((at, p)) = self.latest
            && p != r.partition
            && r.at_micros < at
        {
            out.cross_partition_reorders += 1;
        }
        if self.latest.is_none_or(|(at, _)| r.at_micros > at) {
            self.latest = Some((r.at_micros, r.partition));
        }
    }

    fn finish(&mut self, out: &mut Completeness) {
        if self.partitions.len() > 1 {
            out.cross_partition_rows += 1;
        }
        self.partitions.clear();
        self.latest = None;
        self.row = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::TableRef;

    fn rec(id: u64, version: u64, op: Op) -> Record {
        Record {
            row: RowKey {
                table: TableRef {
                    server: 1,
                    db: 2,
                    table: 3,
                },
                id,
            },
            version,
            op,
            at_micros: version as i64,
            partition: -1,
            offset: -1,
        }
    }

    fn consumed(
        id: u64,
        version: u64,
        op: Op,
        partition: i32,
        offset: i64,
        at: i64,
    ) -> Record {
        Record {
            partition,
            offset,
            at_micros: at,
            ..rec(id, version, op)
        }
    }

    fn write(dir: &Path, name: &str, records: &[Record]) -> PathBuf {
        let path = dir.join(name);
        let mut w = Writer::create(&path).unwrap();
        for r in records {
            w.append(r).unwrap();
        }
        w.finish().unwrap();
        path
    }

    #[test]
    fn records_round_trip() {
        let r = Record {
            partition: 7,
            offset: 1 << 33,
            ..rec(9, 1 << 40, Op::Delete)
        };
        assert_eq!(Record::decode(&r.encode()), r);
    }

    #[test]
    fn external_sort_orders_across_chunks_and_files() {
        let dir = tempfile::tempdir().unwrap();
        let a = write(
            dir.path(),
            "a",
            &[
                rec(3, 9, Op::Insert),
                rec(1, 5, Op::Insert),
                rec(2, 1, Op::Insert),
            ],
        );
        let b = write(
            dir.path(),
            "b",
            &[rec(1, 2, Op::Insert), rec(3, 1, Op::Insert)],
        );
        let sorted = sort_files(&[a, b], &dir.path().join("s"), 2).unwrap();
        let mut r = BufReader::new(File::open(sorted).unwrap());
        let mut got = Vec::new();
        while let Some(b) = read_record(&mut r).unwrap() {
            let d = Record::decode(&b);
            got.push((d.row.id, d.version));
        }
        assert_eq!(got, vec![(1, 2), (1, 5), (2, 1), (3, 1), (3, 9)]);
    }

    #[test]
    fn compare_counts_missing_duplicates_unexpected_and_final_state() {
        let dir = tempfile::tempdir().unwrap();
        let written = [
            rec(1, 1, Op::Insert),
            rec(1, 4, Op::Update),
            rec(2, 2, Op::Insert),
            rec(2, 3, Op::Delete),
            rec(3, 5, Op::Insert),
        ];
        let consumed = [
            rec(1, 1, Op::Insert),
            rec(1, 1, Op::Insert), // duplicate
            rec(1, 4, Op::Update),
            rec(2, 2, Op::Insert), // row 2's delete (v3) missing: final state wrong
            rec(3, 5, Op::Insert),
            rec(4, 6, Op::Insert), // never written
        ];
        let w = sort_files(
            &[write(dir.path(), "w", &written)],
            &dir.path().join("ws"),
            100,
        )
        .unwrap();
        let c = sort_files(
            &[write(dir.path(), "c", &consumed)],
            &dir.path().join("cs"),
            100,
        )
        .unwrap();
        let r = compare(&w, &c, None).unwrap();
        assert_eq!(
            (
                r.written,
                r.consumed,
                r.missing,
                r.duplicates,
                r.unexpected,
                r.final_state_mismatches
            ),
            (5, 6, 1, 1, 1, 1)
        );
        assert!(!r.ok());
        assert_eq!(r.missing_examples.len(), 1);
    }

    #[test]
    fn a_complete_run_is_ok_despite_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let written = [rec(1, 1, Op::Insert), rec(1, 2, Op::Delete)];
        let consumed = [
            rec(1, 2, Op::Delete),
            rec(1, 1, Op::Insert),
            rec(1, 1, Op::Insert),
        ];
        let w = sort_files(
            &[write(dir.path(), "w", &written)],
            &dir.path().join("ws"),
            1,
        )
        .unwrap();
        let c = sort_files(
            &[write(dir.path(), "c", &consumed)],
            &dir.path().join("cs"),
            1,
        )
        .unwrap();
        let r = compare(&w, &c, None).unwrap();
        assert!(r.ok(), "{r:?}");
        assert_eq!(r.duplicates, 1);
    }

    #[test]
    fn a_delete_sorts_after_the_version_it_removes() {
        let dir = tempfile::tempdir().unwrap();
        let written = [
            rec(1, 4, Op::Delete),
            rec(1, 4, Op::Update),
            rec(1, 1, Op::Insert),
        ];
        let sorted = sort_files(
            &[write(dir.path(), "w", &written)],
            &dir.path().join("s"),
            10,
        )
        .unwrap();
        let mut r = BufReader::new(File::open(sorted).unwrap());
        let mut ops = Vec::new();
        while let Some(b) = read_record(&mut r).unwrap() {
            ops.push(Record::decode(&b).op);
        }
        assert_eq!(ops, vec![Op::Insert, Op::Update, Op::Delete]);
    }

    #[test]
    fn ordering_within_and_across_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let written = [
            rec(1, 1, Op::Insert),
            rec(1, 2, Op::Update),
            rec(1, 2, Op::Delete),
            rec(2, 3, Op::Insert),
            rec(2, 4, Op::Update),
        ];
        let got = [
            // Row 1: insert and update in partition 0; the delete keyed to
            // partition 5 and received before the update.
            consumed(1, 1, Op::Insert, 0, 10, 100),
            consumed(1, 2, Op::Update, 0, 11, 300),
            consumed(1, 2, Op::Delete, 5, 3, 200),
            // Row 2: the update precedes the insert in the same partition.
            consumed(2, 3, Op::Insert, 1, 21, 500),
            consumed(2, 4, Op::Update, 1, 20, 400),
        ];
        let w = sort_files(
            &[write(dir.path(), "w", &written)],
            &dir.path().join("ws"),
            100,
        )
        .unwrap();
        let c = sort_files(
            &[write(dir.path(), "c", &got)],
            &dir.path().join("cs"),
            100,
        )
        .unwrap();
        let r = compare(&w, &c, None).unwrap();
        assert_eq!(r.missing, 0);
        assert_eq!(r.cross_partition_rows, 1);
        assert_eq!(r.cross_partition_reorders, 1);
        assert_eq!(r.partition_order_violations, 1);
        assert!(!r.ok(), "a within-partition violation fails the run");
    }

    #[test]
    fn uncertain_operations_are_neither_missing_nor_unexpected() {
        let dir = tempfile::tempdir().unwrap();
        let written = [rec(1, 1, Op::Insert)];
        let uncertain = [rec(2, 2, Op::Insert), rec(3, 3, Op::Insert)];
        let consumed = [rec(1, 1, Op::Insert), rec(2, 2, Op::Insert)];
        let w = sort_files(
            &[write(dir.path(), "w", &written)],
            &dir.path().join("ws"),
            10,
        )
        .unwrap();
        let u = sort_files(
            &[write(dir.path(), "u", &uncertain)],
            &dir.path().join("us"),
            10,
        )
        .unwrap();
        let c = sort_files(
            &[write(dir.path(), "c", &consumed)],
            &dir.path().join("cs"),
            10,
        )
        .unwrap();
        let r = compare(&w, &c, Some(&u)).unwrap();
        assert_eq!((r.missing, r.unexpected, r.uncertain_delivered), (0, 0, 1));
        assert!(r.ok());
    }
}
