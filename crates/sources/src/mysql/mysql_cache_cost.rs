//! What the selection cache's entries hold in memory: the estimate its byte
//! bound counts (`docs/design/mysql-mass-invalidation.md`, section 6).
//!
//! Every allocation an entry keeps is counted at the size the allocator
//! gives it (jemalloc's size classes): the structures themselves, their
//! strings and vectors at capacity, the `Arc` headers, a selected schema's
//! decoder, a read-ahead version's JSON tree, and the maps' buckets. A
//! record's own heap is taken from its stored size (what was read for it),
//! which covers the strings it decodes into. Resident memory is measured
//! against this estimate in `tests/mysql_window_memory_e2e.rs`.

use std::mem::size_of;
use std::sync::Arc;

use super::mysql_activation::Stored;
use super::mysql_decode::{ColumnKind, TableDecoder};
use super::mysql_history::BarrierStream;
use super::mysql_schema_loader::LoadedSchema;
use super::mysql_selection::Timeline;
use super::mysql_table_schema::MySqlTableSchema;

/// An `Arc`'s two counts, ahead of its value.
const ARC_HEADER: u64 = 2 * size_of::<usize>() as u64;
/// A decoded record's heap beyond its stored bytes: the allocator's
/// rounding of its few strings.
const RECORD_HEAP: u64 = 64;

/// The bytes the allocator gives a request of `n` bytes: its size class
/// (16-byte steps up to 64, then four classes per doubling).
pub(crate) fn allocated(n: u64) -> u64 {
    match n {
        0 => 0,
        1..=8 => 8,
        9..=64 => n.next_multiple_of(16),
        _ => {
            let step = n.next_power_of_two() / 8;
            n.next_multiple_of(step)
        }
    }
}

/// `n` values of `T` in one allocation.
fn slots<T>(n: usize) -> u64 {
    allocated((n * size_of::<T>()) as u64)
}

fn string(s: &str, capacity: usize) -> u64 {
    debug_assert!(capacity >= s.len());
    allocated(capacity as u64)
}

fn owned(s: &String) -> u64 {
    string(s, s.capacity())
}

fn optional(s: &Option<String>) -> u64 {
    s.as_ref().map_or(0, owned)
}

/// The buckets a hash map of `capacity` usable slots has (hashbrown: a
/// power of two, seven eighths usable from 8 buckets).
pub(crate) fn buckets_for(capacity: usize) -> usize {
    match capacity {
        0 => 0,
        1..=3 => 4,
        4..=7 => 8,
        _ => (capacity * 8).div_ceil(7).next_power_of_two(),
    }
}

/// A hash map's table of `buckets` buckets of `slot` bytes: the slots, one
/// control byte per bucket and a group of trailing control bytes.
pub(crate) fn table(buckets: usize, slot: usize) -> u64 {
    if buckets == 0 {
        return 0;
    }
    allocated((buckets * (slot + 1) + 16) as u64)
}

/// Decoded records: their vector at capacity, their stored bytes (`bytes`,
/// what was read for them) and their strings' rounding.
fn records(records: &Vec<Stored>, bytes: u64) -> u64 {
    slots::<Stored>(records.capacity())
        + bytes
        + records.len() as u64 * RECORD_HEAP
}

/// A timeline's own memory: the `Arc` it is held in, its records and its
/// list of barrier streams (the streams themselves are counted once each,
/// where they are held: [`stream`]).
pub(crate) fn timeline(t: &Timeline) -> u64 {
    allocated(ARC_HEADER + size_of::<Timeline>() as u64)
        + records(&t.records, t.bytes)
        + slots::<Arc<BarrierStream>>(t.streams.capacity())
}

/// A barrier stream held once: its `Arc`, its name, its records, and the
/// name again as the key it is held under.
pub(crate) fn stream(s: &BarrierStream) -> u64 {
    allocated(ARC_HEADER + size_of::<BarrierStream>() as u64)
        + 2 * owned(&s.stream)
        + records(&s.records, s.bytes)
}

fn schema(s: &MySqlTableSchema) -> u64 {
    let columns: u64 = s
        .columns
        .iter()
        .map(|c| {
            owned(&c.name)
                + owned(&c.column_type)
                + owned(&c.data_type)
                + optional(&c.default_value)
                + optional(&c.extra)
                + optional(&c.comment)
        })
        .sum();
    slots::<super::mysql_table_schema::MySqlColumn>(s.columns.capacity())
        + columns
        + slots::<String>(s.primary_key.capacity())
        + s.primary_key.iter().map(owned).sum::<u64>()
        + optional(&s.engine)
        + optional(&s.charset)
        + optional(&s.collation)
}

fn decoder(d: &TableDecoder) -> u64 {
    let columns: u64 = d
        .columns
        .iter()
        .map(|c| {
            owned(&c.name)
                + owned(&c.column_type)
                + match &c.kind {
                    Ok(ColumnKind::Enum { labels })
                    | Ok(ColumnKind::Set { labels }) => {
                        slots::<String>(labels.capacity())
                            + labels.iter().map(owned).sum::<u64>()
                    }
                    Ok(_) => 0,
                    Err(why) => owned(why),
                }
        })
        .sum();
    allocated(ARC_HEADER + size_of::<TableDecoder>() as u64)
        + slots::<super::mysql_decode::ColumnDecoder>(d.columns.capacity())
        + columns
}

/// A selected schema: its `Arc`, its columns, its column names, its
/// fingerprint and its decoder.
pub(crate) fn loaded(l: &LoadedSchema) -> u64 {
    allocated(ARC_HEADER + size_of::<LoadedSchema>() as u64)
        + schema(&l.schema)
        + allocated(ARC_HEADER + size_of::<Vec<String>>() as u64)
        + slots::<String>(l.column_names.capacity())
        + l.column_names.iter().map(owned).sum::<u64>()
        + allocated(ARC_HEADER + l.fingerprint.len() as u64)
        + decoder(&l.decoder)
}

/// A JSON value's heap: strings, arrays at capacity, and objects as an
/// insertion-ordered map (the workspace builds `serde_json` with
/// `preserve_order`): their entries at the map's capacity, its index
/// table, and their keys.
pub(crate) fn json(v: &serde_json::Value) -> u64 {
    use serde_json::Value;
    match v {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(s) => owned(s),
        Value::Array(a) => {
            slots::<Value>(a.capacity()) + a.iter().map(json).sum::<u64>()
        }
        Value::Object(m) => {
            if m.is_empty() {
                return 0;
            }
            let buckets = buckets_for(m.len());
            let capacity = if buckets < 8 {
                buckets - 1
            } else {
                buckets / 8 * 7
            };
            // An entry: its hash, its key and its value.
            slots::<(usize, String, Value)>(capacity)
                + table(buckets, size_of::<usize>())
                + m.iter().map(|(k, v)| owned(k) + json(v)).sum::<u64>()
        }
    }
}

/// A registered version read ahead: its `Arc`, its hash, its checkpoint
/// and its schema's JSON tree.
pub(crate) fn version(v: &schema_registry::SchemaVersion) -> u64 {
    allocated(ARC_HEADER + size_of::<schema_registry::SchemaVersion>() as u64)
        + owned(&v.hash)
        + v.checkpoint
            .as_ref()
            .map_or(0, |c| allocated(c.capacity() as u64))
        + json(&v.schema_json)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Size classes: never below the request, at most a quarter above it
    /// past the smallest classes.
    #[test]
    fn a_size_class_covers_its_request() {
        assert_eq!(allocated(0), 0);
        assert_eq!(allocated(1), 8);
        assert_eq!(allocated(17), 32);
        assert_eq!(allocated(65), 80);
        assert_eq!(allocated(1025), 1280);
        for n in 1..100_000u64 {
            let a = allocated(n);
            assert!(a >= n);
            if n > 64 {
                assert!(a <= n + n / 4, "{n} -> {a}");
            }
        }
    }

    /// A map's buckets give it at least the slots asked for.
    #[test]
    fn buckets_hold_their_capacity() {
        for n in 1..10_000 {
            let b = buckets_for(n);
            assert!(b.is_power_of_two());
            let usable = if b < 8 { b - 1 } else { b / 8 * 7 };
            assert!(usable >= n, "{n} -> {b}");
        }
    }
}
