//! Activation histories read in bounded batches
//! (`docs/design/mysql-mass-invalidation.md`, section 6).
//!
//! - **One statement per batch:** the barrier streams a batch of tables
//!   needs and the cache does not hold are read first, each once, then the
//!   tables' own streams, in the batch's order, with one bounded
//!   `log_read_streams_bounded` statement; the versions their records name
//!   are checked with one `version_hashes` read.
//! - **Bounded in flight:** a read holds at most [`TIMELINE_MAX_BYTES`] and
//!   [`TIMELINE_MAX_RECORDS`] records plus one; what does not fit is left
//!   for its own read.
//! - **The per-timeline limit:** a table's timeline (its stream with the
//!   barrier streams that apply to it, each once) over the limit is
//!   [`TooLarge`]: never loaded whole, never served uncached.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use storage::adapters::SchemaKey;
use storage::{ArcStorageBackend, DurableSchemaRegistry, LogEntryMeta};

use super::mysql_activation::{
    ACTIVATION_NS, BARRIER_NS, Kind, Stored, TimelineError, barrier_streams,
    decode_stream, table_stream,
};

/// The most records of one timeline (its stream and its barrier streams).
pub(crate) const TIMELINE_MAX_RECORDS: u64 = 100_000;
/// The most estimated bytes ([`storage::log_entry_bytes`]) of one timeline.
pub(crate) const TIMELINE_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// The most tables one batched read takes.
pub(crate) const BATCH_MAX_TABLES: usize = 256;

/// One barrier stream as read: shared by every cached timeline it applies
/// to, and accounted once.
#[derive(Debug, PartialEq)]
pub(crate) struct BarrierStream {
    pub(crate) stream: String,
    pub(crate) records: Vec<Stored>,
    /// The sequence of its last record (0 when empty).
    pub(crate) head: u64,
    /// Its estimated bytes.
    pub(crate) bytes: u64,
}

/// One table's own stream as read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TableStream {
    pub(crate) records: Vec<Stored>,
    pub(crate) head: u64,
    pub(crate) bytes: u64,
}

/// A timeline over its limit: never loaded whole.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error(
    "the activation timeline of {table} holds more than {TIMELINE_MAX_RECORDS} records or {TIMELINE_MAX_BYTES} bytes{} (stream {stream})",
    .measured.map(|(r, b)| format!(": {r} records, {b} bytes")).unwrap_or_default()
)]
pub(crate) struct TooLarge {
    /// `db.table`.
    pub(crate) table: String,
    /// The stream whose read went over the limit.
    pub(crate) stream: String,
    /// Its records and bytes, when read whole (a read stopped at the limit
    /// never counts the rest).
    pub(crate) measured: Option<(u64, u64)>,
}

/// What one bounded batched read produced.
#[derive(Debug, Default)]
pub(crate) struct Read {
    /// The barrier streams read whole.
    pub(crate) barriers: HashMap<String, Arc<BarrierStream>>,
    /// The table streams read whole, by backend key: decoded, their records'
    /// versions checked (or the error the table's own read raises).
    pub(crate) tables: BTreeMap<String, Result<TableStream, TimelineError>>,
    /// The first stream not read whole (everything from it was left), and
    /// what was counted up to it.
    pub(crate) cut: Option<(String, u64, u64)>,
    /// The hashes of every version the read tables' records name, by table
    /// key and version.
    pub(crate) hashes: HashMap<String, HashMap<i32, String>>,
}

/// Read `keys`' histories in one bounded statement: the barrier streams
/// they need that `have` lacks (each once, first), then their own streams
/// in `keys`' order, within `max_bytes` and `max_entries` together (held:
/// at most those plus one entry). At most [`BATCH_MAX_TABLES`] keys.
pub(crate) async fn read(
    backend: &ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    keys: &[SchemaKey],
    have: &HashMap<String, Arc<BarrierStream>>,
    max_bytes: u64,
    max_entries: u64,
) -> anyhow::Result<Read> {
    anyhow::ensure!(
        keys.len() <= BATCH_MAX_TABLES,
        "a batched history read of {} tables exceeds its bound of {BATCH_MAX_TABLES}",
        keys.len()
    );
    let mut wanted_barriers: Vec<String> = Vec::new();
    for k in keys {
        for b in barrier_streams(k) {
            if !have.contains_key(&b) && !wanted_barriers.contains(&b) {
                wanted_barriers.push(b);
            }
        }
    }
    let mut tables: Vec<(String, &SchemaKey)> = Vec::new();
    for k in keys {
        let bk = k.backend_key();
        if !tables.iter().any(|(b, _)| *b == bk) {
            tables.push((bk, k));
        }
    }
    let streams: Vec<(String, String)> = wanted_barriers
        .iter()
        .map(|b| (BARRIER_NS.to_string(), b.clone()))
        .chain(
            tables
                .iter()
                .map(|(_, k)| (ACTIVATION_NS.to_string(), table_stream(k))),
        )
        .collect();
    let mut out = Read::default();
    if streams.is_empty() {
        return Ok(out);
    }
    let got = backend
        .log_read_streams_bounded(&streams, max_bytes, max_entries)
        .await
        .map_err(|e| e.context("read activation histories"))?;
    if got.len() < streams.len() {
        let (records, bytes) = counted(&got);
        out.cut = Some((streams[got.len()].1.clone(), records, bytes));
    }
    for (i, entries) in got.iter().enumerate() {
        let stream = &streams[i].1;
        let bytes = entries_bytes(entries);
        if i < wanted_barriers.len() {
            // A corrupt barrier stream fails every table that needs it:
            // left out, read again (and reported) by the table's own read.
            if let Ok((records, head)) = decode_stream(stream, entries) {
                out.barriers.insert(
                    stream.clone(),
                    Arc::new(BarrierStream {
                        stream: stream.clone(),
                        records,
                        head,
                        bytes,
                    }),
                );
            }
        } else {
            let (bk, _) = &tables[i - wanted_barriers.len()];
            out.tables.insert(
                bk.clone(),
                decode_stream(stream, entries).map(|(records, head)| {
                    TableStream {
                        records,
                        head,
                        bytes,
                    }
                }),
            );
        }
    }
    check_versions(registry, &tables, &mut out).await?;
    Ok(out)
}

/// Check, in one batched registry read per [`VERSION_HASHES_MAX`], that
/// every version the read tables' records name exists: a table whose
/// record names a missing one fails exactly as its own read would (at its
/// first such record).
///
/// [`VERSION_HASHES_MAX`]: storage::adapters::schema_registry::VERSION_HASHES_MAX
async fn check_versions(
    registry: &DurableSchemaRegistry,
    tables: &[(String, &SchemaKey)],
    out: &mut Read,
) -> anyhow::Result<()> {
    let mut wanted: Vec<(SchemaKey, i32)> = Vec::new();
    for (bk, key) in tables {
        let Some(Ok(t)) = out.tables.get(bk) else {
            continue;
        };
        for r in &t.records {
            if let Some(v) = named_version(&r.record.kind)
                && !wanted.iter().any(|(k, w)| k == *key && *w == v)
            {
                wanted.push(((*key).clone(), v));
            }
        }
    }
    let max = storage::adapters::schema_registry::VERSION_HASHES_MAX;
    for chunk in wanted.chunks(max) {
        let hashes = registry.version_hashes(chunk).await?;
        for ((key, v), h) in chunk.iter().zip(hashes) {
            if let Some(h) = h {
                out.hashes
                    .entry(key.backend_key())
                    .or_default()
                    .insert(*v, h);
            }
        }
    }
    for (bk, _) in tables {
        let known = out.hashes.get(bk);
        let Some(Ok(t)) = out.tables.get(bk) else {
            continue;
        };
        let missing = t.records.iter().find_map(|r| {
            named_version(&r.record.kind)
                .filter(|v| known.is_none_or(|k| !k.contains_key(v)))
        });
        if let Some(version) = missing {
            out.tables.insert(
                bk.clone(),
                Err(TimelineError(format!(
                    "record names version {version}, which does not exist for {bk}"
                ))),
            );
        }
    }
    Ok(())
}

/// The version a record names (whose existence its read checks).
fn named_version(kind: &Kind) -> Option<i32> {
    match kind {
        Kind::Observed { version, .. } | Kind::Baseline { version, .. } => {
            Some(*version)
        }
        _ => None,
    }
}

fn entries_bytes(entries: &[LogEntryMeta]) -> u64 {
    entries.iter().map(storage::log_entry_bytes).sum()
}

fn counted(got: &[Vec<LogEntryMeta>]) -> (u64, u64) {
    (
        got.iter().map(|e| e.len() as u64).sum(),
        got.iter().map(|e| entries_bytes(e)).sum(),
    )
}

/// The barrier streams that apply to `key`, from `streams`, in its order
/// (lineage, then database); `None` while any is missing.
pub(crate) fn streams_of(
    key: &SchemaKey,
    streams: &HashMap<String, Arc<BarrierStream>>,
) -> Option<Vec<Arc<BarrierStream>>> {
    barrier_streams(key)
        .iter()
        .map(|b| streams.get(b).cloned())
        .collect()
}

/// Whether a timeline of `table` and `barriers` is within its limit, or
/// what it is over.
pub(crate) fn within_limit(
    key: &SchemaKey,
    table: &TableStream,
    barriers: &[Arc<BarrierStream>],
) -> Result<(), TooLarge> {
    let records = table.records.len() as u64
        + barriers.iter().map(|b| b.records.len() as u64).sum::<u64>();
    let bytes = table.bytes + barriers.iter().map(|b| b.bytes).sum::<u64>();
    if records > TIMELINE_MAX_RECORDS || bytes > TIMELINE_MAX_BYTES {
        return Err(TooLarge {
            table: format!("{}.{}", key.db, key.table),
            stream: table_stream(key),
            measured: Some((records, bytes)),
        });
    }
    Ok(())
}

/// One table's history as read for its timeline.
#[derive(Debug)]
pub(crate) enum Loaded {
    /// Its stream, the barrier streams that apply (lineage, then database),
    /// and the hashes of the versions its records name: within its limit.
    Ok {
        table: TableStream,
        streams: Vec<Arc<BarrierStream>>,
        hashes: HashMap<i32, String>,
    },
    /// Its own read fails this way (corrupt, unsupported, a missing
    /// version, or a corrupt barrier stream).
    Invalid(TimelineError),
    /// Over its limit: never loaded whole.
    TooLarge(TooLarge),
    /// Not read whole by its batch, and not read alone (a prefetch).
    Unread,
}

/// Read every key's history, in `keys`' order: batches of at most
/// [`BATCH_MAX_TABLES`] tables, one bounded statement each, every barrier
/// stream read once across them (those in `have` not at all, and the ones
/// read added to it). A table a batch could not read whole is read alone,
/// within its timeline limit, when `alone` says so (else it is
/// [`Loaded::Unread`]). A store failure is an error.
pub(crate) async fn read_each(
    backend: &ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    keys: &[SchemaKey],
    have: &mut HashMap<String, Arc<BarrierStream>>,
    alone: impl Fn(&SchemaKey) -> bool,
) -> anyhow::Result<Vec<Loaded>> {
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(BATCH_MAX_TABLES) {
        let mut read = read(
            backend,
            registry,
            chunk,
            have,
            TIMELINE_MAX_BYTES,
            TIMELINE_MAX_RECORDS,
        )
        .await?;
        have.extend(read.barriers.drain());
        for key in chunk {
            let bk = key.backend_key();
            let loaded = match (read.tables.remove(&bk), streams_of(key, have))
            {
                (Some(table), Some(streams)) => {
                    let hashes = read.hashes.remove(&bk).unwrap_or_default();
                    loaded(key, table, streams, hashes)
                }
                // Cut short (or a barrier stream it needs is corrupt):
                // read alone.
                _ if alone(key) => {
                    read_alone(backend, registry, key, have).await?
                }
                _ => Loaded::Unread,
            };
            out.push(loaded);
        }
    }
    Ok(out)
}

/// One table's history read alone, within its timeline limit.
pub(crate) async fn read_alone(
    backend: &ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    have: &mut HashMap<String, Arc<BarrierStream>>,
) -> anyhow::Result<Loaded> {
    // The barrier streams held count toward the limit.
    let held: Vec<Arc<BarrierStream>> = barrier_streams(key)
        .iter()
        .filter_map(|b| have.get(b).cloned())
        .collect();
    let held_records: u64 = held.iter().map(|b| b.records.len() as u64).sum();
    let held_bytes: u64 = held.iter().map(|b| b.bytes).sum();
    let too_large = |measured: Option<(u64, u64)>, stream: String| {
        Loaded::TooLarge(TooLarge {
            table: format!("{}.{}", key.db, key.table),
            stream,
            measured,
        })
    };
    if held_records > TIMELINE_MAX_RECORDS || held_bytes > TIMELINE_MAX_BYTES {
        return Ok(too_large(
            Some((held_records, held_bytes)),
            held[0].stream.clone(),
        ));
    }
    let mut read = read(
        backend,
        registry,
        std::slice::from_ref(key),
        have,
        TIMELINE_MAX_BYTES - held_bytes,
        TIMELINE_MAX_RECORDS - held_records,
    )
    .await?;
    if let Some((stream, _, _)) = read.cut.take() {
        // Stopped one record past the limit: the rest is never counted.
        return Ok(too_large(None, stream));
    }
    have.extend(read.barriers.drain());
    let bk = key.backend_key();
    let Some(streams) = streams_of(key, have) else {
        // A barrier stream read whole but corrupt: its error, as the
        // per-table read reports it.
        let b = barrier_streams(key)
            .into_iter()
            .find(|b| !have.contains_key(b))
            .expect("a missing stream");
        let entries = backend
            .log_read_streams_bounded(
                &[(BARRIER_NS.to_string(), b.clone())],
                TIMELINE_MAX_BYTES,
                TIMELINE_MAX_RECORDS,
            )
            .await?;
        return Ok(match entries.first().map(|e| decode_stream(&b, e)) {
            Some(Err(e)) => Loaded::Invalid(e),
            _ => Loaded::Invalid(TimelineError(format!(
                "barrier stream {b} could not be read"
            ))),
        });
    };
    let table = read
        .tables
        .remove(&bk)
        .ok_or_else(|| anyhow::anyhow!("the history of {bk} was not read"))?;
    let hashes = read.hashes.remove(&bk).unwrap_or_default();
    Ok(loaded(key, table, streams, hashes))
}

fn loaded(
    key: &SchemaKey,
    table: Result<TableStream, TimelineError>,
    streams: Vec<Arc<BarrierStream>>,
    hashes: HashMap<i32, String>,
) -> Loaded {
    match table {
        Err(e) => Loaded::Invalid(e),
        Ok(table) => match within_limit(key, &table, &streams) {
            Err(e) => Loaded::TooLarge(e),
            Ok(()) => Loaded::Ok {
                table,
                streams,
                hashes,
            },
        },
    }
}
