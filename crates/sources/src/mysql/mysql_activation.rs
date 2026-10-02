//! MySQL schema activation timeline: which recorded schema version was in
//! effect for a table at a binlog position (design spec section 7).
//!
//! Version history (`schemas.v1`) is a set of distinct shapes; this timeline
//! records their use over time, so a retained row can be decoded with the
//! version in effect at its position instead of the latest one.
//!
//! - One append-only stream per qualified table key in [`ACTIVATION_NS`], and
//!   barrier streams in [`BARRIER_NS`] (one per source lineage, one per
//!   database), which can never collide with a table key.
//! - Records carry no wall-clock time and are appended with a deterministic
//!   capture identity through the backend's idempotent append, so re-deriving
//!   a record on replay is `AlreadyPresent` and a different record under the
//!   same identity is a conflict (fail closed).
//! - [`select`] answers, for a row at position R, whether exactly one version
//!   is PROVEN by position: the unique maximal applicable record (over the
//!   partial position order) must be `observed` or `baseline`.
//!
//! Positions: a record applies to rows whose evaluation position is at or
//! after the record's position (`order(record, row)` is `Before` or `Equal`).

// Wired into the MySQL source (DDL records, row selection, binding) in the
// next commit; this allow is removed there.
#![allow(dead_code)]

use anyhow::{Context, Result};
use deltaforge_core::CheckpointOrder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::adapters::SchemaKey;
use storage::{AppendStatus, ArcStorageBackend, DurableSchemaRegistry};

use crate::durable_checkpoint::{WmPos, order_positions};

/// Per-table activation streams.
pub(crate) const ACTIVATION_NS: &str = "schemas.v1.activation";
/// Barrier streams (per source lineage and per database).
pub(crate) const BARRIER_NS: &str = "schemas.v1.activation.barrier";

const FORMAT_VERSION: u32 = 1;
const CAPTURE_DOMAIN: &[u8] = b"DeltaForge.Activation.v1\0";
const BASELINE_DOMAIN: &[u8] = b"DeltaForge.Activation.Baseline.v1\0";
const FORWARD_DOMAIN: &[u8] = b"DeltaForge.Activation.ForwardProof.v1\0";
const READ_PAGE: usize = 256;

/// The binlog event that establishes a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EventIdentity {
    /// GTID mode: the transaction's `uuid:gno` and the event's ordinal in it.
    Gtid { gtid: String, ordinal: u32 },
    /// File/position mode: the event's binlog file and end position.
    FilePos { file: String, end_pos: u64 },
    /// No event: the stream (re)started at this position without continuing
    /// from its committed resume position (a discontinuity).
    StreamStart { position: WmPos },
}

/// Where a barrier applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub(crate) enum BarrierScope {
    /// Every table of the source lineage.
    Lineage,
    /// Every table of one database.
    Database { db: String },
}

/// What a record says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Kind {
    /// The source streamed through this position with `version` verified as
    /// the table's shape. `binds` names the `ddl` record this binding closes;
    /// `proof` is the forward-proof evidence when it was established by one.
    Observed {
        version: i32,
        binds: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        proof: Option<ForwardProof>,
    },
    /// A DDL affecting the table committed here; the shape after it is
    /// pending until an `observed` record binds it.
    Ddl,
    /// The source cannot vouch for the table's shape from here on.
    Unknown,
    /// A complete read-only pre-scan proved no applicable DDL between this
    /// record's position (R0) and `to` (S), so `version` (observed at S)
    /// was in effect from R0.
    ///
    /// `binds` is the exact capture identity of the discontinuity barrier at
    /// the same position that this baseline closes (`None` when there is
    /// none); it supersedes that barrier only.
    Baseline {
        version: i32,
        schema_hash: String,
        to: WmPos,
        scan_digest: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binds: Option<String>,
    },
    /// Positional proof is invalid for every table in `scope` from here on.
    Barrier { scope: BarrierScope },
}

/// Forward-proof evidence (spec 7.15): the shape registered as `version`
/// was stable-captured at `to` (S) and a complete scan of (D, S], with
/// digest `scan_digest`, found nothing that could have changed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ForwardProof {
    pub to: WmPos,
    pub scan_digest: String,
    pub schema_hash: String,
}

/// One activation record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Record {
    pub format_version: u32,
    pub position: WmPos,
    #[serde(flatten)]
    pub kind: Kind,
}

impl Record {
    pub(crate) fn new(position: WmPos, kind: Kind) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            position,
            kind,
        }
    }

    fn tag(&self) -> &'static str {
        match self.kind {
            Kind::Observed { .. } => "observed",
            Kind::Ddl => "ddl",
            Kind::Unknown => "unknown",
            Kind::Baseline { .. } => "baseline",
            Kind::Barrier { .. } => "barrier",
        }
    }
}

/// A record as stored, with its capture identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stored {
    pub capture_id: String,
    pub record: Record,
}

/// The table's activation stream key.
pub(crate) fn table_stream(key: &SchemaKey) -> String {
    key.backend_key()
}

/// A barrier stream key for `scope` within the key's source lineage.
pub(crate) fn barrier_stream(key: &SchemaKey, scope: &BarrierScope) -> String {
    match scope {
        BarrierScope::Lineage => SchemaKey::source_prefix(
            &key.tenant,
            &key.source_id,
            &key.lineage_hash,
        ),
        // One segment longer than the lineage prefix; never equal to it.
        BarrierScope::Database { db } => SchemaKey::new(
            key.tenant.as_str(),
            key.source_id.as_str(),
            key.lineage_hash.as_str(),
            db.as_str(),
            "",
        )
        .backend_key(),
    }
}

fn lp(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u64).to_be_bytes());
    h.update(bytes);
}

/// Deterministic capture identity of an event-established record: lineage,
/// event identity, stream, record kind and (for `observed`) version.
pub(crate) fn capture_id(
    lineage_hash: &str,
    event: &EventIdentity,
    stream: &str,
    record: &Record,
) -> String {
    let mut h = Sha256::new();
    h.update(CAPTURE_DOMAIN);
    lp(&mut h, lineage_hash.as_bytes());
    match event {
        EventIdentity::Gtid { gtid, ordinal } => {
            lp(&mut h, b"gtid");
            lp(&mut h, gtid.as_bytes());
            lp(&mut h, &ordinal.to_be_bytes());
        }
        EventIdentity::FilePos { file, end_pos } => {
            lp(&mut h, b"filepos");
            lp(&mut h, file.as_bytes());
            lp(&mut h, &end_pos.to_be_bytes());
        }
        EventIdentity::StreamStart { position } => {
            lp(&mut h, b"start");
            lp(
                &mut h,
                &serde_json::to_vec(position).expect("position serializes"),
            );
        }
    }
    lp(&mut h, stream.as_bytes());
    lp(&mut h, record.tag().as_bytes());
    if let Kind::Observed { version, .. } = &record.kind {
        lp(&mut h, &version.to_be_bytes());
    }
    hex::encode(h.finalize())
}

/// Deterministic capture identity of a baseline record: lineage, table, both
/// boundaries, version and schema hash, proof kind and format, and the
/// digest of the completed scan.
pub(crate) fn baseline_capture_id(
    lineage_hash: &str,
    stream: &str,
    record: &Record,
) -> Result<String> {
    let Kind::Baseline {
        version,
        schema_hash,
        to,
        scan_digest,
        binds,
    } = &record.kind
    else {
        anyhow::bail!("baseline_capture_id requires a baseline record");
    };
    let mut h = Sha256::new();
    h.update(BASELINE_DOMAIN);
    lp(&mut h, lineage_hash.as_bytes());
    lp(&mut h, stream.as_bytes());
    lp(&mut h, &serde_json::to_vec(&record.position)?);
    lp(&mut h, &serde_json::to_vec(to)?);
    lp(&mut h, &version.to_be_bytes());
    lp(&mut h, schema_hash.as_bytes());
    lp(&mut h, b"baseline-prescan");
    lp(&mut h, &record.format_version.to_be_bytes());
    lp(&mut h, scan_digest.as_bytes());
    match binds {
        Some(barrier) => {
            lp(&mut h, b"binds");
            lp(&mut h, barrier.as_bytes());
        }
        None => lp(&mut h, b"unbound"),
    }
    Ok(hex::encode(h.finalize()))
}

/// Deterministic capture identity of a forward-proof `observed` record:
/// lineage, stream, the exact pending `ddl` capture identity it binds, D (its
/// position), S, version, schema hash, proof kind and format, classifier
/// version and the scan digest.
pub(crate) fn forward_capture_id(
    lineage_hash: &str,
    stream: &str,
    classifier_version: &str,
    record: &Record,
) -> Result<String> {
    let Kind::Observed {
        version,
        binds: Some(ddl_id),
        proof:
            Some(ForwardProof {
                to,
                scan_digest,
                schema_hash,
            }),
    } = &record.kind
    else {
        anyhow::bail!(
            "forward_capture_id requires a bound, proven observed record"
        );
    };
    let mut h = Sha256::new();
    h.update(FORWARD_DOMAIN);
    lp(&mut h, lineage_hash.as_bytes());
    lp(&mut h, stream.as_bytes());
    lp(&mut h, ddl_id.as_bytes());
    lp(&mut h, &serde_json::to_vec(&record.position)?);
    lp(&mut h, &serde_json::to_vec(to)?);
    lp(&mut h, &version.to_be_bytes());
    lp(&mut h, schema_hash.as_bytes());
    lp(&mut h, b"forward-proof");
    lp(&mut h, &record.format_version.to_be_bytes());
    lp(&mut h, classifier_version.as_bytes());
    lp(&mut h, scan_digest.as_bytes());
    Ok(hex::encode(h.finalize()))
}

/// The timeline contradicts itself: the same identity with different bytes,
/// an unsupported or corrupt record, or an `observed` version that does not
/// exist in the table's history.
#[derive(Debug, thiserror::Error)]
#[error("schema activation timeline: {0}")]
pub(crate) struct TimelineError(pub String);

fn encode(record: &Record) -> Result<Vec<u8>> {
    serde_json::to_vec(record).context("encode activation record")
}

/// Append `record` under `capture_id`, idempotently. `observed` and
/// `baseline` records must name an existing version of `key` (checked
/// against `registry`).
pub(crate) async fn append(
    backend: &ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    ns: &str,
    stream: &str,
    capture_id: &str,
    record: &Record,
) -> Result<AppendStatus> {
    validate_version(registry, key, record).await?;
    let bytes = encode(record)?;
    match backend
        .log_append_if_absent(ns, stream, capture_id, &bytes)
        .await
    {
        Ok(out) => Ok(out.status),
        Err(e)
            if matches!(
                e.downcast_ref::<storage::LogError>(),
                Some(storage::LogError::CaptureIdentityConflict { .. })
            ) =>
        {
            Err(TimelineError(format!(
                "a different record already exists under capture identity \
                 {capture_id} in {stream}"
            ))
            .into())
        }
        Err(e) => Err(e.context("append activation record")),
    }
}

async fn validate_version(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    record: &Record,
) -> Result<()> {
    let version = match &record.kind {
        Kind::Observed { version, .. } | Kind::Baseline { version, .. } => {
            *version
        }
        _ => return Ok(()),
    };
    if registry.version_hash(key, version).await?.is_none() {
        return Err(TimelineError(format!(
            "record names version {version}, which does not exist for {}",
            key.backend_key()
        ))
        .into());
    }
    Ok(())
}

/// Append the `ddl` record that `event` at `position` establishes for the
/// table `key` (spec 7.5): the shape after it is pending.
pub(crate) async fn record_ddl(
    backend: &ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    event: &EventIdentity,
    position: WmPos,
) -> Result<String> {
    let record = Record::new(position, Kind::Ddl);
    let stream = table_stream(key);
    let id = capture_id(&key.lineage_hash, event, &stream, &record);
    append(backend, registry, key, ACTIVATION_NS, &stream, &id, &record)
        .await?;
    Ok(id)
}

/// Append the forward-proof binding of the pending `ddl` record `ddl_id` at
/// its position D: `version` (already registered) with its evidence.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn record_forward_proof(
    backend: &ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    classifier_version: &str,
    ddl_id: &str,
    position: WmPos,
    version: i32,
    proof: ForwardProof,
) -> Result<AppendStatus> {
    let record = Record::new(
        position,
        Kind::Observed {
            version,
            binds: Some(ddl_id.to_string()),
            proof: Some(proof),
        },
    );
    let stream = table_stream(key);
    let id = forward_capture_id(
        &key.lineage_hash,
        &stream,
        classifier_version,
        &record,
    )?;
    append(backend, registry, key, ACTIVATION_NS, &stream, &id, &record).await
}

/// The record that binds the `ddl` record `ddl_id`, if any.
pub(crate) fn binding_of<'a>(
    records: &'a [Stored],
    ddl_id: &str,
) -> Option<&'a Stored> {
    records.iter().find(|s| {
        matches!(&s.record.kind, Kind::Observed { binds: Some(b), .. } if b == ddl_id)
    })
}

/// Append a barrier for `scope` established by `event` at `position`; `key`
/// names the source lineage (its database and table are not used).
pub(crate) async fn record_barrier(
    backend: &ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    scope: BarrierScope,
    event: &EventIdentity,
    position: WmPos,
) -> Result<AppendStatus> {
    let stream = barrier_stream(key, &scope);
    let record = Record::new(position, Kind::Barrier { scope });
    let id = capture_id(&key.lineage_hash, event, &stream, &record);
    append(backend, registry, key, BARRIER_NS, &stream, &id, &record).await
}

/// Every record of a stream, oldest first (paged). Corrupt or unsupported
/// records are a [`TimelineError`].
pub(crate) async fn read_stream(
    backend: &ArcStorageBackend,
    ns: &str,
    stream: &str,
) -> Result<Vec<Stored>> {
    let mut out = Vec::new();
    let mut since = 0u64;
    loop {
        let page = backend
            .log_read_meta_since(ns, stream, since, READ_PAGE)
            .await
            .context("read activation stream")?;
        for e in &page {
            since = e.seq;
            let capture_id = e.capture_id.clone().ok_or_else(|| {
                TimelineError(format!(
                    "record {} in {stream} has no capture identity",
                    e.seq
                ))
            })?;
            let record: Record =
                serde_json::from_slice(&e.value).map_err(|err| {
                    TimelineError(format!(
                        "corrupt record {} in {stream}: {err}",
                        e.seq
                    ))
                })?;
            if record.format_version != FORMAT_VERSION {
                return Err(TimelineError(format!(
                    "record {} in {stream} has format_version {} (this \
                     build reads {FORMAT_VERSION})",
                    e.seq, record.format_version
                ))
                .into());
            }
            out.push(Stored { capture_id, record });
        }
        if page.len() < READ_PAGE {
            return Ok(out);
        }
    }
}

/// Read a table's records and validate every version they name.
pub(crate) async fn read_table(
    backend: &ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
) -> Result<Vec<Stored>> {
    let records =
        read_stream(backend, ACTIVATION_NS, &table_stream(key)).await?;
    for r in &records {
        validate_version(registry, key, &r.record).await?;
    }
    Ok(records)
}

/// Read the barriers that apply to `key` (its lineage and its database).
pub(crate) async fn read_barriers(
    backend: &ArcStorageBackend,
    key: &SchemaKey,
) -> Result<Vec<Stored>> {
    let mut out = read_stream(
        backend,
        BARRIER_NS,
        &barrier_stream(key, &BarrierScope::Lineage),
    )
    .await?;
    out.extend(
        read_stream(
            backend,
            BARRIER_NS,
            &barrier_stream(
                key,
                &BarrierScope::Database { db: key.db.clone() },
            ),
        )
        .await?,
    );
    Ok(out)
}

/// The outcome of positional selection for a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Selection {
    /// Exactly one version is proven at the row's position.
    Proven { version: i32 },
    /// Positional proof does not exist; the reason says why.
    Unproven(Unproven),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unproven {
    /// No record at or before the row.
    NoRecord,
    /// The latest applicable record is a `ddl` not yet bound.
    PendingDdl,
    /// The latest applicable record is `unknown`.
    Unknown,
    /// The latest applicable record is a barrier.
    Barrier,
    /// Some record's position cannot be ordered against the row, or the
    /// maximal applicable records cannot be ordered against each other.
    Incomparable,
    /// Several distinct records share the maximal position.
    Ambiguous,
}

/// Positional selection for a row at `row` over the table's records and
/// the barriers that apply to it (module docs).
pub(crate) fn select(records: &[Stored], row: &WmPos) -> Selection {
    let mut applicable: Vec<&Stored> = Vec::new();
    for r in records {
        match order_positions(&r.record.position, row) {
            CheckpointOrder::Before | CheckpointOrder::Equal => {
                applicable.push(r)
            }
            CheckpointOrder::After => {}
            CheckpointOrder::Incomparable => {
                return Selection::Unproven(Unproven::Incomparable);
            }
        }
    }
    if applicable.is_empty() {
        return Selection::Unproven(Unproven::NoRecord);
    }
    // Maximal elements: no other applicable record is strictly after them.
    let mut maximal: Vec<&Stored> = Vec::new();
    for a in &applicable {
        let mut dominated = false;
        for b in &applicable {
            match order_positions(&a.record.position, &b.record.position) {
                CheckpointOrder::Before => {
                    dominated = true;
                    break;
                }
                CheckpointOrder::Incomparable => {
                    return Selection::Unproven(Unproven::Incomparable);
                }
                CheckpointOrder::Equal | CheckpointOrder::After => {}
            }
        }
        if !dominated {
            maximal.push(a);
        }
    }
    // All maximal elements share one position (they are mutually Equal).
    // Several distinct records there are ambiguous, except an `observed`
    // record binding the `ddl` record at the same position.
    let decisive = match maximal.as_slice() {
        [one] => *one,
        many => {
            let distinct: Vec<&&Stored> = {
                let mut v: Vec<&&Stored> = Vec::new();
                for m in many {
                    if !v.iter().any(|x| x.capture_id == m.capture_id) {
                        v.push(m);
                    }
                }
                v
            };
            match distinct.as_slice() {
                [one] => **one,
                [a, b] => match (&a.record.kind, &b.record.kind) {
                    (
                        Kind::Observed {
                            binds: Some(id), ..
                        },
                        Kind::Ddl,
                    ) if *id == b.capture_id => **a,
                    (
                        Kind::Ddl,
                        Kind::Observed {
                            binds: Some(id), ..
                        },
                    ) if *id == a.capture_id => **b,
                    // A baseline closes exactly the discontinuity barrier
                    // whose capture identity it carries.
                    (
                        Kind::Baseline {
                            binds: Some(id), ..
                        },
                        Kind::Barrier { .. },
                    ) if *id == b.capture_id => **a,
                    (
                        Kind::Barrier { .. },
                        Kind::Baseline {
                            binds: Some(id), ..
                        },
                    ) if *id == a.capture_id => **b,
                    _ => return Selection::Unproven(Unproven::Ambiguous),
                },
                _ => return Selection::Unproven(Unproven::Ambiguous),
            }
        }
    };
    match &decisive.record.kind {
        Kind::Observed { version, .. } | Kind::Baseline { version, .. } => {
            Selection::Proven { version: *version }
        }
        Kind::Ddl => Selection::Unproven(Unproven::PendingDdl),
        Kind::Unknown => Selection::Unproven(Unproven::Unknown),
        Kind::Barrier { .. } => Selection::Unproven(Unproven::Barrier),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use storage::MemoryStorageBackend;

    const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const V: &str = "4f2a0b1c-71ca-11e1-9e33-c80aa9429562";
    const L: &str = "0123456789abcdef0123456789abcdef";

    fn g(n: u64) -> WmPos {
        WmPos::MysqlGtid {
            gtid_set: format!("{U}:1-{n}"),
        }
    }

    fn fp(pos: u64) -> WmPos {
        WmPos::MysqlBinlog {
            file_base: "bin".into(),
            file_index: 1,
            pos,
        }
    }

    fn st(id: &str, position: WmPos, kind: Kind) -> Stored {
        Stored {
            capture_id: id.into(),
            record: Record::new(position, kind),
        }
    }

    fn obs(v: i32) -> Kind {
        Kind::Observed {
            version: v,
            binds: None,
            proof: None,
        }
    }

    fn key() -> SchemaKey {
        SchemaKey::new("acme", "src", L, "app", "orders")
    }

    // ---- selection -------------------------------------------------------

    #[test]
    fn the_latest_applicable_observed_record_proves_its_version() {
        let recs = vec![
            st("a", g(5), obs(1)),
            st("b", g(10), Kind::Ddl),
            st(
                "c",
                g(12),
                Kind::Observed {
                    version: 2,
                    binds: Some("b".into()),
                    proof: None,
                },
            ),
        ];
        assert_eq!(select(&recs, &g(7)), Selection::Proven { version: 1 });
        assert_eq!(
            select(&recs, &g(5)),
            Selection::Proven { version: 1 },
            "inclusive"
        );
        assert_eq!(
            select(&recs, &g(11)),
            Selection::Unproven(Unproven::PendingDdl)
        );
        assert_eq!(select(&recs, &g(20)), Selection::Proven { version: 2 });
        assert_eq!(
            select(&recs, &g(4)),
            Selection::Unproven(Unproven::NoRecord)
        );
    }

    #[test]
    fn file_position_records_select_the_same_way() {
        let recs =
            vec![st("a", fp(100), obs(1)), st("b", fp(500), Kind::Unknown)];
        assert_eq!(select(&recs, &fp(200)), Selection::Proven { version: 1 });
        assert_eq!(
            select(&recs, &fp(600)),
            Selection::Unproven(Unproven::Unknown)
        );
    }

    #[test]
    fn a_revert_is_a_timeline_not_a_set() {
        // A -> B -> A: version 1 is proven again after the second DDL binds it.
        let recs = vec![
            st("a", g(1), obs(1)),
            st("d1", g(5), Kind::Ddl),
            st(
                "b",
                g(6),
                Kind::Observed {
                    version: 2,
                    binds: Some("d1".into()),
                    proof: None,
                },
            ),
            st("d2", g(9), Kind::Ddl),
            st(
                "c",
                g(10),
                Kind::Observed {
                    version: 1,
                    binds: Some("d2".into()),
                    proof: None,
                },
            ),
        ];
        assert_eq!(select(&recs, &g(7)), Selection::Proven { version: 2 });
        assert_eq!(select(&recs, &g(11)), Selection::Proven { version: 1 });
    }

    #[test]
    fn a_barrier_after_the_observation_removes_proof() {
        let recs = vec![
            st("a", g(1), obs(1)),
            st(
                "x",
                g(4),
                Kind::Barrier {
                    scope: BarrierScope::Lineage,
                },
            ),
        ];
        assert_eq!(select(&recs, &g(3)), Selection::Proven { version: 1 });
        assert_eq!(
            select(&recs, &g(4)),
            Selection::Unproven(Unproven::Barrier)
        );
        // A later observation restores proof.
        let mut recs = recs;
        recs.push(st("b", g(6), obs(1)));
        assert_eq!(select(&recs, &g(7)), Selection::Proven { version: 1 });
    }

    fn base(binds: Option<&str>) -> Kind {
        Kind::Baseline {
            version: 7,
            schema_hash: "h".into(),
            to: g(9),
            scan_digest: "d".into(),
            binds: binds.map(Into::into),
        }
    }

    fn lineage_barrier() -> Kind {
        Kind::Barrier {
            scope: BarrierScope::Lineage,
        }
    }

    #[test]
    fn a_baseline_supersedes_only_the_barrier_it_binds() {
        // The start barrier and the baseline that closes it, at one position.
        let recs = vec![
            st("start", g(3), lineage_barrier()),
            st("base", g(3), base(Some("start"))),
        ];
        assert_eq!(select(&recs, &g(5)), Selection::Proven { version: 7 });
        let mut rev = recs.clone();
        rev.reverse();
        assert_eq!(select(&rev, &g(5)), Selection::Proven { version: 7 });

        // No binding, a binding to another record, or another unmatched
        // record at that position: ambiguous, fail closed.
        for recs in [
            vec![
                st("start", g(3), lineage_barrier()),
                st("base", g(3), base(None)),
            ],
            vec![
                st("start", g(3), lineage_barrier()),
                st("base", g(3), base(Some("other"))),
            ],
            vec![
                st("start", g(3), lineage_barrier()),
                st(
                    "db",
                    g(3),
                    Kind::Barrier {
                        scope: BarrierScope::Database { db: "d".into() },
                    },
                ),
                st("base", g(3), base(Some("start"))),
            ],
            vec![
                st("start", g(3), Kind::Ddl),
                st("base", g(3), base(Some("start"))),
            ],
        ] {
            // In either record order (each match arm).
            let mut rev = recs.clone();
            rev.reverse();
            for recs in [recs, rev] {
                assert_eq!(
                    select(&recs, &g(5)),
                    Selection::Unproven(Unproven::Ambiguous),
                    "{recs:?}"
                );
            }
        }
    }

    #[test]
    fn a_baseline_proves_its_version_from_its_start() {
        let recs = vec![st(
            "base",
            g(3),
            Kind::Baseline {
                version: 4,
                schema_hash: "h".into(),
                to: g(9),
                scan_digest: "d".into(),
                binds: None,
            },
        )];
        assert_eq!(select(&recs, &g(3)), Selection::Proven { version: 4 });
        assert_eq!(
            select(&recs, &g(2)),
            Selection::Unproven(Unproven::NoRecord)
        );
    }

    #[test]
    fn incomparable_positions_prove_nothing() {
        // A record in another representation than the row.
        let recs = vec![st("a", fp(1), obs(1))];
        assert_eq!(
            select(&recs, &g(5)),
            Selection::Unproven(Unproven::Incomparable)
        );
        // Two applicable records that cannot be ordered against each other.
        let disjoint = WmPos::MysqlGtid {
            gtid_set: format!("{V}:1-3"),
        };
        let both = WmPos::MysqlGtid {
            gtid_set: format!("{U}:1-9,{V}:1-9"),
        };
        let recs = vec![st("a", g(3), obs(1)), st("b", disjoint, obs(2))];
        assert_eq!(
            select(&recs, &both),
            Selection::Unproven(Unproven::Incomparable)
        );
    }

    #[test]
    fn distinct_records_at_one_position_are_ambiguous_unless_a_binding() {
        let recs = vec![st("a", g(5), obs(1)), st("b", g(5), obs(2))];
        assert_eq!(
            select(&recs, &g(6)),
            Selection::Unproven(Unproven::Ambiguous)
        );
        let recs = vec![
            st("d", g(5), Kind::Ddl),
            st(
                "o",
                g(5),
                Kind::Observed {
                    version: 2,
                    binds: Some("d".into()),
                    proof: None,
                },
            ),
        ];
        assert_eq!(select(&recs, &g(6)), Selection::Proven { version: 2 });
        // An observation that does not bind that ddl is still ambiguous.
        let recs = vec![
            st("d", g(5), Kind::Ddl),
            st(
                "o",
                g(5),
                Kind::Observed {
                    version: 2,
                    binds: Some("other".into()),
                    proof: None,
                },
            ),
        ];
        assert_eq!(
            select(&recs, &g(6)),
            Selection::Unproven(Unproven::Ambiguous)
        );
    }

    // ---- identities --------------------------------------------------------

    #[test]
    fn capture_identities_are_deterministic_and_bind_every_input() {
        let ev = EventIdentity::Gtid {
            gtid: format!("{U}:7"),
            ordinal: 2,
        };
        let r = Record::new(g(7), obs(3));
        let base = capture_id(L, &ev, "s", &r);
        assert_eq!(base, capture_id(L, &ev, "s", &r));
        let other_lineage = capture_id("f".repeat(32).as_str(), &ev, "s", &r);
        let other_event = capture_id(
            L,
            &EventIdentity::Gtid {
                gtid: format!("{U}:7"),
                ordinal: 3,
            },
            "s",
            &r,
        );
        let file_event = capture_id(
            L,
            &EventIdentity::FilePos {
                file: "bin.000001".into(),
                end_pos: 7,
            },
            "s",
            &r,
        );
        let other_stream = capture_id(L, &ev, "t", &r);
        let other_kind = capture_id(L, &ev, "s", &Record::new(g(7), Kind::Ddl));
        let other_version = capture_id(L, &ev, "s", &Record::new(g(7), obs(4)));
        let all = [
            base,
            other_lineage,
            other_event,
            file_event,
            other_stream,
            other_kind,
            other_version,
        ];
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn baseline_identities_bind_boundaries_version_and_scan() {
        let bb = |from: u64,
                  to: u64,
                  v: i32,
                  h: &str,
                  d: &str,
                  binds: Option<&str>| {
            Record::new(
                g(from),
                Kind::Baseline {
                    version: v,
                    schema_hash: h.into(),
                    to: g(to),
                    scan_digest: d.into(),
                    binds: binds.map(Into::into),
                },
            )
        };
        let b = |from, to, v, h, d| bb(from, to, v, h, d, Some("barrier"));
        let id = |r: &Record| baseline_capture_id(L, "s", r).unwrap();
        let base = id(&b(3, 9, 1, "h", "d"));
        assert_eq!(base, id(&b(3, 9, 1, "h", "d")));
        for other in [
            b(4, 9, 1, "h", "d"),
            b(3, 8, 1, "h", "d"),
            b(3, 9, 2, "h", "d"),
            b(3, 9, 1, "x", "d"),
            b(3, 9, 1, "h", "x"),
            bb(3, 9, 1, "h", "d", None),
            bb(3, 9, 1, "h", "d", Some("other-barrier")),
        ] {
            assert_ne!(base, id(&other));
        }
        assert!(
            baseline_capture_id(L, "s", &Record::new(g(1), Kind::Ddl)).is_err()
        );
    }

    #[test]
    fn forward_proof_identities_bind_the_ddl_and_every_piece_of_evidence() {
        let r = |ddl: &str, d: u64, to: u64, v: i32, h: &str, digest: &str| {
            Record::new(
                g(d),
                Kind::Observed {
                    version: v,
                    binds: Some(ddl.into()),
                    proof: Some(ForwardProof {
                        to: g(to),
                        scan_digest: digest.into(),
                        schema_hash: h.into(),
                    }),
                },
            )
        };
        let id = |rec: &Record, cls: &str| {
            forward_capture_id(L, "s", cls, rec).unwrap()
        };
        let base = id(&r("ddl", 3, 9, 1, "h", "d"), "c1");
        assert_eq!(base, id(&r("ddl", 3, 9, 1, "h", "d"), "c1"));
        for other in [
            id(&r("other", 3, 9, 1, "h", "d"), "c1"),
            id(&r("ddl", 4, 9, 1, "h", "d"), "c1"),
            id(&r("ddl", 3, 8, 1, "h", "d"), "c1"),
            id(&r("ddl", 3, 9, 2, "h", "d"), "c1"),
            id(&r("ddl", 3, 9, 1, "x", "d"), "c1"),
            id(&r("ddl", 3, 9, 1, "h", "x"), "c1"),
            id(&r("ddl", 3, 9, 1, "h", "d"), "c2"),
        ] {
            assert_ne!(base, other);
        }
        // Only a bound, proven observed record has a forward identity.
        let unbound = Record::new(g(3), obs(1));
        assert!(forward_capture_id(L, "s", "c1", &unbound).is_err());
    }

    #[test]
    fn barrier_streams_never_collide_with_table_streams() {
        let k = key();
        let lineage = barrier_stream(&k, &BarrierScope::Lineage);
        let db =
            barrier_stream(&k, &BarrierScope::Database { db: "app".into() });
        assert_ne!(lineage, db);
        // A table literally named like a barrier suffix still differs.
        let odd = SchemaKey::new("acme", "src", L, "app", "");
        assert_eq!(
            barrier_stream(&odd, &BarrierScope::Database { db: "app".into() }),
            db
        );
        assert_ne!(table_stream(&k), db);
        assert_ne!(table_stream(&k), lineage);
        // Separate namespaces as well.
        assert_ne!(ACTIVATION_NS, BARRIER_NS);
    }

    // ---- storage -----------------------------------------------------------

    async fn registry_with_versions(
        n: i32,
    ) -> (ArcStorageBackend, Arc<DurableSchemaRegistry>) {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let reg = DurableSchemaRegistry::new(backend.clone()).await.unwrap();
        for v in 1..=n {
            let schema = serde_json::json!({"v": v});
            reg.register_with_checkpoint(
                &key(),
                &format!("h{v}"),
                &schema,
                None,
            )
            .await
            .unwrap();
        }
        (backend, reg)
    }

    #[tokio::test]
    async fn append_is_idempotent_and_conflicts_fail_closed() {
        let (b, reg) = registry_with_versions(2).await;
        let k = key();
        let stream = table_stream(&k);
        let r = Record::new(g(5), obs(1));
        let ev = EventIdentity::Gtid {
            gtid: format!("{U}:5"),
            ordinal: 0,
        };
        let id = capture_id(L, &ev, &stream, &r);
        assert_eq!(
            append(&b, &reg, &k, ACTIVATION_NS, &stream, &id, &r)
                .await
                .unwrap(),
            AppendStatus::Inserted
        );
        assert_eq!(
            append(&b, &reg, &k, ACTIVATION_NS, &stream, &id, &r)
                .await
                .unwrap(),
            AppendStatus::AlreadyPresent
        );
        // Same identity, different bytes.
        let other = Record::new(g(6), obs(1));
        let err = append(&b, &reg, &k, ACTIVATION_NS, &stream, &id, &other)
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<TimelineError>().is_some(), "{err:#}");
        let stored = read_table(&b, &reg, &k).await.unwrap();
        assert_eq!(
            stored,
            vec![Stored {
                capture_id: id,
                record: r
            }]
        );
    }

    #[tokio::test]
    async fn observed_records_must_name_an_existing_version() {
        let (b, reg) = registry_with_versions(1).await;
        let k = key();
        let stream = table_stream(&k);
        let r = Record::new(g(5), obs(7));
        let err = append(&b, &reg, &k, ACTIVATION_NS, &stream, "id", &r)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err:#}");
        // A record of another lineage's key is not accepted for this key.
        let other_key =
            SchemaKey::new("acme", "src", "f".repeat(32), "app", "orders");
        let err = append(
            &b,
            &reg,
            &other_key,
            ACTIVATION_NS,
            &table_stream(&other_key),
            "id2",
            &Record::new(g(5), obs(1)),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err:#}");
        // Read-side validation too: a stored record naming a missing version.
        let bytes = serde_json::to_vec(&r).unwrap();
        b.log_append_if_absent(ACTIVATION_NS, &stream, "planted", &bytes)
            .await
            .unwrap();
        assert!(read_table(&b, &reg, &k).await.is_err());
    }

    #[tokio::test]
    async fn corrupt_and_unsupported_records_are_refused() {
        let (b, _) = registry_with_versions(1).await;
        b.log_append_if_absent(ACTIVATION_NS, "s1", "x", b"{nope")
            .await
            .unwrap();
        assert!(read_stream(&b, ACTIVATION_NS, "s1").await.is_err());
        let mut future =
            serde_json::to_value(Record::new(g(1), Kind::Ddl)).unwrap();
        future["format_version"] = serde_json::json!(9);
        b.log_append_if_absent(
            ACTIVATION_NS,
            "s2",
            "y",
            &serde_json::to_vec(&future).unwrap(),
        )
        .await
        .unwrap();
        let err = read_stream(&b, ACTIVATION_NS, "s2").await.unwrap_err();
        assert!(err.to_string().contains("format_version 9"), "{err:#}");
    }

    #[tokio::test]
    async fn streams_are_read_completely_across_pages() {
        let (b, reg) = registry_with_versions(1).await;
        let k = key();
        let stream = table_stream(&k);
        for i in 0..(READ_PAGE as u64 * 2 + 3) {
            let r = Record::new(g(i + 1), Kind::Ddl);
            append(&b, &reg, &k, ACTIVATION_NS, &stream, &format!("id{i}"), &r)
                .await
                .unwrap();
        }
        assert_eq!(
            read_table(&b, &reg, &k).await.unwrap().len(),
            READ_PAGE * 2 + 3
        );
    }

    #[tokio::test]
    async fn barriers_of_the_lineage_and_the_database_apply_to_a_table() {
        let (b, reg) = registry_with_versions(1).await;
        let k = key();
        for (scope, id) in [
            (BarrierScope::Lineage, "l"),
            (BarrierScope::Database { db: "app".into() }, "d"),
            (BarrierScope::Database { db: "other".into() }, "o"),
        ] {
            let stream = barrier_stream(&k, &scope);
            append(
                &b,
                &reg,
                &k,
                BARRIER_NS,
                &stream,
                id,
                &Record::new(g(3), Kind::Barrier { scope }),
            )
            .await
            .unwrap();
        }
        let got: Vec<String> = read_barriers(&b, &k)
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.capture_id)
            .collect();
        assert_eq!(got, ["l", "d"]);
    }
}

/// Test helpers shared by the activation tests of other modules.
#[cfg(test)]
pub(crate) mod tests_support {
    use storage::DurableSchemaRegistry;
    use storage::adapters::SchemaKey;

    /// Every registered version of `key`, oldest first, as schema JSON text.
    pub(crate) async fn history(
        registry: &DurableSchemaRegistry,
        key: &SchemaKey,
    ) -> Vec<String> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let page = registry.history_page(key, cursor, 256).await.unwrap();
            out.extend(page.versions.iter().map(|v| v.schema_json.to_string()));
            match page.next {
                Some(next) => cursor = Some(next),
                None => return out,
            }
        }
    }
}
