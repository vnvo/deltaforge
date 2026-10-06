//! The recovery operation `pg-adopt-timeline` (`docs/design/recovery-cli.md`,
//! section 5.2): a source whose checkpoints predate continuity records,
//! stopped on a server that has left timeline 1 (`timeline_unrecorded`),
//! adopts the server's current timeline by creating its continuity record at
//! transition 0, proven at the checkpoint position F. No checkpoint moves:
//! the next start runs the ordinary continuity proof against that record and
//! stamps the checkpoints into its chain.
//!
//! Every server observation is made on one gated replication session (the
//! kind the stream proves continuity on), at planning and again at apply.

use std::collections::BTreeMap;
use std::sync::Arc;

use checkpoints::CheckpointStore;
use deltaforge_core::CheckpointOrder;
use pgwire_replication::{Lsn, ReplicationClient};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::ArcStorageBackend;

use super::postgres_continuity::{ContinuityRecord, load_record, store_record};
pub use super::postgres_continuity::{SessionFacts, SlotFacts};

/// Alters what a session showed, right after it is read. For tests only
/// (a fact changing between planning and apply without a second live
/// promotion); `None` in production.
pub type FactsHook = Arc<dyn Fn(&mut SessionFacts) + Send + Sync>;
use super::{AsPgEndpoint, PostgresCheckpoint, compare_pg_checkpoints};
use crate::snapshot_recovery::{CheckpointKind, Engine, classify_checkpoint};

/// The observed state of a continuity record with none stored.
pub const CONTINUITY_ABSENT: &str = "continuity:absent";

/// Why adoption cannot be planned or applied. `NotApplicable`: nothing to
/// adopt; `Precondition`: a server state that may change (a standby, an
/// active slot, WAL not yet at F); `Manual`: evidence recovery never
/// overrides; `Unavailable`: the server or store could not be read.
#[derive(Debug)]
pub enum AdoptionRefusal {
    NotApplicable(String),
    Precondition(String),
    Manual(String),
    Unavailable(anyhow::Error),
}

impl std::fmt::Display for AdoptionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotApplicable(m)
            | Self::Precondition(m)
            | Self::Manual(m) => f.write_str(m),
            Self::Unavailable(e) => write!(f, "{e:#}"),
        }
    }
}

fn unavailable(e: impl Into<anyhow::Error>) -> AdoptionRefusal {
    AdoptionRefusal::Unavailable(e.into())
}

/// The slot as the session showed it (bound into the plan).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdoptionSlot {
    pub slot_type: Option<String>,
    pub temporary: Option<bool>,
    pub invalidation: Option<String>,
    pub wal_status: Option<String>,
    pub restart: Option<String>,
    pub confirmed: Option<String>,
    pub active: Option<bool>,
}

/// What the gated session showed, except the WAL flush position (which
/// moves on its own: observed, re-checked at apply, never bound).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdoptionFacts {
    pub system_identifier: u64,
    pub database_oid: u64,
    pub timeline: u32,
    pub server_major: u32,
    pub in_recovery: bool,
    pub slot: Option<AdoptionSlot>,
}

/// What a plan binds and the apply re-checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptionPlan {
    /// F: the minimum over every checkpoint of the source.
    pub f: String,
    /// Every checkpoint key of the source with its byte digest.
    pub checkpoints: BTreeMap<String, String>,
    pub facts: AdoptionFacts,
    /// The WAL flush position seen (diagnostic).
    pub flush: String,
    /// The slot ownership record's digest, or `absent`.
    pub owner_record: String,
}

/// Where and what to adopt.
pub struct AdoptionInput<'a> {
    pub dsn: &'a str,
    pub slot: &'a str,
    pub publication: &'a str,
    pub source_id: &'a str,
    pub tenant: &'a str,
    pub backend: &'a ArcStorageBackend,
    /// The raw checkpoint store (per-sink keys, not a folding proxy).
    pub checkpoints: &'a Arc<dyn CheckpointStore>,
    /// The configured sinks: each must have a checkpoint once any has.
    pub sinks: &'a [String],
    pub hook: Option<&'a FactsHook>,
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The continuity record's observed state: [`CONTINUITY_ABSENT`] or the
/// digest of its stored bytes.
pub async fn continuity_state(
    backend: &ArcStorageBackend,
    source_id: &str,
) -> anyhow::Result<String> {
    Ok(
        match backend
            .kv_get("failover", &format!("pg_continuity:{source_id}"))
            .await?
        {
            None => CONTINUITY_ABSENT.into(),
            Some(b) => digest(&b),
        },
    )
}

/// The continuity record adoption creates: `chain_id` (derived from the
/// plan seed), the observed identity and timeline, transition 0, proven
/// at F.
pub fn adopted_record(
    chain_id: &str,
    facts: &AdoptionFacts,
    f: &str,
) -> Vec<u8> {
    serde_json::to_vec(&ContinuityRecord {
        format: 1,
        chain_id: chain_id.to_string(),
        system_identifier: facts.system_identifier,
        database_oid: facts.database_oid,
        timeline: facts.timeline,
        transition_id: 0,
        proven_at: Some(f.to_string()),
    })
    .expect("a continuity record serializes")
}

/// The digest [`continuity_state`] reports for `record`.
pub fn record_digest(record: &[u8]) -> String {
    digest(record)
}

/// F, conservatively: the minimum over every per-sink checkpoint of the
/// source (or its aggregate checkpoint when it has no per-sink ones), with
/// no exclusion, ordered by the production PostgreSQL comparator. Each must
/// be an unstamped stream position; a partial set, a snapshot or adoption
/// position, a stamped, malformed or incomparable one refuses.
async fn checkpoint_position(
    input: &AdoptionInput<'_>,
) -> Result<(String, BTreeMap<String, String>), AdoptionRefusal> {
    let source = input.source_id;
    let mut keys = input
        .checkpoints
        .list_with_prefix(&format!("{source}::sink::"))
        .await
        .map_err(|e| {
            unavailable(anyhow::anyhow!("list the checkpoints: {e}"))
        })?;
    keys.sort();
    let mut all = BTreeMap::new();
    let mut values = Vec::new();
    for key in keys.iter().chain(std::iter::once(&source.to_string())) {
        let raw = input.checkpoints.get_raw(key).await.map_err(|e| {
            unavailable(anyhow::anyhow!("read checkpoint {key}: {e}"))
        })?;
        all.insert(
            key.clone(),
            raw.as_deref()
                .map(digest)
                .unwrap_or_else(|| "absent".into()),
        );
        // The aggregate counts only when there is no per-sink checkpoint.
        if key.as_str() != source || keys.is_empty() {
            if let Some(raw) = raw {
                values.push((key.clone(), raw));
            }
        }
    }
    if values.is_empty() {
        return Err(AdoptionRefusal::NotApplicable(
            "the source has no checkpoint to adopt a timeline for".into(),
        ));
    }
    if !keys.is_empty() {
        let missing: Vec<&String> = input
            .sinks
            .iter()
            .filter(|s| !keys.contains(&format!("{source}::sink::{s}")))
            .collect();
        if !missing.is_empty() {
            return Err(AdoptionRefusal::Manual(format!(
                "the checkpoint set is partial: sinks {missing:?} have none"
            )));
        }
    }
    let mut min: Option<(String, Vec<u8>)> = None;
    for (key, raw) in values {
        match classify_checkpoint(Engine::Postgres, &raw) {
            CheckpointKind::Stream => {}
            CheckpointKind::Snapshot => {
                return Err(AdoptionRefusal::Manual(format!(
                    "checkpoint {key} is a snapshot position, not a stream position"
                )));
            }
            CheckpointKind::Malformed => {
                return Err(AdoptionRefusal::Manual(format!(
                    "checkpoint {key} cannot be interpreted"
                )));
            }
        }
        let cp: PostgresCheckpoint =
            serde_json::from_slice(&raw).map_err(|_| {
                AdoptionRefusal::Manual(format!(
                    "checkpoint {key} cannot be interpreted"
                ))
            })?;
        if cp.chain.is_some()
            || cp.timeline.is_some()
            || cp.transition.is_some()
        {
            return Err(AdoptionRefusal::Manual(format!(
                "checkpoint {key} already carries a continuity stamp"
            )));
        }
        min = Some(match min {
            None => (key, raw),
            Some((mk, mraw)) => match compare_pg_checkpoints(&raw, &mraw) {
                CheckpointOrder::Before => (key, raw),
                CheckpointOrder::Equal | CheckpointOrder::After => (mk, mraw),
                CheckpointOrder::Incomparable => {
                    return Err(AdoptionRefusal::Manual(format!(
                        "checkpoints {key} and {mk} cannot be ordered"
                    )));
                }
            },
        });
    }
    let (_, raw) = min.expect("at least one checkpoint");
    let cp: PostgresCheckpoint =
        serde_json::from_slice(&raw).expect("parsed above");
    Ok((cp.lsn, all))
}

/// Open the gated replication session and read what adoption depends on.
async fn observe(
    input: &AdoptionInput<'_>,
) -> Result<(ReplicationClient, SessionFacts), AdoptionRefusal> {
    let components = super::postgres_helpers::parse_dsn(input.dsn)
        .map_err(|e| unavailable(anyhow::anyhow!("parse the DSN: {e}")))?;
    let cfg = super::postgres_helpers::build_replication_config(
        &components,
        input.slot,
        input.publication,
        Lsn::from(0u64),
    );
    let mut client =
        super::postgres_helpers::connect_replication(input.source_id, cfg)
            .await
            .map_err(|e| unavailable(anyhow::anyhow!("{e}")))?;
    let mut facts =
        super::postgres_helpers::read_session_facts(&mut client, input.slot)
            .await
            .map_err(|e| {
                unavailable(anyhow::anyhow!("read the session facts: {e}"))
            })?;
    if let Some(hook) = input.hook {
        hook(&mut facts);
    }
    Ok((client, facts))
}

fn adoption_facts(s: &SessionFacts) -> AdoptionFacts {
    AdoptionFacts {
        system_identifier: s.system_identifier,
        database_oid: s.database_oid,
        timeline: s.timeline,
        server_major: s.server_version_num / 10_000,
        in_recovery: s.in_recovery,
        slot: s.slot.as_ref().map(|f| AdoptionSlot {
            slot_type: f.slot_type.clone(),
            temporary: f.temporary,
            invalidation: f.invalidation.clone(),
            wal_status: f.wal_status.clone(),
            restart: f.restart.map(|l| l.to_string()),
            confirmed: f.confirmed.map(|l| l.to_string()),
            active: f.active,
        }),
    }
}

/// The checks against F on what one session showed.
fn check(
    s: &SessionFacts,
    lineage: (u64, u64),
    slot_name: &str,
    f: &str,
) -> Result<(), AdoptionRefusal> {
    let pre = |m: String| Err(AdoptionRefusal::Precondition(m));
    let manual = |m: String| Err(AdoptionRefusal::Manual(m));
    if s.in_recovery {
        return pre(
            "the endpoint is a standby (in recovery): adopt on the primary"
                .into(),
        );
    }
    if (s.system_identifier, s.database_oid) != lineage {
        return manual(format!(
            "the server ({}, database {}) is not the source's recorded lineage",
            s.system_identifier, s.database_oid
        ));
    }
    let Some(slot) = &s.slot else {
        return manual(format!(
            "replication slot '{slot_name}' does not exist"
        ));
    };
    if slot.slot_type.as_deref() != Some("logical")
        || slot.temporary == Some(true)
    {
        return manual(format!(
            "replication slot '{slot_name}' is not a persistent logical slot"
        ));
    }
    if slot.invalidation.is_some() || slot.wal_status.as_deref() == Some("lost")
    {
        return manual(format!(
            "replication slot '{slot_name}' is invalidated or lost"
        ));
    }
    if slot.active != Some(false) {
        return pre(format!(
            "replication slot '{slot_name}' is in use by another consumer"
        ));
    }
    let f = Lsn::parse(f).map_err(|_| {
        AdoptionRefusal::Manual(format!("checkpoint position {f} is malformed"))
    })?;
    let (Some(restart), Some(confirmed)) = (slot.restart, slot.confirmed)
    else {
        return manual(format!("replication slot '{slot_name}' has no bounds"));
    };
    if restart > f || confirmed > f {
        return manual(format!(
            "replication slot '{slot_name}' bounds (restart {restart}, confirmed \
             {confirmed}) are past the checkpoint {f}: changes before it may be \
             gone"
        ));
    }
    if s.flush < f {
        return pre(format!(
            "the server's WAL flush position {} is behind the checkpoint {f}",
            s.flush
        ));
    }
    Ok(())
}

async fn lineage(
    input: &AdoptionInput<'_>,
) -> Result<(u64, u64), AdoptionRefusal> {
    let rec = storage::adapters::source_lineage::load(
        input.backend,
        input.tenant,
        input.source_id,
    )
    .await
    .map_err(unavailable)?
    .ok_or_else(|| {
        AdoptionRefusal::Manual("the source has no recorded lineage".into())
    })?;
    match rec.current.descriptor.endpoint() {
        super::PgEndpoint {
            system_identifier: Some(s),
            database_oid: Some(d),
        } => Ok((s, d)),
        _ => Err(AdoptionRefusal::Manual(
            "the source's recorded lineage is not a PostgreSQL one".into(),
        )),
    }
}

/// Plan adoption: nothing is written.
pub async fn plan_adoption(
    input: &AdoptionInput<'_>,
) -> Result<AdoptionPlan, AdoptionRefusal> {
    if load_record(input.backend, input.source_id)
        .await
        .map_err(unavailable)?
        .is_some()
    {
        return Err(AdoptionRefusal::NotApplicable(
            "the source already has a continuity record".into(),
        ));
    }
    let lineage = lineage(input).await?;
    let (f, checkpoints) = checkpoint_position(input).await?;
    let (_client, s) = observe(input).await?;
    check(&s, lineage, input.slot, &f)?;
    let owner_record = match input
        .checkpoints
        .get_raw(&format!("slot_owner:{}", input.source_id))
        .await
        .map_err(|e| {
            unavailable(anyhow::anyhow!("read the slot ownership record: {e}"))
        })? {
        Some(b) => digest(&b),
        None => "absent".into(),
    };
    Ok(AdoptionPlan {
        f,
        checkpoints,
        facts: adoption_facts(&s),
        flush: s.flush.to_string(),
        owner_record,
    })
}

/// Apply a planned adoption: on one gated session, require exactly the
/// planned server facts, re-check everything against F, then create the
/// continuity record (once; the same record already present is success).
pub async fn apply_adoption(
    input: &AdoptionInput<'_>,
    planned: &AdoptionPlan,
    record: &[u8],
) -> Result<(), AdoptionRefusal> {
    let lineage = lineage(input).await?;
    let (_client, s) = observe(input).await?;
    if adoption_facts(&s) != planned.facts {
        return Err(AdoptionRefusal::Precondition(
            "the server or slot changed since the plan".into(),
        ));
    }
    check(&s, lineage, input.slot, &planned.f)?;
    match continuity_state(input.backend, input.source_id)
        .await
        .map_err(unavailable)?
    {
        state if state == CONTINUITY_ABSENT => {
            let rec: ContinuityRecord = serde_json::from_slice(record)
                .map_err(|e| {
                    AdoptionRefusal::Manual(format!("the planned record: {e}"))
                })?;
            store_record(input.backend, input.source_id, &rec)
                .await
                .map_err(unavailable)
        }
        state if state == record_digest(record) => Ok(()),
        _ => Err(AdoptionRefusal::Manual(
            "a different continuity record appeared".into(),
        )),
    }
}
