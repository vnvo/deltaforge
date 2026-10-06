//! The engine-neutral generation driver (`docs/design/snapshot-durable-queue.md`,
//! sections 4, 5.4 and 6): the start decision on the control record, the
//! generation start barrier's completion, and policy-frontier completion over
//! the frozen cohort.
//!
//! Engines supply only what is engine-specific: their [`EngineOrder`] (how
//! their stored positions order), how a control record's anchor reads as
//! their anchor, and the snapshot itself. Everything here reads the sinks'
//! stored checkpoints (`{source}::sink::{sink}`) and the control record.

use checkpoints::CheckpointStore;
use deltaforge_config::SnapshotMode;

use crate::snapshot_generation::PersistedLineage;
use crate::snapshot_position::{Classified, EngineOrder, classify_stored};
use crate::snapshot_queue::{
    Completion, EngineAnchor, GenerationControl, PolicyMode, PolicySnapshot,
    QueueError, QueueStore, State, Stored,
};

/// A sink's stored checkpoint key.
pub fn sink_key(source: &str, sink: &str) -> String {
    format!("{source}::sink::{sink}")
}

/// Where one sink stands relative to a generation (design section 6.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkState {
    /// Not at the generation's terminal: an incomplete or start position of
    /// it or an older generation, a legacy position, a stream position not
    /// after its anchor, or nothing.
    Behind,
    /// At or past the terminal: the completing position of this generation
    /// at exactly its anchor, or a stream position strictly after the anchor.
    AtOrPast,
    /// Not a state of this chain and lineage (another chain, a later
    /// generation, an unknown format, an incomparable position).
    Foreign,
}

/// Classify one sink's stored checkpoint against generation `generation` of
/// `chain`, anchored at `anchor` (`None` before the anchor is taken).
pub fn sink_state<E: EngineOrder>(
    engine: &E,
    raw: Option<&[u8]>,
    chain: &str,
    generation: u64,
    anchor: Option<&E::Anchor>,
) -> SinkState {
    use SinkState::*;
    let Some(raw) = raw else {
        return Behind;
    };
    match classify_stored(engine, raw) {
        None => Foreign,
        Some(Classified::Adopted(d)) => {
            if d.snapshot_chain == chain && d.generation <= generation {
                Behind
            } else {
                Foreign
            }
        }
        Some(Classified::Incomplete(i)) => {
            match (&i.snapshot_chain, i.generation) {
                (None, _) => Behind,
                (Some(c), Some(g)) if c == chain && g < generation => Behind,
                (Some(c), Some(g)) if c == chain && g == generation => {
                    match anchor {
                        Some(a) if *a != i.anchor => Foreign,
                        _ => Behind,
                    }
                }
                _ => Foreign,
            }
        }
        Some(Classified::Stream) => match engine.completion_mark(raw) {
            Some((Some(c), g)) if c == chain => {
                match (g.cmp(&generation), anchor) {
                    (std::cmp::Ordering::Less, _) => Behind,
                    (std::cmp::Ordering::Equal, Some(a))
                        if engine.anchor_vs_stream(a, raw)
                            == deltaforge_core::CheckpointOrder::Equal =>
                    {
                        AtOrPast
                    }
                    _ => Foreign,
                }
            }
            Some((Some(_), _)) => Foreign,
            // A completion written before chains (MySQL, #131): it completes
            // only the generation it names, at exactly its anchor - a legacy
            // generation upgraded in place keeps its number; every later one
            // is above the legacy ones. Anything else is a legacy position.
            Some((None, g)) => match anchor {
                Some(a)
                    if g == generation
                        && engine.anchor_vs_stream(a, raw)
                            == deltaforge_core::CheckpointOrder::Equal =>
                {
                    AtOrPast
                }
                _ => Behind,
            },
            None => match anchor.map(|a| engine.anchor_vs_stream(a, raw)) {
                Some(deltaforge_core::CheckpointOrder::Before) => AtOrPast,
                Some(deltaforge_core::CheckpointOrder::Equal)
                    if engine.unmarked_completion_at_anchor() =>
                {
                    AtOrPast
                }
                Some(deltaforge_core::CheckpointOrder::Incomparable) => Foreign,
                _ => Behind,
            },
        },
    }
}

/// Why the driver could not proceed.
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error(transparent)]
    Queue(#[from] QueueError),
    #[error("checkpoint store: {0}")]
    Checkpoint(String),
    #[error(
        "snapshot generation {generation} is blocked ({reason}); it needs an \
         explicit, proof-bound resnapshot"
    )]
    Blocked { generation: u64, reason: String },
    #[error(
        "snapshot generation {generation} is not complete, and snapshot mode \
         'never' cannot complete it; set mode 'initial' to snapshot again"
    )]
    NeverIncomplete { generation: u64 },
    #[error(
        "sink {sink}'s checkpoint is not a state of snapshot chain {chain} \
         (another chain, a later generation, or unreadable)"
    )]
    Foreign { sink: String, chain: String },
    #[error(
        "sinks {first} and {second} acknowledged positions that cannot be \
         ordered against each other; the snapshot cannot complete on them"
    )]
    IncomparableAcks { first: String, second: String },
    #[error(
        "sink {sink} holds a snapshot position, but the source has no \
         snapshot control record: the snapshot state was lost"
    )]
    StateMissing { sink: String },
    #[error("legacy snapshot state cannot be classified: {0}")]
    Legacy(String),
    #[error(
        "the legacy snapshot generation {generation} is not proven complete \
         ({why}), and snapshot mode 'never' cannot complete it; set mode \
         'initial' to copy it again once (a full recopy)"
    )]
    LegacyUnproven { generation: u64, why: String },
    #[error("cancelled")]
    Cancelled,
}

/// Why a start allocates a generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Allocation {
    /// The source's first generation.
    First,
    /// An incomplete generation whose read view is gone.
    ViewLost,
    /// The configured policy or cohort differs from the generation's frozen
    /// one before it completed.
    PolicyChanged,
    /// Mode `always` re-snapshots a completed generation.
    Always,
    /// A legacy snapshot without proof of its completion: one full recopy
    /// in a new chain.
    Legacy,
}

/// What an engine knows about a legacy (pre-queue) snapshot, from its own
/// progress record (design section 10).
#[derive(Debug, Clone)]
pub struct LegacyProof<A> {
    /// The legacy snapshot's anchor, when its progress records one this
    /// release trusts: `None` - nothing can prove it complete (one recopy).
    pub anchor: Option<(A, EngineAnchor)>,
    /// The generation a legacy completion mark names (MySQL #131).
    pub mark_generation: Option<u64>,
    /// Why it is unproven, for an actionable refusal in mode `never`.
    pub unproven: Option<String>,
    /// The legacy state cannot be classified (an unknown anchor version, a
    /// corrupt progress record): refused, everything left untouched.
    pub refused: Option<String>,
}

impl<A> Default for LegacyProof<A> {
    fn default() -> Self {
        Self {
            anchor: None,
            mark_generation: None,
            unproven: None,
            refused: None,
        }
    }
}

/// What a start does.
#[derive(Debug, Clone)]
pub enum StartOutcome {
    /// Stream from the resume checkpoint; no snapshot. `lagging` are the
    /// configured sinks behind the completed generation (each gets a
    /// non-blocking `sink_snapshot_incomplete` incident); `completed_now` is
    /// set when this start completed it.
    Stream {
        lagging: Vec<String>,
        completed_now: Option<GenerationControl>,
    },
    /// Run generation `control` (allocated, its start barrier pending).
    Snapshot {
        version: u64,
        control: GenerationControl,
        why: Allocation,
    },
}

/// What a start knows besides the stored state.
pub struct StartInput<'a, E: EngineOrder> {
    pub store: &'a QueueStore,
    pub engine: &'a E,
    pub checkpoints: &'a dyn CheckpointStore,
    pub source_id: &'a str,
    pub lineage: PersistedLineage,
    pub config_fingerprint: &'a str,
    /// The configured policy and cohort, frozen into a new generation.
    pub policy: PolicySnapshot,
    pub mode: SnapshotMode,
    /// A control record's anchor as the engine's anchor.
    pub anchor_of: &'a (dyn Fn(&EngineAnchor) -> Option<E::Anchor> + Sync),
    /// What the engine knows about a legacy snapshot (read only when the
    /// control record is a legacy one).
    pub legacy: Option<&'a LegacyProof<E::Anchor>>,
}

impl<E: EngineOrder> StartInput<'_, E> {
    /// A configured sink holding a snapshot or generation start position.
    async fn snapshot_position_without_control(
        &self,
    ) -> Result<Option<String>, DriverError> {
        for sink in &self.policy.sinks {
            let raw = self.stored(&sink.id).await?;
            if matches!(
                raw.as_deref().map(|r| classify_stored(self.engine, r)),
                Some(Some(Classified::Incomplete(_) | Classified::Adopted(_)))
            ) {
                return Ok(Some(sink.id.clone()));
            }
        }
        Ok(None)
    }

    /// Whether the source's resume checkpoint is a stream position.
    async fn streams_without_snapshot(&self) -> Result<bool, DriverError> {
        let raw = self
            .checkpoints
            .get_raw(self.source_id)
            .await
            .map_err(|e| DriverError::Checkpoint(e.to_string()))?;
        Ok(matches!(
            raw.as_deref().map(|r| classify_stored(self.engine, r)),
            Some(Some(Classified::Stream))
        ))
    }

    async fn stored(&self, sink: &str) -> Result<Option<Vec<u8>>, DriverError> {
        self.checkpoints
            .get_raw(&sink_key(self.source_id, sink))
            .await
            .map_err(|e| DriverError::Checkpoint(e.to_string()))
    }

    /// Every listed sink's state against `control`.
    async fn states(
        &self,
        control: &GenerationControl,
        sinks: impl Iterator<Item = &str>,
    ) -> Result<Vec<(String, SinkState, Option<Vec<u8>>)>, DriverError> {
        let anchor = control.anchor.as_ref().and_then(|a| (self.anchor_of)(a));
        let mut out = Vec::new();
        for sink in sinks {
            let raw = self.stored(sink).await?;
            let state = sink_state(
                self.engine,
                raw.as_deref(),
                &control.snapshot_chain,
                control.generation,
                anchor.as_ref(),
            );
            if state == SinkState::Foreign {
                return Err(DriverError::Foreign {
                    sink: sink.to_string(),
                    chain: control.snapshot_chain.clone(),
                });
            }
            out.push((sink.to_string(), state, raw));
        }
        Ok(out)
    }

    async fn replace(
        &self,
        stored: &Stored,
        old: u64,
        by_recovery: bool,
        why: Allocation,
    ) -> Result<StartOutcome, DriverError> {
        let (version, control) = self
            .store
            .replace(
                stored,
                &self.lineage,
                self.config_fingerprint,
                self.policy.clone(),
                by_recovery,
            )
            .await?;
        if let Err(e) = self.store.reclaim(old).await {
            tracing::warn!(
                source_id = %self.source_id,
                generation = old,
                error = %e,
                "could not yet reclaim the replaced generation's plan"
            );
        }
        Ok(StartOutcome::Snapshot {
            version,
            control,
            why,
        })
    }
}

/// The policy frontier over the frozen cohort (design section 6.2): `Some`
/// with what justified completion when it covers the terminal.
///
/// Fails closed: the policy must validate, and the acknowledgements that
/// decide (every acknowledgement, under quorum, since all of them rank) must
/// be mutually ordered. Two incomparable acknowledgements are never treated
/// as equal, whatever each of them is relative to the anchor.
pub fn frontier<E: EngineOrder>(
    engine: &E,
    policy: &PolicySnapshot,
    states: &[(String, SinkState, Option<Vec<u8>>)],
) -> Result<Option<Completion>, DriverError> {
    use deltaforge_core::CheckpointOrder;
    policy.validate().map_err(QueueError::InvalidPolicy)?;
    let at = |id: &str| {
        states
            .iter()
            .find(|(s, _, _)| s == id)
            .filter(|(_, st, _)| *st == SinkState::AtOrPast)
            .and_then(|(_, _, raw)| raw.clone())
    };
    let acks: Vec<(String, Vec<u8>)> = policy
        .sinks
        .iter()
        .filter_map(|s| at(&s.id).map(|raw| (s.id.clone(), raw)))
        .collect();
    let deciding: Vec<&(String, Vec<u8>)> = match policy.mode {
        PolicyMode::All => {
            if acks.len() != policy.sinks.len() {
                return Ok(None);
            }
            acks.iter().collect()
        }
        PolicyMode::Required => {
            let required: Vec<&str> = policy
                .sinks
                .iter()
                .filter(|s| s.required)
                .map(|s| s.id.as_str())
                .collect();
            let got: Vec<&(String, Vec<u8>)> = acks
                .iter()
                .filter(|(id, _)| required.contains(&id.as_str()))
                .collect();
            if got.len() != required.len() {
                return Ok(None);
            }
            got
        }
        PolicyMode::Quorum => {
            // `validate` proved 1 <= quorum <= cohort size.
            let q = policy.quorum.unwrap_or(u32::MAX) as usize;
            if acks.len() < q {
                return Ok(None);
            }
            acks.iter().collect()
        }
    };
    // Every pair must be ordered before anything is ranked.
    let ordering = |x: &[u8], y: &[u8]| match crate::snapshot_position::order(
        engine, x, y,
    ) {
        CheckpointOrder::Before => Some(std::cmp::Ordering::Less),
        CheckpointOrder::After => Some(std::cmp::Ordering::Greater),
        CheckpointOrder::Equal => Some(std::cmp::Ordering::Equal),
        CheckpointOrder::Incomparable => None,
    };
    for (i, x) in deciding.iter().enumerate() {
        for y in &deciding[i + 1..] {
            if ordering(&x.1, &y.1).is_none() {
                return Err(DriverError::IncomparableAcks {
                    first: x.0.clone(),
                    second: y.0.clone(),
                });
            }
        }
    }
    let mut ranked = deciding;
    // Highest first.
    ranked.sort_by(|a, b| {
        ordering(&b.1, &a.1).expect("every pair was proven ordered")
    });
    let frontier = match policy.mode {
        // The quorum's position is the q-th highest acknowledged one.
        PolicyMode::Quorum => {
            ranked.get(policy.quorum.unwrap_or(u32::MAX) as usize - 1)
        }
        // Otherwise the lowest deciding one (none: no sink is required).
        _ => ranked.last(),
    };
    Ok(Some(Completion {
        acks: acks.iter().map(|(id, _)| id.clone()).collect(),
        frontier: frontier
            .map(|(_, raw)| String::from_utf8_lossy(raw).into_owned())
            .unwrap_or_default(),
    }))
}

/// Decide what a start does (design section 4).
pub async fn decide_start<E: EngineOrder>(
    input: &StartInput<'_, E>,
) -> Result<StartOutcome, DriverError> {
    input.policy.validate().map_err(QueueError::InvalidPolicy)?;
    let never = input.mode == SnapshotMode::Never;
    let stored = match input.store.read().await? {
        None => {
            // No generation was ever allocated: mode `never`, or (mode
            // `initial` snapshots only a source with no checkpoint) a source
            // that has been streaming without a snapshot, keeps streaming;
            // mode `always` snapshots.
            // Snapshot positions without their control record: the
            // snapshot state was lost; nothing proves where they stand.
            if let Some(sink) =
                input.snapshot_position_without_control().await?
            {
                return Err(DriverError::StateMissing { sink });
            }
            if never
                || (input.mode == SnapshotMode::Initial
                    && input.streams_without_snapshot().await?)
            {
                return Ok(StartOutcome::Stream {
                    lagging: Vec::new(),
                    completed_now: None,
                });
            }
            let (version, control) = input
                .store
                .allocate_first(
                    input.lineage.clone(),
                    input.config_fingerprint,
                    input.policy.clone(),
                )
                .await?;
            return Ok(StartOutcome::Snapshot {
                version,
                control,
                why: Allocation::First,
            });
        }
        Some(s @ Stored::Legacy { .. }) => {
            return decide_legacy(input, &s).await;
        }
        Some(s) => s,
    };
    let Stored::Current { version, control } = &stored else {
        unreachable!("legacy handled above");
    };
    if let Some(b) = &control.blocked {
        return Err(DriverError::Blocked {
            generation: control.generation,
            reason: b.reason.clone(),
        });
    }
    let drift = control.policy.digest != input.policy.digest;
    match control.state {
        State::Completed => {
            if input.mode == SnapshotMode::Always {
                return input
                    .replace(
                        &stored,
                        control.generation,
                        true,
                        Allocation::Always,
                    )
                    .await;
            }
            let lagging = lagging(input, control).await?;
            Ok(StartOutcome::Stream {
                lagging,
                completed_now: None,
            })
        }
        State::RowsProduced => {
            if drift {
                if never {
                    return Err(DriverError::NeverIncomplete {
                        generation: control.generation,
                    });
                }
                return input
                    .replace(
                        &stored,
                        control.generation,
                        false,
                        Allocation::PolicyChanged,
                    )
                    .await;
            }
            match try_complete(input, *version, control).await? {
                // Mode `always` snapshots at every start: the generation
                // just completed is replaced like any completed one.
                Some(completed) if input.mode == SnapshotMode::Always => {
                    let Some(now) = input.store.read().await? else {
                        return Err(DriverError::Queue(
                            QueueError::InvalidTransition(
                                "the control record vanished".into(),
                            ),
                        ));
                    };
                    input
                        .replace(
                            &now,
                            completed.generation,
                            true,
                            Allocation::Always,
                        )
                        .await
                }
                Some(completed) => {
                    let lagging = lagging(input, &completed).await?;
                    Ok(StartOutcome::Stream {
                        lagging,
                        completed_now: Some(completed),
                    })
                }
                None if never => Err(DriverError::NeverIncomplete {
                    generation: control.generation,
                }),
                None => {
                    input
                        .replace(
                            &stored,
                            control.generation,
                            false,
                            Allocation::ViewLost,
                        )
                        .await
                }
            }
        }
        State::Allocated | State::Running => {
            if never {
                return Err(DriverError::NeverIncomplete {
                    generation: control.generation,
                });
            }
            let why = if drift {
                Allocation::PolicyChanged
            } else {
                Allocation::ViewLost
            };
            input.replace(&stored, control.generation, false, why).await
        }
    }
}

/// Where one sink stands against a legacy snapshot (design section 10, the
/// #131 rules): its completion proven by the engine's completion mark of
/// the legacy generation at exactly the anchor, an unmarked position at
/// exactly the anchor where the engine counts that, or a position strictly
/// after it. Anything else - a snapshot position, another mark, a position
/// at, before or incomparable with the anchor - proves nothing. A position
/// of an unknown format is refused.
pub fn legacy_state<E: EngineOrder>(
    engine: &E,
    raw: Option<&[u8]>,
    anchor: &E::Anchor,
    mark_generation: Option<u64>,
) -> SinkState {
    use deltaforge_core::CheckpointOrder::{Before, Equal};
    let Some(raw) = raw else {
        return SinkState::Behind;
    };
    match classify_stored(engine, raw) {
        None => SinkState::Foreign,
        Some(Classified::Incomplete(_) | Classified::Adopted(_)) => {
            SinkState::Behind
        }
        Some(Classified::Stream) => {
            let order = engine.anchor_vs_stream(anchor, raw);
            let proven = match engine.completion_mark(raw) {
                Some((None, g)) => Some(g) == mark_generation && order == Equal,
                Some(_) => false,
                None => {
                    order == Before
                        || (order == Equal
                            && engine.unmarked_completion_at_anchor())
                }
            };
            if proven {
                SinkState::AtOrPast
            } else {
                SinkState::Behind
            }
        }
    }
}

/// A start over a legacy (pre-queue) control record (design section 10):
/// lineage verified first; a completion proven by the sink checkpoints over
/// the configured policy upgrades it in place to `completed` in a new chain;
/// anything else (no trusted anchor, no proof, or mode `always`) is replaced
/// by the next generation in a new chain - one full recopy; mode `never`
/// refuses an unproven one. An unclassifiable one is refused, untouched.
async fn decide_legacy<E: EngineOrder>(
    input: &StartInput<'_, E>,
    stored: &Stored,
) -> Result<StartOutcome, DriverError> {
    let Stored::Legacy { record, .. } = stored else {
        unreachable!("a legacy record");
    };
    let default = LegacyProof::default();
    let proof = input.legacy.unwrap_or(&default);
    if let Some(why) = &proof.refused {
        return Err(DriverError::Legacy(why.clone()));
    }
    if !record.lineage.stable_matches(&input.lineage) {
        return Err(QueueError::ForeignLineage {
            generation: record.generation,
        }
        .into());
    }
    if input.mode != SnapshotMode::Always
        && let Some((anchor, engine_anchor)) = &proof.anchor
    {
        let mut states = Vec::new();
        for sink in &input.policy.sinks {
            let raw = input.stored(&sink.id).await?;
            let state = legacy_state(
                input.engine,
                raw.as_deref(),
                anchor,
                proof.mark_generation,
            );
            if state == SinkState::Foreign {
                return Err(DriverError::Foreign {
                    sink: sink.id.clone(),
                    chain: "(legacy)".into(),
                });
            }
            states.push((sink.id.clone(), state, raw));
        }
        if let Some(completion) =
            frontier(input.engine, &input.policy, &states)?
            && !completion.acks.is_empty()
        {
            let (_, completed) = input
                .store
                .upgrade_completed_legacy(
                    stored,
                    &input.lineage,
                    input.config_fingerprint,
                    input.policy.clone(),
                    completion,
                    engine_anchor.clone(),
                )
                .await?;
            tracing::info!(
                source_id = %input.source_id,
                generation = completed.generation,
                snapshot_chain = %completed.snapshot_chain,
                "a legacy snapshot proven complete by the sink checkpoints: \
                 upgraded in place, not copied again"
            );
            let lagging = lagging(input, &completed).await?;
            return Ok(StartOutcome::Stream {
                lagging,
                completed_now: Some(completed),
            });
        }
    }
    if input.mode == SnapshotMode::Never {
        return Err(DriverError::LegacyUnproven {
            generation: record.generation,
            why: proof.unproven.clone().unwrap_or_else(|| {
                "the sink checkpoints do not prove its completion".into()
            }),
        });
    }
    tracing::warn!(
        source_id = %input.source_id,
        generation = record.generation,
        "a legacy snapshot without proof of its completion: one full recopy \
         as the next generation"
    );
    input
        .replace(stored, record.generation, false, Allocation::Legacy)
        .await
}

/// The configured sinks behind a completed generation.
pub async fn lagging<E: EngineOrder>(
    input: &StartInput<'_, E>,
    control: &GenerationControl,
) -> Result<Vec<String>, DriverError> {
    Ok(input
        .states(control, input.policy.sinks.iter().map(|s| s.id.as_str()))
        .await?
        .into_iter()
        .filter(|(_, st, _)| *st == SinkState::Behind)
        .map(|(id, _, _)| id)
        .collect())
}

/// Complete `control` (`rows_produced`) when the frozen policy's frontier
/// covers its terminal: the `completed` CAS records what justified it, and
/// the generation's plan is reclaimed. `None` while not covered.
pub async fn try_complete<E: EngineOrder>(
    input: &StartInput<'_, E>,
    version: u64,
    control: &GenerationControl,
) -> Result<Option<GenerationControl>, DriverError> {
    let states = input
        .states(control, control.policy.sinks.iter().map(|s| s.id.as_str()))
        .await?;
    let Some(completion) = frontier(input.engine, &control.policy, &states)?
    else {
        return Ok(None);
    };
    let terminal = control
        .terminal
        .as_ref()
        .map(|t| t.digest.clone())
        .unwrap_or_default();
    let (_, completed) = input
        .store
        .complete(
            version,
            control,
            &terminal,
            &control.policy.digest,
            completion,
        )
        .await?;
    if let Err(e) = input.store.reclaim(completed.generation).await {
        tracing::warn!(
            source_id = %input.source_id,
            generation = completed.generation,
            error = %e,
            "could not yet reclaim the completed generation's plan"
        );
    }
    Ok(Some(completed))
}

/// Wait until every sink of the generation's frozen cohort has passed its
/// start barrier (its stored checkpoint is the generation's start), then
/// record `adoption = done`. No row of the generation is published before
/// this returns. Cancellable.
pub async fn await_adoption<E: EngineOrder>(
    input: &StartInput<'_, E>,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<(u64, GenerationControl), DriverError> {
    loop {
        let Some(Stored::Current { version, control }) =
            input.store.read().await?
        else {
            return Err(DriverError::Queue(QueueError::InvalidTransition(
                "the control record changed during the start barrier".into(),
            )));
        };
        let mut all = true;
        for sink in &control.policy.sinks {
            let raw = input.stored(&sink.id).await?;
            let started = matches!(
                raw.as_deref().map(|r| classify_stored(input.engine, r)),
                Some(Some(Classified::Adopted(d)))
                    if d.snapshot_chain == control.snapshot_chain
                        && d.generation == control.generation
            );
            if !started {
                all = false;
                break;
            }
        }
        if all {
            let (v, c) = input.store.adoption_done(version, &control).await?;
            return Ok((v, c));
        }
        tokio::select! {
            _ = input.checkpoints.await_checkpoint_change() => {}
            _ = cancel.cancelled() => return Err(DriverError::Cancelled),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot_position::{encode, encode_adopted, encode_chained};

    #[tokio::test]
    async fn a_start_reports_lagging_sinks_and_policy_replacements() {
        use deltaforge_core::incident::ReasonCode;
        use std::sync::Arc;
        let backend: storage::ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        let store =
            storage::adapters::incidents::IncidentStore::new(backend, "p");
        let q = QueueStore::new(
            Arc::new(storage::MemoryStorageBackend::new()),
            "src",
        );
        let (_, completed) = q
            .allocate_first(
                crate::snapshot_queue::contract::lineage(),
                "fp",
                crate::snapshot_queue::contract::policy(),
            )
            .await
            .unwrap();
        report_start(
            &store,
            "src",
            &StartOutcome::Stream {
                lagging: vec!["kafka".into()],
                completed_now: None,
            },
            Some(&completed),
        )
        .await;
        let stored = q.read().await.unwrap().unwrap();
        let (v, replacement) = q
            .replace(
                &stored,
                &crate::snapshot_queue::contract::lineage(),
                "fp",
                crate::snapshot_queue::contract::policy(),
                false,
            )
            .await
            .unwrap();
        report_start(
            &store,
            "src",
            &StartOutcome::Snapshot {
                version: v,
                control: replacement,
                why: Allocation::PolicyChanged,
            },
            None,
        )
        .await;
        let reasons: Vec<ReasonCode> = store
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.reason_code)
            .collect();
        assert!(reasons.contains(&ReasonCode::SinkSnapshotIncomplete));
        assert!(reasons.contains(&ReasonCode::SnapshotReplaced));
        assert_eq!(reasons.len(), 2);
    }
    use deltaforge_core::CheckpointOrder;

    /// Anchors are numbers; streams are `{"p": n, "mark": [chain, g]?,
    /// "branch": name?}`. Streams on two different branches (two timelines
    /// forked after the anchor) are incomparable.
    struct T;
    fn p(raw: &[u8]) -> Option<u64> {
        serde_json::from_slice::<serde_json::Value>(raw).ok()?["p"].as_u64()
    }
    fn branch(raw: &[u8]) -> Option<String> {
        serde_json::from_slice::<serde_json::Value>(raw).ok()?["branch"]
            .as_str()
            .map(str::to_string)
    }
    impl EngineOrder for T {
        type Anchor = u64;
        fn stream_order(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
            if let (Some(x), Some(y)) = (branch(a), branch(b))
                && x != y
            {
                return CheckpointOrder::Incomparable;
            }
            match (p(a), p(b)) {
                (Some(a), Some(b)) => match a.cmp(&b) {
                    std::cmp::Ordering::Less => CheckpointOrder::Before,
                    std::cmp::Ordering::Equal => CheckpointOrder::Equal,
                    std::cmp::Ordering::Greater => CheckpointOrder::After,
                },
                _ => CheckpointOrder::Incomparable,
            }
        }
        fn anchor_vs_stream(&self, a: &u64, s: &[u8]) -> CheckpointOrder {
            self.stream_order(&stream(*a), s)
        }
        fn completion_mark(&self, s: &[u8]) -> Option<(Option<String>, u64)> {
            let v: serde_json::Value = serde_json::from_slice(s).ok()?;
            let m = v.get("mark")?;
            Some((m[0].as_str().map(str::to_string), m[1].as_u64()?))
        }
    }
    fn stream(p: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "p": p })).unwrap()
    }
    fn marked(p: u64, chain: &str, g: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "p": p, "mark": [chain, g] }))
            .unwrap()
    }
    fn st(raw: Option<&[u8]>) -> SinkState {
        sink_state(&T, raw, "c", 4, Some(&100))
    }

    fn forked(p: u64, b: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "p": p, "branch": b })).unwrap()
    }

    /// Two acknowledgements each past the anchor but on forked timelines:
    /// no policy that ranks both may complete on them.
    #[test]
    fn incomparable_acknowledgements_never_complete() {
        use crate::snapshot_queue::{PolicyMode, PolicySink};
        let (x, y) = (forked(101, "x"), forked(102, "y"));
        assert_eq!(st(Some(&x)), SinkState::AtOrPast);
        assert_eq!(st(Some(&y)), SinkState::AtOrPast);
        let states = vec![
            ("a".to_string(), st(Some(&x)), Some(x.clone())),
            ("b".to_string(), st(Some(&y)), Some(y.clone())),
        ];
        let sinks = |a_required| {
            vec![
                PolicySink {
                    id: "a".into(),
                    required: a_required,
                },
                PolicySink {
                    id: "b".into(),
                    required: true,
                },
            ]
        };
        for (mode, quorum) in [
            (PolicyMode::All, None),
            (PolicyMode::Required, None),
            (PolicyMode::Quorum, Some(1)),
            (PolicyMode::Quorum, Some(2)),
        ] {
            let policy = PolicySnapshot::new(mode, quorum, sinks(true));
            assert!(
                matches!(
                    frontier(&T, &policy, &states),
                    Err(DriverError::IncomparableAcks { .. })
                ),
                "{mode:?} {quorum:?}"
            );
        }
        // Only `b` decides under Required when `a` is optional.
        let policy =
            PolicySnapshot::new(PolicyMode::Required, None, sinks(false));
        let done = frontier(&T, &policy, &states).unwrap().unwrap();
        assert_eq!(done.frontier, String::from_utf8(y).unwrap());
        // Ordered acknowledgements rank: quorum 1 takes the highest.
        let (lo, hi) = (stream(101), stream(150));
        let ordered = vec![
            ("a".to_string(), st(Some(&lo)), Some(lo.clone())),
            ("b".to_string(), st(Some(&hi)), Some(hi.clone())),
        ];
        let q1 = PolicySnapshot::new(PolicyMode::Quorum, Some(1), sinks(true));
        let all = PolicySnapshot::new(PolicyMode::All, None, sinks(true));
        assert_eq!(
            frontier(&T, &q1, &ordered)
                .unwrap()
                .unwrap()
                .frontier
                .as_bytes(),
            hi.as_slice()
        );
        assert_eq!(
            frontier(&T, &all, &ordered)
                .unwrap()
                .unwrap()
                .frontier
                .as_bytes(),
            lo.as_slice()
        );
        // An invalid frozen policy decides nothing.
        let q3 = PolicySnapshot::new(PolicyMode::Quorum, Some(3), sinks(true));
        assert!(matches!(
            frontier(&T, &q3, &ordered),
            Err(DriverError::Queue(QueueError::InvalidPolicy(_)))
        ));
    }

    use crate::snapshot_queue::contract;
    use checkpoints::{CheckpointStore, MemCheckpointStore};
    use std::sync::Arc;

    fn mem() -> storage::ArcStorageBackend {
        Arc::new(storage::MemoryStorageBackend::new())
    }

    fn input<'a>(
        q: &'a QueueStore,
        ck: &'a MemCheckpointStore,
    ) -> StartInput<'a, T> {
        StartInput {
            store: q,
            engine: &T,
            checkpoints: ck,
            source_id: "src",
            lineage: contract::lineage(),
            config_fingerprint: "fp",
            policy: contract::policy(),
            mode: SnapshotMode::Initial,
            anchor_of: &|_| Some(100),
            legacy: None,
        }
    }

    /// Generation 1 sealed and running, owned by `run`.
    async fn running(q: &QueueStore, run: &str) -> (u64, GenerationControl) {
        let (v, c) = q
            .allocate_first(contract::lineage(), "fp", contract::policy())
            .await
            .unwrap();
        let mut plan = crate::snapshot_queue::PlanDigest::default();
        plan.add("k", b"i");
        q.seal_and_run(
            v,
            &c,
            plan.seal(),
            EngineAnchor::Postgres {
                lsn: "100".into(),
                timeline: None,
                chain: None,
                transition: None,
            },
            run,
            0,
        )
        .await
        .unwrap()
    }

    /// Generation g of run A is replaced and owned by run B: A's owner
    /// check stops A, and B's control record stays byte-identical and
    /// unblocked, also when A then tries to block its own generation.
    #[tokio::test]
    async fn a_stale_publisher_never_touches_the_new_owner() {
        let backend = mem();
        let q = QueueStore::new(backend.clone(), "src");
        let incidents =
            storage::adapters::incidents::IncidentStore::new(mem(), "p");
        let (a_version, a) = running(&q, "run-a").await;
        let stored = q.read().await.unwrap().unwrap();
        let (v, next) = q
            .replace(
                &stored,
                &contract::lineage(),
                "fp",
                contract::policy(),
                false,
            )
            .await
            .unwrap();
        let mut plan = crate::snapshot_queue::PlanDigest::default();
        plan.add("k", b"i");
        let (_, b) = q
            .seal_and_run(
                v,
                &next,
                plan.seal(),
                EngineAnchor::Postgres {
                    lsn: "200".into(),
                    timeline: None,
                    chain: None,
                    transition: None,
                },
                "run-b",
                0,
            )
            .await
            .unwrap();
        let raw = |backend: storage::ArcStorageBackend| async move {
            backend
                .slot_get(
                    crate::snapshot_queue::CONTROL_NS,
                    &crate::snapshot_queue::control_key("src"),
                )
                .await
                .unwrap()
        };
        let before = raw(backend.clone()).await;

        let lost =
            owner_lost(&q, &incidents, "src", a.generation, "run-a").await;
        assert!(lost.is_some(), "A stops");
        block_generation(
            &q,
            &incidents,
            a.generation,
            Some("run-a"),
            a_version,
            &incidents::blocked(
                deltaforge_core::incident::ReasonCode::SnapshotAnchorUnavailable,
                "src",
                a.generation,
                "anchor_age",
            ),
        )
        .await
        .unwrap();
        assert_eq!(raw(backend.clone()).await, before, "byte-identical");
        let Some(Stored::Current { control, .. }) = q.read().await.unwrap()
        else {
            panic!("current")
        };
        assert!(control.blocked.is_none(), "B is not blocked");
        assert_eq!(control.run.as_deref(), Some("run-b"));
        // The incident is A's, not blocking.
        let open = incidents.list().await.unwrap();
        assert!(open.iter().any(|r| {
            r.reason_code
                == deltaforge_core::incident::ReasonCode::SnapshotStateInvalid
                && r.safety_state
                    == deltaforge_core::incident::SafetyState::RunningDegraded
        }));
        // B is still the owner.
        assert!(
            owner_lost(&q, &incidents, "src", b.generation, "run-b")
                .await
                .is_none()
        );

        // B itself blocks only at exactly the version it holds.
        let draft = incidents::blocked(
            deltaforge_core::incident::ReasonCode::SnapshotAnchorUnavailable,
            "src",
            b.generation,
            "anchor_age",
        );
        let Some(Stored::Current { version: held, .. }) =
            q.read().await.unwrap()
        else {
            panic!("current")
        };
        block_generation(
            &q,
            &incidents,
            b.generation,
            Some("run-b"),
            held - 1,
            &draft,
        )
        .await
        .unwrap();
        assert_eq!(raw(backend.clone()).await, before, "a stale version");
        block_generation(
            &q,
            &incidents,
            b.generation,
            Some("run-a"),
            held,
            &draft,
        )
        .await
        .unwrap();
        assert_eq!(raw(backend.clone()).await, before, "another run");
        block_generation(
            &q,
            &incidents,
            b.generation,
            Some("run-b"),
            held,
            &draft,
        )
        .await
        .unwrap();
        let Some(Stored::Current { control, .. }) = q.read().await.unwrap()
        else {
            panic!("current")
        };
        assert!(control.blocked.is_some(), "its exact version");
    }

    /// Mode `always`: a start that finds the rows produced and the policy
    /// covered completes the generation and still snapshots again.
    #[tokio::test]
    async fn mode_always_replaces_a_generation_it_just_completed() {
        let q = QueueStore::new(mem(), "src");
        let ck = MemCheckpointStore::new().unwrap();
        let (v, c) = running(&q, "r").await;
        let (_, produced) = q.rows_produced(v, &c, "r").await.unwrap();
        ck.put_raw(
            &sink_key("src", "s3"),
            &marked(100, &produced.snapshot_chain, 1),
        )
        .await
        .unwrap();
        let mut always = input(&q, &ck);
        always.mode = SnapshotMode::Always;
        match decide_start(&always).await.unwrap() {
            StartOutcome::Snapshot { control, why, .. } => {
                assert_eq!(why, Allocation::Always);
                assert_eq!(control.generation, 2);
                assert_eq!(control.replaced, Some(1));
            }
            other => panic!("{other:?}"),
        }
    }

    /// Without a control record, a source streaming without a snapshot
    /// keeps streaming in mode `initial`; mode `always` snapshots.
    #[tokio::test]
    async fn only_mode_initial_keeps_a_snapshot_free_stream() {
        let q = QueueStore::new(mem(), "src");
        let ck = MemCheckpointStore::new().unwrap();
        ck.put_raw("src", &stream(500)).await.unwrap();
        assert!(matches!(
            decide_start(&input(&q, &ck)).await.unwrap(),
            StartOutcome::Stream { .. }
        ));
        assert!(q.read().await.unwrap().is_none());
        let mut always = input(&q, &ck);
        always.mode = SnapshotMode::Always;
        assert!(matches!(
            decide_start(&always).await.unwrap(),
            StartOutcome::Snapshot {
                why: Allocation::First,
                ..
            }
        ));
    }

    /// A legacy control record of generation 3 (status running) with
    /// `lineage`.
    async fn legacy_store(lineage: PersistedLineage) -> (QueueStore, Vec<u8>) {
        let backend = mem();
        let bytes = serde_json::to_vec(&serde_json::json!({
            "generation": 3,
            "lineage": lineage,
            "status": "running",
            "config_fingerprint": "old",
            "fingerprint_format": 2,
        }))
        .unwrap();
        backend
            .slot_create(
                crate::snapshot_queue::CONTROL_NS,
                &crate::snapshot_queue::control_key("src"),
                &bytes,
            )
            .await
            .unwrap();
        (QueueStore::new(backend, "src"), bytes)
    }

    async fn raw(q: &QueueStore) -> Option<Vec<u8>> {
        match q.read().await.unwrap() {
            Some(Stored::Legacy { record, .. }) => {
                Some(serde_json::to_vec(&record).unwrap())
            }
            _ => None,
        }
    }

    fn proof_at(anchor: u64, mark: Option<u64>) -> LegacyProof<u64> {
        LegacyProof {
            anchor: Some((
                anchor,
                EngineAnchor::Postgres {
                    lsn: anchor.to_string(),
                    timeline: None,
                    chain: None,
                    transition: None,
                },
            )),
            mark_generation: mark,
            ..Default::default()
        }
    }

    fn legacy_mark(p: u64, g: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "p": p, "mark": [null, g] }))
            .unwrap()
    }

    /// A legacy snapshot the sink checkpoints prove complete over the
    /// configured policy (the required sink strictly past its anchor) is
    /// upgraded in place to `completed` in a new chain, not copied again.
    #[tokio::test]
    async fn a_proven_legacy_snapshot_is_upgraded_in_place() {
        let (q, _) = legacy_store(contract::lineage()).await;
        let ck = MemCheckpointStore::new().unwrap();
        ck.put_raw(&sink_key("src", "s3"), &stream(150))
            .await
            .unwrap();
        let proof = proof_at(100, None);
        let mut i = input(&q, &ck);
        i.legacy = Some(&proof);
        match decide_start(&i).await.unwrap() {
            StartOutcome::Stream {
                completed_now: Some(c),
                lagging,
            } => {
                assert_eq!(c.state, State::Completed);
                assert_eq!((c.generation, c.legacy_through), (3, Some(3)));
                assert!(c.anchor.is_some());
                assert_eq!(c.completion.unwrap().acks, ["s3"]);
                assert_eq!(lagging, ["kafka"], "the optional sink is behind");
            }
            other => panic!("{other:?}"),
        }
    }

    /// The #131 completion mark proves the generation it names, at exactly
    /// the anchor; an unmarked position at the anchor proves nothing here.
    #[tokio::test]
    async fn a_legacy_mark_proves_only_its_generation_at_the_anchor() {
        for (raw, mark, proven) in [
            (legacy_mark(100, 3), Some(3), true),
            (legacy_mark(100, 2), Some(3), false),
            (legacy_mark(120, 3), Some(3), false),
            (legacy_mark(100, 3), None, false),
            (stream(100), Some(3), false),
        ] {
            let (q, _) = legacy_store(contract::lineage()).await;
            let ck = MemCheckpointStore::new().unwrap();
            ck.put_raw(&sink_key("src", "s3"), &raw).await.unwrap();
            let proof = proof_at(100, mark);
            let mut i = input(&q, &ck);
            i.legacy = Some(&proof);
            let upgraded = matches!(
                decide_start(&i).await.unwrap(),
                StartOutcome::Stream { .. }
            );
            assert_eq!(upgraded, proven, "{}", String::from_utf8_lossy(&raw));
        }
    }

    /// Without proof - no trusted anchor, or the sinks behind it - the
    /// legacy snapshot is copied once more as the next generation of a new
    /// chain; mode `always` does so even when proven; mode `never` refuses
    /// with the reason.
    #[tokio::test]
    async fn an_unproven_legacy_snapshot_is_copied_once_more() {
        let unproven = LegacyProof {
            unproven: Some("taken under the pre-hardening anchor".into()),
            ..Default::default()
        };
        let behind = proof_at(100, None);
        for (proof, sink) in [(&unproven, stream(150)), (&behind, stream(50))] {
            let (q, _) = legacy_store(contract::lineage()).await;
            let ck = MemCheckpointStore::new().unwrap();
            ck.put_raw(&sink_key("src", "s3"), &sink).await.unwrap();
            let mut i = input(&q, &ck);
            i.legacy = Some(proof);
            match decide_start(&i).await.unwrap() {
                StartOutcome::Snapshot { control, why, .. } => {
                    assert_eq!(why, Allocation::Legacy);
                    assert_eq!(control.generation, 4);
                    assert_eq!(control.legacy_through, Some(3));
                    assert_eq!(
                        control.adoption,
                        crate::snapshot_queue::Adoption::Pending
                    );
                }
                other => panic!("{other:?}"),
            }
        }
        let (q, _) = legacy_store(contract::lineage()).await;
        let ck = MemCheckpointStore::new().unwrap();
        ck.put_raw(&sink_key("src", "s3"), &stream(150))
            .await
            .unwrap();
        let proof = proof_at(100, None);
        let mut i = input(&q, &ck);
        i.legacy = Some(&proof);
        i.mode = SnapshotMode::Always;
        assert!(matches!(
            decide_start(&i).await.unwrap(),
            StartOutcome::Snapshot {
                why: Allocation::Legacy,
                ..
            }
        ));
        let (q, before) = legacy_store(contract::lineage()).await;
        let mut i = input(&q, &ck);
        i.legacy = Some(&unproven);
        i.mode = SnapshotMode::Never;
        let err = decide_start(&i).await.unwrap_err();
        assert!(err.to_string().contains("pre-hardening anchor"), "{err}");
        assert_eq!(raw(&q).await.unwrap(), {
            let v: serde_json::Value = serde_json::from_slice(&before).unwrap();
            let r: crate::snapshot_generation::SnapshotGenerationRecord =
                serde_json::from_value(v).unwrap();
            serde_json::to_vec(&r).unwrap()
        });
    }

    /// Unclassifiable legacy state, or another lineage's, is refused and
    /// left untouched.
    #[tokio::test]
    async fn unclassifiable_or_foreign_legacy_state_is_refused_untouched() {
        let refused = LegacyProof {
            refused: Some("anchor version 9".into()),
            ..Default::default()
        };
        let (q, _) = legacy_store(contract::lineage()).await;
        let before = raw(&q).await;
        let ck = MemCheckpointStore::new().unwrap();
        let mut i = input(&q, &ck);
        i.legacy = Some(&refused);
        assert!(matches!(
            decide_start(&i).await,
            Err(DriverError::Legacy(why)) if why.contains("anchor version 9")
        ));
        assert_eq!(raw(&q).await, before);

        let (q, _) = legacy_store(PersistedLineage::Postgres {
            system_identifier: 999,
        })
        .await;
        let before = raw(&q).await;
        let proof = proof_at(100, None);
        let mut i = input(&q, &ck);
        i.legacy = Some(&proof);
        assert!(matches!(
            decide_start(&i).await,
            Err(DriverError::Queue(QueueError::ForeignLineage { .. }))
        ));
        assert_eq!(raw(&q).await, before);
    }

    /// Snapshot positions without a control record fail the start closed.
    #[tokio::test]
    async fn snapshot_positions_without_their_control_record_fail_closed() {
        let q = QueueStore::new(mem(), "src");
        let ck = MemCheckpointStore::new().unwrap();
        ck.put_raw(&sink_key("src", "s3"), &stream(500))
            .await
            .unwrap();
        ck.put_raw(&sink_key("src", "kafka"), &encode_chained("c", 2, 100u64))
            .await
            .unwrap();
        assert!(matches!(
            decide_start(&input(&q, &ck)).await,
            Err(DriverError::StateMissing { sink }) if sink == "kafka"
        ));
        assert!(q.read().await.unwrap().is_none(), "nothing allocated");
    }

    /// The resume fold leaves a sink out only once the generation is
    /// durably completed and the sink is behind it.
    #[tokio::test]
    async fn only_a_completed_generation_excludes_its_lagging_sinks() {
        let q = QueueStore::new(mem(), "src");
        let ck = MemCheckpointStore::new().unwrap();
        let (v, c) = running(&q, "r").await;
        let chain = c.snapshot_chain.clone();
        // kafka (optional) holds an incomplete position, s3 (required) is
        // still behind as well.
        ck.put_raw(
            &sink_key("src", "kafka"),
            &encode_chained(&chain, 1, 100u64),
        )
        .await
        .unwrap();
        ck.put_raw(&sink_key("src", "s3"), &encode_chained(&chain, 1, 100u64))
            .await
            .unwrap();
        assert!(resume_exclusions(&input(&q, &ck)).await.unwrap().is_empty());
        let (v, produced) = q.rows_produced(v, &c, "r").await.unwrap();
        assert!(
            resume_exclusions(&input(&q, &ck)).await.unwrap().is_empty(),
            "rows produced, the policy not covered"
        );
        assert!(
            try_complete(&input(&q, &ck), v, &produced)
                .await
                .unwrap()
                .is_none()
        );
        // The required sink reaches the terminal: completed, kafka behind.
        ck.put_raw(&sink_key("src", "s3"), &marked(100, &chain, 1))
            .await
            .unwrap();
        assert!(
            try_complete(&input(&q, &ck), v, &produced)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            resume_exclusions(&input(&q, &ck)).await.unwrap(),
            ["kafka"]
        );
    }

    #[test]
    fn a_sink_is_classified_against_the_generation() {
        use SinkState::*;
        assert_eq!(st(None), Behind);
        assert_eq!(st(Some(&encode_adopted("c", 4, "d"))), Behind);
        assert_eq!(st(Some(&encode_chained("c", 4, 100u64))), Behind);
        assert_eq!(st(Some(&encode_chained("c", 3, 50u64))), Behind);
        assert_eq!(st(Some(&encode(2, 50u64))), Behind, "legacy");
        assert_eq!(st(Some(&marked(100, "c", 4))), AtOrPast);
        assert_eq!(st(Some(&stream(101))), AtOrPast);
        assert_eq!(st(Some(&stream(100))), Behind, "at the anchor, unmarked");
        assert_eq!(st(Some(&stream(99))), Behind);
        assert_eq!(st(Some(&marked(50, "c", 3))), Behind);
    }

    #[test]
    fn a_foreign_sink_is_never_counted() {
        use SinkState::*;
        // Another chain, a later generation, this generation at another
        // anchor or marked elsewhere, an unknown format.
        assert_eq!(st(Some(&encode_chained("d", 4, 100u64))), Foreign);
        assert_eq!(st(Some(&encode_chained("c", 5, 100u64))), Foreign);
        assert_eq!(st(Some(&encode_chained("c", 4, 101u64))), Foreign);
        assert_eq!(st(Some(&encode_adopted("c", 5, "d"))), Foreign);
        assert_eq!(st(Some(&marked(101, "c", 4))), Foreign);
        assert_eq!(st(Some(&marked(100, "d", 4))), Foreign);
        assert_eq!(st(Some(&marked(100, "c", 5))), Foreign);
        assert_eq!(
            st(Some(
                br#"{"snapshot":{"format":9,"generation":1,"anchor":1}}"#
            )),
            Foreign
        );
    }
}

/// Raise what a start decided: a `sink_snapshot_incomplete` incident per
/// lagging sink, and `snapshot_replaced` when a changed policy replaced an
/// incomplete generation (an ordinary lost read view is only logged). A
/// failure to record an incident is logged, never fatal.
pub async fn report_start(
    store: &storage::adapters::incidents::IncidentStore,
    source_id: &str,
    outcome: &StartOutcome,
    completed: Option<&GenerationControl>,
) {
    let mut drafts = Vec::new();
    match outcome {
        StartOutcome::Stream { lagging, .. } => {
            if let Some(c) = completed {
                for sink in lagging {
                    drafts.push(incidents::sink_snapshot_incomplete(
                        source_id,
                        sink,
                        &c.snapshot_chain,
                        c.generation,
                    ));
                }
            }
        }
        StartOutcome::Snapshot { control, why, .. } => {
            match (why, control.replaced) {
                (Allocation::PolicyChanged, Some(old)) => {
                    drafts.push(incidents::snapshot_replaced(
                        source_id,
                        old,
                        "policy_changed",
                    ));
                }
                (Allocation::Legacy, Some(old)) => {
                    drafts.push(incidents::snapshot_replaced(
                        source_id,
                        old,
                        "legacy_recopy",
                    ));
                }
                (Allocation::ViewLost, Some(old)) => tracing::info!(
                    source_id,
                    replaced = old,
                    generation = control.generation,
                    "an incomplete snapshot generation lost its read view: the \
                 snapshot runs again as the next generation"
                ),
                _ => {}
            }
        }
    }
    for draft in drafts {
        if let Err(e) = store.raise(&draft, 1).await {
            tracing::warn!(
                source_id,
                error = %format!("{e:#}"),
                "could not record a snapshot incident"
            );
        }
    }
}

/// Block generation `generation`, owned by `run`, at control `version`,
/// until an explicit resnapshot (design section 9.2): raise `draft`, then
/// record `blocked` (reason and incident) by one control CAS at exactly
/// that version. Anything else - a generation replaced, owned by another
/// run, at another version, completed or blocked - is left as it is, and
/// nothing is raised. A failure to record either is returned: the next start
/// runs the detection again before any decision.
pub async fn block_generation(
    store: &QueueStore,
    incidents: &storage::adapters::incidents::IncidentStore,
    generation: u64,
    run: Option<&str>,
    version: u64,
    draft: &deltaforge_core::incident::IncidentDraft,
) -> Result<(), DriverError> {
    let Some(Stored::Current {
        version: current,
        control,
    }) = store.read().await?
    else {
        return Ok(());
    };
    // Only the caller's own generation, owned by its own run (`None` before
    // it is sealed), at exactly the version it holds: never another owner's.
    if current != version
        || control.generation != generation
        || control.run.as_deref() != run
        || control.state == State::Completed
        || control.blocked.is_some()
    {
        return Ok(());
    }
    let raised = incidents.raise(draft, 1).await.map_err(|e| {
        DriverError::Checkpoint(format!("record the incident: {e:#}"))
    })?;
    // One CAS at that version; a lost race is not retried.
    store
        .block(
            version,
            &control,
            crate::snapshot_queue::Blocked {
                reason: draft.reason_code.as_str().to_string(),
                incident: raised.record().incident_id.to_string(),
                since_ms: chrono::Utc::now().timestamp_millis(),
            },
        )
        .await?;
    Ok(())
}

/// After a failure of a running generation (design section 8): whether
/// the control record still shows `generation` owned by `run`. When it
/// does not, another process took over: this (stale) process must stop. A
/// non-blocking `concurrent_owner` incident is raised and the control
/// record is never written - the current owner's work goes on. `None`:
/// still the owner.
pub async fn owner_lost(
    store: &QueueStore,
    incidents: &storage::adapters::incidents::IncidentStore,
    source_id: &str,
    generation: u64,
    run: &str,
) -> Option<String> {
    let current = match store.read().await {
        Ok(Some(Stored::Current { control, .. })) => Some(control),
        _ => None,
    };
    if let Some(c) = &current
        && c.generation == generation
        && c.run.as_deref() == Some(run)
    {
        return None;
    }
    let now = current.map_or_else(
        || "no readable control record".to_string(),
        |c| {
            format!("generation {} of run {:?} is current", c.generation, c.run)
        },
    );
    if let Err(e) = incidents
        .raise(&incidents::concurrent_owner(source_id, generation), 1)
        .await
    {
        tracing::warn!(source_id, error = %format!("{e:#}"), "could not record a snapshot incident");
    }
    Some(format!(
        "snapshot generation {generation} is no longer this run's ({now}): \
         another process owns the source's snapshot; this one stops"
    ))
}

/// The sinks the resume fold leaves out (design section 6.3): only after
/// the control record durably shows the current generation `completed`,
/// the sinks of the configuration classified behind it. Nothing otherwise.
pub async fn resume_exclusions<E: EngineOrder>(
    input: &StartInput<'_, E>,
) -> Result<Vec<String>, DriverError> {
    match input.store.read().await? {
        Some(Stored::Current { control, .. })
            if control.state == State::Completed =>
        {
            lagging(input, &control).await
        }
        _ => Ok(Vec::new()),
    }
}

/// Everything a generation start or completion check reads, owned (the
/// completion watcher outlives the start).
#[derive(Clone)]
pub struct GenerationInputs<E: EngineOrder> {
    pub queue: QueueStore,
    pub checkpoints: std::sync::Arc<dyn CheckpointStore>,
    pub source_id: String,
    pub lineage: PersistedLineage,
    pub fingerprint: String,
    pub policy: PolicySnapshot,
    pub mode: SnapshotMode,
    pub engine: E,
    /// A control record's anchor as the engine's anchor.
    pub anchor_of: fn(&EngineAnchor) -> Option<E::Anchor>,
    /// What the engine knows about a legacy snapshot.
    pub legacy: Option<LegacyProof<E::Anchor>>,
}

impl<E: EngineOrder> GenerationInputs<E> {
    pub fn input(&self) -> StartInput<'_, E> {
        StartInput {
            store: &self.queue,
            engine: &self.engine,
            checkpoints: self.checkpoints.as_ref(),
            source_id: &self.source_id,
            lineage: self.lineage.clone(),
            config_fingerprint: &self.fingerprint,
            policy: self.policy.clone(),
            mode: self.mode.clone(),
            anchor_of: &self.anchor_of,
            legacy: self.legacy.as_ref(),
        }
    }
}

/// Record `completed` once the frozen policy's frontier covers the
/// generation's terminal (design section 6.2), then report the sinks behind
/// it and leave them out of the resume fold. Ends with the source, or on
/// anything but `rows_produced`.
pub fn spawn_completion_watch<E>(
    inputs: GenerationInputs<E>,
    incidents: storage::adapters::incidents::IncidentStore,
    shared: crate::SnapshotCohortSlot,
    cancel: tokio_util::sync::CancellationToken,
) where
    E: EngineOrder + Clone + Send + Sync + 'static,
    E::Anchor: Send + Sync,
{
    tokio::spawn(async move {
        loop {
            let input = inputs.input();
            match inputs.queue.read().await {
                Ok(Some(Stored::Current { version, control }))
                    if control.state == State::RowsProduced =>
                {
                    match try_complete(&input, version, &control).await {
                        Ok(Some(done)) => {
                            tracing::info!(
                                source_id = %inputs.source_id,
                                generation = done.generation,
                                "snapshot generation completed"
                            );
                            let behind = lagging(&input, &done)
                                .await
                                .unwrap_or_default();
                            // Durably completed: the sinks behind it leave
                            // the resume fold.
                            shared
                                .lock()
                                .expect("not poisoned")
                                .resume_exclusions = behind.clone();
                            report_start(
                                &incidents,
                                &inputs.source_id,
                                &StartOutcome::Stream {
                                    lagging: behind,
                                    completed_now: Some(done.clone()),
                                },
                                Some(&done),
                            )
                            .await;
                            return;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(
                                source_id = %inputs.source_id,
                                error = %e,
                                "snapshot completion check failed; the next \
                                 start decides"
                            );
                            return;
                        }
                    }
                }
                _ => return,
            }
            tokio::select! {
                _ = inputs.checkpoints.await_checkpoint_change() => {}
                _ = cancel.cancelled() => return,
            }
        }
    });
}

/// What a start decided, as an engine acts on it.
#[derive(Debug, Clone)]
pub enum Decided {
    /// Stream. `completed` is the completed generation, if any: the stream
    /// starts at its anchor when no sink holds a stream position yet.
    Stream {
        completed: Option<GenerationControl>,
    },
    /// Run this generation (allocated, its start barrier pending).
    Generation {
        version: u64,
        control: GenerationControl,
    },
}

/// Decide a start (design section 4) and act on the decision: raise its
/// incidents and set the resume exclusions in `shared` (only from a
/// durably completed generation; none for a new one).
pub async fn decide<E: EngineOrder>(
    inputs: &GenerationInputs<E>,
    incidents: &storage::adapters::incidents::IncidentStore,
    shared: &crate::SnapshotCohortSlot,
) -> Result<Decided, DriverError> {
    let input = inputs.input();
    let outcome = decide_start(&input).await?;
    let completed = match &outcome {
        StartOutcome::Stream { completed_now, .. } => match completed_now {
            Some(c) => Some(c.clone()),
            None => match inputs.queue.read().await? {
                Some(Stored::Current { control, .. }) => Some(*control),
                _ => None,
            },
        },
        StartOutcome::Snapshot { .. } => None,
    };
    report_start(incidents, &inputs.source_id, &outcome, completed.as_ref())
        .await;
    let exclusions = match &outcome {
        StartOutcome::Stream { .. } => resume_exclusions(&input).await?,
        StartOutcome::Snapshot { .. } => Vec::new(),
    };
    shared.lock().expect("not poisoned").resume_exclusions = exclusions;
    Ok(match outcome {
        StartOutcome::Stream { .. } => Decided::Stream { completed },
        StartOutcome::Snapshot {
            version, control, ..
        } => {
            tracing::info!(
                source_id = %inputs.source_id,
                snapshot_chain = %control.snapshot_chain,
                generation = control.generation,
                "snapshot generation allocated"
            );
            Decided::Generation { version, control }
        }
    })
}

/// A driver refusal as the source's error: refusals about the stored
/// state are checkpoint errors (the source does not start).
pub fn source_error(
    source_id: &str,
    e: DriverError,
) -> deltaforge_core::SourceError {
    let msg = format!("source {source_id}: {e}");
    match e {
        DriverError::Blocked { .. }
        | DriverError::NeverIncomplete { .. }
        | DriverError::Foreign { .. }
        | DriverError::IncomparableAcks { .. }
        | DriverError::StateMissing { .. }
        | DriverError::Legacy(_)
        | DriverError::LegacyUnproven { .. } => {
            deltaforge_core::SourceError::Checkpoint {
                details: msg.into(),
            }
        }
        DriverError::Cancelled => deltaforge_core::SourceError::Cancelled,
        _ => deltaforge_core::SourceError::Other(anyhow::anyhow!(msg)),
    }
}

/// What one guard check found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardFinding {
    Ok,
    Warn(&'static str),
    Block(&'static str),
}

/// The anchor-age bound: a warning at 80%, the limit blocks.
pub fn anchor_age_finding(
    age: std::time::Duration,
    max: std::time::Duration,
) -> GuardFinding {
    if age >= max {
        GuardFinding::Block("anchor_age")
    } else if age.as_millis() * 5 >= max.as_millis() * 4 {
        GuardFinding::Warn("anchor_age")
    } else {
        GuardFinding::Ok
    }
}

/// The binlog (or other source log) retention against the anchor's age: a
/// warning once the anchor is older than 80% of the retention.
pub fn retention_finding(
    age: std::time::Duration,
    retention: Option<std::time::Duration>,
) -> GuardFinding {
    match retention {
        Some(r) if !r.is_zero() && age.as_millis() * 5 >= r.as_millis() * 4 => {
            GuardFinding::Warn("log_retention")
        }
        _ => GuardFinding::Ok,
    }
}

/// The incidents the generation driver's decisions raise.
pub mod incidents {
    use deltaforge_core::incident::{
        ActionCode, CauseCode, Component, EvidenceKey as K, IncidentDraft,
        ReasonCode, Retryability, SafetyState,
    };

    /// A sink behind a completed generation: non-blocking, the sink needs a
    /// fresh baseline (design section 6.3).
    pub fn sink_snapshot_incomplete(
        source_id: &str,
        sink: &str,
        chain: &str,
        generation: u64,
    ) -> IncidentDraft {
        IncidentDraft::new(
            ReasonCode::SinkSnapshotIncomplete,
            Component::Sink {
                id: sink.to_string(),
            },
            Retryability::OperatorAction,
            SafetyState::RunningDegraded,
            CauseCode::SinkOther,
        )
        .discriminate("snapshot_chain", chain)
        .discriminate("generation", generation.to_string())
        .with_evidence(|e| {
            e.text(K::SourceId, source_id)
                .text(K::SinkId, sink)
                .text(K::SnapshotChain, chain)
                .text(K::ExpectedGeneration, &generation.to_string());
        })
        .with_actions(&[ActionCode::RebootstrapSink])
    }

    /// An incomplete generation replaced by the next one: non-blocking.
    pub fn snapshot_replaced(
        source_id: &str,
        generation: u64,
        class: &str,
    ) -> IncidentDraft {
        IncidentDraft::new(
            ReasonCode::SnapshotReplaced,
            Component::Source {
                id: source_id.to_string(),
            },
            Retryability::AutoRetry,
            SafetyState::RunningDegraded,
            CauseCode::SourceOther,
        )
        .discriminate("generation", generation.to_string())
        .with_evidence(|e| {
            e.text(K::SourceId, source_id)
                .text(K::ExpectedGeneration, &generation.to_string())
                .text(K::ReasonClass, class);
        })
        .with_actions(&[ActionCode::InspectLogs])
    }

    /// A stale process found another owner of its source's snapshot
    /// (design section 8): it stops; the current owner continues. Not
    /// blocking, but the deployment breaks the single-writer contract.
    pub fn concurrent_owner(source_id: &str, generation: u64) -> IncidentDraft {
        IncidentDraft::new(
            ReasonCode::SnapshotStateInvalid,
            Component::Source {
                id: source_id.to_string(),
            },
            Retryability::OperatorAction,
            SafetyState::RunningDegraded,
            CauseCode::SourceOther,
        )
        .discriminate("generation", generation.to_string())
        .discriminate("class", "concurrent_owner")
        .with_evidence(|e| {
            e.text(K::SourceId, source_id)
                .text(K::ExpectedGeneration, &generation.to_string())
                .text(K::ReasonClass, "concurrent_owner");
        })
        .with_actions(&[ActionCode::InspectLogs])
    }

    /// A failure that blocks a generation until an explicit resnapshot
    /// (design section 9.2).
    pub fn blocked(
        reason: ReasonCode,
        source_id: &str,
        generation: u64,
        class: &str,
    ) -> IncidentDraft {
        IncidentDraft::new(
            reason,
            Component::Source {
                id: source_id.to_string(),
            },
            Retryability::OperatorAction,
            SafetyState::HaltedSafe,
            CauseCode::SourceIncompatible,
        )
        .discriminate("generation", generation.to_string())
        .discriminate("class", class)
        .with_evidence(|e| {
            e.text(K::SourceId, source_id)
                .text(K::ExpectedGeneration, &generation.to_string())
                .text(K::ReasonClass, class);
        })
        .with_actions(&[ActionCode::Resnapshot])
    }

    /// A bound approaching its limit: non-blocking.
    pub fn bound_warning(
        source_id: &str,
        generation: u64,
        class: &str,
    ) -> IncidentDraft {
        IncidentDraft::new(
            ReasonCode::SnapshotBoundWarning,
            Component::Source {
                id: source_id.to_string(),
            },
            Retryability::AutoRetry,
            SafetyState::RunningDegraded,
            CauseCode::SourceOther,
        )
        .discriminate("generation", generation.to_string())
        .discriminate("class", class)
        .with_evidence(|e| {
            e.text(K::SourceId, source_id)
                .text(K::ExpectedGeneration, &generation.to_string())
                .text(K::ReasonClass, class);
        })
        .with_actions(&[ActionCode::InspectLogs])
    }
}
