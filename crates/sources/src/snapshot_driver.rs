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
            // A completion written before chains: a legacy position.
            Some((None, _)) => Behind,
            None => match anchor.map(|a| engine.anchor_vs_stream(a, raw)) {
                Some(deltaforge_core::CheckpointOrder::Before) => AtOrPast,
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
    #[error("legacy snapshot state is classified by the upgrade path")]
    Legacy,
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
}

impl<E: EngineOrder> StartInput<'_, E> {
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
pub fn frontier<E: EngineOrder>(
    engine: &E,
    policy: &PolicySnapshot,
    states: &[(String, SinkState, Option<Vec<u8>>)],
) -> Option<Completion> {
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
    let deciding: Vec<&Vec<u8>> = match policy.mode {
        PolicyMode::All => {
            if acks.len() != policy.sinks.len() {
                return None;
            }
            acks.iter().map(|(_, r)| r).collect()
        }
        PolicyMode::Required => {
            let required: Vec<&str> = policy
                .sinks
                .iter()
                .filter(|s| s.required)
                .map(|s| s.id.as_str())
                .collect();
            let got: Vec<&Vec<u8>> = acks
                .iter()
                .filter(|(id, _)| required.contains(&id.as_str()))
                .map(|(_, r)| r)
                .collect();
            if got.len() != required.len() {
                return None;
            }
            got
        }
        PolicyMode::Quorum => {
            let q = policy.quorum.unwrap_or(u32::MAX) as usize;
            if acks.len() < q {
                return None;
            }
            // The quorum's position is the q-th highest acknowledged one.
            let mut all: Vec<&Vec<u8>> = acks.iter().map(|(_, r)| r).collect();
            all.sort_by(|a, b| {
                match crate::snapshot_position::order(engine, b, a) {
                    deltaforge_core::CheckpointOrder::Before => {
                        std::cmp::Ordering::Less
                    }
                    deltaforge_core::CheckpointOrder::After => {
                        std::cmp::Ordering::Greater
                    }
                    _ => std::cmp::Ordering::Equal,
                }
            });
            all.into_iter().take(q).collect()
        }
    };
    // The frontier position is the lowest of the deciding positions.
    let frontier = deciding
        .iter()
        .copied()
        .reduce(
            |lo, x| match crate::snapshot_position::order(engine, x, lo) {
                deltaforge_core::CheckpointOrder::Before => x,
                _ => lo,
            },
        )
        .map(|r| String::from_utf8_lossy(r).into_owned())
        .unwrap_or_default();
    Some(Completion {
        acks: acks.into_iter().map(|(id, _)| id).collect(),
        frontier,
    })
}

/// Decide what a start does (design section 4).
pub async fn decide_start<E: EngineOrder>(
    input: &StartInput<'_, E>,
) -> Result<StartOutcome, DriverError> {
    let never = input.mode == SnapshotMode::Never;
    let stored = match input.store.read().await? {
        None => {
            if never {
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
        Some(Stored::Legacy { .. }) => return Err(DriverError::Legacy),
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

/// The configured sinks behind a completed generation.
async fn lagging<E: EngineOrder>(
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
    let Some(completion) = frontier(input.engine, &control.policy, &states)
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

    /// Anchors are numbers; streams are `{"p": n, "mark": [chain, g]?}`.
    struct T;
    fn p(raw: &[u8]) -> Option<u64> {
        serde_json::from_slice::<serde_json::Value>(raw).ok()?["p"].as_u64()
    }
    impl EngineOrder for T {
        type Anchor = u64;
        fn stream_order(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
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
