//! Publication of one snapshot generation's rows and barriers
//! (`docs/design/snapshot-durable-queue.md`, sections 3.4, 5 and 8).
//!
//! Every chunk's last row carries an O(1) boundary: the generation's
//! incomplete-snapshot position and a `SnapshotSeq` watermark whose sequence
//! strictly increases in publish order within the run. The generation starts
//! with its start barrier and ends with its terminal barrier, which carries
//! the completing position. Before every publish the control record is read
//! once: the generation must be current, in the expected state and owned by
//! this run (ownership is detected, not fenced: the single-writer contract
//! stays a deployment precondition).

use std::sync::Arc;

use deltaforge_core::{
    Barrier, BarrierKind, CheckpointMeta, Event, GenerationStart,
    SourceBoundary, SourceItem,
};
use tokio::sync::{Mutex, mpsc};

use crate::durable_checkpoint::{DurableWatermark, WmPos};
use crate::snapshot_queue::{
    GenerationControl, QueueError, QueueStore, State, Stored,
};

/// Why a publish did not happen.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("snapshot output channel closed")]
    Closed,
    /// The control record no longer shows this run's generation in the
    /// expected state (`snapshot_state_invalid`, class `concurrent_owner`).
    #[error("snapshot generation {generation} is not this run's: {why}")]
    NotOwner { generation: u64, why: String },
    #[error(transparent)]
    Store(#[from] QueueError),
}

/// Publishes one generation, for one run.
pub struct GenerationPublisher {
    store: QueueStore,
    tx: mpsc::Sender<SourceItem>,
    control: GenerationControl,
    run: String,
    incomplete: CheckpointMeta,
    /// The last sequence published; held across a publish so sequence order
    /// is channel order.
    seq: Mutex<u64>,
}

impl GenerationPublisher {
    /// `control` is the generation as this run sealed it (`running`, owned
    /// by `run`); `incomplete` its incomplete-snapshot position.
    pub fn new(
        store: QueueStore,
        tx: mpsc::Sender<SourceItem>,
        control: GenerationControl,
        run: &str,
        incomplete: Vec<u8>,
    ) -> Self {
        Self {
            store,
            tx,
            control,
            run: run.to_string(),
            incomplete: CheckpointMeta::from_vec(incomplete),
            seq: Mutex::new(0),
        }
    }

    pub fn generation(&self) -> u64 {
        self.control.generation
    }

    fn watermark(&self, pos: WmPos) -> Option<Arc<[u8]>> {
        Some(Arc::from(
            DurableWatermark::new(self.control.lineage.clone(), pos).to_bytes(),
        ))
    }

    fn seq_watermark(&self, seq: u64, completed: bool) -> Option<Arc<[u8]>> {
        self.watermark(WmPos::SnapshotSeq {
            snapshot_chain: self.control.snapshot_chain.clone(),
            generation: self.control.generation,
            seq,
            completed,
        })
    }

    /// The control record must show this run's generation in `state`.
    async fn check_owner(&self, state: State) -> Result<(), PublishError> {
        crate::snapshot_probe::record_owner_check();
        let not_owner = |why: String| PublishError::NotOwner {
            generation: self.control.generation,
            why,
        };
        match self.store.read().await? {
            Some(Stored::Current { control, .. }) => {
                if control.snapshot_chain != self.control.snapshot_chain
                    || control.generation != self.control.generation
                {
                    return Err(not_owner(format!(
                        "the current generation is {} of chain {}",
                        control.generation, control.snapshot_chain
                    )));
                }
                if control.run.as_deref() != Some(self.run.as_str()) {
                    return Err(not_owner("another run owns it".into()));
                }
                if control.blocked.is_some() {
                    return Err(not_owner("it is blocked".into()));
                }
                if control.state != state {
                    return Err(not_owner(format!(
                        "it is {:?}, not {state:?}",
                        control.state
                    )));
                }
                Ok(())
            }
            _ => Err(not_owner("the control record is gone".into())),
        }
    }

    async fn send(&self, item: SourceItem) -> Result<(), PublishError> {
        self.tx.send(item).await.map_err(|_| PublishError::Closed)
    }

    /// The generation start barrier (design section 5.4): each sink moves
    /// its own state into the generation. Sent while `running`, before any
    /// row; the caller then waits for the whole cohort.
    pub async fn start(&self) -> Result<(), PublishError> {
        let _seq = self.seq.lock().await;
        self.check_owner(State::Running).await?;
        let c = &self.control;
        let boundary = SourceBoundary {
            checkpoint: CheckpointMeta::from_vec(
                crate::snapshot_position::encode_adopted(
                    &c.snapshot_chain,
                    c.generation,
                    "",
                ),
            ),
            // Each sink fills `replaced_digest` from the state it replaces.
            durable_watermark: self.watermark(
                WmPos::SnapshotGenerationAdopted {
                    snapshot_chain: c.snapshot_chain.clone(),
                    generation: c.generation,
                    replaced_digest: String::new(),
                    legacy_through: c.legacy_through,
                },
            ),
        };
        self.send(SourceItem::Barrier {
            barrier: Barrier {
                kind: BarrierKind::GenerationStart(GenerationStart {
                    snapshot_chain: c.snapshot_chain.clone(),
                    generation: c.generation,
                    legacy_through: c.legacy_through,
                }),
                boundary,
            },
        })
        .await
    }

    /// Publish one chunk of rows: its last row carries the next boundary.
    /// An empty chunk publishes nothing.
    pub async fn publish_chunk(
        &self,
        mut events: Vec<Event>,
    ) -> Result<(), PublishError> {
        let Some(mut last) = events.pop() else {
            return Ok(());
        };
        let mut seq = self.seq.lock().await;
        self.check_owner(State::Running).await?;
        *seq += 1;
        last.set_boundary(SourceBoundary {
            checkpoint: self.incomplete.clone(),
            durable_watermark: self.seq_watermark(*seq, false),
        });
        crate::snapshot_probe::record_boundary(
            self.incomplete.as_bytes().len(),
            std::time::Duration::ZERO,
        );
        for ev in events {
            self.send(SourceItem::Event(ev)).await?;
        }
        self.send(SourceItem::Event(last)).await?;
        drop(seq);
        crate::snapshot_probe::during_copy().await;
        Ok(())
    }

    /// The terminal barrier (design section 5.3), after `rows_produced`:
    /// it carries the completing position.
    pub async fn terminal(
        &self,
        completing: Vec<u8>,
    ) -> Result<(), PublishError> {
        let mut seq = self.seq.lock().await;
        self.check_owner(State::RowsProduced).await?;
        *seq += 1;
        let boundary = SourceBoundary {
            checkpoint: CheckpointMeta::from_vec(completing),
            durable_watermark: self.seq_watermark(*seq, true),
        };
        self.send(SourceItem::Barrier {
            barrier: Barrier {
                kind: BarrierKind::Terminal,
                boundary,
            },
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot_queue::contract;

    fn event() -> Event {
        let source = deltaforge_core::SourceInfo {
            version: "t".into(),
            connector: "postgresql".into(),
            name: "t".into(),
            db: "public".into(),
            schema: None,
            table: "orders".into(),
            ts_ms: 0,
            snapshot: Some("true".into()),
            position: Default::default(),
        };
        Event::new_snapshot(
            deltaforge_core::EventId::mysql_row_server(1, "orders", 1, 1),
            source,
            serde_json::json!({ "id": 1 }),
            0,
            0,
        )
    }

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
            crate::snapshot_queue::EngineAnchor::Postgres {
                lsn: "0/10".into(),
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

    /// Rows and barriers flow only while the control record shows this
    /// run's generation in the expected state; sequences strictly increase.
    #[tokio::test]
    async fn every_publish_checks_the_owner() {
        let q = QueueStore::new(
            Arc::new(storage::MemoryStorageBackend::new()),
            "src",
        );
        let (v, c) = running(&q, "r1").await;
        let (tx, mut rx) = mpsc::channel(16);
        let p = GenerationPublisher::new(
            q.clone(),
            tx,
            c.clone(),
            "r1",
            b"incomplete".to_vec(),
        );
        p.start().await.unwrap();
        p.publish_chunk(vec![event(), event()]).await.unwrap();
        p.publish_chunk(vec![event()]).await.unwrap();
        p.publish_chunk(Vec::new()).await.unwrap();
        // Rows are refused once the generation produced its rows; the
        // terminal barrier is accepted only then.
        assert!(matches!(
            p.terminal(b"done".to_vec()).await,
            Err(PublishError::NotOwner { .. })
        ));
        let (_, produced) = q.rows_produced(v, &c, "r1").await.unwrap();
        assert!(matches!(
            p.publish_chunk(vec![event()]).await,
            Err(PublishError::NotOwner { .. })
        ));
        p.terminal(b"done".to_vec()).await.unwrap();
        let mut seqs = Vec::new();
        let mut kinds = Vec::new();
        while let Ok(item) = rx.try_recv() {
            match item {
                SourceItem::Event(ev) => {
                    if let Some(b) = ev.boundary {
                        let wm = DurableWatermark::parse(
                            b.durable_watermark.as_deref().unwrap(),
                        )
                        .unwrap();
                        let WmPos::SnapshotSeq { seq, completed, .. } = wm.pos
                        else {
                            panic!("a sequence watermark")
                        };
                        assert!(!completed);
                        seqs.push(seq);
                        assert_eq!(b.checkpoint.as_bytes(), b"incomplete");
                    }
                    kinds.push("row");
                }
                SourceItem::Barrier { barrier } => match barrier.kind {
                    BarrierKind::GenerationStart(s) => {
                        assert_eq!(s.generation, c.generation);
                        kinds.push("start");
                    }
                    BarrierKind::Terminal => {
                        assert_eq!(
                            barrier.boundary.checkpoint.as_bytes(),
                            b"done"
                        );
                        kinds.push("terminal");
                    }
                },
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(kinds, ["start", "row", "row", "row", "terminal"]);
        assert_eq!(seqs, [1, 2]);

        // Another run's generation: nothing more is published.
        let stored = q.read().await.unwrap().unwrap();
        let (_, next) = q
            .replace(
                &stored,
                &contract::lineage(),
                "fp",
                contract::policy(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(next.generation, produced.generation + 1);
        assert!(matches!(
            p.terminal(b"done".to_vec()).await,
            Err(PublishError::NotOwner { .. })
        ));
    }
}
