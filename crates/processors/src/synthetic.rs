//! Automatic synthetic-event detection.
//!
//! Wraps any processor and marks newly created events with the processor's ID
//! after it returns. Detection is based on event IDs: any event whose ID was
//! absent from `ctx.original_ids` (the chain-entry snapshot) is new.
//!
//! # What counts as "new"
//!
//! | Scenario | Result |
//! |---|---|
//! | Processor transforms in place | same ID -> not marked |
//! | JS creates `{...e, after: newVal}` spread | copies event_id -> not marked |
//! | JS creates fresh object with no event_id | `None` -> marked |
//! | Metrics processor calls `Event::new_row(...)` | fresh UUID -> marked |
//! | Outbox reshapes existing row | same ID -> not marked |
//!
//! # Explicit override
//!
//! If a processor already set `event.synthetic` the wrapper leaves it alone,
//! so processors can always override the auto-detected value.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use deltaforge_core::{BatchContext, Event, Processor};

pub struct SyntheticMarkingProcessor {
    inner: Arc<dyn Processor>,
}

impl SyntheticMarkingProcessor {
    /// Wrap a processor with automatic synthetic detection.
    pub fn wrap(inner: Arc<dyn Processor>) -> Arc<dyn Processor> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl Processor for SyntheticMarkingProcessor {
    fn id(&self) -> &str {
        self.inner.id()
    }

    async fn process(
        &self,
        events: Vec<Event>,
        ctx: &BatchContext,
    ) -> Result<Vec<Event>> {
        let mut output = self.inner.process(events, ctx).await?;

        let pid = self.inner.id();
        for event in &mut output {
            if event.synthetic.is_some() {
                // Processor set it explicitly — respect that.
                continue;
            }
            if !ctx.is_original(event) {
                event.synthetic = Some(pid.to_string());
            }
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::{
        BatchContext, Event, Op, SourceInfo, SourcePosition,
    };
    use serde_json::json;

    fn make_event() -> Event {
        Event::new_row(
            SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "db".into(),
                ts_ms: 0,
                db: "shop".into(),
                schema: None,
                table: "orders".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            Op::Create,
            None,
            Some(json!({"id": 1})),
            0,
            64,
        )
    }

    // Passthrough: returns same events untouched.
    struct Passthrough(String);
    #[async_trait]
    impl Processor for Passthrough {
        fn id(&self) -> &str {
            &self.0
        }
        async fn process(
            &self,
            events: Vec<Event>,
            _: &BatchContext,
        ) -> Result<Vec<Event>> {
            Ok(events)
        }
    }

    // FanOut: returns input + one fresh event per input event.
    struct FanOut(String);
    #[async_trait]
    impl Processor for FanOut {
        fn id(&self) -> &str {
            &self.0
        }
        async fn process(
            &self,
            events: Vec<Event>,
            _: &BatchContext,
        ) -> Result<Vec<Event>> {
            let mut out = Vec::with_capacity(events.len() * 2);
            for e in &events {
                out.push(e.clone());
                // Fresh event — new UUID, no explicit synthetic
                out.push(Event::new_row(
                    e.source.clone(),
                    Op::Create,
                    None,
                    Some(json!({"derived": true})),
                    0,
                    32,
                ));
            }
            Ok(out)
        }
    }

    // ExplicitSynthetic: sets synthetic manually to a custom value.
    struct ExplicitSynthetic(String);
    #[async_trait]
    impl Processor for ExplicitSynthetic {
        fn id(&self) -> &str {
            &self.0
        }
        async fn process(
            &self,
            events: Vec<Event>,
            _: &BatchContext,
        ) -> Result<Vec<Event>> {
            let mut out = Vec::new();
            for e in &events {
                let mut fresh = Event::new_row(
                    e.source.clone(),
                    Op::Create,
                    None,
                    Some(json!({"alert": true})),
                    0,
                    32,
                );
                fresh.synthetic = Some("custom-label".into()); // explicit override
                out.push(fresh);
            }
            Ok(out)
        }
    }

    #[tokio::test]
    async fn passthrough_events_not_marked() {
        let e = make_event();
        let ctx = BatchContext::from_batch(std::slice::from_ref(&e));
        let proc =
            SyntheticMarkingProcessor::wrap(Arc::new(Passthrough("p".into())));

        let out = proc.process(vec![e], &ctx).await.unwrap();

        assert_eq!(out.len(), 1);
        assert!(
            out[0].synthetic.is_none(),
            "transformed event must not be marked"
        );
    }

    #[tokio::test]
    async fn fan_out_new_events_marked_original_not() {
        let e = make_event();
        let orig_id = e.event_id.unwrap();
        let ctx = BatchContext::from_batch(std::slice::from_ref(&e));
        let proc =
            SyntheticMarkingProcessor::wrap(Arc::new(FanOut("fan-out".into())));

        let out = proc.process(vec![e], &ctx).await.unwrap();

        assert_eq!(out.len(), 2);

        // Original event — unchanged
        let original =
            out.iter().find(|e| e.event_id == Some(orig_id)).unwrap();
        assert!(original.synthetic.is_none());

        // New event — auto-marked with processor id
        let new_ev = out.iter().find(|e| e.event_id != Some(orig_id)).unwrap();
        assert_eq!(new_ev.synthetic.as_deref(), Some("fan-out"));
    }

    #[tokio::test]
    async fn explicit_synthetic_not_overwritten() {
        let e = make_event();
        let ctx = BatchContext::from_batch(std::slice::from_ref(&e));
        let proc = SyntheticMarkingProcessor::wrap(Arc::new(
            ExplicitSynthetic("wrapper-id".into()),
        ));

        let out = proc.process(vec![e], &ctx).await.unwrap();

        // Even though wrapper-id is the processor, explicit value wins
        assert_eq!(out[0].synthetic.as_deref(), Some("custom-label"));
    }

    #[tokio::test]
    async fn event_without_id_marked_synthetic() {
        let mut e = make_event();
        e.event_id = None; // simulate event with no ID (edge case)
        let ctx = BatchContext::from_batch(&[]); // empty original set

        // Processor that passes it through
        let proc =
            SyntheticMarkingProcessor::wrap(Arc::new(Passthrough("p".into())));
        let out = proc.process(vec![e], &ctx).await.unwrap();

        // No ID and not in original set → treated as new
        assert_eq!(out[0].synthetic.as_deref(), Some("p"));
    }

    #[tokio::test]
    async fn chain_entry_snapshot_not_rebuilt_per_processor() {
        // Simulate: two wrapped processors in sequence.
        // Processor A fans out. Processor B should NOT re-mark A's originals.
        let e = make_event();
        let orig_id = e.event_id.unwrap();

        // ctx built once before any processor runs
        let ctx = BatchContext::from_batch(std::slice::from_ref(&e));

        let proc_a =
            SyntheticMarkingProcessor::wrap(Arc::new(FanOut("proc-a".into())));
        let after_a = proc_a.process(vec![e], &ctx).await.unwrap();
        assert_eq!(after_a.len(), 2);

        let proc_b = SyntheticMarkingProcessor::wrap(Arc::new(Passthrough(
            "proc-b".into(),
        )));
        let after_b = proc_b.process(after_a, &ctx).await.unwrap();

        // Original event: still no synthetic
        let orig = after_b
            .iter()
            .find(|e| e.event_id == Some(orig_id))
            .unwrap();
        assert!(orig.synthetic.is_none());

        // Fan-out event: still marked as proc-a (proc-b passthrough doesn't overwrite)
        let fan = after_b
            .iter()
            .find(|e| e.event_id != Some(orig_id))
            .unwrap();
        assert_eq!(
            fan.synthetic.as_deref(),
            Some("proc-a"),
            "proc-b must not overwrite proc-a's synthetic label"
        );
    }
}
