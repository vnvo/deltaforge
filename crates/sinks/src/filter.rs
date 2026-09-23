//! Filtered sink wrapper.
//!
//! Wraps any `ArcDynSink` and applies a `SinkFilter` before delivery.
//! Events that don't match the filter are silently dropped for this sink -
//! they continue to other sinks normally.
//!
//! Zero overhead when no filter is configured: `build_sinks` only wraps
//! sinks that have an active filter (`filter.is_active() == true`).

use std::sync::Arc;

use async_trait::async_trait;
use deltaforge_config::SinkFilter;
use deltaforge_core::{ArcDynSink, Event, Sink, SinkBatchContext, SinkResult};

pub struct FilteredSink {
    inner: ArcDynSink,
    filter: SinkFilter,
}

impl FilteredSink {
    pub fn wrap(inner: ArcDynSink, filter: SinkFilter) -> ArcDynSink {
        Arc::new(Self { inner, filter })
    }
}

#[async_trait]
impl Sink for FilteredSink {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn required(&self) -> bool {
        self.inner.required()
    }

    async fn send(&self, event: &Event) -> SinkResult<()> {
        if self.filter.allows(event) {
            self.inner.send(event).await
        } else {
            Ok(())
        }
    }

    async fn send_batch(
        &self,
        events: &[Event],
    ) -> SinkResult<deltaforge_core::BatchResult> {
        // Fast path: nothing filtered
        if events.iter().all(|e| self.filter.allows(e)) {
            return self.inner.send_batch(events).await;
        }

        // Collect passing events without cloning the full batch when possible
        let filtered: Vec<&Event> =
            events.iter().filter(|e| self.filter.allows(e)).collect();

        if filtered.is_empty() {
            return Ok(deltaforge_core::BatchResult::ok());
        }

        // Need owned Vec<Event> for send_batch signature - clone only filtered subset
        let owned: Vec<Event> = filtered.into_iter().cloned().collect();
        self.inner.send_batch(&owned).await
    }

    /// Filter events but ALWAYS forward the authoritative context to the inner
    /// sink - even when every event is filtered out. Dropping the call (as the
    /// default trait impl's context-free path would) or short-circuiting on an
    /// empty result would let the coordinator advance its checkpoint while a
    /// durable sink's HEAD never advanced. An empty contextual delivery must
    /// still publish the zero-object manifest and advance HEAD.
    async fn send_batch_with_context(
        &self,
        events: &[Event],
        ctx: &SinkBatchContext,
    ) -> SinkResult<deltaforge_core::BatchResult> {
        if events.iter().all(|e| self.filter.allows(e)) {
            return self.inner.send_batch_with_context(events, ctx).await;
        }
        let owned: Vec<Event> = events
            .iter()
            .filter(|e| self.filter.allows(e))
            .cloned()
            .collect();
        // Forward even if `owned` is empty: the boundary/watermark in `ctx` must
        // still be acknowledged by the inner (durable) sink.
        self.inner.send_batch_with_context(&owned, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_config::SinkFilter;
    use deltaforge_core::{Event, Op, SinkResult, SourceInfo, SourcePosition};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // -------------------------------------------------------------------------
    // Minimal counting sink for tests
    // -------------------------------------------------------------------------

    #[derive(Default)]
    struct CtxRecorder {
        /// (surviving event count, watermark present) per context delivery.
        calls: std::sync::Mutex<Vec<(usize, bool)>>,
    }

    struct CountingSink {
        id: String,
        count: Arc<AtomicUsize>,
        ctx: Arc<CtxRecorder>,
    }

    impl CountingSink {
        #[allow(clippy::new_ret_no_self)]
        fn new(id: &str) -> (ArcDynSink, Arc<AtomicUsize>) {
            let (sink, count, _ctx) = Self::new_recording(id);
            (sink, count)
        }

        #[allow(clippy::new_ret_no_self)]
        fn new_recording(
            id: &str,
        ) -> (ArcDynSink, Arc<AtomicUsize>, Arc<CtxRecorder>) {
            let count = Arc::new(AtomicUsize::new(0));
            let ctx = Arc::new(CtxRecorder::default());
            let sink = Arc::new(Self {
                id: id.into(),
                count: count.clone(),
                ctx: ctx.clone(),
            });
            (sink, count, ctx)
        }
    }

    #[async_trait]
    impl Sink for CountingSink {
        fn id(&self) -> &str {
            &self.id
        }
        fn required(&self) -> bool {
            true
        }
        async fn send(&self, _: &Event) -> SinkResult<()> {
            self.count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        async fn send_batch(
            &self,
            events: &[Event],
        ) -> SinkResult<deltaforge_core::BatchResult> {
            self.count.fetch_add(events.len(), Ordering::Relaxed);
            Ok(deltaforge_core::BatchResult::ok())
        }
        async fn send_batch_with_context(
            &self,
            events: &[Event],
            ctx: &SinkBatchContext,
        ) -> SinkResult<deltaforge_core::BatchResult> {
            self.count.fetch_add(events.len(), Ordering::Relaxed);
            self.ctx
                .calls
                .lock()
                .unwrap()
                .push((events.len(), ctx.durable_watermark.is_some()));
            Ok(deltaforge_core::BatchResult::ok())
        }
    }

    fn ctx_with_wm() -> SinkBatchContext {
        SinkBatchContext {
            checkpoint: deltaforge_core::CheckpointMeta::from_vec(
                b"cp".to_vec(),
            ),
            durable_watermark: Some(b"wm".to_vec()),
            batch_id: None,
        }
    }

    fn source_event() -> Event {
        Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
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
        // synthetic is None - source event
    }

    fn synthetic_event(producer: &str) -> Event {
        source_event().mark_synthetic(producer)
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn exclude_synthetic_drops_synthetic_passes_source() {
        let (inner, count) = CountingSink::new("sink");
        let filter = SinkFilter {
            exclude_synthetic: true,
            ..Default::default()
        };
        let sink = FilteredSink::wrap(inner, filter);

        sink.send(&source_event()).await.unwrap(); // passes
        sink.send(&synthetic_event("analytics")).await.unwrap(); // dropped

        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn synthetic_only_passes_synthetic_drops_source() {
        let (inner, count) = CountingSink::new("sink");
        let filter = SinkFilter {
            synthetic_only: true,
            ..Default::default()
        };
        let sink = FilteredSink::wrap(inner, filter);

        sink.send(&source_event()).await.unwrap(); // dropped
        sink.send(&synthetic_event("metrics")).await.unwrap(); // passes

        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn producers_filter_by_processor_id() {
        let (inner, count) = CountingSink::new("sink");
        let filter = SinkFilter {
            producers: vec!["analytics".into()],
            ..Default::default()
        };
        let sink = FilteredSink::wrap(inner, filter);

        sink.send(&synthetic_event("analytics")).await.unwrap(); // passes
        sink.send(&synthetic_event("other-proc")).await.unwrap(); // dropped
        sink.send(&source_event()).await.unwrap(); // dropped (no producer)

        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn send_batch_fast_path_when_nothing_filtered() {
        let (inner, count) = CountingSink::new("sink");
        let filter = SinkFilter {
            exclude_synthetic: true,
            ..Default::default()
        };
        let sink = FilteredSink::wrap(inner, filter);

        // All source events → fast path, no clone
        let events = vec![source_event(), source_event(), source_event()];
        sink.send_batch(&events).await.unwrap();

        assert_eq!(count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn send_batch_partial_filter() {
        let (inner, count) = CountingSink::new("sink");
        let filter = SinkFilter {
            exclude_synthetic: true,
            ..Default::default()
        };
        let sink = FilteredSink::wrap(inner, filter);

        let events =
            vec![source_event(), synthetic_event("metrics"), source_event()];
        sink.send_batch(&events).await.unwrap();

        assert_eq!(count.load(Ordering::Relaxed), 2); // only the 2 source events
    }

    #[tokio::test]
    async fn no_active_filter_passes_everything() {
        // SinkFilter::default() is inactive - build_sinks won't even wrap
        let filter = SinkFilter::default();
        assert!(!filter.is_active());
        assert!(filter.allows(&source_event()));
        assert!(filter.allows(&synthetic_event("anything")));
    }

    #[tokio::test]
    async fn context_partial_filter_forwards_survivors_with_context() {
        let (inner, count, rec) = CountingSink::new_recording("sink");
        let filter = SinkFilter {
            exclude_synthetic: true,
            ..Default::default()
        };
        let sink = FilteredSink::wrap(inner, filter);

        let events =
            vec![source_event(), synthetic_event("metrics"), source_event()];
        sink.send_batch_with_context(&events, &ctx_with_wm())
            .await
            .unwrap();

        // Two survivors forwarded, and the context (its watermark) preserved.
        assert_eq!(count.load(Ordering::Relaxed), 2);
        let calls = rec.calls.lock().unwrap();
        assert_eq!(calls.as_slice(), &[(2, true)]);
    }

    #[tokio::test]
    async fn context_fully_filtered_still_forwards_empty_with_context() {
        let (inner, count, rec) = CountingSink::new_recording("sink");
        let filter = SinkFilter {
            exclude_synthetic: true,
            ..Default::default()
        };
        let sink = FilteredSink::wrap(inner, filter);

        // Every event filtered out: the inner sink must STILL be called with an
        // empty batch + the context, so a durable sink advances HEAD (the
        // coordinator must not checkpoint past an un-acked boundary).
        let events = vec![synthetic_event("a"), synthetic_event("b")];
        sink.send_batch_with_context(&events, &ctx_with_wm())
            .await
            .unwrap();

        assert_eq!(count.load(Ordering::Relaxed), 0);
        let calls = rec.calls.lock().unwrap();
        assert_eq!(
            calls.as_slice(),
            &[(0, true)],
            "empty contextual delivery still forwarded once, with watermark"
        );
    }

    #[tokio::test]
    async fn context_fast_path_forwards_all_with_context() {
        let (inner, _count, rec) = CountingSink::new_recording("sink");
        let filter = SinkFilter {
            exclude_synthetic: true,
            ..Default::default()
        };
        let sink = FilteredSink::wrap(inner, filter);
        let events = vec![source_event(), source_event()];
        sink.send_batch_with_context(&events, &ctx_with_wm())
            .await
            .unwrap();
        assert_eq!(rec.calls.lock().unwrap().as_slice(), &[(2, true)]);
    }
}
