//! Filtered sink wrapper.
//!
//! Wraps any `ArcDynSink` and applies a `SinkFilter` before delivery.
//! Events that don't match the filter are silently dropped for this sink —
//! they continue to other sinks normally.
//!
//! Zero overhead when no filter is configured: `build_sinks` only wraps
//! sinks that have an active filter (`filter.is_active() == true`).

use std::sync::Arc;

use async_trait::async_trait;
use deltaforge_config::SinkFilter;
use deltaforge_core::{ArcDynSink, Event, Sink, SinkResult};

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

    async fn send_batch(&self, events: &[Event]) -> SinkResult<()> {
        // Fast path: nothing filtered
        if events.iter().all(|e| self.filter.allows(e)) {
            return self.inner.send_batch(events).await;
        }

        // Collect passing events without cloning the full batch when possible
        let filtered: Vec<&Event> =
            events.iter().filter(|e| self.filter.allows(e)).collect();

        if filtered.is_empty() {
            return Ok(());
        }

        // Need owned Vec<Event> for send_batch signature — clone only filtered subset
        let owned: Vec<Event> = filtered.into_iter().cloned().collect();
        self.inner.send_batch(&owned).await
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

    struct CountingSink {
        id: String,
        count: Arc<AtomicUsize>,
    }

    impl CountingSink {
        fn new(id: &str) -> (ArcDynSink, Arc<AtomicUsize>) {
            let count = Arc::new(AtomicUsize::new(0));
            let sink = Arc::new(Self { id: id.into(), count: count.clone() });
            (sink, count)
        }
    }

    #[async_trait]
    impl Sink for CountingSink {
        fn id(&self) -> &str { &self.id }
        fn required(&self) -> bool { true }
        async fn send(&self, _: &Event) -> SinkResult<()> {
            self.count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        async fn send_batch(&self, events: &[Event]) -> SinkResult<()> {
            self.count.fetch_add(events.len(), Ordering::Relaxed);
            Ok(())
        }
    }

    fn source_event() -> Event {
        Event::new_row(
            SourceInfo {
                version: "1".into(), connector: "mysql".into(),
                name: "db".into(), ts_ms: 0, db: "shop".into(),
                schema: None, table: "orders".into(),
                snapshot: None, position: SourcePosition::default(),
            },
            Op::Create, None, Some(json!({"id": 1})), 0, 64,
        )
        // synthetic is None — source event
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
        let filter = SinkFilter { exclude_synthetic: true, ..Default::default() };
        let sink = FilteredSink::wrap(inner, filter);

        sink.send(&source_event()).await.unwrap();         // passes
        sink.send(&synthetic_event("analytics")).await.unwrap(); // dropped

        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn synthetic_only_passes_synthetic_drops_source() {
        let (inner, count) = CountingSink::new("sink");
        let filter = SinkFilter { synthetic_only: true, ..Default::default() };
        let sink = FilteredSink::wrap(inner, filter);

        sink.send(&source_event()).await.unwrap();              // dropped
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
        sink.send(&source_event()).await.unwrap();                // dropped (no producer)

        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn send_batch_fast_path_when_nothing_filtered() {
        let (inner, count) = CountingSink::new("sink");
        let filter = SinkFilter { exclude_synthetic: true, ..Default::default() };
        let sink = FilteredSink::wrap(inner, filter);

        // All source events → fast path, no clone
        let events = vec![source_event(), source_event(), source_event()];
        sink.send_batch(&events).await.unwrap();

        assert_eq!(count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn send_batch_partial_filter() {
        let (inner, count) = CountingSink::new("sink");
        let filter = SinkFilter { exclude_synthetic: true, ..Default::default() };
        let sink = FilteredSink::wrap(inner, filter);

        let events = vec![
            source_event(),
            synthetic_event("metrics"),
            source_event(),
        ];
        sink.send_batch(&events).await.unwrap();

        assert_eq!(count.load(Ordering::Relaxed), 2); // only the 2 source events
    }

    #[tokio::test]
    async fn no_active_filter_passes_everything() {
        // SinkFilter::default() is inactive — build_sinks won't even wrap
        let filter = SinkFilter::default();
        assert!(!filter.is_active());
        assert!(filter.allows(&source_event()));
        assert!(filter.allows(&synthetic_event("anything")));
    }
}