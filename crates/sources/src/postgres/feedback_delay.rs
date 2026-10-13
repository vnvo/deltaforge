//! `deltaforge_wal_feedback_delay_seconds`: how long an acknowledged position
//! waits before PostgreSQL hears of it, in two diagnostic stages.
//!
//! - `refresh`: from the persistence of a durable checkpoint change the
//!   source observes, to the call that raises the replication client's
//!   applied position.
//! - `send`: from the client's first pending notification of a raised
//!   position, to the successful status write that reports it.
//!
//! Positions coalesce in both stages (each keeps the oldest pending start),
//! so `refresh + send` decomposes the delay; it is not an exact per-event
//! end-to-end sum. `..._observations_total` counts measurements and
//! `..._dropped_total` measurements abandoned without one (pending when the
//! source stopped), so a dashboard can tell zero delay from no measurements.

use std::time::Duration;

pub(crate) const REFRESH: &str = "refresh";
pub(crate) const SEND: &str = "send";

#[derive(Clone)]
pub(crate) struct FeedbackDelayMetrics {
    pipeline: String,
    source: String,
}

impl FeedbackDelayMetrics {
    pub(crate) fn new(pipeline: &str, source: &str) -> Self {
        Self {
            pipeline: pipeline.to_string(),
            source: source.to_string(),
        }
    }

    pub(crate) fn observe(&self, stage: &'static str, delay: Duration) {
        metrics::histogram!(
            "deltaforge_wal_feedback_delay_seconds",
            "pipeline" => self.pipeline.clone(),
            "source" => self.source.clone(),
            "stage" => stage,
        )
        .record(delay.as_secs_f64());
        metrics::counter!(
            "deltaforge_wal_feedback_delay_observations_total",
            "pipeline" => self.pipeline.clone(),
            "source" => self.source.clone(),
            "stage" => stage,
        )
        .increment(1);
    }

    pub(crate) fn dropped(&self, stage: &'static str) {
        metrics::counter!(
            "deltaforge_wal_feedback_delay_dropped_total",
            "pipeline" => self.pipeline.clone(),
            "source" => self.source.clone(),
            "stage" => stage,
        )
        .increment(1);
    }

    /// The replication client's observer: the `send` stage.
    pub(crate) fn observer(&self) -> pgwire_replication::FeedbackObserver {
        let m = self.clone();
        pgwire_replication::FeedbackObserver::new(move |sent| {
            m.observe(SEND, sent.pending_for)
        })
    }

    /// Replace the replication client, carrying over a feedback position the
    /// old session did not report: its wait continues in the new one.
    pub(crate) fn replace_client(
        slot: &mut pgwire_replication::ReplicationClient,
        new: pgwire_replication::ReplicationClient,
    ) {
        if let Some(since) = slot.feedback_pending_since() {
            new.carry_feedback_pending(since);
        }
        *slot = new;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    #[test]
    fn stages_are_observed_and_counted_with_bounded_labels() {
        let rec = DebuggingRecorder::new();
        let snap = rec.snapshotter();
        metrics::with_local_recorder(&rec, || {
            let m = FeedbackDelayMetrics::new("p", "s");
            m.observe(REFRESH, Duration::from_millis(250));
            m.observe(SEND, Duration::ZERO);
            m.dropped(SEND);
        });
        let mut seen = Vec::new();
        for (key, _, _, value) in snap.snapshot().into_vec() {
            let labels: Vec<String> = key
                .key()
                .labels()
                .map(|l| format!("{}={}", l.key(), l.value()))
                .collect();
            let v = match value {
                DebugValue::Counter(c) => format!("{c}"),
                DebugValue::Histogram(h) => {
                    format!("{:?}", h.iter().map(|x| x.0).collect::<Vec<_>>())
                }
                DebugValue::Gauge(g) => format!("{}", g.0),
            };
            seen.push(format!("{} {} {v}", key.key().name(), labels.join(",")));
        }
        seen.sort();
        assert_eq!(
            seen,
            [
                "deltaforge_wal_feedback_delay_dropped_total pipeline=p,source=s,stage=send 1",
                "deltaforge_wal_feedback_delay_observations_total pipeline=p,source=s,stage=refresh 1",
                "deltaforge_wal_feedback_delay_observations_total pipeline=p,source=s,stage=send 1",
                "deltaforge_wal_feedback_delay_seconds pipeline=p,source=s,stage=refresh [0.25]",
                "deltaforge_wal_feedback_delay_seconds pipeline=p,source=s,stage=send [0.0]",
            ]
        );
    }
}
