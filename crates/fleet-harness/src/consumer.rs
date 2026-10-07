//! The Kafka side of the verifier: one consumer group per run, subscribed to
//! the topics of the clusters in the run, from the earliest offset.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::sync::watch;

use crate::verify::Verifier;

/// How the consumer ends after the stop signal.
#[derive(Debug, Clone, Copy)]
pub struct Drain {
    /// Stop once no message arrived for this long.
    pub idle: Duration,
    /// And never later than this after the stop signal.
    pub max: Duration,
}

/// `^<prefix>\.(name1|name2|...)$`, for a regex subscription that also picks
/// up topics created after the subscription.
pub fn topic_pattern(prefix: &str, servers: &[String]) -> String {
    let escape = |s: &str| s.replace('.', "\\.");
    format!(
        "^{}\\.({})$",
        escape(prefix),
        servers
            .iter()
            .map(|s| escape(s))
            .collect::<Vec<_>>()
            .join("|")
    )
}

/// Consume until stopped and drained, feeding `verifier`.
pub async fn run(
    brokers: &str,
    group: &str,
    pattern: &str,
    topic_to_server: HashMap<String, u16>,
    mut verifier: Verifier,
    mut stop: watch::Receiver<bool>,
    drain: Drain,
) -> Result<Verifier> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("topic.metadata.refresh.interval.ms", "2000")
        .set("fetch.max.bytes", "52428800")
        .create()
        .context("create the verifying consumer")?;
    consumer
        .subscribe(&[pattern])
        .context("subscribe the verifying consumer")?;
    let mut stopped_at: Option<tokio::time::Instant> = None;
    loop {
        if stopped_at.is_none() && *stop.borrow() {
            stopped_at = Some(tokio::time::Instant::now());
        }
        let wait = if stopped_at.is_some() {
            drain.idle
        } else {
            Duration::from_secs(1)
        };
        tokio::select! {
            msg = tokio::time::timeout(wait, consumer.recv()) => match msg {
                Ok(Ok(m)) => {
                    let Some(&server) = topic_to_server.get(m.topic()) else { continue };
                    verifier.observe(
                        server,
                        m.partition(),
                        m.offset(),
                        m.key(),
                        m.payload().unwrap_or_default(),
                        Utc::now().timestamp_micros(),
                    )?;
                }
                Ok(Err(e)) => eprintln!("  verifier: kafka error: {e}"),
                Err(_) if stopped_at.is_some() => break, // idle after stop
                Err(_) => {}
            },
            _ = stop.changed(), if stopped_at.is_none() => {}
        }
        if let Some(at) = stopped_at
            && at.elapsed() >= drain.max
        {
            break;
        }
    }
    Ok(verifier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pattern_matches_exactly_the_run_topics() {
        let p = topic_pattern("fleet", &["c01".into(), "c02".into()]);
        assert_eq!(p, "^fleet\\.(c01|c02)$");
    }
}
