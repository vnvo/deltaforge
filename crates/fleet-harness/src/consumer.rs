//! The Kafka side of the verifier: one consumer group per run, subscribed to
//! the topics of the clusters in the run, from the earliest offset. The
//! run's topics are created before any pipeline exists, and no write starts
//! before the verifier holds every partition of them.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::types::RDKafkaErrorCode;
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

/// Create `topics` (an existing one is kept) with `partitions` each (`None`:
/// the broker's default), then wait until each has metadata with a leader
/// for every partition. Returns each topic's partition count.
pub async fn ensure_topics(
    brokers: &str,
    topics: &[String],
    partitions: Option<i32>,
    timeout: Duration,
) -> Result<BTreeMap<String, usize>> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create the kafka admin client")?;
    let new: Vec<NewTopic<'_>> = topics
        .iter()
        .map(|t| {
            NewTopic::new(
                t,
                partitions.unwrap_or(-1),
                TopicReplication::Fixed(-1),
            )
        })
        .collect();
    let opts = AdminOptions::new().operation_timeout(Some(timeout));
    for r in admin.create_topics(&new, &opts).await? {
        match r {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((t, e)) => anyhow::bail!("create topic {t}: {e}"),
        }
    }
    let meta: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create the kafka metadata client")?;
    let deadline = Instant::now() + timeout;
    let mut counts = BTreeMap::new();
    for t in topics {
        loop {
            let ready = meta
                .fetch_metadata(Some(t), Duration::from_secs(5))
                .ok()
                .and_then(|m| {
                    let topic = m.topics().iter().find(|x| x.name() == t)?;
                    let ps = topic.partitions();
                    (topic.error().is_none()
                        && !ps.is_empty()
                        && ps.iter().all(|p| p.leader() >= 0))
                    .then_some(ps.len())
                });
            if let Some(n) = ready {
                counts.insert(t.clone(), n);
                break;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "topic {t} has no complete metadata after {timeout:?}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    Ok(counts)
}

/// Consume until stopped and drained, feeding `verifier`. `ready` turns
/// true once the consumer is assigned every partition in `expected`.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    brokers: &str,
    group: &str,
    pattern: &str,
    topic_to_server: HashMap<String, u16>,
    mut verifier: Verifier,
    mut stop: watch::Receiver<bool>,
    drain: Drain,
    expected: BTreeMap<String, usize>,
    ready: watch::Sender<bool>,
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
    let mut assigned = false;
    loop {
        if !assigned && let Ok(list) = consumer.assignment() {
            let mut held: BTreeMap<String, usize> = BTreeMap::new();
            for e in list.elements() {
                *held.entry(e.topic().to_string()).or_default() += 1;
            }
            if expected.iter().all(|(t, n)| held.get(t) >= Some(n)) {
                assigned = true;
                ready.send(true).ok();
            }
        }
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
