//! The Kafka side of the verifier: one consumer group per run, subscribed to
//! the topics of the clusters in the run, from the earliest offset. The
//! run's topics are created before any pipeline exists, and no write starts
//! before the verifier holds every partition of them.

use std::collections::{BTreeMap, HashMap, VecDeque};
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

use crate::avro::{Registry, Slot, frame_id};
use crate::topology::Naming;
use crate::verify::{Event, Verifier, decode, decode_value};

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
    observed: std::sync::Arc<std::sync::atomic::AtomicU64>,
    mut registry: Option<Registry>,
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
    // Messages behind one whose writer schema is still being fetched wait
    // here, in arrival order (so in partition order).
    let mut queue: VecDeque<Queued> = VecDeque::new();
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
        while let Some(q) = queue.front() {
            let Some(event) = event_of(
                registry.as_ref(),
                verifier.naming(),
                q.server,
                &q.payload,
            ) else {
                break;
            };
            let q = queue.pop_front().expect("front");
            verifier.observe_event(
                q.server,
                q.partition,
                q.offset,
                q.key.as_deref(),
                event,
                Utc::now().timestamp_micros(),
            )?;
            observed.store(
                verifier.report.row_events,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        let fetching = registry.as_ref().is_some_and(Registry::in_flight);
        let wait = if stopped_at.is_some() {
            drain.idle
        } else {
            Duration::from_secs(1)
        };
        tokio::select! {
            msg = tokio::time::timeout(wait, consumer.recv()), if queue.len() < QUEUED => match msg {
                Ok(Ok(m)) => {
                    let Some(&server) = topic_to_server.get(m.topic()) else { continue };
                    let payload = m.payload().unwrap_or_default();
                    if let Some(r) = registry.as_mut()
                        && let Some(id) = frame_id(payload)
                    {
                        r.request(id);
                    }
                    let event = if queue.is_empty() {
                        event_of(registry.as_ref(), verifier.naming(), server, payload)
                    } else {
                        None
                    };
                    let Some(event) = event else {
                        queue.push_back(Queued {
                            server,
                            partition: m.partition(),
                            offset: m.offset(),
                            key: m.key().map(<[u8]>::to_vec),
                            payload: payload.to_vec(),
                        });
                        continue;
                    };
                    verifier.observe_event(
                        server,
                        m.partition(),
                        m.offset(),
                        m.key(),
                        event,
                        Utc::now().timestamp_micros(),
                    )?;
                    observed.store(
                        verifier.report.row_events,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                Ok(Err(e)) => eprintln!("  verifier: kafka error: {e}"),
                // Idle after stop, with nothing left to decode.
                Err(_) if stopped_at.is_some() && queue.is_empty() && !fetching => break,
                Err(_) => {}
            },
            () = async { registry.as_mut().expect("fetching").arrive().await }, if fetching => {}
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

/// Messages held while an earlier message's writer schema is fetched.
const QUEUED: usize = 100_000;

/// A consumed message waiting for its turn.
struct Queued {
    server: u16,
    partition: i32,
    offset: i64,
    key: Option<Vec<u8>>,
    payload: Vec<u8>,
}

/// The decoded event of `payload`, or `None` while its writer schema is
/// being fetched. A framed payload without a registry is read as JSON (and
/// fails as such).
fn event_of(
    registry: Option<&Registry>,
    naming: &Naming,
    server: u16,
    payload: &[u8],
) -> Option<Result<Event>> {
    let (Some(r), Some(id)) = (registry, frame_id(payload)) else {
        return Some(decode(naming, server, payload));
    };
    match r.slot(id) {
        Slot::Ready(d) => Some(
            r.decode(d, &payload[5..])
                .and_then(|v| decode_value(naming, server, v)),
        ),
        Slot::Failed(e) => {
            Some(Err(anyhow::anyhow!("writer schema {id}: {e}")))
        }
        Slot::Pending | Slot::Unknown => None,
    }
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
