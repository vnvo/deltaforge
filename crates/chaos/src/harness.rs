use anyhow::{Context, Result, bail};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::toxiproxy::ToxiproxyClient;

pub const KAFKA_BROKERS: &str = "localhost:9092";
pub const CHAOS_TOPIC: &str = "chaos.cdc";

pub struct Harness {
    pub toxi: ToxiproxyClient,
    pub port: u16,
}

impl Harness {
    pub fn new(port: u16) -> Self {
        Self {
            toxi: ToxiproxyClient::new(),
            port,
        }
    }

    /// Wait until DeltaForge health endpoint returns 200.
    pub async fn wait_for_deltaforge(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            match reqwest::get(format!("http://localhost:{}/health", self.port)).await {
                Ok(r) if r.status().is_success() => return Ok(()),
                _ => {}
            }
            if Instant::now() > deadline {
                bail!("timed out waiting for DeltaForge to become healthy");
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    /// Reset all Toxiproxy state before each scenario.
    pub async fn setup(&self) -> Result<()> {
        self.toxi.reset_all().await.context("toxiproxy reset")?;
        info!("harness: toxiproxy reset to clean state");
        Ok(())
    }

    /// Always called in scenario cleanup — ensures faults don't leak between runs.
    #[allow(dead_code)]
    pub async fn teardown(&self) {
        let _ = self.toxi.reset_all().await;
        info!("harness: teardown complete");
    }

    /// Return the current high-watermark offset for the chaos topic.
    /// This is instant — no consumer group assignment needed.
    pub async fn kafka_offset(&self) -> Result<u64> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", KAFKA_BROKERS)
            .set("group.id", "chaos-watermark")
            .create()
            .context("kafka consumer")?;

        // fetch_watermarks is blocking — run on a thread
        let topic = CHAOS_TOPIC;
        let result = tokio::task::spawn_blocking(move || {
            consumer.fetch_watermarks(
                topic,
                0,
                std::time::Duration::from_secs(5),
            )
        })
        .await?;

        match result {
            Ok((_, high)) => Ok(high as u64),
            // Topic doesn't exist yet (no events produced) — treat as offset 0.
            Err(rdkafka::error::KafkaError::MetadataFetch(
                rdkafka::types::RDKafkaErrorCode::UnknownPartition
                | rdkafka::types::RDKafkaErrorCode::UnknownTopicOrPartition,
            )) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    /// Fetch current source event counter from DeltaForge Prometheus metrics.
    #[allow(dead_code)]
    pub async fn source_event_count(&self) -> Result<f64> {
        let metrics_port = self.port - 8080 + 9000;
        let body = reqwest::get(format!("http://localhost:{}/metrics", metrics_port))
            .await?
            .text()
            .await?;
        parse_counter(&body, "deltaforge_source_events_total")
    }

    /// Fetch current reconnect counter.
    pub async fn reconnect_count(&self) -> Result<f64> {
        let metrics_port = self.port - 8080 + 9000;
        let body = reqwest::get(format!("http://localhost:{}/metrics", metrics_port))
            .await?
            .text()
            .await?;
        parse_counter(&body, "deltaforge_source_reconnects_total")
    }
}

/// Wait until any health endpoint returns 200 (URL configurable).
pub async fn wait_for_url(url: &str, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match reqwest::get(url).await {
            Ok(r) if r.status().is_success() => return Ok(()),
            _ => {}
        }
        if Instant::now() > deadline {
            bail!("timed out waiting for health at {url}");
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Count messages visible to a `read_committed` consumer for the chaos topic
/// (partition 0) from `start_offset` to the current end. This is the true
/// committed message count — uncommitted transactional messages are excluded.
pub async fn kafka_committed_count(start_offset: u64) -> Result<u64> {
    use rdkafka::Message;
    use rdkafka::TopicPartitionList;
    use rdkafka::consumer::StreamConsumer;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BROKERS)
        .set("group.id", format!("chaos-rc-{}", start_offset))
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .set("isolation.level", "read_committed")
        .create()
        .context("read_committed consumer")?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(
        CHAOS_TOPIC,
        0,
        rdkafka::Offset::Offset(start_offset as i64),
    )?;
    consumer.assign(&tpl)?;

    let mut count = 0u64;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            consumer.recv(),
        )
        .await
        {
            Ok(Ok(msg)) => {
                if msg.payload().is_some() {
                    count += 1;
                }
            }
            Ok(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break,
            Ok(Err(_)) => break,
            Err(_) => break, // timeout
        }
    }
    Ok(count)
}

/// Return the total high-watermark offset across all topics matching a prefix.
/// Used when the topic is a template like `chaos.avro.${source.table}`.
pub async fn kafka_offset_for_prefix(prefix: &str) -> Result<u64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BROKERS)
        .set("group.id", "chaos-watermark-prefix")
        .create()
        .context("kafka consumer")?;

    let prefix_owned = prefix.to_string();
    let result = tokio::task::spawn_blocking(move || {
        let metadata = consumer
            .fetch_metadata(None, std::time::Duration::from_secs(10))?;
        let mut total: u64 = 0;
        for topic in metadata.topics() {
            if topic.name().starts_with(&prefix_owned) {
                for partition in topic.partitions() {
                    if let Ok((_, high)) = consumer.fetch_watermarks(
                        topic.name(),
                        partition.id(),
                        std::time::Duration::from_secs(5),
                    ) {
                        total += high as u64;
                    }
                }
            }
        }
        Ok::<u64, rdkafka::error::KafkaError>(total)
    })
    .await??;

    Ok(result)
}

/// Return the high-watermark offset for any topic (partition 0).
pub async fn kafka_offset_for_topic(topic: &str) -> Result<u64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BROKERS)
        .set("group.id", "chaos-watermark")
        .create()
        .context("kafka consumer")?;
    let topic_owned = topic.to_string();
    let result = tokio::task::spawn_blocking(move || {
        consumer.fetch_watermarks(
            &topic_owned,
            0,
            std::time::Duration::from_secs(5),
        )
    })
    .await?;
    match result {
        Ok((_, high)) => Ok(high as u64),
        Err(rdkafka::error::KafkaError::MetadataFetch(
            rdkafka::types::RDKafkaErrorCode::UnknownPartition
            | rdkafka::types::RDKafkaErrorCode::UnknownTopicOrPartition,
        )) => Ok(0),
        Err(e) => Err(e.into()),
    }
}

/// Parse a simple counter value from a Prometheus text exposition.
fn parse_counter(body: &str, name: &str) -> Result<f64> {
    for line in body.lines() {
        if line.starts_with(name) && !line.starts_with('#') {
            let val = line
                .split_whitespace()
                .last()
                .context("missing value")?
                .parse::<f64>()
                .context("parse float")?;
            return Ok(val);
        }
    }
    Ok(0.0)
}

// ── Proxy bypass ─────────────────────────────────────────────────────────────

/// Address mapping for proxy bypass: (proxy_name, proxied, direct).
const PROXY_ADDRS: &[(&str, &str, &str)] = &[
    ("mysql", "toxiproxy:5100", "mysql:3306"),
    ("postgres", "toxiproxy:5101", "postgres:5432"),
    ("kafka", "toxiproxy:5102", "kafka:9094"),
];

/// DeltaForge instance ports to scan for pipelines.
const DF_PORTS: &[u16] = &[8080, 8081, 8082];

/// PATCH all running pipelines to use direct connections (bypass proxy)
/// or restore proxied connections. Returns the number of pipelines patched.
pub async fn set_proxy_bypass(bypass: bool) -> u32 {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();

    let mut patched = 0u32;
    for port in DF_PORTS {
        let Ok(resp) = client
            .get(format!("http://localhost:{port}/pipelines"))
            .send()
            .await
        else {
            continue;
        };
        let Ok(pipelines) = resp.json::<serde_json::Value>().await else {
            continue;
        };
        let Some(arr) = pipelines.as_array() else {
            continue;
        };

        for p in arr {
            let Some(name) = p["name"].as_str() else {
                continue;
            };
            let mut spec_patch = serde_json::Map::new();

            // Swap source DSN (spec is double-nested: p.spec.spec.source)
            if let Some(dsn) =
                p["spec"]["spec"]["source"]["config"]["dsn"].as_str()
            {
                for &(_, proxied, direct) in PROXY_ADDRS {
                    let (from, to) = if bypass {
                        (proxied, direct)
                    } else {
                        (direct, proxied)
                    };
                    if dsn.contains(from) {
                        let new_dsn = dsn.replace(from, to);
                        spec_patch.insert(
                            "source".into(),
                            serde_json::json!({"config": {"dsn": new_dsn}}),
                        );
                        break;
                    }
                }
            }

            // Swap sink brokers
            if let Some(sinks) = p["spec"]["spec"]["sinks"].as_array() {
                let mut new_sinks = Vec::new();
                let mut sink_changed = false;
                for s in sinks {
                    if let Some(brokers) = s["config"]["brokers"].as_str() {
                        for &(_, proxied, direct) in PROXY_ADDRS {
                            let (from, to) = if bypass {
                                (proxied, direct)
                            } else {
                                (direct, proxied)
                            };
                            if brokers.contains(from) {
                                new_sinks.push(serde_json::json!({"config": {"brokers": brokers.replace(from, to)}}));
                                sink_changed = true;
                                break;
                            }
                        }
                        if !sink_changed {
                            new_sinks.push(serde_json::json!({}));
                        }
                    } else {
                        new_sinks.push(serde_json::json!({}));
                    }
                }
                if sink_changed {
                    spec_patch
                        .insert("sinks".into(), serde_json::json!(new_sinks));
                }
            }

            if spec_patch.is_empty() {
                continue;
            }

            let body = serde_json::json!({"spec": spec_patch});
            if client
                .patch(format!("http://localhost:{port}/pipelines/{name}"))
                .json(&body)
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false)
            {
                patched += 1;
            }
        }
    }

    info!(
        bypass,
        patched,
        "proxy bypass: {} pipelines switched to {}",
        patched,
        if bypass { "direct" } else { "proxied" }
    );
    patched
}

/// Check if a pipeline is using proxied or direct connections by inspecting its DSN.
pub async fn connection_mode_summary(df_base: &str, pipeline: &str) -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();

    let Ok(resp) = client
        .get(format!("{df_base}/pipelines/{pipeline}"))
        .send()
        .await
    else {
        return "unknown".to_string();
    };
    let Ok(p) = resp.json::<serde_json::Value>().await else {
        return "unknown".to_string();
    };

    let mut parts = Vec::new();

    // Check source DSN (spec is double-nested: p.spec.spec.source)
    if let Some(dsn) = p["spec"]["spec"]["source"]["config"]["dsn"].as_str() {
        if dsn.contains("toxiproxy") {
            parts.push("source=proxied");
        } else {
            parts.push("source=direct");
        }
    }

    // Check sink brokers
    if let Some(sinks) = p["spec"]["spec"]["sinks"].as_array() {
        for s in sinks {
            if let Some(brokers) = s["config"]["brokers"].as_str() {
                if brokers.contains("toxiproxy") {
                    parts.push("sink=proxied");
                } else {
                    parts.push("sink=direct");
                }
                break;
            }
        }
    }

    if parts.is_empty() {
        "unknown".to_string()
    } else {
        parts.join(", ")
    }
}

/// Print a banner describing what a scenario does and what to expect.
pub fn print_scenario_banner(name: &str, description: &str, expects: &str) {
    info!("───────────────────────────────────────────────────");
    info!("  Scenario: {name}");
    info!("  {description}");
    info!("  Expected: {expects}");
    info!("───────────────────────────────────────────────────");
}

/// Scenario outcome — printed at the end of every run.
#[derive(Debug)]
pub struct ScenarioResult {
    pub name: String,
    pub passed: bool,
    pub notes: Vec<String>,
}

impl ScenarioResult {
    pub fn pass(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            passed: true,
            notes: vec![],
        }
    }

    pub fn fail(name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            passed: false,
            notes: vec![reason.into()],
        }
    }

    pub fn note(mut self, msg: impl Into<String>) -> Self {
        self.notes.push(msg.into());
        self
    }

    pub fn print(&self) {
        let status = if self.passed { "PASS ✓" } else { "FAIL ✗" };
        println!("\n[{status}] {}", self.name);
        for note in &self.notes {
            println!("       {note}");
        }
    }
}
