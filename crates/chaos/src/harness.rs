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
}

impl Harness {
    pub fn new() -> Self {
        Self {
            toxi: ToxiproxyClient::new(),
        }
    }

    /// Wait until DeltaForge health endpoint returns 200.
    pub async fn wait_for_deltaforge(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            match reqwest::get("http://localhost:8080/health").await {
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
        let body = reqwest::get("http://localhost:9000/metrics")
            .await?
            .text()
            .await?;
        parse_counter(&body, "deltaforge_source_events_total")
    }

    /// Fetch current reconnect counter.
    pub async fn reconnect_count(&self) -> Result<f64> {
        let body = reqwest::get("http://localhost:9000/metrics")
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
