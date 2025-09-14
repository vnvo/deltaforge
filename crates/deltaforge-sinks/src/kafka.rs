use anyhow::{Ok, Result};
use async_trait::async_trait;
use deltaforge_core::{Event, Sink};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tracing::info;

pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
}

impl KafkaSink {
    pub fn new(brokers: &str, topic: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self {
            producer: producer,
            topic: topic.into(),
        })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn send(&self, event: Event) -> Result<()> {
        let payload = serde_json::to_vec(&event)?;
        let key = event.idempotency_key();
        let _ = self
            .producer
            .send(
                FutureRecord::to(&self.topic).payload(&payload).key(&key),
                Duration::from_secs(5),
            )
            .await;

        info!(topic = %self.topic, "event sent to kafka sink");
        Ok(())
    }
}
