use anyhow::{Context, Ok, Result};
use async_trait::async_trait;
use deltaforge_core::{Event, Sink};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tracing::{debug, info, instrument};

pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
}

impl KafkaSink {
    #[instrument(skip_all)]
    pub fn new(brokers: &str, topic: &str, exactly_once: bool) -> Result<Self> {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", brokers) 
            .set("client.id", "deltaforge-sink")
            .set("message.timeout.ms", "60000") // producer send timeout
            .set("socket.keepalive.enable", "true")
            .set("compression.type", "lz4")
            .set("linger.ms", "5") // tiny batch
            .set("delivery.timeout.ms", "120000") // overall delivery timeout (Kafka broker side)
            .set("request.timeout.ms", "30000")
            .set("retry.backoff.ms", "100");

        if exactly_once {
            cfg.set("enable.idempotence", "true")
                .set("acks", "all")
                .set("retries", "1000000") // librdkafka will cap appropriately
                .set("max.in.flight.requests.per.connection", "5");
        } else {
            cfg.set("enable.idempotence", "true")
                .set("acks", "all")
                .set("retries", "10")
                .set("max.in.flight.requests.per.connection", "5");
        }

        let producer: FutureProducer =
            cfg.create().with_context(|| "creating kafka producer")?;

        info!(brokers=%brokers, topic=%topic, "kafka client connected", );
        Ok(Self {
            producer,
            topic: topic.into(),
        })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn send(&self, event: Event) -> Result<()> {
        let payload =
            serde_json::to_vec(&event).context("serialize event to json")?;
        let key = event.idempotency_key();

        let _ = self
            .producer
            .send(
                FutureRecord::to(&self.topic).payload(&payload).key(&key),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _msg)| anyhow::anyhow!("kafka send error: {e}"))?;

        debug!(topic = %self.topic, "event sent to kafka sink");
        Ok(())
    }
}
