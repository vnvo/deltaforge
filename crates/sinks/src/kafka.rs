use anyhow::Context;
use async_trait::async_trait;
use deltaforge_config::KafkaSinkCfg;
use deltaforge_core::{Event, Sink, SinkError, SinkResult};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tracing::{debug, info, instrument};

pub struct KafkaSink {
    id: String,
    producer: FutureProducer,
    topic: String,
}

impl KafkaSink {
    #[instrument(skip_all)]
    pub fn new(ks_cfg: &KafkaSinkCfg) -> anyhow::Result<Self> {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", ks_cfg.brokers.clone())
            .set("client.id", "deltaforge-sink")
            .set("message.timeout.ms", "60000") // producer send timeout
            .set("socket.keepalive.enable", "true")
            .set("compression.type", "lz4")
            .set("linger.ms", "5") // tiny batch
            .set("delivery.timeout.ms", "120000") // overall delivery timeout (Kafka broker side)
            .set("request.timeout.ms", "30000")
            .set("retry.backoff.ms", "100");

        if ks_cfg.exactly_once == Some(true) {
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

        // apply user overrides, if any
        for (k, v) in &ks_cfg.client_conf {
            cfg.set(k, v);
        }

        let producer: FutureProducer =
            cfg.create().with_context(|| "creating kafka producer")?;

        info!(brokers=%ks_cfg.brokers, topic=%ks_cfg.topic, "kafka client connected", );
        Ok(Self {
            id: ks_cfg.id.clone(),
            producer,
            topic: ks_cfg.topic.clone(),
        })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    fn id(&self) -> &str {
        &self.id
    }

    async fn send(&self, event: Event) -> SinkResult<()> {
        let payload = serde_json::to_vec(&event)?;
        let key = event.idempotency_key();

        let _ = self
            .producer
            .send(
                FutureRecord::to(&self.topic).payload(&payload).key(&key),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _msg)| SinkError::Backpressure {
                details: format!("kafka send error: {e}").into(),
            })?;

        debug!(topic = %self.topic, "event sent to kafka sink");
        Ok(())
    }
}
