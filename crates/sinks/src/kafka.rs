//! Kafka sink implementation.
//!
//! This sink writes CDC events to Kafka topics using rdkafka with support
//! for idempotent production and optional exactly-once semantics.
//!
//! # Features
//!
//! - **Idempotent producer**: Enabled by default for at-least-once delivery
//! - **Exactly-once semantics**: Optional transactional mode
//! - **Batching**: Leverages rdkafka's internal batching with concurrent delivery awaits
//! - **Compression**: LZ4 compression by default
//! - **Graceful shutdown**: Respects cancellation tokens
//! - **Configurable envelope**: Native, Debezium, or CloudEvents
//! - **Configurable encoding**: JSON (Avro/Protobuf planned)
//!
//! # Configuration
//!
//! ```yaml
//! sinks:
//!   - kafka:
//!       id: kafka-events
//!       brokers: localhost:9092
//!       topic: deltaforge-events
//!       envelope: debezium          # native | debezium | cloudevents
//!       encoding: json              # json (avro, protobuf planned)
//!       exactly_once: false
//!       required: true
//!       send_timeout_secs: 30
//!       client_conf:
//!         security.protocol: SASL_SSL
//!         sasl.mechanism: PLAIN
//! ```

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use common::CompiledTemplate;
use common::{RetryOutcome, RetryPolicy, retry_async};
use deltaforge_config::EncodingCfg;
use deltaforge_config::KafkaSinkCfg;
use deltaforge_core::encoding::EncodingType;
use deltaforge_core::encoding::avro::{AvroEncoder, SourceSchemaProvider};
use deltaforge_core::envelope::Envelope;
use deltaforge_core::{BatchResult, Event, Sink, SinkError, SinkResult};
use futures::future::try_join_all;
use metrics::{counter, gauge};
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Statistics};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

/// Default timeout for individual send operations.
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// Statistics interval for rdkafka broker metrics.
const STATS_INTERVAL_MS: &str = "5000";

// =============================================================================
// rdkafka metrics context
// =============================================================================

/// Custom rdkafka ClientContext that emits broker-level stats as Prometheus metrics.
struct KafkaMetricsContext {
    pipeline: String,
    sink_id: String,
}

impl ClientContext for KafkaMetricsContext {
    fn stats(&self, statistics: Statistics) {
        let (total_retries, total_connects) =
            statistics.brokers.values().fold((0u64, 0u64), |acc, b| {
                (acc.0 + b.txretries, acc.1 + b.connects.unwrap_or(0) as u64)
            });

        counter!(
            "deltaforge_kafka_produce_retries_total",
            "pipeline" => self.pipeline.clone(),
            "sink" => self.sink_id.clone(),
        )
        .absolute(total_retries);

        counter!(
            "deltaforge_sink_reconnects_total",
            "pipeline" => self.pipeline.clone(),
            "sink" => self.sink_id.clone(),
        )
        .absolute(total_connects);

        gauge!(
            "deltaforge_kafka_producer_queue_messages",
            "pipeline" => self.pipeline.clone(),
            "sink" => self.sink_id.clone(),
        )
        .set(statistics.msg_cnt as f64);

        gauge!(
            "deltaforge_kafka_producer_queue_bytes",
            "pipeline" => self.pipeline.clone(),
            "sink" => self.sink_id.clone(),
        )
        .set(statistics.msg_size as f64);
    }
}

/// Default linger time for batching (ms).
/// Keep at 5ms: the coordinator enqueues entire batches (hundreds–thousands of
/// messages) in one burst, so rdkafka batches naturally without needing a long
/// linger wait. Higher values (20ms+) bottleneck throughput on small batches.
const DEFAULT_LINGER_MS: &str = "5";
const DEFAULT_BATCH_SIZE: &str = "1048576"; // 1 MB — rdkafka default (16 KB) is too small for high throughput

/// Kafka sink with idempotent production and optional exactly-once semantics.
pub struct KafkaSink {
    id: String,
    pipeline: String,
    cfg: KafkaSinkCfg,
    producer: FutureProducer<KafkaMetricsContext>,

    /// static fallback (kept for fast path)
    topic: String,

    topic_template: CompiledTemplate,
    key_template: Option<CompiledTemplate>,

    /// Envelope for wrapping events (Native, Debezium, CloudEvents)
    envelope: Box<dyn Envelope>,

    /// Encoding type (used for content-type headers)
    #[allow(dead_code)]
    encoding: EncodingType,

    /// Avro encoder (present when encoding is Avro)
    avro_encoder: Option<AvroEncoder>,

    /// Cancellation token for graceful shutdown.
    cancel: CancellationToken,

    /// Per-message send timeout.
    send_timeout: Duration,

    /// Whether this sink uses Kafka transactions for exactly-once delivery.
    transactional: bool,
}

impl KafkaSink {
    /// Create a new Kafka sink.
    ///
    /// # Arguments
    ///
    /// * `cfg` - Kafka sink configuration
    /// * `cancel` - Cancellation token for graceful shutdown
    ///
    /// # Producer Configuration
    ///
    /// The producer is configured with sensible defaults:
    /// - Idempotent production (prevents duplicates on retry)
    /// - LZ4 compression
    /// - 5ms linger for micro-batching
    /// - Appropriate timeouts for reliability
    ///
    /// User-provided `client_conf` overrides are applied last.
    #[instrument(skip_all, fields(sink_id = %cfg.id, topic = %cfg.topic))]
    pub fn new(
        cfg: &KafkaSinkCfg,
        cancel: CancellationToken,
        pipeline: &str,
        source_schemas: Option<Arc<dyn SourceSchemaProvider>>,
    ) -> anyhow::Result<Self> {
        let mut client_cfg = ClientConfig::new();

        // Core connection settings
        client_cfg
            .set("bootstrap.servers", &cfg.brokers)
            .set("client.id", format!("deltaforge-{}", cfg.id))
            .set("socket.keepalive.enable", "true");

        // Batching and compression
        client_cfg
            .set("compression.type", "lz4")
            .set("linger.ms", DEFAULT_LINGER_MS)
            .set("batch.size", DEFAULT_BATCH_SIZE);

        // Reliability settings
        let transactional = cfg.exactly_once == Some(true);

        // Timeout configuration.
        // When transactional, all timeouts must be <= transaction.timeout.ms
        // (default 60000ms). Use shorter timeouts to stay within the bound.
        if transactional {
            client_cfg
                .set("transaction.timeout.ms", "60000")
                .set("message.timeout.ms", "30000")
                .set("delivery.timeout.ms", "30000")
                .set("request.timeout.ms", "15000")
                .set("retry.backoff.ms", "100");
        } else {
            client_cfg
                .set("message.timeout.ms", "60000")
                .set("delivery.timeout.ms", "120000")
                .set("request.timeout.ms", "30000")
                .set("retry.backoff.ms", "100");
        }
        if transactional {
            // Exactly-once: transactional semantics.
            // transactional.id must be stable across restarts to allow the
            // broker to fence zombie producers from a previous incarnation.
            let txn_id = format!("deltaforge-{}-{}", pipeline, cfg.id);
            client_cfg
                .set("transactional.id", &txn_id)
                .set("enable.idempotence", "true")
                .set("acks", "all")
                .set("retries", "1000000") // rdkafka caps appropriately
                .set("max.in.flight.requests.per.connection", "5");
        } else {
            // At-least-once: idempotent production
            client_cfg
                .set("enable.idempotence", "true")
                .set("acks", "all")
                .set("retries", "10")
                .set("max.in.flight.requests.per.connection", "5");
        }

        // Enable rdkafka statistics for broker-level metrics
        client_cfg.set("statistics.interval.ms", STATS_INTERVAL_MS);

        // Apply user overrides last
        for (k, v) in &cfg.client_conf {
            client_cfg.set(k, v);
        }

        let context = KafkaMetricsContext {
            pipeline: pipeline.to_string(),
            sink_id: cfg.id.clone(),
        };
        let producer: FutureProducer<KafkaMetricsContext> =
            client_cfg.create_with_context(context).with_context(|| {
                format!(
                    "creating kafka producer for {}",
                    redact_brokers(&cfg.brokers)
                )
            })?;

        let send_timeout = cfg
            .send_timeout_secs
            .map(|s| Duration::from_secs(s as u64))
            .unwrap_or(DEFAULT_SEND_TIMEOUT);

        // Build envelope and encoding from config
        let envelope_type = cfg.envelope.to_envelope_type();
        let encoding_type = cfg.encoding.to_encoding_type();

        // Build Avro encoder if configured
        let avro_encoder = if let EncodingCfg::Avro {
            ref schema_registry_url,
            ref subject_strategy,
            ref username,
            ref password,
        } = cfg.encoding
        {
            use deltaforge_config::SubjectStrategy as CfgStrategy;
            use deltaforge_core::encoding::avro::SubjectStrategy as CoreStrategy;

            let strategy = match subject_strategy {
                CfgStrategy::TopicName => CoreStrategy::TopicName,
                CfgStrategy::RecordName => CoreStrategy::RecordName,
                CfgStrategy::TopicRecordName => CoreStrategy::TopicRecordName,
            };

            Some(
                AvroEncoder::with_source_schemas(
                    schema_registry_url,
                    strategy,
                    username.as_deref(),
                    password.as_deref(),
                    source_schemas,
                )
                .context("creating Avro encoder")?,
            )
        } else {
            None
        };

        // routing templates
        let topic_template = CompiledTemplate::parse(&cfg.topic)
            .map_err(|e| anyhow::anyhow!("invalid topic template: {}", e))?;

        let key_template = cfg
            .key
            .as_ref()
            .map(|k| CompiledTemplate::parse(k))
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid key template: {}", e))?;

        if !topic_template.is_static() {
            info!(
                template = %cfg.topic,
                "topic uses dynamic routing template"
            );
        }

        // Initialize transactions if exactly-once is enabled.
        // Retry with backoff because the broker may not be ready yet during startup.
        if transactional {
            let mut retry_policy = common::retry::RetryPolicy::new(
                Duration::from_secs(1),
                Duration::from_secs(30),
                0.2,
                Some(10),
            );
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                match producer
                    .init_transactions(std::time::Duration::from_secs(30))
                {
                    Ok(()) => {
                        info!(sink_id = %cfg.id, "kafka transactions initialized");
                        break;
                    }
                    Err(e) => {
                        if !retry_policy.should_retry(attempt) {
                            anyhow::bail!(
                                "init_transactions failed after retries for sink {} — \
                                 is the Kafka broker reachable? Last error: {e}",
                                cfg.id
                            );
                        }
                        let delay = retry_policy.next_backoff();
                        warn!(
                            sink_id = %cfg.id,
                            error = %e,
                            delay_ms = delay.as_millis(),
                            "init_transactions failed, retrying"
                        );
                        // Blocking sleep is acceptable here — this is sync construction
                        // and runs once at pipeline startup, not on the hot path.
                        std::thread::sleep(delay);
                    }
                }
            }
        }

        info!(
            brokers = %redact_brokers(&cfg.brokers),
            topic = %cfg.topic,
            envelope = %envelope_type.name(),
            encoding = encoding_type.name(),
            exactly_once = transactional,
            send_timeout_ms = send_timeout.as_millis(),
            "kafka sink created"
        );

        Ok(Self {
            id: cfg.id.clone(),
            pipeline: pipeline.to_string(),
            cfg: cfg.clone(),
            producer,
            topic: cfg.topic.clone(),
            topic_template,
            key_template,
            envelope: envelope_type.build(),
            encoding: encoding_type,
            avro_encoder,
            cancel,
            send_timeout,
            transactional,
        })
    }

    /// Resolve topic, strict. Returns Err if template produces empty/invalid topic.
    fn resolve_topic(&self, event: &Event) -> SinkResult<String> {
        // 1. Check explicit routing override (empty string = no override)
        if let Some(t) =
            event.routing.as_ref().and_then(|r| r.effective_topic())
        {
            return Ok(t.to_string());
        }

        // 2. Resolve template (or return static)
        if self.topic_template.is_static() {
            return Ok(self.topic.clone());
        }

        let event_json = serde_json::to_value(event).map_err(|e| {
            SinkError::Serialization {
                details: e.to_string().into(),
            }
        })?;

        let resolved = self
            .topic_template
            .resolve_strict(&event_json)
            .map_err(|e| SinkError::Routing {
                details: e.to_string().into(),
            })?;

        if resolved.is_empty() {
            return Err(SinkError::Routing {
                details: format!(
                    "topic template '{}' resolved to empty string",
                    self.topic_template.raw()
                )
                .into(),
            });
        }
        Ok(resolved)
    }

    /// Resolve key, lenient. Unresolvable = falls back to idempotency_key.
    fn resolve_key(&self, event: &Event) -> String {
        // 1. Check explicit routing override
        if let Some(k) = event.routing.as_ref().and_then(|r| r.key.as_deref()) {
            return k.to_string();
        }

        // 2. Resolve key template if configured
        if let Some(ref tmpl) = self.key_template {
            if tmpl.is_static() {
                return tmpl.resolve_lenient(&serde_json::Value::Null);
            }
            if let Ok(event_json) = serde_json::to_value(event) {
                return tmpl.resolve_lenient(&event_json);
            }
        }

        // 3. Default to idempotency key
        event.idempotency_key()
    }

    /// Build rdkafka OwnedHeaders from routing.headers.
    fn build_headers(&self, event: &Event) -> Option<OwnedHeaders> {
        let map = event.routing.as_ref()?.headers.as_ref()?;
        if map.is_empty() {
            return None;
        }
        let mut headers = OwnedHeaders::new();
        for (k, v) in map {
            headers = headers.insert(Header {
                key: k,
                value: Some(v.as_bytes()),
            });
        }
        Some(headers)
    }

    /// Serialize event using configured envelope (JSON path only).
    fn serialize_event(&self, event: &Event) -> SinkResult<Vec<u8>> {
        if event.routing.as_ref().is_some_and(|r| r.raw_payload) {
            // Outbox raw mode: write event.after directly
            return serde_json::to_vec(
                event.after.as_ref().unwrap_or(&Value::Null),
            )
            .map_err(Into::into);
        }
        let envelope = self.envelope.wrap(event).map_err(|e| {
            SinkError::Serialization {
                details: e.to_string().into(),
            }
        })?;

        self.encoding
            .encode(&envelope)
            .map(|b| b.to_vec())
            .map_err(|e| SinkError::Serialization {
                details: e.to_string().into(),
            })
    }

    /// Serialize event using Avro encoding (async — requires Schema Registry).
    async fn serialize_event_avro(
        &self,
        event: &Event,
        topic: &str,
    ) -> SinkResult<Vec<u8>> {
        let encoder = self
            .avro_encoder
            .as_ref()
            .expect("avro_encoder must be set");

        if event.routing.as_ref().is_some_and(|r| r.raw_payload) {
            return serde_json::to_vec(
                event.after.as_ref().unwrap_or(&Value::Null),
            )
            .map_err(Into::into);
        }

        let envelope = self.envelope.wrap(event).map_err(|e| {
            SinkError::Serialization {
                details: e.to_string().into(),
            }
        })?;

        let bytes = encoder
            .encode_event(
                topic,
                &envelope,
                &event.source.connector,
                &event.source.db,
                &event.source.table,
            )
            .await
            .map_err(|e| SinkError::Serialization {
                details: e.to_string().into(),
            })?;

        Ok(bytes.to_vec())
    }

    /// Serialize event, choosing JSON or Avro path.
    async fn encode_event(
        &self,
        event: &Event,
        topic: &str,
    ) -> SinkResult<Vec<u8>> {
        if self.avro_encoder.is_some() {
            self.serialize_event_avro(event, topic).await
        } else {
            self.serialize_event(event)
        }
    }
}

#[async_trait]
impl Sink for KafkaSink {
    fn id(&self) -> &str {
        &self.id
    }

    fn required(&self) -> bool {
        self.cfg.required.unwrap_or(true)
    }

    #[instrument(skip_all, fields(sink_id = %self.id))]
    async fn send(&self, event: &Event) -> SinkResult<()> {
        // Transactional producers must wrap every send in begin/commit.
        // Delegate to send_batch which handles the transaction boundary.
        if self.transactional {
            return self
                .send_batch(std::slice::from_ref(event))
                .await
                .map(|_| ());
        }

        let topic = self.resolve_topic(event)?;
        let payload = self.encode_event(event, &topic).await?;
        let key = self.resolve_key(event);

        let policy = RetryPolicy::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
            0.2,
            Some(3),
        );

        let headers = self.build_headers(event);

        let result = retry_async(
            |attempt| {
                let payload = payload.clone();
                let key = key.clone();
                let topic = topic.clone();
                let headers = headers.clone();
                async move {
                    debug!(attempt, key = %key, topic = %topic, "sending to kafka");

                    let mut record = FutureRecord::to(&topic)
                        .payload(&payload)
                        .key(&key);

                    if let Some(ref h) = headers {
                        record = record.headers(h.clone());
                    }

                    self.producer
                        .send(record, Timeout::After(self.send_timeout))
                        .await
                        .map(|_| ())
                        .map_err(|(e, _msg)| KafkaRetryError::from(e))
                }
            },
            |e| e.is_retryable(),
            self.send_timeout,
            policy,
            &self.cancel,
            "kafka_send",
        )
        .await;

        match result {
            Ok(_) => {
                debug!(topic = %topic, "event sent to kafka");
                Ok(())
            }
            Err(outcome) => Err(outcome_to_sink_error(outcome)),
        }
    }

    /// Batch send for Kafka: queue all messages, then await all deliveries.
    ///
    /// This leverages rdkafka's internal batching by:
    /// 1. Pre-serializing all events (fail fast on serialization errors)
    /// 2. Enqueuing all messages to rdkafka's buffer
    /// 3. Awaiting all delivery futures concurrently
    ///
    /// The `linger.ms` setting controls how long rdkafka waits to batch
    /// messages before sending.
    #[instrument(skip_all, fields(sink_id = %self.id, count = events.len()))]
    async fn send_batch(&self, events: &[Event]) -> SinkResult<BatchResult> {
        if events.is_empty() {
            return Ok(BatchResult::ok());
        }

        // Pre-serialize with resolved topic/key/headers.
        // Per-event DLQ-eligible errors (serialization, routing) are collected
        // instead of failing the batch. Non-DLQ errors fail the batch as before.
        let mut serialized: Vec<(
            Vec<u8>,
            String,
            String,
            Option<OwnedHeaders>,
        )> = Vec::with_capacity(events.len());
        let mut dlq_failures: Vec<(usize, SinkError)> = Vec::new();

        for (i, e) in events.iter().enumerate() {
            let prepared = async {
                let topic = self.resolve_topic(e)?;
                let payload = self.encode_event(e, &topic).await?;
                let key = self.resolve_key(e);
                let headers = self.build_headers(e);
                Ok::<_, SinkError>((payload, topic, key, headers))
            }
            .await;
            match prepared {
                Ok(prepared) => serialized.push(prepared),
                Err(e) if e.is_dlq_eligible() => {
                    counter!(
                        "deltaforge_sink_routing_errors_total",
                        "pipeline" => self.pipeline.clone(),
                        "sink" => self.id.clone(),
                        "kind" => e.kind(),
                    )
                    .increment(1);
                    dlq_failures.push((i, e));
                }
                Err(e) => {
                    counter!(
                        "deltaforge_sink_routing_errors_total",
                        "pipeline" => self.pipeline.clone(),
                        "sink" => self.id.clone(),
                        "kind" => e.kind(),
                    )
                    .increment(1);
                    return Err(e);
                }
            }
        }

        if serialized.is_empty() {
            // All events failed preparation — nothing to send.
            return Ok(BatchResult { dlq_failures });
        }

        // Begin transaction if exactly-once is enabled.
        if self.transactional {
            if let Err(e) = self.producer.begin_transaction() {
                let is_fatal =
                    matches!(&e, KafkaError::Transaction(re) if re.is_fatal());
                if is_fatal {
                    // ProducerFenced or other fatal error — cannot recover.
                    // The pipeline must stop; retrying is pointless.
                    return Err(SinkError::Fatal {
                        details: format!("begin_transaction fatal: {e}").into(),
                    });
                }
                return Err(SinkError::Connect {
                    details: format!("begin_transaction failed: {e}").into(),
                });
            }
        }

        // Enqueue all messages
        let futures: Vec<_> = serialized
            .iter()
            .map(|(payload, topic, key, headers)| {
                let mut record =
                    FutureRecord::to(topic).payload(payload).key(key);

                if let Some(h) = headers {
                    record = record.headers(h.clone());
                }

                self.producer
                    .send(record, Timeout::After(self.send_timeout))
            })
            .collect();

        // await all deliveries
        let result = try_join_all(futures.into_iter().map(|f| async move {
            f.await.map_err(|(e, _)| KafkaRetryError::from(e))
        }))
        .await;

        match result {
            Ok(_) => {
                // Commit transaction if exactly-once.
                if self.transactional {
                    if let Err(e) = self
                        .producer
                        .commit_transaction(std::time::Duration::from_secs(30))
                    {
                        let is_fatal = matches!(&e, KafkaError::Transaction(re) if re.is_fatal());
                        counter!(
                            "deltaforge_sink_txn_aborts_total",
                            "pipeline" => self.pipeline.clone(),
                            "sink" => self.id.clone(),
                        )
                        .increment(1);
                        if let Err(abort_err) = self.producer.abort_transaction(
                            std::time::Duration::from_secs(10),
                        ) {
                            warn!(error = %abort_err, "abort_transaction failed after commit failure");
                        }
                        return Err(if is_fatal {
                            SinkError::Fatal {
                                details: format!(
                                    "commit_transaction fatal: {e}"
                                )
                                .into(),
                            }
                        } else {
                            SinkError::Connect {
                                details: format!(
                                    "commit_transaction failed: {e}"
                                )
                                .into(),
                            }
                        });
                    }
                    counter!(
                        "deltaforge_sink_txn_commits_total",
                        "pipeline" => self.pipeline.clone(),
                        "sink" => self.id.clone(),
                    )
                    .increment(1);
                }

                let total_bytes: u64 =
                    serialized.iter().map(|(p, ..)| p.len() as u64).sum();
                counter!(
                    "deltaforge_sink_bytes_total",
                    "pipeline" => self.pipeline.clone(),
                    "sink" => self.id.clone(),
                )
                .increment(total_bytes);
                debug!(
                    count = events.len(),
                    dlq_failures = dlq_failures.len(),
                    transactional = self.transactional,
                    "batch sent to kafka"
                );
                Ok(BatchResult { dlq_failures })
            }
            Err(e) => {
                // Abort transaction on delivery failure.
                let mut fatal = false;
                if self.transactional {
                    counter!(
                        "deltaforge_sink_txn_aborts_total",
                        "pipeline" => self.pipeline.clone(),
                        "sink" => self.id.clone(),
                    )
                    .increment(1);
                    if let Err(abort_err) = self
                        .producer
                        .abort_transaction(std::time::Duration::from_secs(10))
                    {
                        warn!(error = %abort_err, "abort_transaction failed after delivery failure");
                        // If abort itself fails with a fatal/fenced error, the
                        // producer is permanently broken.
                        let msg = abort_err.to_string();
                        if msg.contains("fenced") || msg.contains("Fatal") {
                            fatal = true;
                        }
                    }
                }
                warn!(error = %e, "batch delivery failed");
                if fatal {
                    Err(SinkError::Fatal {
                        details: format!(
                            "producer fenced during delivery: {e}"
                        )
                        .into(),
                    })
                } else {
                    Err(SinkError::Backpressure {
                        details: format!("kafka batch error: {}", e).into(),
                    })
                }
            }
        }
    }
}

// =============================================================================
// Error Handling
// =============================================================================

/// Internal error type for retry classification.
#[derive(Debug, Clone)]
enum KafkaRetryError {
    /// Queue is full (backpressure).
    QueueFull,
    /// Message timed out.
    Timeout,
    /// Broker connection error.
    BrokerConnection(String),
    /// Authentication/authorization error.
    Auth(String),
    /// Message too large.
    MessageTooLarge,
    /// Other Kafka error.
    Other(String),
}

impl From<KafkaError> for KafkaRetryError {
    fn from(e: KafkaError) -> Self {
        match &e {
            KafkaError::MessageProduction(code) => match code {
                rdkafka::types::RDKafkaErrorCode::QueueFull => Self::QueueFull,
                rdkafka::types::RDKafkaErrorCode::MessageTimedOut => {
                    Self::Timeout
                }
                rdkafka::types::RDKafkaErrorCode::MessageSizeTooLarge => {
                    Self::MessageTooLarge
                }
                _ => Self::Other(e.to_string()),
            },
            KafkaError::ClientCreation(msg) => {
                if is_auth_error(msg) {
                    Self::Auth(msg.clone())
                } else {
                    Self::BrokerConnection(msg.clone())
                }
            }
            _ => {
                let msg = e.to_string();
                if is_auth_error(&msg) {
                    Self::Auth(msg)
                } else if msg.contains("timeout") || msg.contains("timed out") {
                    Self::Timeout
                } else {
                    Self::Other(msg)
                }
            }
        }
    }
}

impl std::fmt::Display for KafkaRetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueFull => write!(f, "producer queue full (backpressure)"),
            Self::Timeout => write!(f, "message timed out"),
            Self::BrokerConnection(msg) => {
                write!(f, "broker connection: {}", msg)
            }
            Self::Auth(msg) => write!(f, "authentication error: {}", msg),
            Self::MessageTooLarge => write!(f, "message too large"),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl KafkaRetryError {
    /// Determine if this error is retryable.
    fn is_retryable(&self) -> bool {
        match self {
            // Backpressure: retry after producer clears
            Self::QueueFull => true,
            // Timeouts are retryable
            Self::Timeout => true,
            // Broker connection issues are retryable
            Self::BrokerConnection(_) => true,
            // Auth errors are NOT retryable
            Self::Auth(_) => false,
            // Message too large is NOT retryable (won't change on retry)
            Self::MessageTooLarge => false,
            // Other errors: check message content
            Self::Other(msg) => {
                !is_auth_error(msg) && !msg.contains("too large")
            }
        }
    }
}

/// Check if an error message indicates an authentication error.
fn is_auth_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("authentication")
        || lower.contains("sasl")
        || lower.contains("unauthorized")
        || lower.contains("access denied")
        || lower.contains("permission denied")
}

/// Redact sensitive information from broker list for logging.
///
/// If brokers contain credentials (unlikely but possible in some setups),
/// this provides basic redaction.
fn redact_brokers(brokers: &str) -> String {
    // Most Kafka setups don't have credentials in broker string,
    // but handle the case where they might
    if brokers.contains('@') {
        // Has credentials, redact password portion
        brokers
            .split(',')
            .map(|b| {
                if let Some(at_idx) = b.find('@') {
                    if let Some(colon_idx) = b[..at_idx].rfind(':') {
                        format!("{}:***{}", &b[..colon_idx], &b[at_idx..])
                    } else {
                        b.to_string()
                    }
                } else {
                    b.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    } else {
        brokers.to_string()
    }
}

/// Convert a RetryOutcome to SinkError.
fn outcome_to_sink_error(outcome: RetryOutcome<KafkaRetryError>) -> SinkError {
    match outcome {
        RetryOutcome::Cancelled => {
            SinkError::Other(anyhow::anyhow!("operation cancelled"))
        }
        RetryOutcome::Timeout { action } => SinkError::Backpressure {
            details: format!("timeout: {}", action).into(),
        },
        RetryOutcome::Exhausted {
            attempts,
            last_error,
        } => {
            let details = format!(
                "exhausted after {} attempts: {}",
                attempts, last_error
            );
            match last_error {
                KafkaRetryError::QueueFull => SinkError::Backpressure {
                    details: details.into(),
                },
                _ => SinkError::Connect {
                    details: details.into(),
                },
            }
        }
        RetryOutcome::Failed(e) => match e {
            KafkaRetryError::Auth(msg) => SinkError::Connect {
                details: msg.into(),
            },
            KafkaRetryError::MessageTooLarge => {
                SinkError::Other(anyhow::anyhow!("message too large for Kafka"))
            }
            _ => SinkError::Connect {
                details: e.to_string().into(),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_errors_are_not_retryable() {
        let auth = KafkaRetryError::Auth("SASL authentication failed".into());
        assert!(!auth.is_retryable());

        let other_auth = KafkaRetryError::Other("authentication error".into());
        assert!(!other_auth.is_retryable());
    }

    #[test]
    fn transient_errors_are_retryable() {
        assert!(KafkaRetryError::QueueFull.is_retryable());
        assert!(KafkaRetryError::Timeout.is_retryable());
        assert!(
            KafkaRetryError::BrokerConnection("connection reset".into())
                .is_retryable()
        );
    }

    #[test]
    fn message_too_large_is_not_retryable() {
        assert!(!KafkaRetryError::MessageTooLarge.is_retryable());
    }

    #[test]
    fn broker_redaction_without_credentials() {
        let brokers = "broker1:9092,broker2:9092";
        assert_eq!(redact_brokers(brokers), brokers);
    }

    #[test]
    fn broker_redaction_with_credentials() {
        let brokers = "user:password@broker1:9092,user:secret@broker2:9092";
        let redacted = redact_brokers(brokers);
        assert!(!redacted.contains("password"));
        assert!(!redacted.contains("secret"));
        assert!(redacted.contains("***"));
    }
}
