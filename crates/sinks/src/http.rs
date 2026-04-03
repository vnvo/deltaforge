//! HTTP/Webhook sink — delivers CDC events via HTTP POST/PUT to any URL.
//!
//! Supports dynamic URL templates, custom headers with env var expansion,
//! batch mode (JSON array), and retry with exponential backoff on 5xx/network errors.

use std::borrow::Cow;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use common::retry::{RetryOutcome, RetryPolicy, retry_async};
use common::routing::CompiledTemplate;
use deltaforge_config::EncodingCfg;
use deltaforge_config::HttpSinkCfg;
use deltaforge_core::encoding::EncodingType;
use deltaforge_core::encoding::avro::AvroEncoder;
use deltaforge_core::envelope::Envelope;
use deltaforge_core::{BatchResult, Event, Sink, SinkError, SinkResult};
use metrics::counter;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

/// Default timeout for individual HTTP requests.
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// Default timeout for batch operations.
const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_secs(30);
/// Default timeout for TCP connection establishment.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub struct HttpSink {
    id: String,
    pipeline: String,
    client: reqwest::Client,
    url: String,
    url_template: CompiledTemplate,
    method: reqwest::Method,
    headers: reqwest::header::HeaderMap,
    batch_mode: bool,
    envelope: Box<dyn Envelope>,
    encoding: EncodingType,
    avro_encoder: Option<AvroEncoder>,
    required: bool,
    send_timeout: Duration,
    batch_timeout: Duration,
    cancel: CancellationToken,
}

impl HttpSink {
    #[instrument(skip_all, fields(sink_id = %cfg.id, url = %cfg.url))]
    pub fn new(
        cfg: &HttpSinkCfg,
        cancel: CancellationToken,
        pipeline: &str,
    ) -> anyhow::Result<Self> {
        let connect_timeout = cfg
            .connect_timeout_secs
            .map(|s| Duration::from_secs(s as u64))
            .unwrap_or(DEFAULT_CONNECT_TIMEOUT);

        let send_timeout = cfg
            .send_timeout_secs
            .map(|s| Duration::from_secs(s as u64))
            .unwrap_or(DEFAULT_SEND_TIMEOUT);

        let batch_timeout = cfg
            .batch_timeout_secs
            .map(|s| Duration::from_secs(s as u64))
            .unwrap_or(DEFAULT_BATCH_TIMEOUT);

        // Build reqwest client with connection pooling and timeouts.
        let client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(send_timeout)
            .pool_max_idle_per_host(10)
            .build()
            .context("failed to build HTTP client")?;

        // Parse URL template.
        let url_template = CompiledTemplate::parse(&cfg.url)
            .map_err(|e| anyhow::anyhow!("invalid URL template: {e}"))?;

        if !url_template.is_static() {
            info!(
                id = %cfg.id,
                template = %cfg.url,
                "URL uses dynamic routing template"
            );
        }

        // Parse HTTP method.
        let method = cfg.method.parse::<reqwest::Method>().map_err(|e| {
            anyhow::anyhow!("invalid HTTP method '{}': {e}", cfg.method)
        })?;

        // Build static headers with env var expansion.
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );
        for (key, value) in &cfg.headers {
            // Expand ${ENV_VAR} in header values.
            let expanded =
                shellexpand::env(value).unwrap_or(Cow::Borrowed(value));
            let header_name =
                key.parse::<reqwest::header::HeaderName>().map_err(|e| {
                    anyhow::anyhow!("invalid header name '{key}': {e}")
                })?;
            let header_value = expanded
                .parse::<reqwest::header::HeaderValue>()
                .map_err(|e| {
                    anyhow::anyhow!("invalid header value for '{key}': {e}")
                })?;
            headers.insert(header_name, header_value);
        }

        let envelope_type = cfg.envelope.to_envelope_type();
        let encoding_type = cfg.encoding.to_encoding_type();

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

            Some(AvroEncoder::new(
                schema_registry_url,
                strategy,
                username.as_deref(),
                password.as_deref(),
            ).context("creating Avro encoder")?)
        } else {
            None
        };

        info!(
            url = %cfg.url,
            method = %cfg.method,
            batch_mode = cfg.batch_mode,
            headers = cfg.headers.len(),
            envelope = %envelope_type.name(),
            encoding = encoding_type.name(),
            send_timeout_ms = send_timeout.as_millis(),
            "http sink created"
        );

        Ok(Self {
            id: cfg.id.clone(),
            pipeline: pipeline.to_string(),
            client,
            url: cfg.url.clone(),
            url_template,
            method,
            headers,
            batch_mode: cfg.batch_mode,
            envelope: envelope_type.build(),
            encoding: encoding_type,
            avro_encoder,
            required: cfg.required.unwrap_or(true),
            send_timeout,
            batch_timeout,
            cancel,
        })
    }

    fn resolve_url(&self, event: &Event) -> SinkResult<String> {
        // Check event routing override.
        if let Some(t) =
            event.routing.as_ref().and_then(|r| r.effective_topic())
        {
            return Ok(t.to_string());
        }
        // Fast path: static URL.
        if self.url_template.is_static() {
            return Ok(self.url.clone());
        }
        // Resolve template against event JSON.
        let event_json = serde_json::to_value(event).map_err(|e| {
            SinkError::Serialization {
                details: e.to_string().into(),
            }
        })?;
        self.url_template.resolve_strict(&event_json).map_err(|e| {
            SinkError::Routing {
                details: e.to_string().into(),
            }
        })
    }

    fn serialize_event(&self, event: &Event) -> SinkResult<Vec<u8>> {
        // Outbox raw mode: write event.after directly.
        if event.routing.as_ref().is_some_and(|r| r.raw_payload) {
            return serde_json::to_vec(
                event.after.as_ref().unwrap_or(&Value::Null),
            )
            .map_err(Into::into);
        }
        // Wrap with envelope and encode.
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
        dest: &str,
    ) -> SinkResult<Vec<u8>> {
        let encoder = self.avro_encoder.as_ref().expect("avro_encoder must be set");

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
            .encode(dest, &envelope, None)
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
        dest: &str,
    ) -> SinkResult<Vec<u8>> {
        if self.avro_encoder.is_some() {
            self.serialize_event_avro(event, dest).await
        } else {
            self.serialize_event(event)
        }
    }

    /// Send a single HTTP request with the given body to the given URL.
    async fn do_send(
        &self,
        url: &str,
        body: Vec<u8>,
    ) -> Result<(), HttpRetryError> {
        let resp = self
            .client
            .request(self.method.clone(), url)
            .headers(self.headers.clone())
            .body(body)
            .send()
            .await
            .map_err(|e| {
                if e.is_connect() {
                    HttpRetryError::Connect(e.to_string())
                } else if e.is_timeout() {
                    HttpRetryError::Timeout
                } else {
                    HttpRetryError::Network(e.to_string())
                }
            })?;

        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }

        let body = resp.text().await.unwrap_or_default();
        let msg = format!("{status}: {body}");

        match status.as_u16() {
            401 | 403 => Err(HttpRetryError::Auth(msg)),
            408 | 429 => Err(HttpRetryError::Retryable(msg)),
            s if s >= 500 => Err(HttpRetryError::Retryable(msg)),
            _ => Err(HttpRetryError::Permanent(msg)),
        }
    }
}

#[async_trait]
impl Sink for HttpSink {
    fn id(&self) -> &str {
        &self.id
    }

    fn required(&self) -> bool {
        self.required
    }

    #[instrument(skip_all, fields(sink_id = %self.id))]
    async fn send(&self, event: &Event) -> SinkResult<()> {
        let url = self.resolve_url(event)?;
        let payload = self.encode_event(event, &url).await?;

        let policy = RetryPolicy::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
            0.2,
            Some(3),
        );

        let result = retry_async(
            |attempt| {
                let url = url.clone();
                let payload = payload.clone();
                async move {
                    debug!(attempt, url = %url, "sending event via HTTP");
                    self.do_send(&url, payload).await
                }
            },
            |e| e.is_retryable(),
            self.send_timeout,
            policy,
            &self.cancel,
            "http_send",
        )
        .await;

        match result {
            Ok(_) => {
                counter!(
                    "deltaforge_sink_bytes_total",
                    "pipeline" => self.pipeline.clone(),
                    "sink" => self.id.clone(),
                )
                .increment(payload.len() as u64);
                debug!("event sent via HTTP");
                Ok(())
            }
            Err(outcome) => Err(outcome_to_sink_error(outcome)),
        }
    }

    #[instrument(skip_all, fields(sink_id = %self.id, count = events.len()))]
    async fn send_batch(&self, events: &[Event]) -> SinkResult<BatchResult> {
        if events.is_empty() {
            return Ok(BatchResult::ok());
        }

        // Pre-serialize with resolved URLs.
        let mut serialized: Vec<(String, Vec<u8>)> =
            Vec::with_capacity(events.len());
        let mut dlq_failures: Vec<(usize, SinkError)> = Vec::new();

        for (i, e) in events.iter().enumerate() {
            let prepared = async {
                let url = self.resolve_url(e)?;
                let payload = self.encode_event(e, &url).await?;
                Ok::<_, SinkError>((url, payload))
            }.await;
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
            return Ok(BatchResult { dlq_failures });
        }

        if self.batch_mode {
            // Batch mode: combine all payloads into a JSON array.
            let url = &serialized[0].0;
            let array_body = {
                let items: Vec<Value> = serialized
                    .iter()
                    .filter_map(|(_, payload)| {
                        serde_json::from_slice(payload).ok()
                    })
                    .collect();
                serde_json::to_vec(&items).unwrap_or_default()
            };

            let policy = RetryPolicy::new(
                Duration::from_millis(200),
                Duration::from_secs(15),
                0.2,
                Some(3),
            );

            let url = url.clone();
            let event_count = serialized.len();
            let result = retry_async(
                |attempt| {
                    let url = url.clone();
                    let body = array_body.clone();
                    async move {
                        debug!(
                            attempt,
                            count = event_count,
                            "sending batch via HTTP"
                        );
                        self.do_send(&url, body).await
                    }
                },
                |e| e.is_retryable(),
                self.batch_timeout,
                policy,
                &self.cancel,
                "http_batch",
            )
            .await;

            match result {
                Ok(_) => {
                    let total_bytes: u64 =
                        serialized.iter().map(|(_, p)| p.len() as u64).sum();
                    counter!(
                        "deltaforge_sink_bytes_total",
                        "pipeline" => self.pipeline.clone(),
                        "sink" => self.id.clone(),
                    )
                    .increment(total_bytes);
                    debug!(
                        count = events.len(),
                        dlq_failures = dlq_failures.len(),
                        "batch sent via HTTP"
                    );
                    Ok(BatchResult { dlq_failures })
                }
                Err(outcome) => Err(outcome_to_sink_error(outcome)),
            }
        } else {
            // Per-event mode: send each event individually.
            let policy = RetryPolicy::new(
                Duration::from_millis(100),
                Duration::from_secs(10),
                0.2,
                Some(3),
            );

            for (url, payload) in &serialized {
                let url = url.clone();
                let payload = payload.clone();
                let result = retry_async(
                    |attempt| {
                        let url = url.clone();
                        let payload = payload.clone();
                        async move {
                            debug!(attempt, "sending event via HTTP");
                            self.do_send(&url, payload).await
                        }
                    },
                    |e| e.is_retryable(),
                    self.send_timeout,
                    policy.clone(),
                    &self.cancel,
                    "http_send",
                )
                .await;

                if let Err(outcome) = result {
                    return Err(outcome_to_sink_error(outcome));
                }
            }

            let total_bytes: u64 =
                serialized.iter().map(|(_, p)| p.len() as u64).sum();
            counter!(
                "deltaforge_sink_bytes_total",
                "pipeline" => self.pipeline.clone(),
                "sink" => self.id.clone(),
            )
            .increment(total_bytes);
            debug!(
                count = events.len(),
                dlq_failures = dlq_failures.len(),
                "batch sent via HTTP (per-event)"
            );
            Ok(BatchResult { dlq_failures })
        }
    }
}

// =============================================================================
// Error handling
// =============================================================================

#[derive(Debug, Clone)]
enum HttpRetryError {
    Connect(String),
    Timeout,
    Network(String),
    Auth(String),
    Retryable(String),
    Permanent(String),
}

impl HttpRetryError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Connect(_)
                | Self::Timeout
                | Self::Network(_)
                | Self::Retryable(_)
        )
    }
}

impl std::fmt::Display for HttpRetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(msg) => write!(f, "connection error: {msg}"),
            Self::Timeout => write!(f, "request timed out"),
            Self::Network(msg) => write!(f, "network error: {msg}"),
            Self::Auth(msg) => write!(f, "auth error: {msg}"),
            Self::Retryable(msg) => write!(f, "retryable error: {msg}"),
            Self::Permanent(msg) => write!(f, "permanent error: {msg}"),
        }
    }
}

fn outcome_to_sink_error(outcome: RetryOutcome<HttpRetryError>) -> SinkError {
    match outcome {
        RetryOutcome::Cancelled => {
            SinkError::Other(anyhow::anyhow!("operation cancelled"))
        }
        RetryOutcome::Timeout { action } => SinkError::Backpressure {
            details: format!("timeout: {action}").into(),
        },
        RetryOutcome::Exhausted {
            attempts,
            last_error,
        } => match &last_error {
            HttpRetryError::Auth(msg) => SinkError::Auth {
                details: msg.clone().into(),
            },
            _ => SinkError::Connect {
                details: format!(
                    "exhausted after {attempts} attempts: {last_error}"
                )
                .into(),
            },
        },
        RetryOutcome::Failed(e) => match &e {
            HttpRetryError::Auth(msg) => SinkError::Auth {
                details: msg.clone().into(),
            },
            HttpRetryError::Permanent(msg) => SinkError::Connect {
                details: msg.clone().into(),
            },
            _ => SinkError::Connect {
                details: e.to_string().into(),
            },
        },
    }
}
