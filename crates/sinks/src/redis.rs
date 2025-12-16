use anyhow::Context;
use async_trait::async_trait;
use deltaforge_config::RedisSinkCfg;
use deltaforge_core::{Event, Sink, SinkError, SinkResult};
use tokio::time::{Duration, timeout};
use tracing::debug;

pub struct RedisSink {
    id: String,
    pub client: redis::Client,
    stream: String,
}

impl RedisSink {
    pub fn new(cfg: &RedisSinkCfg) -> anyhow::Result<Self> {
        Ok(Self {
            id: cfg.id.clone(),
            client: redis::Client::open(cfg.uri.clone())
                .context("open redis uri")?,
            stream: cfg.stream.to_string(),
        })
    }
}

#[async_trait]
impl Sink for RedisSink {
    fn id(&self) -> &str {
        &self.id
    }

    async fn send(&self, event: &Event) -> SinkResult<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SinkError::Connect {
                details: format!("redis connect: {e}").into(),
            })?;

        let payload = serde_json::to_string(&event)?;
        let event_id = event.event_id.to_string();

        let _: String = timeout(Duration::from_secs(3), async {
            redis::cmd("XADD")
                .arg(&self.stream)
                .arg("*")
                .arg("event_id")
                .arg(&event_id)
                .arg("df-event")
                .arg(&payload)
                .query_async::<_>(&mut conn)
                .await
        })
        .await
        .map_err(|_| SinkError::Backpressure {
            details: "redis send timeout".into(),
        })?
        .map_err(|e| SinkError::Other(e.into()))?;

        debug!(stream = %self.stream, "event sent to redis sink");
        Ok(())
    }

    async fn send_batch(&self, events: &[Event]) -> SinkResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // pre-serialize all payloads
        // separates serialization errors from network errors
        let serialized: Vec<(String, String)> = events
            .iter()
            .map(|e| Ok((e.event_id.to_string(), serde_json::to_string(e)?)))
            .collect::<Result<Vec<_>, SinkError>>()?;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SinkError::Connect {
                details: format!("redis: {e}").into(),
            })?;

        // build pipeline - all commands batched into ONE round-trip
        // without pipeline: N commands = N round-trips = N * RTT
        // with pipeline:    N commands = 1 round-trip  = 1 * RTT
        let mut pipe = redis::pipe();

        for (event_id, payload) in &serialized {
            pipe.cmd("XADD")
                .arg(&self.stream)
                .arg("*") // Auto-generate stream entry ID
                .arg("event_id")
                .arg(event_id)
                .arg("df-event")
                .arg(payload)
                .ignore(); // Don't need individual return values
        }

        // execute pipeline - single network round-trip, fail fast
        // pipeline is atomic at network level, partial failure not possible
        timeout(Duration::from_secs(30), async {
            pipe.query_async::<()>(&mut conn).await
        })
        .await
        .map_err(|_| SinkError::Backpressure {
            details: "redis pipeline timeout".into(),
        })?
        .map_err(|e: redis::RedisError| SinkError::Other(e.into()))?;

        debug!(stream = %self.stream, count = events.len(), "pipeline executed");
        Ok(())
    }
}
