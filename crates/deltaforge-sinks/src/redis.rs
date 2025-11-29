use anyhow::Context;
use async_trait::async_trait;
use deltaforge_config::RedisSinkCfg;
use deltaforge_core::{Event, Sink, SinkError, SinkResult};
use tokio::time::{Duration, timeout};
use tracing::{debug, info};

pub struct RedisSink {
    id: String,
    client: redis::Client,
    stream: String,
}

impl RedisSink {
    pub fn new(cfg: &RedisSinkCfg) -> anyhow::Result<Self> {
        Ok(Self {
            id: cfg.id.clone(),
            client: redis::Client::open(cfg.uri.clone()).context("open redis uri")?,
            stream: cfg.stream.to_string(),
        })
    }
}

#[async_trait]
impl Sink for RedisSink {
    fn id(&self) -> &str {
        &self.id
    }

    async fn send(&self, event: Event) -> SinkResult<()> {
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
}
