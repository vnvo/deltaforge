use anyhow::{Context, Ok, Result};
use async_trait::async_trait;
use deltaforge_core::{Event, Sink};
use tokio::time::{timeout, Duration};
use tracing::{debug, info};

pub struct RedisSink {
    client: redis::Client,
    stream: String,
}

impl RedisSink {
    pub fn new(uri: &str, stream: &str) -> Result<Self> {
        Ok(Self {
            client: redis::Client::open(uri).context("open redis uri")?,
            stream: stream.to_string(),
        })
    }
}

#[async_trait]
impl Sink for RedisSink {
    async fn send(&self, event: Event) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .context("redis connect")?;

        let payload = serde_json::to_string(&event)
            .context("serialize event for redis")?;
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
            //.expect("redis xadd fialed");
        })
        .await
        .map_err(|_| anyhow::anyhow!("redis send timeout"))?
        .map_err(|e| anyhow::anyhow!("redis xadd failed: {e}"))?;

        debug!(stream = %self.stream, "event sent to redis sink");
        Ok(())
    }
}
