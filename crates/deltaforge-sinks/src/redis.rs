use anyhow::{Ok, Result};
use async_trait::async_trait;
use deltaforge_core::{Event, Sink};
use tracing::info;

pub struct RedisSink {
    client: redis::Client,
    stream: String,
}

impl RedisSink {
    pub fn new(uri: &str, stream: &str) -> Result<Self> {
        Ok(Self {
            client: redis::Client::open(uri)?,
            stream: stream.to_string(),
        })
    }
}

#[async_trait]
impl Sink for RedisSink {
    async fn send(&self, event: Event) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        let payload = serde_json::to_string(&event)?;
        let _: String = redis::cmd("XADD")
            .arg(&self.stream)
            .arg("*")
            .arg("df-event")
            .arg(payload)
            .query_async(&mut conn)
            .await?;

        info!(stream = %self.stream, "event send to redis sink");
        Ok(())
    }
}
