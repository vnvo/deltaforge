use anyhow::{Ok, Result};
use async_trait::async_trait;
use chrono::Utc;
use deltaforge_core::{Event, Op, Source, SourceMeta};
use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;

pub struct PostgresSource {
    pub id: String,
    pub dsn: String,
    pub tenant: String,
    pub tables: Vec<String>,
    pub publication: Option<String>,
    pub slot: Option<String>,
}

#[async_trait]
impl Source for PostgresSource {
    async fn run(&self, tx: mpsc::Sender<Event>) -> Result<()> {
        info!(source = %self.id,  "starting Postgres source (logical replication)");

        let meta = SourceMeta {
            kind: "postgres".into(),
            host: "localhost".into(),
            db: "orders".into(),
        };

        let mut i = 0u64;
        loop {
            i += 1;
            let ev = Event {
                event_id: Uuid::new_v4(),
                tenant_id: self.tenant.clone(),
                source: meta.clone(),
                table: self
                    .tables
                    .get(0)
                    .cloned()
                    .unwrap_or_else(|| "public.orders".into()),
                op: Op::Insert,
                tx_id: None,
                before: None,
                after: Some(serde_json::json!({"id": i, "note": "pg-poll"})),
                schema_version: None,
                ddl: None,
                timestamp: Utc::now(),
                trace_id: None,
                tags: None,
            };
            if tx.send(ev).await.is_err() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        }

        Ok(())
    }
}
