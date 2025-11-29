use mysql_async::{Pool, Row, prelude::Queryable};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::mysql::mysql_helpers::redact_password;
use deltaforge_core::{SourceError, SourceResult};

/// INFORMATION_SCHEMA column-name cache, with logging and latency measurement.
#[derive(Clone)]
pub(super) struct MySqlSchemaCache {
    pool: Pool,
    map: Arc<RwLock<HashMap<(String, String), Arc<Vec<String>>>>>,
}

impl MySqlSchemaCache {
    pub(super) fn new(dsn: &str) -> Self {
        info!("creating mysql schema cache for {}", redact_password(dsn));
        Self {
            pool: Pool::new(dsn),
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub(super) async fn column_names(
        &self,
        db: &str,
        table: &str,
    ) -> SourceResult<Arc<Vec<String>>> {
        if let Some(found) = self
            .map
            .read()
            .await
            .get(&(db.to_string(), table.to_string()))
            .cloned()
        {
            debug!(db = %db, table = %table, "schema cache hit");
            return Ok(found);
        }
        let t0 = Instant::now();
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| SourceError::Other(e.into()))?;

        let rows: Vec<Row> = conn
            .exec(
                r#"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            "#,
                (db, table),
            )
            .await
            .map_err(|e| SourceError::Other(e.into()))?;

        let ms = t0.elapsed().as_millis() as u64;
        if ms > 200 {
            warn!(db = %db, table = %table, ms, "slow schema fetch");
        } else {
            debug!(db = %db, table = %table, ms, "schema fetched");
        }
        let cols: Vec<String> = rows
            .into_iter()
            .map(|mut r| r.take::<String, _>(0).unwrap())
            .collect();
        if cols.is_empty() {
            warn!(db = %db, table = %table, "schema fetch returned 0 columns");
        }
        let arc = Arc::new(cols);
        self.map
            .write()
            .await
            .insert((db.to_string(), table.to_string()), arc.clone());
        Ok(arc)
    }

    pub(super) async fn invalidate(&self, db: &str, _wild: &str) {
        let before = self.map.read().await.len();
        self.map.write().await.retain(|(d, _), _| d != db);
        let after = self.map.read().await.len();
        info!(db = %db, removed = before.saturating_sub(after), "schema cache invalidated");
    }
}
