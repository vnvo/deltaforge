//! ClickHouse HTTP client: `INSERT … FORMAT RowBinary` and DDL over reqwest.

use async_trait::async_trait;
use deltaforge_config::ClickHouseSinkCfg;
use deltaforge_core::SinkError;
use std::time::Duration;

/// The transport the sink drives. Abstracted as a trait so tests can inject a
/// capturing fake without a live ClickHouse.
#[async_trait]
pub trait ChTransport: Send + Sync {
    /// Insert a RowBinary body into `db.table` (the client knows the database).
    async fn insert_rowbinary(
        &self,
        table: &str,
        dedup_token: &str,
        body: Vec<u8>,
    ) -> Result<(), SinkError>;

    /// Execute a DDL statement (e.g. `CREATE TABLE …`).
    async fn execute_ddl(&self, sql: &str) -> Result<(), SinkError>;
}

pub struct ClickHouseClient {
    http: reqwest::Client,
    base: String,
    database: String,
    user: Option<String>,
    password: Option<String>,
    timeout: Duration,
}

impl ClickHouseClient {
    pub fn new(cfg: &ClickHouseSinkCfg) -> anyhow::Result<Self> {
        let mut b =
            reqwest::Client::builder().timeout(Duration::from_secs(cfg.send_timeout_secs));
        if let Some(tls) = &cfg.tls {
            if tls.insecure_skip_verify {
                b = b.danger_accept_invalid_certs(true);
            }
        }
        Ok(Self {
            http: b.build()?,
            base: cfg.url.trim_end_matches('/').to_string(),
            database: cfg.database.clone(),
            user: cfg.user.clone(),
            password: cfg.password.clone(),
            timeout: Duration::from_secs(cfg.send_timeout_secs),
        })
    }

    /// POST a body with a `?query=…` (+ extra params); map the response to a
    /// `SinkError` on failure.
    async fn post(
        &self,
        query: String,
        extra: &[(&str, String)],
        body: Vec<u8>,
    ) -> Result<(), SinkError> {
        let mut params = vec![("query".to_string(), query)];
        for (k, v) in extra {
            params.push((k.to_string(), v.clone()));
        }
        let url = reqwest::Url::parse_with_params(&self.base, &params)
            .map_err(|e| SinkError::Routing { details: e.to_string().into() })?;

        let mut req = self.http.post(url).body(body);
        if let Some(u) = &self.user {
            req = req.header("X-ClickHouse-User", u);
        }
        if let Some(p) = &self.password {
            req = req.header("X-ClickHouse-Key", p);
        }

        let resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                SinkError::Backpressure {
                    details: format!("clickhouse request timeout after {:?}", self.timeout).into(),
                }
            } else if e.is_connect() {
                SinkError::Connect { details: e.to_string().into() }
            } else {
                SinkError::Io(std::io::Error::other(e.to_string()))
            }
        })?;

        if resp.status().is_success() {
            return Ok(());
        }
        let code = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if code == reqwest::StatusCode::UNAUTHORIZED || code == reqwest::StatusCode::FORBIDDEN {
            return Err(SinkError::Auth { details: text.into() });
        }
        Err(SinkError::Io(std::io::Error::other(format!("clickhouse {code}: {text}"))))
    }

    /// The `INSERT` query string for a table (public for unit testing).
    pub fn insert_query(&self, table: &str) -> String {
        format!("INSERT INTO {}.{} FORMAT RowBinary", self.database, table)
    }
}

#[async_trait]
impl ChTransport for ClickHouseClient {
    async fn insert_rowbinary(
        &self,
        table: &str,
        dedup_token: &str,
        body: Vec<u8>,
    ) -> Result<(), SinkError> {
        self.post(
            self.insert_query(table),
            &[("insert_deduplication_token", dedup_token.to_string())],
            body,
        )
        .await
    }

    async fn execute_ddl(&self, sql: &str) -> Result<(), SinkError> {
        self.post(sql.to_string(), &[], Vec::new()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_config::{ChMode, ChVersionSource};

    fn cfg() -> ClickHouseSinkCfg {
        ClickHouseSinkCfg {
            id: "c".into(),
            url: "http://ch:8123".into(),
            database: "analytics".into(),
            table: "orders".into(),
            mode: ChMode::Upsert,
            user: Some("default".into()),
            password: None,
            tls: None,
            version_source: ChVersionSource::SourcePosition,
            send_timeout_secs: 30,
            required: Some(true),
            auto_create: true,
        }
    }

    #[test]
    fn builds_insert_query() {
        let c = ClickHouseClient::new(&cfg()).unwrap();
        assert_eq!(
            c.insert_query("orders"),
            "INSERT INTO analytics.orders FORMAT RowBinary"
        );
    }
}
