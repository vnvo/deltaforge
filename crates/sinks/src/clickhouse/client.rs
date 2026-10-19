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

    /// Run a query; its response text (`FORMAT TabSeparated` output).
    async fn query_text(&self, sql: &str) -> Result<String, SinkError>;

    /// Run a statement; its HTTP status and answer text, whatever the
    /// status. `Err` only when no answer arrived (connect, timeout, I/O):
    /// the outcome of a statement is then unknown.
    async fn query_status(&self, sql: &str)
    -> Result<(u16, String), SinkError>;
}

/// The error a fenced insert fails with: the table's fence constraint
/// rejected rows carrying this writer's fence (it was rotated away).
pub const FENCE_REJECTED: &str =
    "clickhouse: the fence constraint rejected the insert";

/// Whether `e` is a fenced insert's error.
pub fn is_fence_rejection(e: &SinkError) -> bool {
    matches!(e, SinkError::Fatal { details } if details.starts_with(FENCE_REJECTED))
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
    /// Build the client from non-secret config plus already-resolved credentials
    /// (protected runtime values from a reference, or inline fallback). Credentials are
    /// never read from `cfg` here, so a resolved secret is not taken from the serializable
    /// config.
    pub fn new(
        cfg: &ClickHouseSinkCfg,
        user: Option<String>,
        password: Option<String>,
    ) -> anyhow::Result<Self> {
        let mut b = reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.send_timeout_secs));
        if let Some(tls) = &cfg.tls {
            if tls.insecure_skip_verify {
                b = b.danger_accept_invalid_certs(true);
            }
        }
        Ok(Self {
            http: b.build()?,
            base: cfg.url.trim_end_matches('/').to_string(),
            database: cfg.database.clone(),
            user,
            password,
            timeout: Duration::from_secs(cfg.send_timeout_secs),
        })
    }

    /// POST a body with a `?query=…` (+ extra params); the status and the
    /// answer text, or a `SinkError` when no answer arrived.
    async fn post_raw(
        &self,
        query: String,
        extra: &[(&str, String)],
        body: Vec<u8>,
    ) -> Result<(u16, String), SinkError> {
        let mut params: Vec<(String, String)> = Vec::new();
        // Only add ?query= when non-empty. DDL is sent as the request *body*
        // (see `execute_ddl`) — a query-in-URL with an empty body triggers
        // HTTP 411 (no Content-Length / not chunked).
        if !query.is_empty() {
            params.push(("query".to_string(), query));
        }
        for (k, v) in extra {
            params.push((k.to_string(), v.clone()));
        }
        let url = if params.is_empty() {
            reqwest::Url::parse(&self.base)
        } else {
            reqwest::Url::parse_with_params(&self.base, &params)
        }
        .map_err(|e| SinkError::Routing {
            details: e.to_string().into(),
        })?;

        let mut req = self.http.post(url).body(body);
        // Pair the auth headers: some ClickHouse builds reject an
        // `X-ClickHouse-User` sent without an `X-ClickHouse-Key` (even for a
        // no-password user), so always send the key (empty when no password).
        if let Some(u) = &self.user {
            req = req.header("X-ClickHouse-User", u).header(
                "X-ClickHouse-Key",
                self.password.as_deref().unwrap_or(""),
            );
        }

        let resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                SinkError::Backpressure {
                    details: format!(
                        "clickhouse request timeout after {:?}",
                        self.timeout
                    )
                    .into(),
                }
            } else if e.is_connect() {
                SinkError::Connect {
                    details: e.to_string().into(),
                }
            } else {
                SinkError::Io(std::io::Error::other(e.to_string()))
            }
        })?;

        let code = resp.status().as_u16();
        let text = resp
            .text()
            .await
            .map_err(|e| SinkError::Io(std::io::Error::other(e.to_string())))?;
        Ok((code, text))
    }

    /// POST a body with a `?query=…` (+ extra params); the response body,
    /// or a `SinkError` on failure.
    async fn post(
        &self,
        query: String,
        extra: &[(&str, String)],
        body: Vec<u8>,
    ) -> Result<String, SinkError> {
        let (code, text) = self.post_raw(query, extra, body).await?;
        if (200..300).contains(&code) {
            return Ok(text);
        }
        let code = reqwest::StatusCode::from_u16(code)
            .unwrap_or(reqwest::StatusCode::INTERNAL_SERVER_ERROR);
        if super::ownership::error_code(&text) == Some(469)
            && text.contains("df_fence_")
        {
            return Err(SinkError::Fatal {
                details: format!(
                    "{FENCE_REJECTED}: {}",
                    &text[..text.len().min(300)]
                )
                .into(),
            });
        }
        if code == reqwest::StatusCode::UNAUTHORIZED
            || code == reqwest::StatusCode::FORBIDDEN
        {
            return Err(SinkError::Auth {
                details: text.into(),
            });
        }
        Err(SinkError::Io(std::io::Error::other(format!(
            "clickhouse {code}: {text}"
        ))))
    }

    /// The `INSERT` query string for a table (public for unit testing).
    pub fn insert_query(&self, table: &str) -> String {
        format!(
            "INSERT INTO {}.{} FORMAT RowBinary",
            super::ddl::quote_ident(&self.database),
            super::ddl::quote_ident(table)
        )
    }
}

/// The settings every insert carries. `async_insert=0` makes every insert
/// synchronous, whatever the server or user profile enables: a 2xx means
/// the rows are written to the table, after the fence constraint accepted
/// them (an asynchronous insert would acknowledge before either). A server
/// whose constraints forbid the setting rejects the insert: it fails, never
/// acknowledging less. `wait_for_async_insert=1` is kept as a second line.
pub(crate) fn insert_settings(
    dedup_token: &str,
) -> [(&'static str, String); 3] {
    [
        ("insert_deduplication_token", dedup_token.to_string()),
        ("async_insert", "0".to_string()),
        ("wait_for_async_insert", "1".to_string()),
    ]
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
            &insert_settings(dedup_token),
            body,
        )
        .await
        .map(drop)
    }

    async fn execute_ddl(&self, sql: &str) -> Result<(), SinkError> {
        // Send the DDL as the request body (ClickHouse executes the POST body as
        // SQL). An empty body with the query in the URL returns HTTP 411.
        self.post(String::new(), &[], sql.as_bytes().to_vec())
            .await
            .map(drop)
    }

    async fn query_text(&self, sql: &str) -> Result<String, SinkError> {
        self.post(String::new(), &[], sql.as_bytes().to_vec()).await
    }

    async fn query_status(
        &self,
        sql: &str,
    ) -> Result<(u16, String), SinkError> {
        self.post_raw(String::new(), &[], sql.as_bytes().to_vec())
            .await
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
            user_ref: None,
            password_ref: None,
            tls: None,
            version_source: ChVersionSource::SourcePosition,
            send_timeout_secs: 30,
            required: Some(true),
            auto_create: true,
            extensions_column: "_extensions".into(),
        }
    }

    #[test]
    fn every_insert_is_synchronous() {
        let settings = insert_settings("tok");
        // The fence constraint is evaluated before the insert answers.
        assert!(settings.contains(&("async_insert", "0".to_string())));
        assert!(settings.contains(&("wait_for_async_insert", "1".to_string())));
        assert!(
            settings
                .contains(&("insert_deduplication_token", "tok".to_string()))
        );
    }

    #[test]
    fn builds_insert_query() {
        let c = ClickHouseClient::new(&cfg(), None, None).unwrap();
        assert_eq!(
            c.insert_query("orders"),
            "INSERT INTO `analytics`.`orders` FORMAT RowBinary"
        );
    }
}
