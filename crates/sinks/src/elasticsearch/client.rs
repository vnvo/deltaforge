//! Elasticsearch HTTP transport: `_bulk` and index creation over reqwest.

use async_trait::async_trait;
use deltaforge_config::{ElasticsearchSinkCfg, EsAuth};
use deltaforge_core::SinkError;
use serde_json::{Value, json};
use std::time::Duration;

/// The transport the sink drives. A trait so tests can inject a capturing fake
/// without a live Elasticsearch.
#[async_trait]
pub trait EsTransport: Send + Sync {
    /// POST an NDJSON `_bulk` body; return the response body bytes on success.
    async fn bulk(&self, body: Vec<u8>) -> Result<Vec<u8>, SinkError>;

    /// Create `index` with the given mapping if it does not already exist.
    async fn ensure_index(
        &self,
        index: &str,
        mapping: &Value,
    ) -> Result<(), SinkError>;
}

pub struct ElasticsearchClient {
    http: reqwest::Client,
    base: String,
    auth: Option<EsAuth>,
    timeout: Duration,
}

impl ElasticsearchClient {
    pub fn new(cfg: &ElasticsearchSinkCfg) -> anyhow::Result<Self> {
        let mut b = reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.send_timeout_secs));
        if let Some(tls) = &cfg.tls {
            if tls.insecure_skip_verify {
                b = b.danger_accept_invalid_certs(true);
            }
            if let Some(ca) = &tls.ca_file {
                let pem = std::fs::read(ca).map_err(|e| {
                    anyhow::anyhow!("reading tls.ca_file '{ca}': {e}")
                })?;
                let cert = reqwest::Certificate::from_pem(&pem)?;
                b = b.add_root_certificate(cert);
            }
        }
        Ok(Self {
            http: b.build()?,
            base: cfg.url.trim_end_matches('/').to_string(),
            auth: cfg.auth.clone(),
            timeout: Duration::from_secs(cfg.send_timeout_secs),
        })
    }

    pub fn bulk_url(&self) -> String {
        format!("{}/_bulk", self.base)
    }

    pub fn index_url(&self, index: &str) -> String {
        format!("{}/{}", self.base, index)
    }

    /// Apply the configured auth to a request builder.
    fn with_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Some(EsAuth::Basic { username, password }) => {
                req.basic_auth(username, Some(password))
            }
            Some(EsAuth::ApiKey { api_key }) => {
                req.header("Authorization", format!("ApiKey {api_key}"))
            }
            Some(EsAuth::None) | None => req,
        }
    }

    /// Map a transport-level send error to a `SinkError`.
    fn send_err(&self, e: reqwest::Error) -> SinkError {
        if e.is_timeout() {
            SinkError::Backpressure {
                details: format!("elasticsearch request timeout after {:?}", self.timeout)
                    .into(),
            }
        } else if e.is_connect() {
            SinkError::Connect {
                details: e.to_string().into(),
            }
        } else {
            SinkError::Io(std::io::Error::other(e.to_string()))
        }
    }

    /// Map a non-2xx status to a `SinkError` (retryable vs terminal).
    fn status_err(code: reqwest::StatusCode, text: String) -> SinkError {
        use reqwest::StatusCode as S;
        match code {
            S::UNAUTHORIZED | S::FORBIDDEN => SinkError::Auth { details: text.into() },
            // Overloaded / unavailable → retry the whole batch.
            S::TOO_MANY_REQUESTS | S::SERVICE_UNAVAILABLE | S::GATEWAY_TIMEOUT => {
                SinkError::Backpressure {
                    details: format!("elasticsearch {code}: {text}").into(),
                }
            }
            _ => SinkError::Io(std::io::Error::other(format!(
                "elasticsearch {code}: {text}"
            ))),
        }
    }
}

#[async_trait]
impl EsTransport for ElasticsearchClient {
    async fn bulk(&self, body: Vec<u8>) -> Result<Vec<u8>, SinkError> {
        let req = self
            .http
            .post(self.bulk_url())
            .header("Content-Type", "application/x-ndjson")
            .body(body);
        let resp = self.with_auth(req).send().await.map_err(|e| self.send_err(e))?;
        let code = resp.status();
        if code.is_success() {
            let bytes = resp.bytes().await.map_err(|e| self.send_err(e))?;
            return Ok(bytes.to_vec());
        }
        let text = resp.text().await.unwrap_or_default();
        Err(Self::status_err(code, text))
    }

    async fn ensure_index(
        &self,
        index: &str,
        mapping: &Value,
    ) -> Result<(), SinkError> {
        let body = json!({ "mappings": mapping });
        let req = self.http.put(self.index_url(index)).json(&body);
        let resp = self.with_auth(req).send().await.map_err(|e| self.send_err(e))?;
        let code = resp.status();
        if code.is_success() {
            return Ok(());
        }
        let text = resp.text().await.unwrap_or_default();
        // A concurrent/prior create is fine — the index exists either way.
        if text.contains("resource_already_exists_exception") {
            return Ok(());
        }
        Err(Self::status_err(code, text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_config::EsVersionSource;

    fn cfg() -> ElasticsearchSinkCfg {
        ElasticsearchSinkCfg {
            id: "e".into(),
            url: "http://es:9200".into(),
            index: "t".into(),
            auto_create_index: true,
            id_fields: vec![],
            id_separator: "_".into(),
            version_source: EsVersionSource::SourcePosition,
            auth: Some(EsAuth::Basic {
                username: "elastic".into(),
                password: "pw".into(),
            }),
            tls: None,
            send_timeout_secs: 30,
            required: Some(true),
        }
    }

    #[test]
    fn builds_client_and_urls() {
        let c = ElasticsearchClient::new(&cfg()).unwrap();
        assert_eq!(c.bulk_url(), "http://es:9200/_bulk");
        assert_eq!(c.index_url("orders"), "http://es:9200/orders");
    }

    #[test]
    fn trims_trailing_slash_from_base() {
        let mut cf = cfg();
        cf.url = "http://es:9200/".into();
        let c = ElasticsearchClient::new(&cf).unwrap();
        assert_eq!(c.bulk_url(), "http://es:9200/_bulk");
    }
}
