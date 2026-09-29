//! Elasticsearch HTTP transport: `_bulk` and index creation over reqwest.

use async_trait::async_trait;
use deltaforge_config::ElasticsearchSinkCfg;

/// Effective ES auth after reference resolution (protected runtime values; never
/// serialized). Distinct from the config `EsAuth`, which may carry references.
#[derive(Clone, Default)]
pub enum EsAuthResolved {
    Basic {
        username: String,
        password: zeroize::Zeroizing<String>,
    },
    ApiKey {
        api_key: zeroize::Zeroizing<String>,
    },
    #[default]
    None,
}

/// Compute the effective ES auth from config auth (which may carry references) plus the
/// resolved credential values. A reference wins over an inline value; inline values are
/// `${ENV}`-expanded. Basic requires both username and password; ApiKey requires the key.
pub fn resolve_es_auth(
    cfg_auth: &Option<deltaforge_config::EsAuth>,
    creds: &crate::ResolvedSinkCreds,
) -> anyhow::Result<EsAuthResolved> {
    use deltaforge_config::EsAuth;
    let eff = |field: &str,
               inline: &Option<String>|
     -> anyhow::Result<Option<String>> {
        if let Some(v) = creds.get(field) {
            return Ok(Some(v.to_string()));
        }
        match inline {
            Some(s) => Ok(Some(shellexpand::env(s)?.into_owned())),
            None => Ok(None),
        }
    };
    match cfg_auth {
        None | Some(EsAuth::None) => Ok(EsAuthResolved::None),
        Some(EsAuth::Basic {
            username, password, ..
        }) => match (eff("username", username)?, eff("password", password)?) {
            (Some(u), Some(p)) => Ok(EsAuthResolved::Basic {
                username: u,
                password: zeroize::Zeroizing::new(p),
            }),
            _ => anyhow::bail!(
                "elasticsearch basic auth requires both username and password \
                 (inline or reference)"
            ),
        },
        Some(EsAuth::ApiKey { api_key, .. }) => {
            match eff("api_key", api_key)? {
                Some(k) => Ok(EsAuthResolved::ApiKey {
                    api_key: zeroize::Zeroizing::new(k),
                }),
                None => anyhow::bail!(
                    "elasticsearch api_key auth requires api_key (inline or \
                     reference)"
                ),
            }
        }
    }
}
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
    auth: EsAuthResolved,
    timeout: Duration,
}

impl ElasticsearchClient {
    pub fn new(
        cfg: &ElasticsearchSinkCfg,
        auth: EsAuthResolved,
    ) -> anyhow::Result<Self> {
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
            auth,
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
    fn with_auth(
        &self,
        req: reqwest::RequestBuilder,
    ) -> reqwest::RequestBuilder {
        match &self.auth {
            EsAuthResolved::Basic { username, password } => {
                req.basic_auth(username, Some(password.as_str()))
            }
            EsAuthResolved::ApiKey { api_key } => req.header(
                "Authorization",
                format!("ApiKey {}", api_key.as_str()),
            ),
            EsAuthResolved::None => req,
        }
    }

    /// Map a transport-level send error to a `SinkError`.
    fn send_err(&self, e: reqwest::Error) -> SinkError {
        if e.is_timeout() {
            SinkError::Backpressure {
                details: format!(
                    "elasticsearch request timeout after {:?}",
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
    }

    /// Map a non-2xx status to a `SinkError` (retryable vs terminal).
    fn status_err(code: reqwest::StatusCode, text: String) -> SinkError {
        use reqwest::StatusCode as S;
        match code {
            S::UNAUTHORIZED | S::FORBIDDEN => SinkError::Auth {
                details: text.into(),
            },
            // Overloaded / unavailable → retry the whole batch.
            S::TOO_MANY_REQUESTS
            | S::SERVICE_UNAVAILABLE
            | S::GATEWAY_TIMEOUT => SinkError::Backpressure {
                details: format!("elasticsearch {code}: {text}").into(),
            },
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
        let resp = self
            .with_auth(req)
            .send()
            .await
            .map_err(|e| self.send_err(e))?;
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
        let resp = self
            .with_auth(req)
            .send()
            .await
            .map_err(|e| self.send_err(e))?;
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
            auth: Some(deltaforge_config::EsAuth::Basic {
                username: Some("elastic".into()),
                password: Some("pw".into()),
                username_ref: None,
                password_ref: None,
            }),
            tls: None,
            send_timeout_secs: 30,
            required: Some(true),
        }
    }

    #[test]
    fn builds_client_and_urls() {
        let c = ElasticsearchClient::new(&cfg(), EsAuthResolved::None).unwrap();
        assert_eq!(c.bulk_url(), "http://es:9200/_bulk");
        assert_eq!(c.index_url("orders"), "http://es:9200/orders");
    }

    #[test]
    fn trims_trailing_slash_from_base() {
        let mut cf = cfg();
        cf.url = "http://es:9200/".into();
        let c = ElasticsearchClient::new(&cf, EsAuthResolved::None).unwrap();
        assert_eq!(c.bulk_url(), "http://es:9200/_bulk");
    }
}
