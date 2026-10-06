//! The S3-compatible server DeltaForge's integration tests run against:
//! RustFS, pinned by release and image digest (`docs/src/sinks/s3-test-backends.md`).
//!
//! It proves portable S3-server behavior: conditional writes, ETags, listing,
//! multipart, persistence. The AWS-shaped path is proven separately by the
//! MiniStack canaries, and neither replaces qualification against real AWS.
//!
//! Every test binary shares one server ([`shared`]), removed when the binary
//! exits; a test that restarts the server starts its own ([`S3Server::start`]).

use std::time::Duration;

use anyhow::{Context, Result, bail};
use gate_ownership::GateOwned;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::OnceCell;

/// The server image: RustFS 1.0.1, pinned by digest.
pub const IMAGE: &str = "rustfs/rustfs";
pub const TAG: &str = "1.0.1@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c";
pub const ACCESS_KEY: &str = "deltaforge-test";
pub const SECRET_KEY: &str = "deltaforge-test-secret";
pub const REGION: &str = "us-east-1";
const PORT: u16 = 9000;
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// A running server.
pub struct S3Server {
    container: ContainerAsync<GenericImage>,
    /// `http://host:port`, path-style.
    pub endpoint: String,
}

impl S3Server {
    /// Start a server and wait until it serves requests.
    pub async fn start() -> Result<Self> {
        let container = GenericImage::new(IMAGE, TAG)
            .with_exposed_port(PORT.tcp())
            .with_wait_for(WaitFor::seconds(1))
            .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
            .gate_owned()
            .start()
            .await
            .context("start the S3 test server")?;
        let endpoint = endpoint_of(&container).await?;
        wait_ready(&endpoint).await?;
        Ok(Self {
            container,
            endpoint,
        })
    }

    /// The container id.
    pub fn id(&self) -> &str {
        self.container.id()
    }

    /// Stop and start the server again (its data volume is kept); the
    /// endpoint is re-read, since the host port can change.
    pub async fn restart(&mut self) -> Result<()> {
        self.container
            .stop()
            .await
            .context("stop the S3 test server")?;
        self.container
            .start()
            .await
            .context("start the S3 test server again")?;
        self.endpoint = endpoint_of(&self.container).await?;
        wait_ready(&self.endpoint).await
    }

    /// Create `bucket` (an existing one is fine).
    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        create_bucket(&self.endpoint, bucket).await
    }
}

async fn endpoint_of(
    container: &ContainerAsync<GenericImage>,
) -> Result<String> {
    let host = container.get_host().await.context("S3 test server host")?;
    let port = container
        .get_host_port_ipv4(PORT)
        .await
        .context("S3 test server port")?;
    Ok(format!("http://{host}:{port}"))
}

async fn wait_ready(endpoint: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client
            .get(format!("{endpoint}/health"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            && resp.status().is_success()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    bail!("the S3 test server never became ready at {endpoint}")
}

static SHARED: OnceCell<S3Server> = OnceCell::const_new();

/// The test binary's shared server, started once (removed at exit).
pub async fn shared() -> &'static S3Server {
    SHARED
        .get_or_init(|| async {
            S3Server::start().await.expect("start the S3 test server")
        })
        .await
}

/// Remove the shared server's container: call from a test binary's exit
/// hook (testcontainers cannot drop a `static`).
pub fn remove_shared() {
    if let Some(server) = SHARED.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", "-v", server.id()])
            .output()
            .ok();
    }
}

/// Create `bucket` on the server at `endpoint` with a SigV4-signed,
/// path-style `PUT /bucket` (200, or 409 when it already exists).
pub async fn create_bucket(endpoint: &str, bucket: &str) -> Result<()> {
    let resp = signed_request(
        endpoint,
        reqwest::Method::PUT,
        &format!("/{bucket}"),
        &[],
        &[],
        Vec::new(),
    )
    .await
    .context("create bucket")?;
    let status = resp.status().as_u16();
    if status == 200 || status == 409 {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    bail!("create bucket {bucket}: status {status}: {body}")
}

/// Send one SigV4-signed, path-style request to the server at `endpoint`, so
/// a test can assert the exact wire response (status, headers) rather than
/// a client library's interpretation of it. `path` is `/bucket/key`; `query`
/// and `headers` are signed as given (header names lowercase).
pub async fn signed_request(
    endpoint: &str,
    method: reqwest::Method,
    path: &str,
    query: &[(&str, &str)],
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> Result<reqwest::Response> {
    use chrono::Utc;
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type HmacSha256 = Hmac<Sha256>;
    fn sign(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key).expect("any key length");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }
    fn encode(s: &str, keep_slash: bool) -> String {
        s.bytes()
            .map(|b| match b {
                b'A'..=b'Z'
                | b'a'..=b'z'
                | b'0'..=b'9'
                | b'-'
                | b'_'
                | b'.'
                | b'~' => (b as char).to_string(),
                b'/' if keep_slash => "/".to_string(),
                _ => format!("%{b:02X}"),
            })
            .collect()
    }

    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = now.format("%Y%m%d").to_string();
    let url = url::Url::parse(endpoint)?;
    let host = format!(
        "{}:{}",
        url.host_str().context("endpoint host")?,
        url.port().unwrap_or(80)
    );
    let canonical_uri = encode(path, true);
    let mut query: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (encode(k, false), encode(v, false)))
        .collect();
    query.sort();
    let canonical_query = query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let payload_hash = hex::encode(Sha256::digest(&body));
    let mut signed: Vec<(String, String)> = vec![
        ("host".into(), host),
        ("x-amz-content-sha256".into(), payload_hash.clone()),
        ("x-amz-date".into(), amz_date.clone()),
    ];
    signed.extend(
        headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.trim().to_string())),
    );
    signed.sort();
    let canonical_headers: String =
        signed.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed_headers = signed
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let scope = format!("{date_stamp}/{REGION}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let k_date = sign(
        format!("AWS4{SECRET_KEY}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = sign(&k_date, REGION.as_bytes());
    let k_service = sign(&k_region, b"s3");
    let k_signing = sign(&k_service, b"aws4_request");
    let signature = hex::encode(sign(&k_signing, string_to_sign.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{scope}, \
         SignedHeaders={signed_headers}, Signature={signature}"
    );
    let mut target = format!("{endpoint}{canonical_uri}");
    if !canonical_query.is_empty() {
        target = format!("{target}?{canonical_query}");
    }
    let mut req = reqwest::Client::new()
        .request(method, target)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &payload_hash)
        .header("Authorization", &auth);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    Ok(req.body(body).send().await?)
}
