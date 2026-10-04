//! Shared MiniStack (AWS emulator) setup for the S3 canaries: one pinned
//! container per test binary, with the canary bucket created.

use std::time::Duration;

use anyhow::Result;
use ctor::dtor;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use tokio::sync::OnceCell;

// =============================================================================
// MiniStack testcontainer
// =============================================================================

pub const MS_PORT: u16 = 4566;
pub const MS_KEY: &str = "test";
pub const MS_SECRET: &str = "test";
pub const BUCKET: &str = "deltaforge-canary";

pub struct MinistackInfra {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub endpoint: String,
}

static MINISTACK: OnceCell<MinistackInfra> = OnceCell::const_new();

#[dtor]
fn cleanup_ministack() {
    if let Some(infra) = MINISTACK.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", infra.container.id()])
            .output()
            .ok();
    }
}

pub async fn ministack() -> &'static MinistackInfra {
    MINISTACK
        .get_or_init(|| async {
            // Pinned (tag and digest) so the canary is reproducible.
            let container =
                GenericImage::new(
                    "ministackorg/ministack",
                    "1.4.9@sha256:9acaad157381441088506b2c2db11e790f84f6b6bd4baa46cf1acebb541c76cd",
                )
                    .with_wait_for(WaitFor::seconds(3))
                    .with_exposed_port(MS_PORT.tcp())
                    .start()
                    .await
                    .expect("start MiniStack");
            let host = container.get_host().await.expect("ministack host");
            let port = container
                .get_host_port_ipv4(MS_PORT)
                .await
                .expect("ministack port");
            let endpoint = format!("http://{host}:{port}");
            wait_for_http(&endpoint, Duration::from_secs(60))
                .await
                .expect("MiniStack ready");
            ensure_bucket(&endpoint)
                .await
                .expect("create canary bucket");
            MinistackInfra {
                container,
                endpoint,
            }
        })
        .await
}

async fn wait_for_http(endpoint: &str, timeout: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        // MiniStack accepts any path on 4566 and replies with S3 XML if
        // it's up. A simple HEAD on /healthz or root with an OK-ish status
        // is enough.
        if let Ok(resp) = client
            .get(endpoint)
            .timeout(Duration::from_secs(2))
            .send()
            .await
            && resp.status().as_u16() < 500
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    anyhow::bail!("MiniStack never became ready at {endpoint}")
}

/// Create the test bucket via a SigV4-signed PUT through object_store's
/// own client. Easiest path: use object_store::aws::AmazonS3 to issue an
/// `put_opts` on the bucket root (object_store doesn't expose CreateBucket
/// directly, but PUTs to `s3://bucket/` are idempotent on most emulators
/// — both MiniStack and MinIO accept this). If the emulator returns
/// `BucketAlreadyOwnedByYou` (409) or similar, we treat it as success.
async fn ensure_bucket(endpoint: &str) -> Result<()> {
    // Drop down to a raw SigV4 client via reqwest. Implementing SigV4 by
    // hand is the simplest dependency-free path for the canary. We use the
    // path-style URL.
    use chrono::Utc;
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};

    type HmacSha256 = Hmac<Sha256>;

    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = now.format("%Y%m%d").to_string();
    let region = "us-east-1";
    let service = "s3";

    let url = url::Url::parse(endpoint)?;
    let host =
        format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(80));
    let canonical_uri = format!("/{BUCKET}");
    let canonical_querystring = "";
    let payload_hash = hex::encode(Sha256::digest(b""));
    let canonical_headers = format!(
        "host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
    );
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "PUT\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let credential_scope =
        format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );

    let k_date =
        sign(format!("AWS4{MS_SECRET}").as_bytes(), date_stamp.as_bytes());
    let k_region = sign(&k_date, region.as_bytes());
    let k_service = sign(&k_region, service.as_bytes());
    let k_signing = sign(&k_service, b"aws4_request");
    let signature = hex::encode(sign(&k_signing, string_to_sign.as_bytes()));

    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={MS_KEY}/{credential_scope}, \
         SignedHeaders={signed_headers}, Signature={signature}"
    );

    let put_url = format!("{endpoint}{canonical_uri}");
    let resp = reqwest::Client::new()
        .put(&put_url)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &payload_hash)
        .header("Authorization", &auth)
        .send()
        .await?;
    let status = resp.status().as_u16();
    if status == 200 || status == 409 {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    anyhow::bail!("create bucket failed: status={status}, body={body}");

    fn sign(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }
}
