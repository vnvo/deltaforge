//! Object-store client construction.
//!
//! Builds an `object_store::ObjectStore` from a config-style URL plus optional
//! credentials. Supports:
//! - `s3://bucket/...`       — AWS S3 or any S3-compatible service (MinIO, Ceph, R2)
//! - `file:///path/...`      — local filesystem (testing, single-node deploys)
//!
//! GCS / Azure are intentionally deferred until Phase 2 — the abstraction
//! shape stays the same, we just enable additional `object_store` features.

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;

/// Connection parameters for the object store. Phase 1a accepts a narrow set;
/// later phases will load this from `S3SinkCfg`.
#[derive(Debug, Clone)]
pub struct ObjectStoreParams {
    /// Bucket name (S3) or root directory (local FS).
    pub bucket: String,
    /// `Some("https://minio:9000")` for non-AWS S3 endpoints; `None` for AWS S3.
    pub endpoint: Option<String>,
    /// AWS region; required for AWS S3, ignored by MinIO/local.
    pub region: Option<String>,
    /// Inline access key (env-expanded by caller).
    pub access_key_id: Option<String>,
    /// Inline secret key (env-expanded by caller).
    pub secret_access_key: Option<String>,
    /// Force path-style addressing (`endpoint/bucket/key` vs `bucket.endpoint/key`).
    /// MinIO and most non-AWS S3 services need this.
    pub virtual_hosted_style: bool,
    /// `true` => use `LocalFileSystem` instead of S3. For tests and local sinks.
    pub local: bool,
}

impl ObjectStoreParams {
    pub fn s3_minio(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            endpoint: Some(endpoint.into()),
            region: Some("us-east-1".to_string()),
            access_key_id: Some(access_key_id.into()),
            secret_access_key: Some(secret_access_key.into()),
            virtual_hosted_style: false,
            local: false,
        }
    }

    pub fn local(root: impl Into<String>) -> Self {
        Self {
            bucket: root.into(),
            endpoint: None,
            region: None,
            access_key_id: None,
            secret_access_key: None,
            virtual_hosted_style: false,
            local: true,
        }
    }
}

/// Build an `ObjectStore` client from params. Lives behind an Arc because
/// every writer that targets this store will clone it.
pub fn build_object_store(
    params: &ObjectStoreParams,
) -> Result<Arc<dyn ObjectStore>> {
    if params.local {
        let root = &params.bucket;
        std::fs::create_dir_all(root)
            .with_context(|| format!("create local store root {root}"))?;
        let store = LocalFileSystem::new_with_prefix(root)
            .with_context(|| format!("open local store at {root}"))?;
        return Ok(Arc::new(store));
    }

    let mut builder = AmazonS3Builder::new().with_bucket_name(&params.bucket);

    if let Some(endpoint) = &params.endpoint {
        builder = builder.with_endpoint(endpoint);
        // MinIO + most S3-compatible services are HTTP unless a TLS cert is set up.
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
    }
    if let Some(region) = &params.region {
        builder = builder.with_region(region);
    }
    if let (Some(key), Some(secret)) =
        (&params.access_key_id, &params.secret_access_key)
    {
        builder = builder
            .with_access_key_id(key)
            .with_secret_access_key(secret);
    }
    builder =
        builder.with_virtual_hosted_style_request(params.virtual_hosted_style);

    let store = builder.build().context("build S3 object store")?;
    Ok(Arc::new(store))
}

/// Test helper: thin wrapper that owns the store so tests can clone it
/// for readers and writers.
#[cfg(test)]
pub(crate) struct ObjectStoreHandle {
    pub store: Arc<dyn ObjectStore>,
}

#[cfg(test)]
impl ObjectStoreHandle {
    pub fn new(params: ObjectStoreParams) -> Result<Self> {
        let store = build_object_store(&params)?;
        Ok(Self { store })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    #[tokio::test]
    async fn local_store_roundtrip() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let handle = ObjectStoreHandle::new(params)?;
        let path = Path::from("hello.txt");
        handle
            .store
            .put(&path, PutPayload::from(Bytes::from_static(b"world")))
            .await?;
        let got = handle.store.get(&path).await?.bytes().await?;
        assert_eq!(&got[..], b"world");
        Ok(())
    }
}
