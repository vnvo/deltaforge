//! Composite resolver: routes a reference to the provider named by
//! `reference.provider`, and preserves batched `resolve_set` semantics by
//! dispatching each provider its own subset so per-record single-read provenance
//! is retained. It never fabricates provenance across providers or files: a set
//! mixing an env value and a file record has no single resolution group.

use async_trait::async_trait;

use crate::credential_set::CredentialSet;
use crate::error::SecretError;
use crate::providers::{EnvResolver, FileResolver};
use crate::reference::{SecretProvider, SecretReference};
use crate::resolved::ResolvedSecret;
use crate::resolver::{
    CredentialFieldRequest, SecretResolver, check_no_duplicate_fields,
};

/// Routes references to the environment and file resolvers. Vault references
/// return a typed unsupported-provider error until the Vault provider is installed.
pub struct CompositeResolver {
    env: EnvResolver,
    file: FileResolver,
}

impl CompositeResolver {
    pub fn new(env: EnvResolver, file: FileResolver) -> Self {
        Self { env, file }
    }
}

#[async_trait]
impl SecretResolver for CompositeResolver {
    async fn resolve(
        &self,
        reference: &SecretReference,
    ) -> Result<ResolvedSecret, SecretError> {
        match reference.provider {
            SecretProvider::Env => self.env.resolve(reference).await,
            SecretProvider::File => self.file.resolve(reference).await,
            SecretProvider::Vault => {
                Err(SecretError::UnsupportedProvider(reference.safe()))
            }
        }
    }

    async fn resolve_set(
        &self,
        requests: &[CredentialFieldRequest],
    ) -> Result<CredentialSet, SecretError> {
        // Reject duplicate connector field names before any provider access.
        check_no_duplicate_fields(requests)?;

        // Partition by provider. A Vault reference fails here, still before access.
        let mut env_reqs: Vec<CredentialFieldRequest> = Vec::new();
        let mut file_reqs: Vec<CredentialFieldRequest> = Vec::new();
        for req in requests {
            match req.reference.provider {
                SecretProvider::Env => env_reqs.push(req.clone()),
                SecretProvider::File => file_reqs.push(req.clone()),
                SecretProvider::Vault => {
                    return Err(SecretError::UnsupportedProvider(
                        req.reference.safe(),
                    ));
                }
            }
        }

        // Each provider resolves its own subset (file grouping/provenance intact),
        // then results merge. Duplicate names were already excluded globally, so
        // each merged field keeps the provenance its provider assigned.
        let mut cs = CredentialSet::new();
        if !env_reqs.is_empty() {
            let sub = self.env.resolve_set(&env_reqs).await?;
            for (name, secret) in sub.into_fields() {
                cs.insert(name, secret)?;
            }
        }
        if !file_reqs.is_empty() {
            let sub = self.file.resolve_set(&file_reqs).await?;
            for (name, secret) in sub.into_fields() {
                cs.insert(name, secret)?;
            }
        }
        Ok(cs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::PathBuf;

    use crate::error::InconsistencyKind;
    use crate::providers::{FileMode, FilePolicy};

    fn env_resolver(vars: &[(&str, &str)]) -> EnvResolver {
        let map: std::collections::HashMap<String, OsString> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), OsString::from(*v)))
            .collect();
        EnvResolver::with_lookup(
            Box::new(move |name| map.get(name).cloned()),
            crate::material::DEFAULT_MAX_SECRET_BYTES,
        )
    }

    static UNIQ: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    struct Temp {
        dir: PathBuf,
    }
    impl Temp {
        fn new() -> Self {
            let n = UNIQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "df-reg-{}-{}",
                std::process::id(),
                n
            ));
            fs::create_dir_all(&dir).unwrap();
            Self { dir }
        }
        fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
            let p = self.dir.join(name);
            let mut f = File::create(&p).unwrap();
            f.write_all(bytes).unwrap();
            f.sync_all().unwrap();
            p
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    fn composite(vars: &[(&str, &str)]) -> CompositeResolver {
        CompositeResolver::new(
            env_resolver(vars),
            FileResolver::new(FilePolicy::default()),
        )
    }

    #[tokio::test]
    async fn routes_env_and_file() {
        let t = Temp::new();
        let p = t.write("pw", b"filepass");
        let c = composite(&[("DF_USER", "envuser")]);

        let e = c
            .resolve(&SecretReference::new(SecretProvider::Env, "DF_USER"))
            .await
            .unwrap();
        assert_eq!(e.material().as_utf8(), Some("envuser"));

        let f = c
            .resolve(&SecretReference::new(
                SecretProvider::File,
                p.to_str().unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(f.material().as_utf8(), Some("filepass"));
    }

    #[tokio::test]
    async fn vault_reference_is_unsupported() {
        let c = composite(&[]);
        let err = c
            .resolve(&SecretReference::new(
                SecretProvider::Vault,
                "secret/data/x",
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, SecretError::UnsupportedProvider(_)));
    }

    #[tokio::test]
    async fn batch_preserves_file_grouping_and_env_independence() {
        let t = Temp::new();
        let p = t.write("db.json", br#"{"u":"du","p":"dp"}"#);
        let c = composite(&[("DF_TOK", "tok")]);
        let reqs = vec![
            CredentialFieldRequest::new(
                "username",
                SecretReference::new(SecretProvider::File, p.to_str().unwrap())
                    .with_selector("u"),
            ),
            CredentialFieldRequest::new(
                "password",
                SecretReference::new(SecretProvider::File, p.to_str().unwrap())
                    .with_selector("p"),
            ),
            CredentialFieldRequest::new(
                "token",
                SecretReference::new(SecretProvider::Env, "DF_TOK"),
            ),
        ];
        let cs = c.resolve_set(&reqs).await.unwrap();
        assert_eq!(cs.field_names().count(), 3);
        // The two file fields share a group; the env field has none, so the whole
        // set is not one atomic read (provenance is never falsely combined).
        assert!(matches!(
            cs.single_resolution_group(),
            Err(SecretError::Inconsistent {
                kind: InconsistencyKind::GenerationMismatch,
                ..
            })
        ));
        // The two file fields, taken alone, do share one group.
        let file_only = vec![
            CredentialFieldRequest::new(
                "u",
                SecretReference::new(SecretProvider::File, p.to_str().unwrap())
                    .with_selector("u"),
            ),
            CredentialFieldRequest::new(
                "p",
                SecretReference::new(SecretProvider::File, p.to_str().unwrap())
                    .with_selector("p"),
            ),
        ];
        let cs2 = c.resolve_set(&file_only).await.unwrap();
        assert!(cs2.single_resolution_group().is_ok());
    }

    #[tokio::test]
    async fn duplicate_field_names_rejected_before_access() {
        let c = composite(&[("A", "1"), ("B", "2")]);
        let reqs = vec![
            CredentialFieldRequest::new(
                "dup",
                SecretReference::new(SecretProvider::Env, "A"),
            ),
            CredentialFieldRequest::new(
                "dup",
                SecretReference::new(SecretProvider::Env, "B"),
            ),
        ];
        let err = c.resolve_set(&reqs).await.unwrap_err();
        assert!(matches!(err, SecretError::DuplicateField(f) if f == "dup"));
    }

    #[tokio::test]
    async fn vault_in_batch_rejected_before_access() {
        let c = composite(&[("A", "1")]);
        let reqs = vec![
            CredentialFieldRequest::new(
                "ok",
                SecretReference::new(SecretProvider::Env, "A"),
            ),
            CredentialFieldRequest::new(
                "bad",
                SecretReference::new(SecretProvider::Vault, "secret/x")
                    .with_selector("k"),
            ),
        ];
        let err = c.resolve_set(&reqs).await.unwrap_err();
        assert!(matches!(err, SecretError::UnsupportedProvider(_)));
    }

    #[tokio::test]
    async fn secret_values_absent_from_errors() {
        // An oversized env value must not leak into the composite's error path.
        let c = CompositeResolver::new(
            env_resolver(&[("BIG", "S3NT1NEL-composite")]).with_limit(4),
            FileResolver::new(FilePolicy {
                mode: FileMode::Strict,
                ..FilePolicy::default()
            }),
        );
        let err = c
            .resolve(&SecretReference::new(SecretProvider::Env, "BIG"))
            .await
            .unwrap_err();
        assert!(!format!("{err}").contains("S3NT1NEL-composite"));
        assert!(!format!("{err:?}").contains("S3NT1NEL-composite"));
    }
}
