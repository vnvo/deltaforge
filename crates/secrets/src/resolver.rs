//! The connector-agnostic resolution abstraction. Every source, sink, and
//! durable-store credential resolves through this one trait; there is no second
//! path (Deliverable 5.5 §1). This slice defines the abstraction only - concrete
//! env/file/Vault resolvers arrive in later slices, behind optional features so
//! env/file users never pull provider runtimes.
//!
//! Two entry points:
//! - [`SecretResolver::resolve`] for a single reference.
//! - [`SecretResolver::resolve_set`] for a batch of connector fields, so a
//!   provider backed by one structured record (e.g. Vault KV) can read that
//!   record **once** and select every field from it, giving the resulting
//!   [`CredentialSet`] true single-read provenance. Resolving fields with
//!   repeated `resolve` calls would reintroduce the rotation race the batch path
//!   exists to close.

use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::credential_set::CredentialSet;
use crate::error::{InconsistencyKind, SecretError};
use crate::reference::SecretReference;
use crate::resolved::ResolvedSecret;

/// One requested connector credential field: the connector's field name (e.g.
/// "username", "tls_key") paired with the reference that locates it. Field names
/// are free-form connector strings, so a plain `String` rather than a fixed enum.
#[derive(Clone)]
pub struct CredentialFieldRequest {
    pub field: String,
    pub reference: SecretReference,
}

impl CredentialFieldRequest {
    pub fn new(field: impl Into<String>, reference: SecretReference) -> Self {
        Self {
            field: field.into(),
            reference,
        }
    }
}

/// Reject duplicate connector field names before any provider access. Used by
/// `resolve_set` implementations so a duplicate fails without a network/disk read.
pub fn check_no_duplicate_fields(
    requests: &[CredentialFieldRequest],
) -> Result<(), SecretError> {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for req in requests {
        if !seen.insert(req.field.as_str()) {
            return Err(SecretError::DuplicateField(req.field.clone()));
        }
    }
    Ok(())
}

/// Resolves a [`SecretReference`] to a [`ResolvedSecret`]. Connector-agnostic:
/// the resolver returns opaque protected material and does not interpret it;
/// connectors interpret and validate via their typed credential specs.
#[async_trait]
pub trait SecretResolver: Send + Sync {
    async fn resolve(
        &self,
        reference: &SecretReference,
    ) -> Result<ResolvedSecret, SecretError>;

    /// Resolve a batch of connector fields into one owned candidate
    /// [`CredentialSet`].
    ///
    /// The default is **sequential, independent** resolution: each field is
    /// resolved via [`Self::resolve`] and inserted under its connector name, with
    /// representation enforced per field and duplicate field names rejected before
    /// any provider access. It makes **no atomic-provenance claim** - independently
    /// resolved fields keep whatever provenance `resolve` attached (typically
    /// none), so [`CredentialSet::single_resolution_group`] will not report them as
    /// one read. This suits genuinely independent single-value sources: environment
    /// variables are process-immutable, and separately mounted files may rotate
    /// independently, so neither can honestly claim one atomic read.
    ///
    /// A provider backed by structured records (e.g. Vault KV) **overrides** this
    /// to group references addressing the same record, read each record once, and
    /// select all requested fields from that single read - preserving distinct
    /// provenance for genuinely separate records/providers.
    async fn resolve_set(
        &self,
        requests: &[CredentialFieldRequest],
    ) -> Result<CredentialSet, SecretError> {
        check_no_duplicate_fields(requests)?;
        let mut cs = CredentialSet::new();
        for req in requests {
            let secret = self.resolve(&req.reference).await?;
            if secret.material().repr() != req.reference.repr {
                return Err(SecretError::Inconsistent {
                    kind: InconsistencyKind::RepresentationMismatch,
                    field: Some(req.field.clone()),
                });
            }
            cs.insert(req.field.clone(), secret)?;
        }
        Ok(cs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::material::{DEFAULT_MAX_SECRET_BYTES, SecretString};
    use crate::reference::SecretProvider;
    use crate::resolved::SecretMaterial;
    use crate::structured::StructuredSecret;

    fn utf8(v: &str, reference: &SecretReference) -> SecretMaterial {
        SecretMaterial::Utf8(
            SecretString::new(v.into(), DEFAULT_MAX_SECRET_BYTES, reference)
                .unwrap(),
        )
    }

    /// Independent single-value resolver (env-var-like): every field is a
    /// separate read with no shared provenance.
    struct IndependentResolver;

    #[async_trait]
    impl SecretResolver for IndependentResolver {
        async fn resolve(
            &self,
            reference: &SecretReference,
        ) -> Result<ResolvedSecret, SecretError> {
            match reference.provider {
                SecretProvider::Env => {
                    Ok(ResolvedSecret::new(utf8("value", reference)))
                }
                _ => Err(SecretError::NotFound(reference.safe())),
            }
        }
    }

    /// Structured resolver (Vault-KV-like): each location is one record; a batch
    /// reads each distinct record exactly once and selects fields from it.
    struct StructuredResolver {
        reads: AtomicUsize,
    }

    impl StructuredResolver {
        fn new() -> Self {
            Self {
                reads: AtomicUsize::new(0),
            }
        }

        /// One simulated provider read of the record at `location`.
        fn read_record_once(&self, location: &str) -> StructuredSecret {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let anchor = SecretReference::new(SecretProvider::Vault, location);
            let mut m = BTreeMap::new();
            m.insert("username".to_string(), utf8("deltaforge", &anchor));
            m.insert("password".to_string(), utf8("s3cr3t", &anchor));
            StructuredSecret::new(m, Some("kv-v3".into()))
        }
    }

    #[async_trait]
    impl SecretResolver for StructuredResolver {
        async fn resolve(
            &self,
            reference: &SecretReference,
        ) -> Result<ResolvedSecret, SecretError> {
            // Single-value resolve reads the whole record then selects one field.
            let mut record = self.read_record_once(&reference.location);
            record.take(reference)
        }

        async fn resolve_set(
            &self,
            requests: &[CredentialFieldRequest],
        ) -> Result<CredentialSet, SecretError> {
            check_no_duplicate_fields(requests)?;
            // Group requests by the record (location) they address.
            let mut by_loc: BTreeMap<&str, Vec<&CredentialFieldRequest>> =
                BTreeMap::new();
            for req in requests {
                by_loc
                    .entry(req.reference.location.as_str())
                    .or_default()
                    .push(req);
            }
            let mut cs = CredentialSet::new();
            for (loc, reqs) in by_loc {
                let mut record = self.read_record_once(loc); // one read per record
                for req in reqs {
                    let secret = record.take(&req.reference)?;
                    cs.insert(req.field.clone(), secret)?;
                }
            }
            Ok(cs)
        }
    }

    #[tokio::test]
    async fn trait_object_resolves_and_errors_are_safe() {
        let r: &dyn SecretResolver = &IndependentResolver;
        let ok = r
            .resolve(&SecretReference::new(SecretProvider::Env, "X"))
            .await
            .unwrap();
        assert_eq!(ok.material().as_utf8(), Some("value"));

        let err = r
            .resolve(&SecretReference::new(SecretProvider::Vault, "secret/x"))
            .await
            .unwrap_err();
        // Error is safe: references the location, never a value.
        let shown = format!("{err}");
        assert!(shown.contains("secret/x"));
        assert!(!shown.contains("value"));
    }

    #[tokio::test]
    async fn batch_two_selectors_one_record_is_a_single_read() {
        let r = StructuredResolver::new();
        let loc = "secret/data/db";
        let reqs = vec![
            CredentialFieldRequest::new(
                "username",
                SecretReference::new(SecretProvider::Vault, loc)
                    .with_selector("username"),
            ),
            CredentialFieldRequest::new(
                "password",
                SecretReference::new(SecretProvider::Vault, loc)
                    .with_selector("password"),
            ),
        ];
        let cs = r.resolve_set(&reqs).await.unwrap();
        // Exactly one provider read for two selectors of the same record.
        assert_eq!(r.reads.load(Ordering::Relaxed), 1);
        // The set is one atomic read.
        assert!(cs.single_resolution_group().is_ok());
        assert_eq!(
            cs.require("username").unwrap().material().as_utf8(),
            Some("deltaforge")
        );
    }

    #[tokio::test]
    async fn sequential_independent_reads_cannot_claim_one_group() {
        let r = IndependentResolver;
        let reqs = vec![
            CredentialFieldRequest::new(
                "username",
                SecretReference::new(SecretProvider::Env, "DF_USER"),
            ),
            CredentialFieldRequest::new(
                "password",
                SecretReference::new(SecretProvider::Env, "DF_PASS"),
            ),
        ];
        let cs = r.resolve_set(&reqs).await.unwrap();
        // Both fields present, but no single-read provenance is claimed.
        assert_eq!(cs.field_names().count(), 2);
        assert!(matches!(
            cs.single_resolution_group(),
            Err(SecretError::Inconsistent {
                kind: InconsistencyKind::GenerationMismatch,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn mixed_records_keep_distinct_provenance() {
        let r = StructuredResolver::new();
        let reqs = vec![
            CredentialFieldRequest::new(
                "username",
                SecretReference::new(SecretProvider::Vault, "secret/data/a")
                    .with_selector("username"),
            ),
            CredentialFieldRequest::new(
                "password",
                SecretReference::new(SecretProvider::Vault, "secret/data/b")
                    .with_selector("password"),
            ),
        ];
        let cs = r.resolve_set(&reqs).await.unwrap();
        // Two distinct records => two reads, two provenance groups.
        assert_eq!(r.reads.load(Ordering::Relaxed), 2);
        assert!(matches!(
            cs.single_resolution_group(),
            Err(SecretError::Inconsistent {
                kind: InconsistencyKind::GenerationMismatch,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn duplicate_batch_field_names_fail_before_provider_access() {
        let r = StructuredResolver::new();
        let loc = "secret/data/db";
        let reqs = vec![
            CredentialFieldRequest::new(
                "cred",
                SecretReference::new(SecretProvider::Vault, loc)
                    .with_selector("username"),
            ),
            CredentialFieldRequest::new(
                "cred",
                SecretReference::new(SecretProvider::Vault, loc)
                    .with_selector("password"),
            ),
        ];
        let err = r.resolve_set(&reqs).await.unwrap_err();
        assert!(matches!(err, SecretError::DuplicateField(f) if f == "cred"));
        // No provider read happened.
        assert_eq!(r.reads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn default_resolve_set_enforces_representation() {
        let r = IndependentResolver;
        // Env resolver returns UTF-8; request it as binary.
        let reqs = vec![CredentialFieldRequest::new(
            "key",
            SecretReference::new(SecretProvider::Env, "X")
                .with_repr(crate::reference::SecretRepr::Bytes),
        )];
        let err = r.resolve_set(&reqs).await.unwrap_err();
        assert!(matches!(
            err,
            SecretError::Inconsistent {
                kind: InconsistencyKind::RepresentationMismatch,
                ..
            }
        ));
    }
}
