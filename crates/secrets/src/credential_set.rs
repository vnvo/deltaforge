//! `CredentialSet` - a connector-level **candidate generation** of resolved
//! credential fields for one authentication mode, validated as a whole before
//! use (Deliverable 5.5 §1A). "Atomic" means all required fields are validated
//! together as one candidate generation; it does **not** mean storing fields
//! durably (they never touch durable state). The connector supplies the
//! validation boundary that builds/authenticates a client from the complete set;
//! a partial or inconsistent set fails validation and the current verified set
//! stays in use.

use std::collections::BTreeMap;

use crate::error::{InconsistencyKind, SecretError};
use crate::resolved::{ResolutionGroup, ResolvedSecret};

/// An in-memory candidate generation of named credential fields.
#[derive(Default)]
pub struct CredentialSet {
    fields: BTreeMap<String, ResolvedSecret>,
}

impl CredentialSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a field, failing closed on a duplicate name rather than silently
    /// overwriting (a silent overwrite could mix a stale field into a candidate
    /// generation without any signal).
    pub fn insert(
        &mut self,
        field: impl Into<String>,
        secret: ResolvedSecret,
    ) -> Result<(), SecretError> {
        let name = field.into();
        if self.fields.contains_key(&name) {
            return Err(SecretError::DuplicateField(name));
        }
        self.fields.insert(name, secret);
        Ok(())
    }

    pub fn get(&self, field: &str) -> Option<&ResolvedSecret> {
        self.fields.get(field)
    }

    /// Require a field, failing closed if the candidate generation lacks it.
    pub fn require(&self, field: &str) -> Result<&ResolvedSecret, SecretError> {
        self.fields
            .get(field)
            .ok_or_else(|| SecretError::MissingField(field.to_string()))
    }

    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.fields.keys().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Provider versions for every field (opaque, non-secret). Diagnostic only:
    /// version equality is **not** proof of a single provider read (two
    /// independent reads can carry equal or absent versions). Use
    /// [`Self::single_resolution_group`] for that proof.
    pub fn provider_versions(&self) -> BTreeMap<String, Option<String>> {
        self.fields
            .iter()
            .map(|(k, v)| (k.clone(), v.provider_version().map(str::to_string)))
            .collect()
    }

    /// Prove every field came from **one** provider read and return that
    /// [`ResolutionGroup`]. Each field must carry the **same** `Some(group)`; any
    /// field with no group, or two distinct groups, fails closed with
    /// [`InconsistencyKind::GenerationMismatch`]. Absence is never treated as
    /// consistency: fields lacking provenance cannot masquerade as co-generated.
    /// An empty set has no read to prove.
    ///
    /// This is a narrow claim about single-record provenance, not a validity
    /// verdict. A valid credential set may **intentionally** combine fields from
    /// several independent sources (e.g. a username from an env var and a TLS key
    /// from a mounted file); such a set has no single group and relies on the
    /// connector's [`Self::validate`] boundary, not on this method.
    pub fn single_resolution_group(
        &self,
    ) -> Result<ResolutionGroup, SecretError> {
        let mut iter = self.fields.iter();
        let (first_name, first) =
            iter.next().ok_or(SecretError::Inconsistent {
                kind: InconsistencyKind::GenerationMismatch,
                field: None,
            })?;
        let group = first.resolution_group().ok_or_else(|| {
            SecretError::Inconsistent {
                kind: InconsistencyKind::GenerationMismatch,
                field: Some(first_name.clone()),
            }
        })?;
        for (name, secret) in iter {
            match secret.resolution_group() {
                Some(g) if g == group => {}
                _ => {
                    return Err(SecretError::Inconsistent {
                        kind: InconsistencyKind::GenerationMismatch,
                        field: Some(name.clone()),
                    });
                }
            }
        }
        Ok(group)
    }

    /// The validation boundary: a connector-supplied closure inspects the
    /// **complete** candidate set (required fields present, internally
    /// consistent, cert/key correspondence, etc.) and builds a validated artifact
    /// (e.g. an authenticated client). Returning an error means the candidate set
    /// is rejected as a whole - the caller keeps the current verified set.
    pub fn validate<T, F>(&self, validator: F) -> Result<T, SecretError>
    where
        F: FnOnce(&CredentialSet) -> Result<T, SecretError>,
    {
        validator(self)
    }
}

impl std::fmt::Debug for CredentialSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Show field names and versions only; ResolvedSecret redacts material.
        f.debug_struct("CredentialSet")
            .field("fields", &self.field_names().collect::<Vec<_>>())
            .field("provider_versions", &self.provider_versions())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::material::{DEFAULT_MAX_SECRET_BYTES, SecretString};
    use crate::reference::{SecretProvider, SecretReference};
    use crate::resolved::SecretMaterial;

    fn sref() -> SecretReference {
        SecretReference::new(SecretProvider::Env, "X")
    }

    fn field(value: &str) -> ResolvedSecret {
        ResolvedSecret::new(SecretMaterial::Utf8(
            SecretString::new(value.into(), DEFAULT_MAX_SECRET_BYTES, &sref())
                .unwrap(),
        ))
    }

    #[test]
    fn require_missing_field_fails_closed() {
        let cs = CredentialSet::new();
        let err = cs.require("password").unwrap_err();
        assert!(matches!(err, SecretError::MissingField(f) if f == "password"));
    }

    #[test]
    fn duplicate_field_is_rejected() {
        let mut cs = CredentialSet::new();
        cs.insert("password", field("first")).unwrap();
        let err = cs.insert("password", field("second")).unwrap_err();
        assert!(
            matches!(err, SecretError::DuplicateField(f) if f == "password")
        );
        // The original value is untouched by the rejected insert.
        assert_eq!(
            cs.require("password").unwrap().material().as_utf8(),
            Some("first")
        );
    }

    #[test]
    fn single_generation_proves_atomic() {
        let group = ResolutionGroup::next();
        let mut cs = CredentialSet::new();
        cs.insert("username", field("deltaforge").in_group(group))
            .unwrap();
        cs.insert("password", field("s3cr3t").in_group(group))
            .unwrap();
        assert_eq!(cs.single_resolution_group().unwrap(), group);
    }

    #[test]
    fn two_generations_cannot_masquerade_as_one() {
        // Two independent reads: distinct groups, yet equal (absent) versions.
        let g1 = ResolutionGroup::next();
        let g2 = ResolutionGroup::next();
        let mut cs = CredentialSet::new();
        cs.insert("username", field("deltaforge").in_group(g1))
            .unwrap();
        cs.insert("password", field("s3cr3t").in_group(g2)).unwrap();
        let err = cs.single_resolution_group().unwrap_err();
        assert!(matches!(
            err,
            SecretError::Inconsistent {
                kind: InconsistencyKind::GenerationMismatch,
                ..
            }
        ));
    }

    #[test]
    fn absent_provenance_is_not_consistency_proof() {
        // Neither field carries a group. None == None must NOT pass as atomic.
        let mut cs = CredentialSet::new();
        cs.insert("username", field("deltaforge")).unwrap();
        cs.insert("password", field("s3cr3t")).unwrap();
        assert!(matches!(
            cs.single_resolution_group(),
            Err(SecretError::Inconsistent {
                kind: InconsistencyKind::GenerationMismatch,
                ..
            })
        ));
    }

    #[test]
    fn validate_rejecting_candidate_leaks_no_material() {
        let g1 = ResolutionGroup::next();
        let g2 = ResolutionGroup::next();
        let mut cs = CredentialSet::new();
        cs.insert("username", field("deltaforge2").in_group(g1))
            .unwrap();
        cs.insert("password", field("s3cr3t").in_group(g2)).unwrap();
        let err = cs
            .validate(|set| {
                set.single_resolution_group()?;
                Ok::<(), SecretError>(())
            })
            .unwrap_err();
        assert!(matches!(err, SecretError::Inconsistent { .. }));
        assert!(!format!("{err:?}").contains("s3cr3t"));
    }

    #[test]
    fn debug_does_not_leak_material() {
        let mut cs = CredentialSet::new();
        cs.insert("password", field("hunter2").with_provider_version("g1"))
            .unwrap();
        let shown = format!("{cs:?}");
        assert!(!shown.contains("hunter2"));
        assert!(shown.contains("password")); // field name is not secret
        assert!(shown.contains("g1")); // provider version is not secret
    }
}
