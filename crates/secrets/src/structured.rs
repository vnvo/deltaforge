//! Structured secrets and the atomic single-read resolution boundary. A provider
//! reads one structured record (e.g. a Vault KV v2 record) exactly once, yielding
//! a [`StructuredSecret`] tagged with a single [`ResolutionGroup`]. Fields are
//! **moved** out into an owned [`CredentialSet`], all sharing that one group - so
//! the set has explicit single-generation provenance, not merely equal/absent
//! versions (Deliverable 5.5 §1A). Parsing a provider payload into the record is
//! a later slice's resolver job.

use std::collections::BTreeMap;

use crate::credential_set::CredentialSet;
use crate::error::{InconsistencyKind, SecretError};
use crate::reference::SecretReference;
use crate::resolved::{ResolutionGroup, ResolvedSecret, SecretMaterial};

/// One provider read of a structured record: field name -> material, tagged with
/// a single resolution group and the record's provider version.
pub struct StructuredSecret {
    fields: BTreeMap<String, SecretMaterial>,
    provider_version: Option<String>,
    group: ResolutionGroup,
}

impl StructuredSecret {
    /// Construct from one provider read. Mints a fresh resolution group so every
    /// field taken from this record shares one provenance. Crate-internal: only
    /// trusted resolution code produces a `StructuredSecret`, which is what makes
    /// its group a trustworthy single-read token.
    #[allow(dead_code)] // wired up by the structured-provider slice
    pub(crate) fn new(
        fields: BTreeMap<String, SecretMaterial>,
        provider_version: Option<String>,
    ) -> Self {
        Self {
            fields,
            provider_version,
            group: ResolutionGroup::next(),
        }
    }

    /// This read's resolution group.
    pub fn group(&self) -> ResolutionGroup {
        self.group
    }

    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.fields.keys().map(String::as_str)
    }

    /// Move the field named by `reference.selector` out of the record as a
    /// [`ResolvedSecret`] tagged with this read's group and provider version.
    ///
    /// Fails closed - **without consuming the field** - when the selector is
    /// missing/absent or when the stored material's representation does not match
    /// `reference.repr`. A representation mismatch therefore leaves the record
    /// intact so a corrected request can still use it.
    pub fn take(
        &mut self,
        reference: &SecretReference,
    ) -> Result<ResolvedSecret, SecretError> {
        let selector = reference.selector.as_deref().ok_or_else(|| {
            SecretError::SelectorMissing {
                reference: reference.safe(),
                selector: None,
            }
        })?;
        // Peek first: a representation mismatch must not consume the field.
        let material = self.fields.get(selector).ok_or_else(|| {
            SecretError::SelectorMissing {
                reference: reference.safe(),
                selector: Some(selector.to_string()),
            }
        })?;
        if material.repr() != reference.repr {
            return Err(SecretError::Inconsistent {
                kind: InconsistencyKind::RepresentationMismatch,
                field: Some(selector.to_string()),
            });
        }
        // Representation matches: now move it out.
        let material = self
            .fields
            .remove(selector)
            .expect("field present: just peeked");
        let mut resolved = ResolvedSecret::new(material).in_group(self.group);
        if let Some(v) = &self.provider_version {
            resolved = resolved.with_provider_version(v.clone());
        }
        Ok(resolved)
    }
}

/// Build a [`CredentialSet`] from **one** structured provider read: each named
/// field is moved out of the single record (so all share one resolution group)
/// and inserted under its connector field name. Duplicate field names are
/// rejected, and each field's representation is enforced against its reference.
/// The result has single-read provenance provable via
/// [`CredentialSet::single_resolution_group`].
pub fn credential_set_from_record(
    mut record: StructuredSecret,
    fields: &[(&str, &SecretReference)],
) -> Result<CredentialSet, SecretError> {
    let mut cs = CredentialSet::new();
    for (name, reference) in fields {
        let secret = record.take(reference)?;
        cs.insert(*name, secret)?;
    }
    Ok(cs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::material::{
        DEFAULT_MAX_SECRET_BYTES, SecretBytes, SecretString,
    };
    use crate::reference::{SecretProvider, SecretReference, SecretRepr};

    fn sref() -> SecretReference {
        SecretReference::new(SecretProvider::Vault, "secret/data/orders")
    }

    fn record() -> StructuredSecret {
        let lim = DEFAULT_MAX_SECRET_BYTES;
        let mut m = BTreeMap::new();
        m.insert(
            "username".to_string(),
            SecretMaterial::Utf8(
                SecretString::new("deltaforge".into(), lim, &sref()).unwrap(),
            ),
        );
        m.insert(
            "password".to_string(),
            SecretMaterial::Utf8(
                SecretString::new("s3cr3t".into(), lim, &sref()).unwrap(),
            ),
        );
        m.insert(
            "client_key".to_string(),
            SecretMaterial::Bytes(
                SecretBytes::new(vec![0xde, 0xad], lim, &sref()).unwrap(),
            ),
        );
        StructuredSecret::new(m, Some("kv-v7".into()))
    }

    #[test]
    fn take_moves_selected_field_with_provenance() {
        let mut rec = record();
        let group = rec.group();
        let r = sref().with_selector("password");
        let taken = rec.take(&r).unwrap();
        assert_eq!(taken.material().as_utf8(), Some("s3cr3t"));
        assert_eq!(taken.resolution_group(), Some(group));
        assert_eq!(taken.provider_version(), Some("kv-v7"));
        // Field was moved out.
        assert!(!rec.field_names().any(|f| f == "password"));
    }

    #[test]
    fn missing_and_absent_selectors_fail_closed() {
        let mut rec = record();
        let missing = sref().with_selector("api_token");
        assert!(matches!(
            rec.take(&missing),
            Err(SecretError::SelectorMissing {
                selector: Some(_),
                ..
            })
        ));
        let absent = sref();
        assert!(matches!(
            rec.take(&absent),
            Err(SecretError::SelectorMissing { selector: None, .. })
        ));
    }

    #[test]
    fn utf8_reference_rejects_binary_material_without_consuming() {
        let mut rec = record();
        // "client_key" is Bytes; request it as UTF-8.
        let r = sref()
            .with_selector("client_key")
            .with_repr(SecretRepr::Utf8);
        let err = rec.take(&r).unwrap_err();
        assert!(matches!(
            err,
            SecretError::Inconsistent {
                kind: InconsistencyKind::RepresentationMismatch,
                ..
            }
        ));
        // Field remains: a corrected (binary) request still succeeds.
        assert!(rec.field_names().any(|f| f == "client_key"));
        let ok = sref()
            .with_selector("client_key")
            .with_repr(SecretRepr::Bytes);
        assert_eq!(
            rec.take(&ok).unwrap().material().expose_bytes(),
            &[0xde, 0xad]
        );
    }

    #[test]
    fn binary_reference_rejects_utf8_material_without_consuming() {
        let mut rec = record();
        // "password" is UTF-8; request it as binary.
        let r = sref()
            .with_selector("password")
            .with_repr(SecretRepr::Bytes);
        let err = rec.take(&r).unwrap_err();
        assert!(matches!(
            err,
            SecretError::Inconsistent {
                kind: InconsistencyKind::RepresentationMismatch,
                ..
            }
        ));
        // Field remains protected and available for a corrected request.
        assert!(rec.field_names().any(|f| f == "password"));
        let ok = sref().with_selector("password"); // repr defaults to Utf8
        assert_eq!(rec.take(&ok).unwrap().material().as_utf8(), Some("s3cr3t"));
    }

    #[test]
    fn one_read_produces_complete_owned_set_with_single_generation() {
        let rec = record();
        let group = rec.group();
        let user_ref = sref().with_selector("username");
        let pass_ref = sref().with_selector("password");
        let cs = credential_set_from_record(
            rec,
            &[("username", &user_ref), ("password", &pass_ref)],
        )
        .unwrap();
        assert_eq!(
            cs.require("username").unwrap().material().as_utf8(),
            Some("deltaforge")
        );
        assert_eq!(
            cs.require("password").unwrap().material().as_utf8(),
            Some("s3cr3t")
        );
        // Explicit single-read provenance.
        assert_eq!(cs.single_resolution_group().unwrap(), group);
    }

    #[test]
    fn duplicate_field_from_record_is_rejected() {
        let rec = record();
        // Two same-representation selectors both mapped to one connector field.
        let u1 = sref().with_selector("username");
        let u2 = sref().with_selector("password");
        let err =
            credential_set_from_record(rec, &[("cred", &u1), ("cred", &u2)])
                .unwrap_err();
        assert!(matches!(err, SecretError::DuplicateField(f) if f == "cred"));
    }
}
