//! `SecretReference` - a durable, serializable descriptor of *where* a secret
//! lives. It never carries the value, a temporary output path, a shell command,
//! or connector-specific parsing logic (Deliverable 5.5 §1). References are the
//! only secret-related data that is serialized or observable.

use serde::{Deserialize, Serialize};

/// Which provider backs a reference. Identifier only - resolution lives in later
/// slices.
///
/// Kubernetes is intentionally **not** a provider: it is a delivery/injection
/// mechanism, not an API-backed resolver. `secretKeyRef` is injected as an
/// environment variable ([`SecretProvider::Env`]) and a projected volume as a
/// file ([`SecretProvider::File`]). A direct Kubernetes-API provider may be added
/// later only if such a resolver is deliberately implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretProvider {
    Env,
    File,
    Vault,
}

/// Expected representation of the resolved material. The connector declares this
/// per field; requesting binary material as UTF-8 fails closed at resolution
/// (Deliverable 5.5 §1A, timeline M8).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SecretRepr {
    #[default]
    Utf8,
    Bytes,
}

/// A durable, serializable pointer to a secret. Safe to `Debug`/serialize - it
/// contains no secret material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretReference {
    /// The backing provider.
    pub provider: SecretProvider,
    /// Provider-specific location (env var name, absolute file path, Vault path).
    pub location: String,
    /// Optional field/sub-key selector into a structured record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    /// Expected representation of the resolved material.
    #[serde(default)]
    pub repr: SecretRepr,
    /// Optional immutable version pin (provider-specific, opaque, non-secret).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Optional human purpose for diagnostics, e.g. "source-password",
    /// "tls-private-key". Non-secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
}

impl SecretReference {
    /// Minimal reference (UTF-8 representation, no selector/version/purpose).
    pub fn new(provider: SecretProvider, location: impl Into<String>) -> Self {
        Self {
            provider,
            location: location.into(),
            selector: None,
            repr: SecretRepr::default(),
            version: None,
            purpose: None,
        }
    }

    pub fn with_selector(mut self, selector: impl Into<String>) -> Self {
        self.selector = Some(selector.into());
        self
    }

    pub fn with_repr(mut self, repr: SecretRepr) -> Self {
        self.repr = repr;
        self
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn with_purpose(mut self, purpose: impl Into<String>) -> Self {
        self.purpose = Some(purpose.into());
        self
    }

    /// A non-secret descriptor of this reference for errors/diagnostics.
    pub fn safe(&self) -> SafeRef {
        SafeRef {
            provider: self.provider,
            location: self.location.clone(),
            selector: self.selector.clone(),
            purpose: self.purpose.clone(),
        }
    }
}

/// A non-secret, displayable descriptor of a reference for use in errors and
/// diagnostics. Carries only reference metadata - never resolved material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeRef {
    pub provider: SecretProvider,
    pub location: String,
    pub selector: Option<String>,
    pub purpose: Option<String>,
}

impl std::fmt::Display for SafeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}:{}", self.provider, self.location)?;
        if let Some(sel) = &self.selector {
            write!(f, "#{sel}")?;
        }
        if let Some(purpose) = &self.purpose {
            write!(f, " ({purpose})")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_serializes_and_round_trips() {
        let r =
            SecretReference::new(SecretProvider::Vault, "secret/data/orders")
                .with_selector("password")
                .with_repr(SecretRepr::Utf8)
                .with_version("7")
                .with_purpose("source-password");
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"provider\":\"vault\""));
        assert!(json.contains("secret/data/orders"));
        assert!(json.contains("source-password"));
        let back: SecretReference = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn repr_defaults_to_utf8_and_optional_fields_omitted() {
        let r = SecretReference::new(SecretProvider::Env, "PG_PASSWORD");
        assert_eq!(r.repr, SecretRepr::Utf8);
        let json = serde_json::to_string(&r).unwrap();
        // Optional None fields are skipped.
        assert!(!json.contains("selector"));
        assert!(!json.contains("version"));
        assert!(!json.contains("purpose"));
    }

    #[test]
    fn provider_set_is_env_file_vault_only_no_kubernetes() {
        // Only the three delivery-agnostic providers serialize; Kubernetes is not
        // a provider (it injects via Env/File).
        assert_eq!(
            serde_json::to_string(&SecretProvider::Env).unwrap(),
            "\"env\""
        );
        assert_eq!(
            serde_json::to_string(&SecretProvider::File).unwrap(),
            "\"file\""
        );
        assert_eq!(
            serde_json::to_string(&SecretProvider::Vault).unwrap(),
            "\"vault\""
        );
        // "kubernetes" is not an accepted provider tag.
        assert!(
            serde_json::from_str::<SecretProvider>("\"kubernetes\"").is_err()
        );
    }

    #[test]
    fn safe_ref_display_is_reference_only() {
        let s = SecretReference::new(SecretProvider::File, "/etc/df/pg.key")
            .with_purpose("tls-private-key")
            .safe();
        let shown = s.to_string();
        assert!(shown.contains("/etc/df/pg.key"));
        assert!(shown.contains("tls-private-key"));
    }
}
