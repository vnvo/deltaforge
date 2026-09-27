//! `ResolvedSecret` - opaque protected material plus safe, non-secret metadata.
//! The metadata (`provider_version`, `resolution_group`, `expires_at`,
//! `renewable`) is present now so deferred dynamic/renewable credentials add
//! later without redesign, and so atomic multi-field consistency can be proven by
//! **explicit provenance** rather than by comparing (possibly absent) versions.
//!
//! Provenance is **sealed**: only trusted resolution code inside this crate can
//! mint a [`ResolutionGroup`] or attach one (and the other metadata) to a
//! resolved secret. External connectors can *inspect* provenance via read-only
//! getters but cannot fabricate or rewrite it, so
//! [`crate::CredentialSet::single_resolution_group`] proves a real single
//! provider read rather than a caller that assigned equal integers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use crate::material::{SecretBytes, SecretString};
use crate::reference::SecretRepr;

// Wired up by the structured-provider slice (e.g. Vault KV); exercised now by the
// resolver/structured tests that simulate a single-record read.
#[allow(dead_code)]
static NEXT_GROUP: AtomicU64 = AtomicU64::new(1);

/// Opaque, non-secret provenance token identifying **one provider read**. Fields
/// extracted from a single structured record share one `ResolutionGroup`, which
/// is how a credential set proves it is a single atomic generation (rather than
/// several independent reads that merely happen to have equal/absent versions).
///
/// The token is **not constructible outside this crate**: there is no public
/// constructor and the inner counter is private. Connectors can compare and
/// store a group they were handed, but cannot mint one.
///
/// ```compile_fail
/// // `ResolutionGroup::next` is crate-private; external code cannot mint one.
/// let _ = secrets::ResolutionGroup::next();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResolutionGroup(u64);

impl ResolutionGroup {
    /// Mint a fresh, process-unique group (one per provider read). Crate-internal:
    /// only trusted resolution/batch code calls this. Not derived from any secret;
    /// never persisted.
    #[allow(dead_code)] // wired up by the structured-provider slice
    pub(crate) fn next() -> Self {
        Self(NEXT_GROUP.fetch_add(1, Ordering::Relaxed))
    }
}

/// The resolved value, in its declared representation. Redacted in `Debug`; no
/// `Display`/`Serialize`.
pub enum SecretMaterial {
    Utf8(SecretString),
    Bytes(SecretBytes),
}

impl SecretMaterial {
    pub fn repr(&self) -> SecretRepr {
        match self {
            SecretMaterial::Utf8(_) => SecretRepr::Utf8,
            SecretMaterial::Bytes(_) => SecretRepr::Bytes,
        }
    }

    /// Raw bytes of the material (auditable accessor).
    pub fn expose_bytes(&self) -> &[u8] {
        match self {
            SecretMaterial::Utf8(s) => s.expose_secret().as_bytes(),
            SecretMaterial::Bytes(b) => b.expose_bytes(),
        }
    }

    /// UTF-8 view, if this material is UTF-8.
    pub fn as_utf8(&self) -> Option<&str> {
        match self {
            SecretMaterial::Utf8(s) => Some(s.expose_secret()),
            SecretMaterial::Bytes(_) => None,
        }
    }

    pub fn len(&self) -> usize {
        self.expose_bytes().len()
    }

    pub fn is_empty(&self) -> bool {
        self.expose_bytes().is_empty()
    }
}

impl std::fmt::Debug for SecretMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecretMaterial::Utf8(_) => {
                f.write_str("SecretMaterial::Utf8(REDACTED)")
            }
            SecretMaterial::Bytes(_) => {
                f.write_str("SecretMaterial::Bytes(REDACTED)")
            }
        }
    }
}

/// A resolved secret: protected material + non-secret metadata. Metadata is
/// **read-only from outside the crate** (private fields + getters); provenance
/// and metadata are attached only by crate-internal resolution code.
///
/// ```compile_fail
/// use secrets::ResolvedSecret;
/// // Metadata fields are private: external code cannot rewrite provenance.
/// fn tamper(s: &mut ResolvedSecret) {
///     s.resolution_group = None;
/// }
/// ```
pub struct ResolvedSecret {
    material: SecretMaterial,
    provider_version: Option<String>,
    resolution_group: Option<ResolutionGroup>,
    expires_at: Option<SystemTime>,
    renewable: bool,
}

impl ResolvedSecret {
    /// Construct a resolved secret with **no provenance** (group `None`). Any
    /// resolver may build one for an independent single-value read; such a secret
    /// can never masquerade as part of an atomic multi-field generation. Attaching
    /// provenance/metadata is crate-internal.
    pub fn new(material: SecretMaterial) -> Self {
        Self {
            material,
            provider_version: None,
            resolution_group: None,
            expires_at: None,
            renewable: false,
        }
    }

    // --- Sealed builders: crate-internal resolution code only. ---

    pub(crate) fn with_provider_version(
        mut self,
        v: impl Into<String>,
    ) -> Self {
        self.provider_version = Some(v.into());
        self
    }

    pub(crate) fn in_group(mut self, group: ResolutionGroup) -> Self {
        self.resolution_group = Some(group);
        self
    }

    #[allow(dead_code)] // wired up by the dynamic-credential slice
    pub(crate) fn with_expires_at(mut self, at: SystemTime) -> Self {
        self.expires_at = Some(at);
        self
    }

    #[allow(dead_code)] // wired up by the dynamic-credential slice
    pub(crate) fn with_renewable(mut self, renewable: bool) -> Self {
        self.renewable = renewable;
        self
    }

    // --- Read-only getters (public). ---

    pub fn material(&self) -> &SecretMaterial {
        &self.material
    }

    pub fn provider_version(&self) -> Option<&str> {
        self.provider_version.as_deref()
    }

    pub fn resolution_group(&self) -> Option<ResolutionGroup> {
        self.resolution_group
    }

    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }

    pub fn renewable(&self) -> bool {
        self.renewable
    }
}

impl std::fmt::Debug for ResolvedSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Metadata is non-secret and shown; material is redacted.
        f.debug_struct("ResolvedSecret")
            .field("material", &self.material)
            .field("provider_version", &self.provider_version)
            .field("resolution_group", &self.resolution_group)
            .field("expires_at", &self.expires_at)
            .field("renewable", &self.renewable)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::material::DEFAULT_MAX_SECRET_BYTES;
    use crate::reference::{SecretProvider, SecretReference};

    fn sref() -> SecretReference {
        SecretReference::new(SecretProvider::Env, "X")
    }

    fn utf8(v: &str) -> SecretString {
        SecretString::new(v.to_string(), DEFAULT_MAX_SECRET_BYTES, &sref())
            .unwrap()
    }

    #[test]
    fn resolution_groups_are_unique() {
        assert_ne!(ResolutionGroup::next(), ResolutionGroup::next());
    }

    #[test]
    fn material_repr_and_views() {
        let u = SecretMaterial::Utf8(utf8("abc"));
        assert_eq!(u.repr(), SecretRepr::Utf8);
        assert_eq!(u.as_utf8(), Some("abc"));
        assert_eq!(u.expose_bytes(), b"abc");

        let b = SecretMaterial::Bytes(
            SecretBytes::new(
                vec![0xff, 0x00],
                DEFAULT_MAX_SECRET_BYTES,
                &sref(),
            )
            .unwrap(),
        );
        assert_eq!(b.repr(), SecretRepr::Bytes);
        assert_eq!(b.as_utf8(), None);
        assert_eq!(b.expose_bytes(), &[0xff, 0x00]);
    }

    #[test]
    fn resolved_secret_debug_redacts_material_but_shows_metadata() {
        let r = ResolvedSecret::new(SecretMaterial::Utf8(utf8("topsecret")))
            .with_provider_version("v7")
            .with_renewable(true);
        let shown = format!("{r:?}");
        assert!(!shown.contains("topsecret"));
        assert!(shown.contains("REDACTED"));
        assert!(shown.contains("v7"));
        assert!(shown.contains("renewable: true"));
    }

    #[test]
    fn getters_expose_metadata_read_only() {
        let group = ResolutionGroup::next();
        let r = ResolvedSecret::new(SecretMaterial::Utf8(utf8("x")))
            .in_group(group)
            .with_provider_version("v1")
            .with_renewable(true);
        assert_eq!(r.resolution_group(), Some(group));
        assert_eq!(r.provider_version(), Some("v1"));
        assert!(r.renewable());
        assert_eq!(r.expires_at(), None);
    }
}
