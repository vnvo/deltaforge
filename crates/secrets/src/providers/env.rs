//! Environment-variable resolver. Resolves exactly the named variable and
//! nothing else: it never enumerates, lists, or logs the environment, and never
//! includes a value in an error.
//!
//! UTF-8 is the native representation of an environment value. A `Bytes` request
//! returns the UTF-8 bytes of the value, but environment variables cannot safely
//! carry arbitrary binary material (the OS models them as C strings and they are
//! commonly copied through shells), so binary secrets belong in files or Vault.
//!
//! There is no provider version and no atomic-provenance claim: each variable is
//! an independent value. Environment changes are not watched; a process restart is
//! required to pick up a new value.

use std::ffi::OsString;

use async_trait::async_trait;

use crate::error::{ReferenceOption, SecretError};
use crate::material::{DEFAULT_MAX_SECRET_BYTES, SecretBytes, SecretString};
use crate::reference::{SecretProvider, SecretReference, SecretRepr};
use crate::resolved::{ResolvedSecret, SecretMaterial};
use crate::resolver::SecretResolver;

/// How a single environment variable is looked up. Injectable so the resolver
/// reads *only* the requested name and so tests need not mutate process
/// environment (which is `unsafe` on edition 2024 and racy across threads).
type Lookup = Box<dyn Fn(&str) -> Option<OsString> + Send + Sync>;

/// Resolves `SecretProvider::Env` references.
pub struct EnvResolver {
    lookup: Lookup,
    limit: usize,
}

impl EnvResolver {
    /// Resolve against the real process environment.
    pub fn from_process() -> Self {
        Self {
            lookup: Box::new(|name| std::env::var_os(name)),
            limit: DEFAULT_MAX_SECRET_BYTES,
        }
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Construct with a caller-supplied single-name lookup. Test-only: it exists so
    /// tests need not mutate process environment. The closure must resolve only the
    /// name it is given and never enumerate the environment.
    #[cfg(test)]
    pub(crate) fn with_lookup(lookup: Lookup, limit: usize) -> Self {
        Self { lookup, limit }
    }
}

#[async_trait]
impl SecretResolver for EnvResolver {
    async fn resolve(
        &self,
        reference: &SecretReference,
    ) -> Result<ResolvedSecret, SecretError> {
        if reference.provider != SecretProvider::Env {
            return Err(SecretError::UnsupportedProvider(reference.safe()));
        }
        // An env reference addresses a whole variable: a selector or version is
        // meaningless here and must not be silently ignored (a caller could believe
        // a selected field or pinned version is enforced when it is not).
        if reference.selector.is_some() {
            return Err(SecretError::UnsupportedReferenceOption {
                reference: reference.safe(),
                option: ReferenceOption::Selector,
            });
        }
        if reference.version.is_some() {
            return Err(SecretError::UnsupportedReferenceOption {
                reference: reference.safe(),
                option: ReferenceOption::Version,
            });
        }
        // Present? (absence and non-UTF-8 are distinct, typed failures)
        let raw = (self.lookup)(&reference.location)
            .ok_or_else(|| SecretError::NotFound(reference.safe()))?;
        let value = raw
            .to_str()
            .ok_or_else(|| SecretError::NotUtf8(reference.safe()))?;
        if value.is_empty() {
            return Err(SecretError::Empty(reference.safe()));
        }
        let material = match reference.repr {
            SecretRepr::Utf8 => SecretMaterial::Utf8(SecretString::new(
                value.to_owned(),
                self.limit,
                reference,
            )?),
            SecretRepr::Bytes => SecretMaterial::Bytes(SecretBytes::new(
                value.as_bytes().to_vec(),
                self.limit,
                reference,
            )?),
        };
        // No provider version, no resolution group: independent single value.
        Ok(ResolvedSecret::new(material))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn resolver_with(vars: &[(&str, &str)], limit: usize) -> EnvResolver {
        let map: HashMap<String, OsString> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), OsString::from(*v)))
            .collect();
        EnvResolver::with_lookup(
            Box::new(move |name| map.get(name).cloned()),
            limit,
        )
    }

    fn env_ref(name: &str) -> SecretReference {
        SecretReference::new(SecretProvider::Env, name)
    }

    #[tokio::test]
    async fn missing_variable_fails_closed() {
        let r = resolver_with(&[], DEFAULT_MAX_SECRET_BYTES);
        let err = r.resolve(&env_ref("DF_ABSENT")).await.unwrap_err();
        assert!(matches!(err, SecretError::NotFound(_)));
    }

    #[tokio::test]
    async fn empty_variable_fails_closed() {
        let r = resolver_with(&[("DF_EMPTY", "")], DEFAULT_MAX_SECRET_BYTES);
        let err = r.resolve(&env_ref("DF_EMPTY")).await.unwrap_err();
        assert!(matches!(err, SecretError::Empty(_)));
    }

    #[tokio::test]
    async fn resolves_utf8_value() {
        let r =
            resolver_with(&[("DF_PASS", "hunter2")], DEFAULT_MAX_SECRET_BYTES);
        let secret = r.resolve(&env_ref("DF_PASS")).await.unwrap();
        assert_eq!(secret.material().as_utf8(), Some("hunter2"));
        // Independent value: no provenance group.
        assert_eq!(secret.resolution_group(), None);
    }

    #[tokio::test]
    async fn resolves_bytes_as_utf8_bytes() {
        let r = resolver_with(&[("DF_KEY", "abc")], DEFAULT_MAX_SECRET_BYTES);
        let secret = r
            .resolve(&env_ref("DF_KEY").with_repr(SecretRepr::Bytes))
            .await
            .unwrap();
        assert_eq!(secret.material().expose_bytes(), b"abc");
    }

    #[tokio::test]
    async fn oversized_value_fails_closed() {
        let r = resolver_with(&[("DF_BIG", "abcdefgh")], 4);
        let err = r.resolve(&env_ref("DF_BIG")).await.unwrap_err();
        assert!(matches!(err, SecretError::SizeExceeded { limit: 4, .. }));
    }

    #[tokio::test]
    async fn selector_on_env_reference_is_rejected() {
        let r = resolver_with(&[("DF_X", "v")], DEFAULT_MAX_SECRET_BYTES);
        let err = r
            .resolve(&env_ref("DF_X").with_selector("field"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SecretError::UnsupportedReferenceOption {
                option: crate::error::ReferenceOption::Selector,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn version_on_env_reference_is_rejected() {
        let r = resolver_with(&[("DF_X", "v")], DEFAULT_MAX_SECRET_BYTES);
        let err = r
            .resolve(&env_ref("DF_X").with_version("7"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SecretError::UnsupportedReferenceOption {
                option: crate::error::ReferenceOption::Version,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn value_never_appears_in_errors() {
        let secret_value = "S3NT1NEL-env";
        let r = resolver_with(&[("DF_BIG", secret_value)], 4);
        let err = r.resolve(&env_ref("DF_BIG")).await.unwrap_err();
        assert!(!format!("{err}").contains(secret_value));
        assert!(!format!("{err:?}").contains(secret_value));
    }
}
