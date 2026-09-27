//! Redacted error type. Errors identify a **safe reference** (provider /
//! location / selector / purpose) and use **typed, non-secret reason codes** -
//! there is no free-form displayable string that a caller could accidentally
//! populate with a URL, header, response body, token, or credential fragment.

use thiserror::Error;

use crate::reference::SafeRef;

/// Non-secret classification of a provider-level failure. Providers map their SDK
/// errors onto one of these; the underlying detail is never rendered here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderFailureKind {
    Unavailable,
    Unauthorized,
    Forbidden,
    InvalidResponse,
    Tls,
    Timeout,
    Other,
}

impl std::fmt::Display for ProviderFailureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ProviderFailureKind::Unavailable => "unavailable",
            ProviderFailureKind::Unauthorized => "unauthorized",
            ProviderFailureKind::Forbidden => "forbidden",
            ProviderFailureKind::InvalidResponse => "invalid response",
            ProviderFailureKind::Tls => "tls error",
            ProviderFailureKind::Timeout => "timeout",
            ProviderFailureKind::Other => "other",
        };
        f.write_str(s)
    }
}

/// Non-secret classification of a credential-set inconsistency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InconsistencyKind {
    /// Fields came from different rotation generations (or provenance is unknown).
    GenerationMismatch,
    /// A field's representation does not match what the connector requires.
    RepresentationMismatch,
    /// A certificate and its private key do not correspond.
    CertKeyMismatch,
    /// A field is past its expiry.
    ExpiredField,
}

impl std::fmt::Display for InconsistencyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            InconsistencyKind::GenerationMismatch => "generation mismatch",
            InconsistencyKind::RepresentationMismatch => {
                "representation mismatch"
            }
            InconsistencyKind::CertKeyMismatch => "certificate/key mismatch",
            InconsistencyKind::ExpiredField => "expired field",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Error)]
pub enum SecretError {
    /// No secret found at the reference.
    #[error("secret not found: {0}")]
    NotFound(SafeRef),

    /// A provider-level failure, classified by a typed reason (never free text).
    #[error("secret provider error for {reference}: {kind}")]
    Provider {
        reference: SafeRef,
        kind: ProviderFailureKind,
    },

    /// The resolved (or to-be-read) material exceeds the configured size limit.
    #[error("secret {reference} exceeds size limit ({limit} bytes)")]
    SizeExceeded { reference: SafeRef, limit: usize },

    /// Material was requested as UTF-8 but is not valid UTF-8.
    #[error("secret {0} is not valid UTF-8 (expected utf8 representation)")]
    NotUtf8(SafeRef),

    /// A structured record has no field matching the selector, or a selector was
    /// required but absent.
    #[error("structured secret {reference}: selector {selector:?} not found")]
    SelectorMissing {
        reference: SafeRef,
        selector: Option<String>,
    },

    /// A required credential-set field was missing in the candidate generation.
    #[error("credential set missing required field {0:?}")]
    MissingField(String),

    /// A field name was inserted twice into a credential set.
    #[error("credential set has duplicate field {0:?}")]
    DuplicateField(String),

    /// The candidate credential set is not internally consistent, by typed
    /// reason. `field` (a non-secret field name) is included when applicable.
    #[error("credential set inconsistent: {kind} (field: {field:?})")]
    Inconsistent {
        kind: InconsistencyKind,
        field: Option<String>,
    },
}
