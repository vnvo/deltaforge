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

/// A reference option a given provider does not honor. Naming the option in a typed
/// way prevents a caller from believing an unenforced constraint is enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceOption {
    /// A structured-field selector.
    Selector,
    /// An immutable version pin.
    Version,
}

impl std::fmt::Display for ReferenceOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ReferenceOption::Selector => "selector",
            ReferenceOption::Version => "version",
        })
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

    /// A present-but-empty value (e.g. an empty env var or empty file) where a
    /// non-empty secret is required. Fails closed.
    #[error("secret {0} is present but empty")]
    Empty(SafeRef),

    /// The reference names a provider that no installed provider handles (e.g. a
    /// Vault reference before the Vault provider feature is installed).
    #[error("no provider installed for {0}")]
    UnsupportedProvider(SafeRef),

    /// A file reference resolved to something other than a regular file
    /// (directory, device, socket, ...).
    #[error("file secret {0} is not a regular file")]
    NotRegularFile(SafeRef),

    /// A file reference used a relative path; only absolute paths are accepted.
    #[error("file secret {0} must be an absolute path")]
    PathNotAbsolute(SafeRef),

    /// A symlink was encountered under a policy that forbids following it.
    #[error("file secret {0} is a symlink (rejected by policy)")]
    SymlinkRejected(SafeRef),

    /// A file reference resolved outside its configured trusted root.
    #[error("file secret {0} resolves outside the trusted root")]
    OutsideTrustedRoot(SafeRef),

    /// The file changed underneath the read (size/mtime moved between stat and
    /// the completed read). Fails closed for this resolution.
    #[error("file secret {0} was replaced during read")]
    ReplacedDuringRead(SafeRef),

    /// A structured-record file could not be parsed as the expected bounded
    /// object. The underlying parser message is deliberately **not** included, as
    /// it can echo file content.
    #[error("structured secret {0} is malformed")]
    MalformedRecord(SafeRef),

    /// The reference carries an option the resolving provider does not enforce
    /// (e.g. a selector or version on an environment reference). Fails closed so a
    /// caller never believes an unenforced constraint is applied.
    #[error("reference {reference} does not support option: {option}")]
    UnsupportedReferenceOption {
        reference: SafeRef,
        option: ReferenceOption,
    },
}
