//! DeltaForge secret model and built-in providers: the connector-agnostic core of
//! the secrets capability - reference descriptors, protected material,
//! resolved-secret metadata, the resolver abstraction, structured-record
//! selection, the atomic connector-level `CredentialSet` boundary - plus the
//! environment and file providers and the composite resolver that dispatches to
//! them.
//!
//! Deliberately **not** here yet: Vault resolution (a later slice behind an
//! optional feature or a separate provider crate, so env/file users never pull its
//! runtime), connector credential specifications, config migration, live
//! rotation/watchers, and mTLS integration. Kubernetes is not a provider; it
//! injects via env (`secretKeyRef`) and projected-volume files.
//!
//! Change-detection fingerprints are intentionally absent: a fingerprint that
//! meets the security claim needs a keyed cryptographic hash, which belongs with
//! the rotation slice that consumes it, not the core model.
//!
//! Security invariants enforced here:
//! - Only [`SecretReference`] is serializable; resolved material implements no
//!   value-revealing `Debug`/`Display`/`Serialize`.
//! - Access to raw material is only via explicit `expose_*` accessors.
//! - Material is best-effort zeroized on drop and is not `Clone`.
//! - Errors identify a [`SafeRef`] but never carry resolved content, file bytes,
//!   environment values, or free-form provider text.
//! - A multi-field [`CredentialSet`] proves single-read provenance by an explicit
//!   [`ResolutionGroup`], never by comparing (possibly absent) versions, and
//!   provenance is never combined across providers or files.

mod credential_set;
mod error;
mod material;
mod providers;
mod reference;
mod registry;
mod resolved;
mod resolver;
mod structured;
mod watch;

pub use credential_set::CredentialSet;
pub use error::{
    InconsistencyKind, ProviderFailureKind, ReferenceOption, SecretError,
};
pub use material::{
    DEFAULT_MAX_SECRET_BYTES, SecretBytes, SecretString, check_size,
};
pub use providers::{
    EnvResolver, FileIdentity, FileMode, FilePolicy, FileResolver,
};
pub use reference::{SafeRef, SecretProvider, SecretReference, SecretRepr};
pub use registry::CompositeResolver;
pub use resolved::{ResolutionGroup, ResolvedSecret, SecretMaterial};
pub use resolver::{
    CredentialFieldRequest, SecretResolver, check_no_duplicate_fields,
};
pub use structured::{StructuredSecret, credential_set_from_record};
pub use watch::{FileWatcher, RotationOutcome};
