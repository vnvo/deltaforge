//! DeltaForge secret model: the connector-agnostic core of the secrets
//! capability - reference descriptors, protected material, resolved-secret
//! metadata, the resolver abstraction, structured-record selection, and the
//! atomic connector-level `CredentialSet` boundary.
//!
//! This first slice deliberately does **not**: resolve real env/file/Vault
//! material, migrate connector configs, reconnect sources/sinks, change
//! authentication behavior, or add Kubernetes/Vault dependencies. Those are
//! later, separately reviewed slices; providers arrive behind optional features
//! so environment and file users never pull provider runtimes.
//!
//! Change-detection fingerprints are intentionally absent here: a fingerprint
//! that meets the security claim needs a keyed cryptographic hash, which belongs
//! with the rotation slice that consumes it, not the core model.
//!
//! Security invariants enforced here:
//! - Only [`SecretReference`] is serializable; resolved material implements no
//!   value-revealing `Debug`/`Display`/`Serialize`.
//! - Access to raw material is only via explicit `expose_*` accessors.
//! - Material is best-effort zeroized on drop and is not `Clone`.
//! - Errors identify a [`SafeRef`] but never carry resolved content or free-form
//!   provider text.
//! - A multi-field [`CredentialSet`] proves single-generation provenance by an
//!   explicit [`ResolutionGroup`], never by comparing (possibly absent) versions.

mod credential_set;
mod error;
mod material;
mod reference;
mod resolved;
mod resolver;
mod structured;

pub use credential_set::CredentialSet;
pub use error::{InconsistencyKind, ProviderFailureKind, SecretError};
pub use material::{
    DEFAULT_MAX_SECRET_BYTES, SecretBytes, SecretString, check_size,
};
pub use reference::{SafeRef, SecretProvider, SecretReference, SecretRepr};
pub use resolved::{ResolutionGroup, ResolvedSecret, SecretMaterial};
pub use resolver::{
    CredentialFieldRequest, SecretResolver, check_no_duplicate_fields,
};
pub use structured::{StructuredSecret, credential_set_from_record};
