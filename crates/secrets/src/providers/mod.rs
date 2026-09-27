//! Built-in secret providers: environment variables and mounted files. These have
//! no runtime dependencies beyond std and serde_json (for structured JSON files).
//! Vault is deliberately not here; it arrives in a later slice behind an optional
//! feature or a separate provider crate, so env/file users never pull that runtime.

pub mod env;
pub mod file;

pub use env::EnvResolver;
pub use file::{FileIdentity, FileMode, FilePolicy, FileResolver};
