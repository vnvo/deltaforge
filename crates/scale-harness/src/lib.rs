//! Dense-catalog measurement harness (PR1 Layer 4).
//!
//! One implementation for two tiers: the CI tier (1K tables, structural
//! assertions only, see `tests/ci_tier.rs`) and the scale tier (the
//! `registry-scale` binary, 100K / 1M tables, numbers recorded, not asserted).
//!
//! - [`fixture`]: synthetic durable registries written through the real
//!   registration path, with a validated manifest for reuse.
//! - [`counting`]: a storage wrapper counting every primitive call and the
//!   bytes it returns, with optional key-level read attribution.
//! - [`scenarios`]: registry startup, cold/hot lookup, a working set larger
//!   than the cache, single-flight, history paging, cross-source isolation,
//!   and migration.
//! - [`live`]: time to first CDC event on a CDC-only restart against live
//!   PostgreSQL / MySQL containers (scale tier only).

pub mod counting;
pub mod fixture;
pub mod live;
pub mod memory;
pub mod scenarios;
