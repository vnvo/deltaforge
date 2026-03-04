//! Unified durable storage abstraction for DeltaForge operational runtime state.
//!
//! # Architecture
//!
//! [`StorageBackend`] exposes four primitives:
//! - **KV** - mutable point-in-time state with optional TTL (checkpoints, FSM, leases, dedup)
//! - **Log** - append-only monotonic history with global sequence (schema registry)
//! - **Slot** - mutable record with compare-and-swap (snapshot cursors, leader election)
//! - **Queue** - ordered bounded FIFO (quarantine buffer, DLQ)
//!
//! Two implementations are provided: [`MemoryStorageBackend`] (testing) and
//! [`SqliteStorageBackend`] (single-node production). A PostgreSQL backend
//! is planned for HA deployments.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

pub mod adapters;
pub mod memory;
pub mod sqlite;
pub mod postgres;

pub use memory::MemoryStorageBackend;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStorageBackend;
#[cfg(feature = "postgres")]
pub use postgres::PostgresStorageBackend;

// Adapter re-exports for ergonomic top-level imports
pub use adapters::BackendCheckpointStore;
pub use adapters::DurableSchemaRegistry;

/// Unified storage backend trait.
///
/// All four primitives operate within a `(ns, key)` address space.
/// Namespaces are enforced by convention — see the namespace table in the spec.
#[async_trait]
pub trait StorageBackend: Send + Sync + std::fmt::Debug {
    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>>;
    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()>;
    /// Store with TTL. Lazy expiry on read + periodic sweep.
    async fn kv_put_with_ttl(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
        ttl_secs: u64,
    ) -> Result<()>;
    /// Returns `true` if the key existed.
    async fn kv_delete(&self, ns: &str, key: &str) -> Result<bool>;
    /// List keys, optionally filtered by prefix.
    async fn kv_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>>;

    /// Append a value; returns the **global** monotonic sequence number.
    async fn log_append(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64>;
    async fn log_list(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Vec<(u64, Vec<u8>)>>;
    /// Returns entries with seq > since_seq.
    async fn log_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>>;
    async fn log_latest(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>>;

    /// Upsert a slot; returns the new version number.
    async fn slot_upsert(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<u64>;
    async fn slot_get(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>>;
    /// Compare-and-swap. Returns `false` on version mismatch (not an error).
    async fn slot_cas(
        &self,
        ns: &str,
        key: &str,
        expected_version: u64,
        state: &[u8],
    ) -> Result<bool>;
    /// Returns `true` if the slot existed.
    async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool>;

    /// Push a value; returns the entry id.
    async fn queue_push(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64>;
    /// Peek at up to `limit` oldest entries without consuming them.
    async fn queue_peek(
        &self,
        ns: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>>;
    /// Acknowledge (delete) all entries with id <= up_to_id. Returns count deleted.
    async fn queue_ack(
        &self,
        ns: &str,
        key: &str,
        up_to_id: u64,
    ) -> Result<usize>;
    async fn queue_len(&self, ns: &str, key: &str) -> Result<u64>;
    /// Drop the oldest `count` entries. Returns count actually dropped.
    async fn queue_drop_oldest(
        &self,
        ns: &str,
        key: &str,
        count: usize,
    ) -> Result<usize>;
}

pub type ArcStorageBackend = Arc<dyn StorageBackend>;
