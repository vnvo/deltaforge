//! Sink implementations for DeltaForge.
//!
//! This crate provides sink implementations for writing CDC events to various
//! destinations. All sinks implement the `Sink` trait from `deltaforge_core`.
//!
//! # Available Sinks
//!
//! - **Kafka**: High-throughput message streaming with idempotent production
//! - **Redis**: Redis Streams for real-time event processing
//!
//! # Design Principles
//!
//! All sinks follow these patterns:
//!
//! - **Retry with backoff**: Transient failures trigger exponential backoff
//! - **Error classification**: Permanent vs transient errors for smart retry
//! - **Credential safety**: All connection strings are redacted in logs
//! - **Graceful shutdown**: Cancellation tokens respected throughout
//! - **Batch optimization**: Efficient batch operations where supported
//!
//! # Example
//!
//! ```ignore
//! use deltaforge_sinks::build_sinks;
//! use tokio_util::sync::CancellationToken;
//!
//! let cancel = CancellationToken::new();
//! let sinks = build_sinks(&pipeline_spec, cancel.clone())?;
//!
//! for sink in &sinks {
//!     sink.send(&event).await?;
//! }
//! ```

use std::sync::Arc;

use deltaforge_config::{PipelineSpec, SinkCfg};
use deltaforge_core::ArcDynSink;
use tokio_util::sync::CancellationToken;

pub mod kafka;
pub mod redis;

// Re-export sink types for direct usage
pub use kafka::KafkaSink;
pub use redis::RedisSink;

/// Build all sinks from a pipeline specification.
///
/// This function creates sink instances for each sink configuration in the
/// pipeline spec. All sinks share the same cancellation token for coordinated
/// shutdown.
///
/// # Arguments
///
/// * `ps` - Pipeline specification containing sink configurations
/// * `cancel` - Cancellation token for graceful shutdown
///
/// # Returns
///
/// A vector of arc-wrapped dynamic sink trait objects, or an error if any
/// sink fails to initialize.
///
/// # Errors
///
/// Returns an error if:
/// - A sink configuration is invalid (e.g., malformed connection string)
/// - A required resource cannot be accessed (e.g., Kafka broker unreachable
///   during initial metadata fetch)
///
/// # Example
///
/// ```ignore
/// use deltaforge_sinks::build_sinks;
/// use tokio_util::sync::CancellationToken;
///
/// let cancel = CancellationToken::new();
/// let sinks = build_sinks(&spec, cancel.clone())?;
///
/// // Send events to all sinks
/// for event in events {
///     for sink in &sinks {
///         sink.send(&event).await?;
///     }
/// }
///
/// // Graceful shutdown
/// cancel.cancel();
/// ```
pub fn build_sinks(
    ps: &PipelineSpec,
    cancel: CancellationToken,
) -> anyhow::Result<Vec<ArcDynSink>> {
    ps.spec
        .sinks
        .iter()
        .map(|s| {
            let sink: ArcDynSink = match s {
                SinkCfg::Kafka(kafka_cfg) => {
                    let sink = KafkaSink::new(kafka_cfg, cancel.clone())?;
                    Arc::new(sink) as ArcDynSink
                }
                SinkCfg::Redis(redis_cfg) => {
                    let sink = RedisSink::new(redis_cfg, cancel.clone())?;
                    Arc::new(sink) as ArcDynSink
                }
            };
            Ok(sink)
        })
        .collect()
}

/// Build a single sink from configuration.
///
/// Lower-level API for when you need to create a specific sink type
/// without going through the full pipeline spec.
///
/// # Example
///
/// ```ignore
/// let redis_sink = build_sink(&SinkCfg::Redis(redis_cfg), cancel)?;
/// ```
pub fn build_sink(
    cfg: &SinkCfg,
    cancel: CancellationToken,
) -> anyhow::Result<ArcDynSink> {
    let sink: ArcDynSink = match cfg {
        SinkCfg::Kafka(kafka_cfg) => {
            Arc::new(KafkaSink::new(kafka_cfg, cancel)?) as ArcDynSink
        }
        SinkCfg::Redis(redis_cfg) => {
            Arc::new(RedisSink::new(redis_cfg, cancel)?) as ArcDynSink
        }
    };
    Ok(sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_config::{KafkaSinkCfg, RedisSinkCfg};

    // Note: Integration tests for sinks require running infrastructure
    // (Kafka, Redis) and are in the tests/ directory.

    #[test]
    fn module_exports_are_available() {
        // Compile-time check that re-exports work
        let _: fn(
            &KafkaSinkCfg,
            CancellationToken,
        ) -> anyhow::Result<KafkaSink> = KafkaSink::new;
        let _: fn(
            &RedisSinkCfg,
            CancellationToken,
        ) -> anyhow::Result<RedisSink> = RedisSink::new;
    }
}