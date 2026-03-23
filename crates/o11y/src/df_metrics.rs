use axum::{Router, routing::get};
use metrics::{
    Unit, describe_counter, describe_gauge, describe_histogram, gauge,
};
use metrics_exporter_prometheus::{
    Matcher, PrometheusBuilder, PrometheusHandle,
};
use once_cell::sync::OnceCell;
use std::{net::SocketAddr, time::Duration};
use tokio::net::TcpListener;

static HANDLE: OnceCell<PrometheusHandle> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct Config {
    pub enable: bool,
    pub http_listener: Option<SocketAddr>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enable: true,
            http_listener: Some(([0, 0, 0, 0], 9000).into()),
        }
    }
}

pub fn init(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    if !cfg.enable {
        return Ok(());
    }

    if HANDLE.get().is_none() {
        // Latency buckets: 1 ms → 30 s, covering CDC e2e and sink/stage latency.
        let latency_buckets: &[f64] = &[
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            10.0, 30.0,
        ];
        // Batch-size buckets: 1 event up to max configured batch.
        let batch_buckets: &[f64] =
            &[1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0];

        let builder = PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Suffix("_latency_seconds".to_string()),
                latency_buckets,
            )
            .expect("invalid latency buckets")
            .set_buckets_for_metric(
                Matcher::Full("deltaforge_e2e_latency_seconds".to_string()),
                latency_buckets,
            )
            .expect("invalid e2e latency buckets")
            .set_buckets_for_metric(
                Matcher::Full("deltaforge_batch_events".to_string()),
                batch_buckets,
            )
            .expect("invalid batch buckets");
        let handle = builder
            .install_recorder()
            .expect("failed to install recorder");
        HANDLE.set(handle).ok();
    }

    if let Some(addr) = cfg.http_listener {
        tokio::spawn(async move {
            let router = Router::new().route("/metrics", get(metrics_handler));
            // Retry binding a few times in case of startup races (tests)
            let mut tries = 0;
            loop {
                match TcpListener::bind(addr).await {
                    Ok(l) => {
                        axum::serve(l, router).await.ok();
                        break;
                    }
                    Err(e) if tries < 5 => {
                        tries += 1;
                        tracing::warn!(error=%e, tries, "metrics listener bind failed; retrying");
                        tokio::time::sleep(Duration::from_millis(150)).await;
                    }
                    Err(e) => {
                        tracing::error!(error=%e, "metrics listener failed; giving up");
                        break;
                    }
                }
            }
        });
    }

    describe_metrics();

    Ok(())
}

/// Axum handler that renders the current metrics snapshot.
pub async fn metrics_handler() -> String {
    HANDLE
        .get()
        .map(|h| h.render())
        .unwrap_or_else(|| "# recorder not installed\n".into())
}

pub fn router_with_metrics() -> Router {
    Router::new().route("/metrics", get(metrics_handler))
}

pub fn describe_metrics() {
    describe_counter!(
        "deltaforge_pipelines_total",
        Unit::Count,
        "Total number of pipelines"
    );
    describe_counter!(
        "deltaforge_source_events_total",
        Unit::Count,
        "Total number of events received from source"
    );
    describe_counter!(
        "deltaforge_source_reconnects_total",
        Unit::Count,
        "Reconnects performed by a source reader"
    );
    describe_counter!(
        "deltaforge_source_errors_total",
        Unit::Count,
        "Fatal (non-retryable) source errors, labelled by kind"
    );
    describe_counter!(
        "deltaforge_sink_batch_total",
        Unit::Count,
        "Total number of batches successfully delivered to a sink"
    );
    describe_counter!(
        "deltaforge_sink_events_total",
        Unit::Count,
        "Total number of events sent to sink"
    );
    describe_histogram!(
        "deltaforge_stage_latency_seconds",
        Unit::Seconds,
        "Latency per pipeline stage."
    );
    describe_histogram!(
        "deltaforge_processor_latency_seconds",
        Unit::Seconds,
        "Per-processor latency for batch execution"
    );
    describe_histogram!(
        "deltaforge_sink_latency_seconds",
        Unit::Seconds,
        "Latency to deliver an event to a sink"
    );
    describe_histogram!(
        "deltaforge_batch_events",
        Unit::Count,
        "Distribution of events per batch"
    );
    describe_histogram!(
        "deltaforge_batch_bytes",
        Unit::Bytes,
        "Distribution of serialized batch sizes"
    );
    describe_counter!(
        "deltaforge_checkpoints_total",
        Unit::Count,
        "Total number of checkpoints committed"
    );
    describe_counter!(
        "deltaforge_sink_errors_total",
        Unit::Count,
        "Total number of sink delivery errors"
    );
    describe_counter!(
        "deltaforge_sink_routing_errors_total",
        Unit::Count,
        "Serialization or routing errors during sink send, labelled by kind"
    );
    describe_counter!(
        "deltaforge_sink_reconnects_total",
        Unit::Count,
        "Sink connections (re)established"
    );
    describe_counter!(
        "deltaforge_sink_bytes_total",
        Unit::Bytes,
        "Total bytes delivered to a sink (batch path)"
    );
    describe_counter!(
        "deltaforge_processor_events_in_total",
        Unit::Count,
        "Events entering a processor"
    );
    describe_counter!(
        "deltaforge_processor_events_out_total",
        Unit::Count,
        "Events leaving a processor after processing (drops reduce this vs in)"
    );
    describe_counter!(
        "deltaforge_kafka_produce_retries_total",
        Unit::Count,
        "Cumulative producer-level retries reported by rdkafka (absolute counter)"
    );
    describe_gauge!(
        "deltaforge_kafka_producer_queue_messages",
        Unit::Count,
        "Current number of messages in all rdkafka internal queues"
    );
    describe_gauge!(
        "deltaforge_kafka_producer_queue_bytes",
        Unit::Bytes,
        "Current bytes in all rdkafka internal queues"
    );
    describe_counter!(
        "deltaforge_schema_drift_detected",
        Unit::Count,
        "Number of batches where schema drift was detected"
    );
    describe_counter!(
        "deltaforge_pipeline_evolutions_total",
        Unit::Count,
        "Schema evolution events detected per pipeline (pipeline-level aggregate)"
    );
    describe_gauge!(
        "deltaforge_source_lag_seconds",
        Unit::Seconds,
        "Lag between the latest source event timestamp and wall clock time"
    );
    describe_gauge!(
        "deltaforge_build_info",
        Unit::Count,
        "Build metadata (version, commit, build_date labels); always 1"
    );
    describe_counter!(
        "deltaforge_snapshot_rows_total",
        Unit::Count,
        "Total rows emitted during initial snapshot, per pipeline and table"
    );

    describe_histogram!(
        "deltaforge_e2e_latency_seconds",
        Unit::Seconds,
        "End-to-end latency from first event's source timestamp to sink delivery"
    );
    describe_gauge!(
        "deltaforge_last_checkpoint_ts",
        Unit::Seconds,
        "Unix epoch seconds when the last checkpoint was committed; use time()-this for age"
    );
    describe_counter!(
        "deltaforge_processor_errors_total",
        Unit::Count,
        "Processor failures per pipeline and processor"
    );
    describe_counter!(
        "deltaforge_pipeline_pauses_total",
        Unit::Count,
        "Number of times a pipeline was paused"
    );
    describe_counter!(
        "deltaforge_pipeline_resumes_total",
        Unit::Count,
        "Number of times a pipeline was resumed"
    );

    // Schema sensing metrics
    describe_counter!(
        "deltaforge_schema_events_total",
        Unit::Count,
        "Total events observed by schema sensor"
    );
    describe_counter!(
        "deltaforge_schema_sensing_cache_hits_total",
        Unit::Count,
        "Schema sensing structure fingerprint cache hits (requires schema_sensing.enabled)"
    );
    describe_counter!(
        "deltaforge_schema_sensing_cache_misses_total",
        Unit::Count,
        "Schema sensing structure fingerprint cache misses (requires schema_sensing.enabled)"
    );
    describe_counter!(
        "deltaforge_source_schema_cache_hits_total",
        Unit::Count,
        "Source schema loader cache hits — table column definitions served from memory (labels: pipeline, source)"
    );
    describe_counter!(
        "deltaforge_source_schema_cache_misses_total",
        Unit::Count,
        "Source schema loader cache misses — triggered an information_schema fetch (labels: pipeline, source)"
    );
    describe_counter!(
        "deltaforge_schema_evolutions_total",
        Unit::Count,
        "Schema evolution events detected"
    );
    describe_gauge!(
        "deltaforge_schema_tables_total",
        Unit::Count,
        "Number of tables with detected schemas"
    );
    describe_gauge!(
        "deltaforge_schema_dynamic_maps_total",
        Unit::Count,
        "Number of paths classified as dynamic maps"
    );
    describe_histogram!(
        "deltaforge_schema_sensing_seconds",
        Unit::Seconds,
        "Time spent in schema sensing per batch"
    );
}

/// Set the `deltaforge_build_info` gauge with version metadata.
///
/// Call once at startup after [`init`]. The gauge is always 1.0; the
/// information is carried in the labels so it can be joined with other
/// metrics in Prometheus/Grafana.
pub fn set_build_info(
    version: &'static str,
    commit: &'static str,
    build_date: &'static str,
) {
    gauge!(
        "deltaforge_build_info",
        "version" => version,
        "commit" => commit,
        "build_date" => build_date,
    )
    .set(1.0);
}
