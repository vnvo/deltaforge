use axum::{Router, routing::get};
use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
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
        let builder = PrometheusBuilder::new();
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
    describe_gauge!(
        "deltaforge_running_pipeline",
        Unit::Count,
        "Pipelines in the running state"
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
        "deltaforge_sink_batch_total",
        Unit::Count,
        "Total number of individual batches sent to sink"
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
}
