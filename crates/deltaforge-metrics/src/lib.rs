use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::{fmt, EnvFilter};

pub fn init() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).init();
}

pub fn install_prometheus(addr: &str) {
    let builder = PrometheusBuilder::new();
    let _ = builder
        .with_http_listener(addr.parse::<SocketAddr>().expect("metrics addr"))
        .install()
        .expect("prom installed");
}
