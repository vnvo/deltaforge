use std::sync::Once;
use tracing_log::LogTracer;
use tracing_subscriber::{
    EnvFilter, Layer, Registry, fmt, layer::SubscriberExt,
};

static INIT: Once = Once::new();

#[derive(Clone, Debug)]
pub struct Config {
    /// Either a simple level like "info" or a full EnvFilter string
    /// e.g. "info,deltaforge=debug,rdkafka=warn".
    pub level: Option<String>,
    /// Emit logs as JSON lines when true; otherwise pretty text.
    pub json: bool,
    /// Include file/line/target info in logs.
    pub with_targets: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            level: Some("info".to_owned()),
            json: true,
            with_targets: false,
        }
    }
}

pub fn init(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    INIT.call_once(|| {
        let _ = LogTracer::init();

        let env = std::env::var("RUST_LOG").ok();
        let level = cfg.level.clone().or(env).unwrap_or_else(|| "info".into());

        let filter = EnvFilter::try_from_env("RUST_LOG")
            .or_else(|_| EnvFilter::try_new(level))
            .unwrap_or_else(|_| EnvFilter::new("info"));

        let fmt_layer = if cfg.json {
            fmt::layer()
                .with_target(cfg.with_targets)
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .boxed()
        } else {
            fmt::layer()
                .with_target(cfg.with_targets)
                .with_ansi(true)
                .boxed()
        };

        let subscriber = Registry::default().with(filter).with(fmt_layer);
        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global tracing subscriber");
    });
    Ok(())
}
