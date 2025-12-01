pub mod logging;
pub mod df_metrics;
pub mod panic;
pub mod tracing;

/// Top-level config for observability.
#[derive(Clone, Debug)]
pub struct O11yConfig {
    pub logging: logging::Config,
    pub metrics: df_metrics::Config,
    pub install_panic_hook: bool,
}

impl Default for O11yConfig {
    fn default() -> Self {
        Self {
            logging: logging::Config::default(),
            metrics: df_metrics::Config::default(),
            install_panic_hook: true,
        }
    }
}

pub fn init_all(cfg: &O11yConfig) -> Result<(), Box<dyn std::error::Error>> {
    logging::init(&cfg.logging)?;
    df_metrics::init(&cfg.metrics)?;
    if cfg.install_panic_hook {
        panic::install_hook();
    }
    Ok(())
}
