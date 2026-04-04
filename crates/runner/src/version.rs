/// Git-derived version (includes tags like 0.1.0-beta.1, 0.1.0-rc.2, etc.)
pub const GIT_VERSION: &str = env!("GIT_VERSION");

/// Short git commit hash
pub const GIT_HASH: &str = env!("GIT_HASH");

/// Build date (UTC)
pub const BUILD_DATE: &str = env!("BUILD_DATE");

/// Build target triple
pub const BUILD_TARGET: &str = env!("BUILD_TARGET");

/// Version string for --version (compile-time)
pub const VERSION: &str =
    concat!(env!("GIT_VERSION"), " (", env!("GIT_HASH"), ")");

/// Full version info for startup banner
pub fn startup_banner() -> String {
    format!(
        r#"
  ____       _ _        _____
 |  _ \  ___| | |_ __ _|  ___|__  _ __ __ _  ___
 | | | |/ _ \ | __/ _` | |_ / _ \| '__/ _` |/ _ \
 | |_| |  __/ | || (_| |  _| (_) | | | (_| |  __/
 |____/ \___|_|\__\__,_|_|  \___/|_|  \__, |\___|
                                       |___/
  {version}  ({hash})  built {date}  {target}
"#,
        version = GIT_VERSION,
        hash = GIT_HASH,
        date = BUILD_DATE,
        target = BUILD_TARGET,
    )
}

/// Print runtime info after startup (API addr, storage, pipelines).
pub fn print_runtime_info(
    api_addr: &str,
    metrics_addr: &str,
    storage: &str,
    pipeline_count: usize,
) {
    eprintln!("  API:       http://{api_addr}");
    eprintln!("  Metrics:   http://{metrics_addr}/metrics");
    eprintln!("  Storage:   {storage}");
    eprintln!("  Pipelines: {pipeline_count}");
    eprintln!();
}
