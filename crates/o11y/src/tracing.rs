pub use tracing::{debug, error, info, instrument, span, trace, warn, Level};

pub fn record_err<E: std::fmt::Display>(err: &E) {
    tracing::Span::current().record("error", &tracing::field::display(err));
}

/// Wrap a future to log start/finish and duration.
pub async fn timed<F, T>(label: &str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let start = std::time::Instant::now();
    let _span = tracing::span!(Level::DEBUG, "timed", %label).entered();
    let out = fut.await;
    let dur = start.elapsed();
    tracing::debug!(%label, ms = dur.as_millis() as u64, "timed.future.complete");
    out
}
