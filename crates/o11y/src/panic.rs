use once_cell::sync::OnceCell;
use std::{panic, thread};
use tracing::{error, info};
use metrics::counter;

static INSTALLED: OnceCell<()> = OnceCell::new();

/// Install a panic hook that logs and increments a counter.
pub fn install_hook() {
    if INSTALLED.set(()).is_err() {
        return; // already installed
    }

    let prev = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        let thread = thread::current();
        let name = thread.name().unwrap_or("<unnamed>");
        let payload = match panic_info.payload().downcast_ref::<&str>() {
            Some(s) => *s,
            None => match panic_info.payload().downcast_ref::<String>() {
                Some(s) => s.as_str(),
                None => "<non-string panic payload>",
            },
        };

        let location = panic_info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "<unknown>".into());

        error!(%name, %location, %payload, "panic captured");
        counter!("deltaforge_panics_total").increment(1);

        // Call previous hook to keep default printing/backtraces, if any.
        prev(panic_info);
    }));

    info!("panic hook installed");
}
