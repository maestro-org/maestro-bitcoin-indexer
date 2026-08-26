use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use tracing::warn;
use tracing::{debug, Level};
use tracing_subscriber::fmt;
use tracing_subscriber::{filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

#[serde_as]
#[derive(Deserialize, Default, Debug)]
pub struct LoggingConfig {
    #[serde_as(as = "Option<DisplayFromStr>")]
    max_level: Option<tracing::Level>,
}

pub fn setup_tracing(config: &LoggingConfig) -> miette::Result<()> {
    let level = config.max_level.unwrap_or(Level::INFO);

    let filter = Targets::new()
        .with_target("compressor_xbt", level)
        .with_target("gasket", level);

    let format = fmt::format()
        .with_level(true)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_ansi(false) // Remove colors and styling
        .without_time(); // Remove timestamps

    tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(level)
        .event_format(format)
        .finish()
        .with(filter)
        .init();

    Ok(())
}

pub fn setup_os_signal_hooks() -> miette::Result<()> {
    // Workaround for the broken OS signal handling in Gasket.
    // Gasket registers an OS signal hook, but it does not work.
    debug!("Registering OS signal hook...");
    let term = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    for sig in signal_hook::consts::TERM_SIGNALS {
        signal_hook::flag::register(*sig, std::sync::Arc::clone(&term))
            .expect("[FAILURE] Can't register OS signal hook.");
    }
    debug!("[OK] Registered OS signal hook.");

    std::thread::spawn(move || loop {
        if term.load(std::sync::atomic::Ordering::Relaxed) {
            warn!("Received OS signal. Terminate application.");
            std::process::exit(128);
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    });

    Ok(())
}
