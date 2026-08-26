use clap::{Parser, Subcommand};
use common::LoggingConfig;
use compressor_xbt::storage::options::ChainDBConfig;
use miette::{Context, IntoDiagnostic, Result};
use serde::Deserialize;
use tracing::info;

mod common;
mod daemon;
mod mempool;
mod rollback;
mod serve;
mod sync;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Debug, Subcommand)]
enum Command {
    Sync(sync::Args),
    Serve(serve::Args),
    /// Sync and Serve
    Daemon(daemon::Args),
    /// Sync, Serve and Mempool
    Mempool(mempool::Args),
    Rollback(rollback::Args),
}

#[derive(Debug, Parser)]
#[clap(name = "CompressorXBT")]
#[clap(bin_name = "compressor-xbt")]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    config: Option<std::path::PathBuf>,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub logging: LoggingConfig,
    pub chain_db: ChainDBConfig,
    pub sync: compressor_xbt::sync::Config,
    pub serve: compressor_xbt::serve::Config,
}

impl Config {
    pub fn new(explicit_file: &Option<std::path::PathBuf>) -> Result<Self, config::ConfigError> {
        let mut s = config::Config::builder();

        // file in the working dir
        s = s.add_source(config::File::with_name("compressor-xbt.toml").required(false));

        // if an explicit file was passed, then we load it as mandatory
        if let Some(explicit) = explicit_file.as_ref().and_then(|x| x.to_str()) {
            s = s.add_source(config::File::with_name(explicit).required(true));
        }

        // finally, we use env vars to make some last-step overrides
        s = s.add_source(config::Environment::with_prefix("COMPR").separator("_"));

        s.build()?.try_deserialize()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let config = Config::new(&args.config)
        .into_diagnostic()
        .context("parsing configuration")?;

    info!("running with config: {config:?}");

    match args.command {
        Command::Sync(x) => sync::run(&config, &x)?,
        Command::Serve(x) => serve::run(&config, &x).await?,
        Command::Daemon(x) => daemon::run(&config, &x).await?,
        Command::Rollback(x) => rollback::run(&config, &x)?,
        Command::Mempool(x) => mempool::run(&config, &x).await?,
    };

    Ok(())
}
