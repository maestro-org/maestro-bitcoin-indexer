use compressor_xbt::storage::ChainDB;
use miette::{Context, IntoDiagnostic};
use tracing::{info, warn};

#[derive(Debug, clap::Args)]
pub struct Args {
    block_height: u64,
}

pub fn run(config: &super::Config, args: &Args) -> miette::Result<()> {
    super::common::setup_tracing(&config.logging)?;
    super::common::setup_os_signal_hooks()?;

    let mut chain_db = ChainDB::open(
        config.chain_db.clone(),
        config.sync.network,
        config.sync.first_rune_height.unwrap_or_default(),
        config.sync.first_inscription_height.unwrap_or_default(),
        config.sync.jubilee_height,
        config.sync.utxos_in_memory.unwrap_or(false),
    )
    .into_diagnostic()
    .context("opening chaindb")?;

    for _ in 0..3 {
        warn!(
            "sleeping 10 seconds then rolling back chaindb to height: {}",
            args.block_height
        );
    }

    std::thread::sleep(std::time::Duration::from_secs(10));

    info!("attempting to rolling back");

    chain_db
        .large_forced_rollback(args.block_height)
        .into_diagnostic()
        .context("rolling back chaindb")?;

    Ok(())
}
