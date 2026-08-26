use compressor_xbt::storage::ChainDB;
use miette::{Context, IntoDiagnostic};

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &super::Config, _args: &Args) -> miette::Result<()> {
    super::common::setup_tracing(&config.logging)?;
    super::common::setup_os_signal_hooks()?;

    let chain_db = ChainDB::open(
        config.chain_db.clone(),
        config.sync.network,
        config.sync.first_rune_height.unwrap_or_default(),
        config.sync.first_inscription_height.unwrap_or_default(),
        config.sync.jubilee_height,
        config.sync.utxos_in_memory.unwrap_or(false),
    )
    .into_diagnostic()
    .context("opening chaindb")?;

    compressor_xbt::sync::pipeline(&config.sync, chain_db, &None, None)
        .into_diagnostic()
        .context("initialising sync pipeline")?
        .block();

    Ok(())
}
