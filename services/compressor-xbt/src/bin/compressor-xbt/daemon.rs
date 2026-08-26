use compressor_xbt::storage::ChainDB;
use miette::{Context, IntoDiagnostic};
use tracing::{error, info};

#[derive(Debug, clap::Args)]
pub struct Args {}

pub async fn run(config: &super::Config, _args: &Args) -> miette::Result<()> {
    super::common::setup_tracing(&config.logging)?;
    super::common::setup_os_signal_hooks()?;

    info!("running daemon with config: {:?}", config);

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

    let sync = compressor_xbt::sync::pipeline(&config.sync, chain_db.clone(), &None, None)
        .into_diagnostic()
        .context("initialising sync pipeline")?;

    // Monitor sync pipeline health in background
    let sync_monitor = tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            // Check if any stage has ended (using gasket's should_stop logic)
            let should_stop = sync.0.iter().any(|tether| {
                matches!(
                    tether.check_state(),
                    gasket::runtime::TetherState::Alive(gasket::runtime::StagePhase::Ended)
                        | gasket::runtime::TetherState::Dropped
                )
            });

            if should_stop {
                error!("sync pipeline has stopped, shutting down daemon");
                std::process::exit(1);
            }
        }
    });

    let serve_result = compressor_xbt::serve::serve(&config.serve, chain_db, None).await;

    // If we get here, gRPC server stopped
    sync_monitor.abort();

    serve_result
        .into_diagnostic()
        .context("gRPC server stopped")?;

    info!("compressor-xbt is stopping...");

    Ok(())
}
