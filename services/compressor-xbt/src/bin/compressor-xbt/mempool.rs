use compressor_xbt::storage::ChainDB;
use miette::{Context, IntoDiagnostic};
use tracing::{error, info};

#[derive(Debug, clap::Args)]
pub struct Args {}

pub async fn run(config: &super::Config, _args: &Args) -> miette::Result<()> {
    super::common::setup_tracing(&config.logging)?;
    super::common::setup_os_signal_hooks()?;

    info!("running daemon + mempool with config: {:?}", config);

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

    // Create shared mempool cache
    let shared_cache = compressor_xbt::mempool::create_shared_cache();

    let sync = compressor_xbt::sync::pipeline(
        &config.sync,
        chain_db.clone(),
        &None,
        Some(shared_cache.clone()),
    )
    .into_diagnostic()
    .context("initialising sync pipeline")?;

    let mempool = compressor_xbt::mempool::pipeline(
        &config.sync,
        chain_db.clone(),
        &None,
        shared_cache.clone(),
    )
    .into_diagnostic()
    .context("initialising mempool pipeline")?;

    // Monitor sync and mempool pipeline health in background
    let pipeline_monitor = tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            // Check if any sync stage has ended
            let sync_stopped = sync.0.iter().any(|tether| {
                matches!(
                    tether.check_state(),
                    gasket::runtime::TetherState::Alive(gasket::runtime::StagePhase::Ended)
                        | gasket::runtime::TetherState::Dropped
                )
            });

            // Check if any mempool stage has ended
            let mempool_stopped = mempool.0.iter().any(|tether| {
                matches!(
                    tether.check_state(),
                    gasket::runtime::TetherState::Alive(gasket::runtime::StagePhase::Ended)
                        | gasket::runtime::TetherState::Dropped
                )
            });

            if sync_stopped {
                error!("sync pipeline has stopped, shutting down daemon");
                std::process::exit(1);
            }

            if mempool_stopped {
                error!("mempool pipeline has stopped, shutting down daemon");
                std::process::exit(1);
            }
        }
    });

    let serve_result =
        compressor_xbt::serve::serve(&config.serve, chain_db, Some(shared_cache)).await;

    // If we get here, gRPC server stopped
    pipeline_monitor.abort();

    serve_result
        .into_diagnostic()
        .context("gRPC server stopped")?;

    info!("compressor-xbt is stopping...");

    Ok(())
}
