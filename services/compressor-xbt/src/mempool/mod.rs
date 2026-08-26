use std::time::Duration;

use bitcoincore_rpc::Auth;

use crate::prelude::Error;
use crate::storage::ChainDB;
use crate::sync;

pub use cache::{create_shared_cache, MempoolCache, SharedMempoolCache};

pub mod cache;
mod model;
mod processor;
pub mod roll;

fn define_gasket_policy(config: &Option<gasket::retries::Policy>) -> gasket::runtime::Policy {
    let default_retries = gasket::retries::Policy {
        max_retries: 20,
        backoff_unit: Duration::from_secs(1),
        backoff_factor: 2,
        max_backoff: Duration::from_secs(60),
        dismissible: false,
    };

    let retries = config.clone().unwrap_or(default_retries);

    gasket::runtime::Policy {
        tick_timeout: std::time::Duration::from_secs(600).into(),
        bootstrap_retry: retries.clone(),
        work_retry: retries.clone(),
        teardown_retry: retries.clone(),
    }
}

pub fn pipeline(
    config: &sync::Config,
    chain_db: ChainDB,
    retries: &Option<gasket::retries::Policy>,
    shared_cache: SharedMempoolCache,
) -> Result<gasket::daemon::Daemon, Error> {
    let rpc_auth = Auth::UserPass(config.node_rpc_user.clone(), config.node_rpc_pass.clone());

    let mempool_refresh_rate = if let Some(millis) = config.mempool_refresh_rate {
        millis
    } else {
        match config.network {
            sync::BitcoinCompatibleNetwork::Bitcoin => 1000 * 30, // 30 seconds in mainnet
            sync::BitcoinCompatibleNetwork::BitcoinTestnet => 1000 * 2, // 2 seconds in testnet
            _ => unreachable!(),
        }
    };

    let notifier = chain_db.notifier.clone();

    let roll = roll::Stage::new(
        chain_db,
        config.network,
        notifier,
        config.mgm_address.clone(),
        config.node_rpc.clone(),
        rpc_auth,
        mempool_refresh_rate,
        config.max_mempool_blocks.unwrap_or(1),
        shared_cache,
    );

    let policy = define_gasket_policy(retries);

    let roll = gasket::runtime::spawn_stage(roll, policy);

    Ok(gasket::daemon::Daemon(vec![roll]))
}
