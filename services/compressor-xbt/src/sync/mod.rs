use std::time::Duration;

use bitcoin::consensus::Decodable;
use bitcoin::p2p::Magic;
use bitcoin::{Block, Network};
use bitcoincore_rpc::Auth;
use gasket::messaging::{RecvPort, SendPort};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::prelude::Error;
use crate::storage::ChainDB;

mod health;
mod model;
mod pull;
pub mod roll;

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum BitcoinCompatibleNetwork {
    Bitcoin,
    BitcoinTestnet,
    // BitcoinSignet,
    // BitcoinRegtest,
    Dogecoin,
    DogecoinTestnet,
}

impl BitcoinCompatibleNetwork {
    pub fn genesis_block(&self) -> Block {
        match self {
            Self::Bitcoin => bitcoin::constants::genesis_block(Network::Bitcoin),
            Self::BitcoinTestnet => {
                let b_bytes = hex::decode("0100000000000000000000000000000000000000000000000000000000000000000000004e7b2b9128fe0291db0693af2ae418b767e657cd407e80cb1434221eaea7a07a046f3566ffff001dbb0c78170101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff5504ffff001d01044c4c30332f4d61792f323032342030303030303030303030303030303030303030303165626435386332343439373062336161396437383362623030313031316662653865613865393865303065ffffffff0100f2052a010000002321000000000000000000000000000000000000000000000000000000000000000000ac00000000").unwrap();
                Block::consensus_decode_from_finite_reader(&mut &b_bytes[..]).unwrap()
            }
            // https://github.com/sandshrewmetaprotocols/metashrew/blob/master/src/chain.rs
            Self::Dogecoin => todo!(),
            Self::DogecoinTestnet => todo!(),
        }
    }

    pub fn magic(&self) -> Magic {
        match self {
            Self::Bitcoin => Network::Bitcoin.magic(),
            Self::BitcoinTestnet => Magic::from_bytes([0x1c, 0x16, 0x3f, 0x28]),
            Self::Dogecoin => Magic::from_bytes([0xc0, 0xc0, 0xc0, 0xc0]),
            Self::DogecoinTestnet => Magic::from_bytes([0xfc, 0xc1, 0xb7, 0xdc]),
        }
    }
}

impl Into<bitcoin::Network> for BitcoinCompatibleNetwork {
    fn into(self) -> bitcoin::Network {
        match self {
            Self::Bitcoin => bitcoin::Network::Bitcoin,
            Self::BitcoinTestnet => bitcoin::Network::Testnet4,
            Self::Dogecoin => bitcoin::Network::Regtest, // regtest has start height 0 for runes indexing
            Self::DogecoinTestnet => bitcoin::Network::Regtest, // regtest has start height 0 for runes indexing
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub node_address: String,
    pub node_rpc: String,
    pub node_rpc_user: String,
    pub node_rpc_pass: String,
    pub mgm_address: Option<String>,
    pub network: BitcoinCompatibleNetwork,
    pub health_endpoint: String,
    pub first_rune_height: Option<u64>,
    pub first_inscription_height: Option<u64>,
    pub jubilee_height: u64,
    // amount of blocks to fetch from node per request
    pub block_page_size: Option<usize>,
    // max messages to queue between sync stages
    pub sync_channel_queue: Option<usize>,
    // store all UTxOs in memory as well as database
    pub utxos_in_memory: Option<bool>,
    // at least mempool_refresh_rate millis must elapsed before refreshing mempool blocks
    pub mempool_refresh_rate: Option<u64>,
    pub max_mempool_blocks: Option<usize>,
}

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
    config: &Config,
    chain_db: ChainDB,
    retries: &Option<gasket::retries::Policy>,
    mempool_cache: Option<crate::mempool::SharedMempoolCache>,
) -> Result<gasket::daemon::Daemon, Error> {
    let rpc_auth = Auth::UserPass(config.node_rpc_user.clone(), config.node_rpc_pass.clone());

    let mut pull = pull::Stage::new(
        config.node_address.clone(),
        config.node_rpc.clone(),
        rpc_auth.clone(),
        config.network,
        chain_db.clone(),
        config.block_page_size.unwrap_or(50),
    );

    let chain_cursor = chain_db.cursor().map_err(Error::storage)?;
    info!(?chain_cursor, "chain cursor");

    let mut health = health::Stage::new(
        config.health_endpoint.clone(),
        config.node_rpc.clone(),
        rpc_auth.clone(),
        mempool_cache,
    );

    let mut roll = roll::Stage::new(chain_db, config.utxos_in_memory);

    let queue_size = config.sync_channel_queue.unwrap_or(250);

    let (to_roll, from_pull) = gasket::messaging::tokio::mpsc_channel(queue_size);
    pull.downstream.connect(to_roll);
    roll.upstream.connect(from_pull);

    let (roll_to_health, roll_from_health) = gasket::messaging::tokio::mpsc_channel(queue_size);

    health.roll_upstream.connect(roll_from_health);
    roll.health_downstream.connect(roll_to_health);

    let policy = define_gasket_policy(retries);

    let pull = gasket::runtime::spawn_stage(pull, policy.clone());
    let roll = gasket::runtime::spawn_stage(roll, policy.clone());
    let health = gasket::runtime::spawn_stage(health, policy);

    Ok(gasket::daemon::Daemon(vec![pull, roll, health]))
}
