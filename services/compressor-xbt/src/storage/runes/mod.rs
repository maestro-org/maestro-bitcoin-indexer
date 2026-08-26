use rocksdb::OptimisticTransactionDB;
use rocksdb::Transaction;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::{DBSerde, DBUInt128, KVTable, RuneId};
use crate::sync::BitcoinCompatibleNetwork;

pub mod updater;

// UNCOMMON•GOODS genesis rune constants (hardcoded in ord protocol)
// See: https://github.com/ordinals/ord/blob/master/src/index.rs
pub const UNCOMMON_GOODS_RUNE: u128 = 2055900680524219742;
pub const SUBSIDY_HALVING_INTERVAL: u64 = 210_000;

/// Bootstrap the genesis rune (UNCOMMON•GOODS) for Bitcoin mainnet.
/// This rune has no etching transaction - it's hardcoded into the protocol.
pub fn bootstrap_genesis_rune(
    db: &OptimisticTransactionDB,
    tx: &Transaction<OptimisticTransactionDB>,
    network: BitcoinCompatibleNetwork,
) -> Result<(), super::Error> {
    // Only bootstrap on Bitcoin mainnet
    if !matches!(network, BitcoinCompatibleNetwork::Bitcoin) {
        return Ok(());
    }

    let genesis_rune_id = RuneId { block: 1, tx: 0 };

    // Check if already bootstrapped
    if RuneTermsByIdKV::get_by_key(db, tx, genesis_rune_id.clone())?.is_some() {
        return Ok(());
    }

    info!("bootstrapping genesis rune UNCOMMON•GOODS (1:0)");

    // Insert rune name -> rune ID mapping
    RuneIdByNameKV::stage_upsert(
        db,
        DBUInt128(UNCOMMON_GOODS_RUNE),
        DBSerde(genesis_rune_id.clone()),
        tx,
    )?;

    // Insert rune ID -> rune terms mapping
    let terms = RuneTerms {
        name: UNCOMMON_GOODS_RUNE,
        amount: Some(1),
        cap: Some(u128::MAX),
        start_height: Some(SUBSIDY_HALVING_INTERVAL * 4), // 840,000
        end_height: Some(SUBSIDY_HALVING_INTERVAL * 5),   // 1,050,000
    };

    RuneTermsByIdKV::stage_upsert(db, genesis_rune_id, DBSerde(terms), tx)?;

    Ok(())
}

// Rune name -> Rune ID
pub struct RuneIdByNameKV;

impl KVTable<DBUInt128, DBSerde<RuneId>> for RuneIdByNameKV {
    const CF_NAME: &'static str = "RuneIdByNameKV";
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RuneTerms {
    pub name: u128,
    pub amount: Option<u128>,
    pub cap: Option<u128>,
    pub start_height: Option<u64>,
    pub end_height: Option<u64>,
}

// Rune ID -> Rune Terms
pub struct RuneTermsByIdKV;

impl KVTable<RuneId, DBSerde<RuneTerms>> for RuneTermsByIdKV {
    const CF_NAME: &'static str = "RuneTermsByIdKV";
}

// Rune ID -> Number of times rune minted
pub struct RuneMintsByIdKV;

impl KVTable<RuneId, DBUInt128> for RuneMintsByIdKV {
    const CF_NAME: &'static str = "RuneMintsByIdKV";
}
