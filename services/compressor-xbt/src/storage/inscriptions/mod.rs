use ord::InscriptionId;
use serde::{Deserialize, Serialize};

use super::{DBBytes, DBSerde, DBUInt128, KVTable};

pub mod brc20;
pub mod updater;

// Inscription Id -> Any value signals it was cursed or vindicated
pub struct CursedOrVindicatedByInscriptionId;

impl KVTable<DBSerde<InscriptionId>, DBBytes> for CursedOrVindicatedByInscriptionId {
    const CF_NAME: &'static str = "CursedOrVindicatedByInscriptionIdKV";
}

// INSCRIPTION_COUNTERS_KEY -> current counters needed for inscriptions
pub struct InscriptionCountersKV;

impl KVTable<DBBytes, DBSerde<Counters>> for InscriptionCountersKV {
    const CF_NAME: &'static str = "InscriptionCountersKV";
}

pub static INSCRIPTION_COUNTERS_KEY: &[u8] = &[0x14];

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Counters {
    pub cursed_count: u64,
    pub blessed_count: u64,
    pub next_sequence_num: u64,
    pub lost_sats: u64,
    pub unbound_count: u64,
}

//
pub struct TermsByBRC20Ticker;

impl KVTable<DBBytes, DBSerde<BRC20Terms>> for TermsByBRC20Ticker {
    const CF_NAME: &'static str = "TermsByBRC20TickerKV";
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct BRC20Terms {
    pub max: u128,
    pub mint_amt_limit: u128,
    pub dec: u8,
    pub self_mint: bool,
    pub deploy_id: InscriptionId,
}

// BRC20 ticker -> current minted supply
pub struct SupplyByBRC20;

impl KVTable<DBBytes, DBUInt128> for SupplyByBRC20 {
    const CF_NAME: &'static str = "SupplyByBRC20KV";
}

// (script hash, brc20 ticker) -> available balance
pub struct BRC20Balances;

impl KVTable<DBSerde<ScriptAndBRC20Kind>, DBUInt128> for BRC20Balances {
    const CF_NAME: &'static str = "BRC20BalancesKV";
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ScriptAndBRC20Kind {
    pub script: [u8; 20],
    pub brc20_ticker: Vec<u8>,
}
