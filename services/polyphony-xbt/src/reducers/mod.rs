use bitcoin::{Block, Transaction, Txid};
use gasket::runtime::spawn_stage;
use serde::Deserialize;
use std::time::Duration;

use crate::{
    bootstrap,
    crosscut::{self, Point},
    model::{self, StorageActionPayload},
};

type InputPort = gasket::messaging::tokio::InputPort<model::EnrichedBlockPayload>;
type OutputPort = gasket::messaging::tokio::OutputPort<StorageActionPayload>;

mod worker;

pub mod balances_by_brc20;
pub mod balances_by_rune_id;
pub mod block_by_tx_hash;
pub mod block_info;
pub mod brc20_balances_by_script_hash;
pub mod brc20_terms_by_ticker;
pub mod content_by_inscription_id;
pub mod etching_by_rune_id;
pub mod height_by_block_hash;
pub mod height_by_timestamp;
pub mod historical_sat_balance_by_script_hash;
pub mod inscription_activity_by_script_hash;
pub mod inscription_activity_by_tx;
pub mod inscription_activity_by_tx_v2;
pub mod inscription_utxos_by_script_hash;
pub mod mints_by_rune_id;
pub mod rune_id_by_rune_name;
pub mod rune_txs_by_script_hash;
pub mod rune_utxos_by_script_hash;
pub mod sat_balance_by_script_hash;
pub mod sat_txs_by_script_hash;
pub mod sats_per_vb_by_block;
pub mod script_by_script_hash;
pub mod script_hash_by_address_payload_hash;
pub mod spending_tx_by_txo;
pub mod total_inscriptions_by_script_hash;
pub mod total_outputs_by_script_hash;
pub mod total_sat_in_inputs_by_script_hash;
pub mod total_sat_in_outputs_by_script_hash;
pub mod total_txs_by_script_hash;
pub mod total_utxos_by_script_hash;
pub mod transfer_inscriptions_by_script_hash;
pub mod tx_first_seen_timestamp;
pub mod tx_info;
pub mod txs_by_block;
pub mod txs_by_inscription;
pub mod txs_by_rune_id;
pub mod txs_by_script_hash;
pub mod utxos_by_rune_id;
pub mod utxos_by_script_hash;

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Config {
    BalancesByBrc20(balances_by_brc20::Config),
    BalancesByRuneId(balances_by_rune_id::Config),
    BlockByTxHash(block_by_tx_hash::Config),
    BlockInfo(block_info::Config),
    Brc20BalancesByScriptHash(brc20_balances_by_script_hash::Config),
    Brc20TermsByTicker(brc20_terms_by_ticker::Config),
    ContentByInscriptionId(content_by_inscription_id::Config),
    EtchingByRuneId(etching_by_rune_id::Config),
    HeightByBlockHash(height_by_block_hash::Config),
    HeightByTimestamp(height_by_timestamp::Config),
    InscriptionUtxosByScriptHash(inscription_utxos_by_script_hash::Config),
    InscriptionActivityByScriptHash(inscription_activity_by_script_hash::Config),
    InscriptionActivityByTx(inscription_activity_by_tx::Config),
    InscriptionActivityByTxV2(inscription_activity_by_tx_v2::Config),
    MintsByRuneId(mints_by_rune_id::Config),
    RuneTxsByScriptHash(rune_txs_by_script_hash::Config),
    RuneIdByRuneName(rune_id_by_rune_name::Config),
    RuneUtxosByScriptHash(rune_utxos_by_script_hash::Config),
    SatBalanceByScriptHash(sat_balance_by_script_hash::Config),
    HistoricalSatBalanceByScriptHash(historical_sat_balance_by_script_hash::Config),
    SatTxsByScriptHash(sat_txs_by_script_hash::Config),
    SatsPerVbByBlock(sats_per_vb_by_block::Config),
    ScriptByScriptHash(script_by_script_hash::Config),
    ScriptHashByAddressPayloadHash(script_hash_by_address_payload_hash::Config),
    SpendingTxByTxo(spending_tx_by_txo::Config),
    TotalInscriptionsByScriptHash(total_inscriptions_by_script_hash::Config),
    TotalOutputsByScriptHash(total_outputs_by_script_hash::Config),
    TotalSatInInputsByScriptHash(total_sat_in_inputs_by_script_hash::Config),
    TotalSatInOutputsByScriptHash(total_sat_in_outputs_by_script_hash::Config),
    TotalTxsByScriptHash(total_txs_by_script_hash::Config),
    TotalUtxosByScriptHash(total_utxos_by_script_hash::Config),
    TransferInscriptionsByScriptHash(transfer_inscriptions_by_script_hash::Config),
    TxFirstSeenTimestamp(tx_first_seen_timestamp::Config),
    TxInfo(tx_info::Config),
    TxsByBlock(txs_by_block::Config),
    TxsByInscription(txs_by_inscription::Config),
    TxsByRuneId(txs_by_rune_id::Config),
    TxsByScriptHash(txs_by_script_hash::Config),
    UtxosByRuneId(utxos_by_rune_id::Config),
    UtxosByScriptHash(utxos_by_script_hash::Config),
}

impl Config {
    /// Kebab-case reducer name, as used in the Redis instance registry keys
    /// (`{bitcoin:<network>:<reducer>}:scores`) that the API layer resolves
    /// instances from. Must match the API's reducer naming.
    pub fn kebab_name(&self) -> &'static str {
        match self {
            Config::BalancesByBrc20(_) => "balances-by-brc20",
            Config::BalancesByRuneId(_) => "balances-by-rune-id",
            Config::BlockByTxHash(_) => "block-by-tx-hash",
            Config::BlockInfo(_) => "block-info",
            Config::Brc20BalancesByScriptHash(_) => "brc20-balances-by-script-hash",
            Config::Brc20TermsByTicker(_) => "brc20-terms-by-ticker",
            Config::ContentByInscriptionId(_) => "content-by-inscription-id",
            Config::EtchingByRuneId(_) => "etching-by-rune-id",
            Config::HeightByBlockHash(_) => "height-by-block-hash",
            Config::HeightByTimestamp(_) => "height-by-timestamp",
            Config::InscriptionUtxosByScriptHash(_) => "inscription-utxos-by-script-hash",
            Config::InscriptionActivityByScriptHash(_) => "inscription-activity-by-script-hash",
            Config::InscriptionActivityByTx(_) => "inscription-activity-by-tx",
            Config::InscriptionActivityByTxV2(_) => "inscription-activity-by-tx-v2",
            Config::MintsByRuneId(_) => "mints-by-rune-id",
            Config::RuneTxsByScriptHash(_) => "rune-txs-by-script-hash",
            Config::RuneIdByRuneName(_) => "rune-id-by-rune-name",
            Config::RuneUtxosByScriptHash(_) => "rune-utxos-by-script-hash",
            Config::SatBalanceByScriptHash(_) => "sat-balance-by-script-hash",
            Config::HistoricalSatBalanceByScriptHash(_) => "historical-sat-balance-by-script-hash",
            Config::SatTxsByScriptHash(_) => "sat-txs-by-script-hash",
            Config::SatsPerVbByBlock(_) => "sats-per-vb-by-block",
            Config::ScriptByScriptHash(_) => "script-by-script-hash",
            Config::ScriptHashByAddressPayloadHash(_) => "script-hash-by-address-payload-hash",
            Config::SpendingTxByTxo(_) => "spending-tx-by-txo",
            Config::TotalInscriptionsByScriptHash(_) => "total-inscriptions-by-script-hash",
            Config::TotalOutputsByScriptHash(_) => "total-outputs-by-script-hash",
            Config::TotalSatInInputsByScriptHash(_) => "total-sat-in-inputs-by-script-hash",
            Config::TotalSatInOutputsByScriptHash(_) => "total-sat-in-outputs-by-script-hash",
            Config::TotalTxsByScriptHash(_) => "total-txs-by-script-hash",
            Config::TotalUtxosByScriptHash(_) => "total-utxos-by-script-hash",
            Config::TransferInscriptionsByScriptHash(_) => "transfer-inscriptions-by-script-hash",
            Config::TxFirstSeenTimestamp(_) => "tx-first-seen-timestamp",
            Config::TxInfo(_) => "tx-info",
            Config::TxsByBlock(_) => "txs-by-block",
            Config::TxsByInscription(_) => "txs-by-inscription",
            Config::TxsByRuneId(_) => "txs-by-rune-id",
            Config::TxsByScriptHash(_) => "txs-by-script-hash",
            Config::UtxosByRuneId(_) => "utxos-by-rune-id",
            Config::UtxosByScriptHash(_) => "utxos-by-script-hash",
        }
    }

    fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> Reducer {
        match self {
            Config::BalancesByBrc20(c) => c.plugin(policy),
            Config::BalancesByRuneId(c) => c.plugin(policy),
            Config::BlockByTxHash(c) => c.plugin(),
            Config::BlockInfo(c) => c.plugin(policy),
            Config::Brc20BalancesByScriptHash(c) => c.plugin(policy),
            Config::Brc20TermsByTicker(c) => c.plugin(),
            Config::ContentByInscriptionId(c) => c.plugin(),
            Config::HeightByBlockHash(c) => c.plugin(),
            Config::HeightByTimestamp(c) => c.plugin(),
            Config::EtchingByRuneId(c) => c.plugin(),
            Config::InscriptionUtxosByScriptHash(c) => c.plugin(policy),
            Config::InscriptionActivityByScriptHash(c) => c.plugin(policy),
            Config::InscriptionActivityByTx(c) => c.plugin(policy),
            Config::InscriptionActivityByTxV2(c) => c.plugin(policy),
            Config::MintsByRuneId(c) => c.plugin(),
            Config::RuneTxsByScriptHash(c) => c.plugin(policy),
            Config::RuneIdByRuneName(c) => c.plugin(),
            Config::RuneUtxosByScriptHash(c) => c.plugin(policy),
            Config::SatBalanceByScriptHash(c) => c.plugin(policy),
            Config::HistoricalSatBalanceByScriptHash(c) => c.plugin(policy),
            Config::SatTxsByScriptHash(c) => c.plugin(policy),
            Config::SatsPerVbByBlock(c) => c.plugin(policy),
            Config::ScriptByScriptHash(c) => c.plugin(policy),
            Config::ScriptHashByAddressPayloadHash(c) => c.plugin(),
            Config::SpendingTxByTxo(c) => c.plugin(),
            Config::TotalInscriptionsByScriptHash(c) => c.plugin(policy),
            Config::TotalOutputsByScriptHash(c) => c.plugin(),
            Config::TotalSatInInputsByScriptHash(c) => c.plugin(policy),
            Config::TotalSatInOutputsByScriptHash(c) => c.plugin(),
            Config::TotalTxsByScriptHash(c) => c.plugin(policy),
            Config::TotalUtxosByScriptHash(c) => c.plugin(policy),
            Config::TransferInscriptionsByScriptHash(c) => c.plugin(policy),
            Config::TxFirstSeenTimestamp(c) => c.plugin(),
            Config::TxInfo(c) => c.plugin(policy),
            Config::TxsByBlock(c) => c.plugin(),
            Config::TxsByInscription(c) => c.plugin(),
            Config::TxsByRuneId(c) => c.plugin(policy),
            Config::TxsByScriptHash(c) => c.plugin(policy),
            Config::UtxosByRuneId(c) => c.plugin(policy),
            Config::UtxosByScriptHash(c) => c.plugin(policy),
        }
    }
}

pub struct Bootstrapper {
    input: InputPort,
    output: OutputPort,
    reducers: Vec<Reducer>,
}

impl Bootstrapper {
    pub fn new(configs: Vec<Config>, policy: &crosscut::policies::RuntimePolicy) -> Self {
        Self {
            reducers: configs.into_iter().map(|x| x.plugin(policy)).collect(),
            input: Default::default(),
            output: Default::default(),
        }
    }

    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort {
        &mut self.input
    }

    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort {
        &mut self.output
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline, timeout: u64) {
        let worker = worker::Worker::new(self.reducers, self.input, self.output);
        pipeline.register_stage(spawn_stage(
            worker,
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(timeout)),
                ..Default::default()
            },
            Some("reducers"),
        ));
    }
}

pub enum Reducer {
    BalancesByBrc20(balances_by_brc20::Reducer),
    BalancesByRuneId(balances_by_rune_id::Reducer),
    BlockByTxHash(block_by_tx_hash::Reducer),
    BlockInfo(block_info::Reducer),
    Brc20BalancesByScriptHash(brc20_balances_by_script_hash::Reducer),
    Brc20TermsByTicker(brc20_terms_by_ticker::Reducer),
    ContentByInscriptionId(content_by_inscription_id::Reducer),
    EtchingByRuneId(etching_by_rune_id::Reducer),
    HeightByBlockHash(height_by_block_hash::Reducer),
    HeightByTimestamp(height_by_timestamp::Reducer),
    InscriptionUtxosByScriptHash(inscription_utxos_by_script_hash::Reducer),
    InscriptionActivityByScriptHash(inscription_activity_by_script_hash::Reducer),
    InscriptionActivityByTx(inscription_activity_by_tx::Reducer),
    InscriptionActivityByTxV2(inscription_activity_by_tx_v2::Reducer),
    MintsByRuneId(mints_by_rune_id::Reducer),
    RuneTxsByScriptHash(rune_txs_by_script_hash::Reducer),
    RuneIdByRuneName(rune_id_by_rune_name::Reducer),
    RuneUtxosByScriptHash(rune_utxos_by_script_hash::Reducer),
    SatBalanceByScriptHash(sat_balance_by_script_hash::Reducer),
    HistoricalSatBalanceByScriptHash(historical_sat_balance_by_script_hash::Reducer),
    SatTxsByScriptHash(sat_txs_by_script_hash::Reducer),
    SatsPerVbByBlock(sats_per_vb_by_block::Reducer),
    ScriptByScriptHash(script_by_script_hash::Reducer),
    ScriptHashByAddressPayloadHash(script_hash_by_address_payload_hash::Reducer),
    SpendingTxByTxo(spending_tx_by_txo::Reducer),
    TotalInscriptionsByScriptHash(total_inscriptions_by_script_hash::Reducer),
    TotalOutputsByScriptHash(total_outputs_by_script_hash::Reducer),
    TotalSatInInputsByScriptHash(total_sat_in_inputs_by_script_hash::Reducer),
    TotalSatInOutputsByScriptHash(total_sat_in_outputs_by_script_hash::Reducer),
    TotalTxsByScriptHash(total_txs_by_script_hash::Reducer),
    TotalUtxosByScriptHash(total_utxos_by_script_hash::Reducer),
    TransferInscriptionsByScriptHash(transfer_inscriptions_by_script_hash::Reducer),
    TxFirstSeenTimestamp(tx_first_seen_timestamp::Reducer),
    TxInfo(tx_info::Reducer),
    TxsByBlock(txs_by_block::Reducer),
    TxsByInscription(txs_by_inscription::Reducer),
    TxsByRuneId(txs_by_rune_id::Reducer),
    TxsByScriptHash(txs_by_script_hash::Reducer),
    UtxosByRuneId(utxos_by_rune_id::Reducer),
    UtxosByScriptHash(utxos_by_script_hash::Reducer),
}

#[derive(Clone, Debug)]
pub enum ReducerOutput {
    BalancesByBrc20(balances_by_brc20::Output),
    BalancesByRuneId(balances_by_rune_id::Output),
    BlockByTxHash(block_by_tx_hash::Output),
    BlockInfo(block_info::Output),
    Brc20BalancesByScriptHash(brc20_balances_by_script_hash::Output),
    Brc20TermsByTicker(brc20_terms_by_ticker::Output),
    ContentByInscriptionId(content_by_inscription_id::Output),
    EtchingByRuneId(etching_by_rune_id::Output),
    HeightByBlockHash(height_by_block_hash::Output),
    HeightByTimestamp(height_by_timestamp::Output),
    InscriptionUtxosByScriptHash(inscription_utxos_by_script_hash::Output),
    InscriptionActivityByScriptHash(inscription_activity_by_script_hash::Output),
    InscriptionActivityByTx(inscription_activity_by_tx::Output),
    InscriptionActivityByTxV2(inscription_activity_by_tx_v2::Output),
    MintsByRuneId(mints_by_rune_id::Output),
    RuneTxsByScriptHash(rune_txs_by_script_hash::Output),
    RuneIdByRuneName(rune_id_by_rune_name::Output),
    RuneUtxosByScriptHash(rune_utxos_by_script_hash::Output),
    SatBalanceByScriptHash(sat_balance_by_script_hash::Output),
    HistoricalSatBalanceByScriptHash(historical_sat_balance_by_script_hash::Output),
    SatTxsByScriptHash(sat_txs_by_script_hash::Output),
    SatsPerVbByBlock(sats_per_vb_by_block::Output),
    ScriptByScriptHash(script_by_script_hash::Output),
    ScriptHashByAddressPayloadHash(script_hash_by_address_payload_hash::Output),
    SpendingTxByTxo(spending_tx_by_txo::Output),
    TotalInscriptionsByScriptHash(total_inscriptions_by_script_hash::Output),
    TotalOutputsByScriptHash(total_outputs_by_script_hash::Output),
    TotalSatInInputsByScriptHash(total_sat_in_inputs_by_script_hash::Output),
    TotalSatInOutputsByScriptHash(total_sat_in_outputs_by_script_hash::Output),
    TotalTxsByScriptHash(total_txs_by_script_hash::Output),
    TotalUtxosByScriptHash(total_utxos_by_script_hash::Output),
    TransferInscriptionsByScriptHash(transfer_inscriptions_by_script_hash::Output),
    TxFirstSeenTimestamp(tx_first_seen_timestamp::Output),
    TxInfo(tx_info::Output),
    TxsByBlock(txs_by_block::Output),
    TxsByInscription(txs_by_inscription::Output),
    TxsByRuneId(txs_by_rune_id::Output),
    TxsByScriptHash(txs_by_script_hash::Output),
    UtxosByRuneId(utxos_by_rune_id::Output),
    UtxosByScriptHash(utxos_by_script_hash::Output),

    /// Point, was mempool, timestamp, optional mempool chaintip and mempool view ts
    Cursor(Point, bool, u64, Option<((u64, [u8; 32]), u64)>),
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        block: &Option<Block>,
        timestamp: u64,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        match self {
            Reducer::BalancesByBrc20(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::BalancesByRuneId(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::BlockByTxHash(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::BlockInfo(x) => x.reduce_block(height, txs, block, ctx, outputs),
            Reducer::Brc20BalancesByScriptHash(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::Brc20TermsByTicker(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::ContentByInscriptionId(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::EtchingByRuneId(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::HeightByBlockHash(x) => x.reduce_block(height, txs, block, ctx, outputs),
            Reducer::HeightByTimestamp(x) => x.reduce_block(height, block, outputs),
            Reducer::InscriptionUtxosByScriptHash(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::InscriptionActivityByScriptHash(x) => {
                x.reduce_block(height, txs, ctx, outputs)
            }
            Reducer::InscriptionActivityByTx(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::InscriptionActivityByTxV2(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::MintsByRuneId(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::RuneTxsByScriptHash(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::RuneIdByRuneName(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::RuneUtxosByScriptHash(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::SatBalanceByScriptHash(x) => x.reduce_block(txs, ctx, outputs),
            Reducer::HistoricalSatBalanceByScriptHash(x) => {
                x.reduce_block(txs, height, ctx, outputs)
            }
            Reducer::SatTxsByScriptHash(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::SatsPerVbByBlock(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::ScriptByScriptHash(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::ScriptHashByAddressPayloadHash(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::SpendingTxByTxo(x) => x.reduce_block(height, txs, outputs),
            Reducer::TotalInscriptionsByScriptHash(x) => x.reduce_block(txs, ctx, outputs),
            Reducer::TotalOutputsByScriptHash(x) => x.reduce_block(txs, outputs),
            Reducer::TotalSatInInputsByScriptHash(x) => x.reduce_block(txs, ctx, outputs),
            Reducer::TotalSatInOutputsByScriptHash(x) => x.reduce_block(txs, outputs),
            Reducer::TotalUtxosByScriptHash(x) => x.reduce_block(txs, ctx, outputs),
            Reducer::TotalTxsByScriptHash(x) => x.reduce_block(txs, ctx, outputs),
            Reducer::TransferInscriptionsByScriptHash(x) => {
                x.reduce_block(height, txs, ctx, outputs)
            }
            Reducer::TxFirstSeenTimestamp(x) => x.reduce_block(txs, timestamp, outputs),
            Reducer::TxInfo(x) => x.reduce_block(height, txs, block, ctx, outputs),
            Reducer::TxsByBlock(x) => x.reduce_block(height, txs, outputs),
            Reducer::TxsByInscription(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::TxsByRuneId(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::TxsByScriptHash(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::UtxosByRuneId(x) => x.reduce_block(height, txs, ctx, outputs),
            Reducer::UtxosByScriptHash(x) => x.reduce_block(height, txs, ctx, outputs),
        }
    }

    pub fn reset_state(&mut self) {
        match self {
            Reducer::SatTxsByScriptHash(x) => x.reset_state(),
            Reducer::RuneTxsByScriptHash(x) => x.reset_state(),
            Reducer::InscriptionActivityByScriptHash(x) => x.reset_state(),
            _ => {}
        }
    }
}

#[derive(Clone, Debug)]
pub enum UtxoAction<T> {
    Consumed,
    Produced(T),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IncrOrDecr<T> {
    Increment(T),
    Decrement(T),
}

impl std::ops::Add for IncrOrDecr<u128> {
    type Output = IncrOrDecr<u128>;

    fn add(self, rhs: Self) -> Self::Output {
        use IncrOrDecr::*;

        match (self, rhs) {
            (Increment(a), Increment(b)) => Increment(a.checked_add(b).unwrap()),
            (Decrement(a), Decrement(b)) => Decrement(a.checked_add(b).unwrap()),
            (Increment(a), Decrement(b)) | (Decrement(b), Increment(a)) => {
                if a >= b {
                    Increment(a.checked_sub(b).unwrap())
                } else {
                    Decrement(b.checked_sub(a).unwrap())
                }
            }
        }
    }
}

// We define x - y = z iff x = z + y.
impl std::ops::Sub for IncrOrDecr<u128> {
    type Output = IncrOrDecr<u128>;

    fn sub(self, rhs: Self) -> Self::Output {
        use IncrOrDecr::*;

        match (self, rhs) {
            (Increment(a), Increment(b)) => {
                if a >= b {
                    // Example: Increment(10) - Increment(2) = Increment(8), because
                    // Increment(10) = Increment(8) + Increment(2).
                    Increment(a.checked_sub(b).unwrap())
                } else {
                    // Example: Increment(2) - Increment(10) = Decrement(8), because
                    // Increment(2) = Decrement(8) + Increment(10).
                    Decrement(b.checked_sub(a).unwrap())
                }
            }
            (Decrement(a), Decrement(b)) => {
                if a >= b {
                    // Example: Decrement(10) - Decrement(2) = Decrement(8), because
                    // Decrement(10) = Decrement(8) + Decrement(2).
                    Decrement(a.checked_sub(b).unwrap())
                } else {
                    // Example: Decrement(2) - Decrement(10) = Increment(8), because
                    // Decrement(2) = Increment(8) + Decrement(10).
                    Increment(b.checked_sub(a).unwrap())
                }
            }
            (Increment(a), Decrement(b)) => {
                // Examples:
                //      - Increment(10) - Decrement(2) = Increment(12), because
                //              Increment(10) = Increment(12) + Decrement(2).
                //      - Increment(2) - Decrement(10) = Increment(12), because
                //              Increment(2) = Increment(12) + Decrement(10).
                Increment(a.checked_add(b).unwrap())
            }
            (Decrement(a), Increment(b)) => {
                // Examples:
                //      - Decrement(10) - Increment(2) = Decrement(12), because
                //              Decrement(10) = Decrement(12) + Increment(2).
                //      - Decrement(2) - Increment(10) = Decrement(12), because
                //              Decrement(2) = Decrement(12) + Increment(10).
                Decrement(a.checked_add(b).unwrap())
            }
        }
    }
}

impl std::ops::Neg for IncrOrDecr<u128> {
    type Output = Self;

    fn neg(self) -> Self {
        use IncrOrDecr::*;

        match self {
            Increment(a) => Decrement(a),
            Decrement(a) => Increment(a),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IncrOrDecr::*;

    #[test]
    fn test_add() {
        let op1 = Increment(1);
        let op2 = Increment(1);

        assert_eq!(op1 + op2, Increment(2));

        let op1 = Increment(2);
        let op2 = Decrement(1);

        assert_eq!(op1 + op2, Increment(1));

        let op1 = Increment(1);
        let op2 = Decrement(2);

        assert_eq!(op1 + op2, Decrement(1));

        let op1 = Decrement(2);
        let op2 = Increment(1);

        assert_eq!(op1 + op2, Decrement(1));

        let op1 = Decrement(1);
        let op2 = Increment(2);

        assert_eq!(op1 + op2, Increment(1));

        let op1 = Decrement(1);
        let op2 = Decrement(1);

        assert_eq!(op1 + op2, Decrement(2));
    }

    #[test]
    fn test_sub_and_add() {
        // Increment(10) - Increment(2) = Increment(8), because
        // Increment(10) = Increment(8) + Increment(2)
        let op1 = Increment(10);
        let op2 = Increment(2);
        let res = Increment(8);

        assert_eq!(op1 - op2, res);
        assert_eq!(op1, res + op2);

        // Increment(2) - Increment(10) = Decrement(8), because
        // Increment(2) = Decrement(8) + Increment(10)
        let op1 = Increment(2);
        let op2 = Increment(10);
        let res = Decrement(8);

        assert_eq!(op1 - op2, res);
        assert_eq!(op1, res + op2);

        // Decrement(10) - Decrement(2) = Decrement(8), because
        // Decrement(10) = Decrement(8) + Decrement(2)
        let op1 = Decrement(10);
        let op2 = Decrement(2);
        let res = Decrement(8);

        assert_eq!(op1 - op2, res);
        assert_eq!(op1, res + op2);

        // Decrement(2) - Decrement(10) = Increment(8), because
        // Decrement(2) = Increment(8) + Decrement(10)
        let op1 = Decrement(2);
        let op2 = Decrement(10);
        let res = Increment(8);

        assert_eq!(op1 - op2, res);
        assert_eq!(op1, res + op2);

        // Increment(10) - Decrement(2) = Increment(12), because
        // Increment(10) = Increment(12) + Decrement(2)
        let op1 = Increment(10);
        let op2 = Decrement(2);
        let res = Increment(12);

        assert_eq!(op1 - op2, res);
        assert_eq!(op1, res + op2);

        // Increment(2) - Decrement(10) = Increment(12), because
        // Increment(2) = Increment(12) + Decrement(10)
        let op1 = Increment(2);
        let op2 = Decrement(10);
        let res = Increment(12);

        assert_eq!(op1 - op2, res);
        assert_eq!(op1, res + op2);

        // Decrement(10) - Increment(2) = Decrement(12), because
        // Decrement(10) = Decrement(12) + Increment(2)
        let op1 = Decrement(10);
        let op2 = Increment(2);
        let res = Decrement(12);

        assert_eq!(op1 - op2, res);
        assert_eq!(op1, res + op2);

        // Decrement(2) - Increment(10) = Decrement(12), because
        // Decrement(2) = Decrement(12) + Increment(10)
        let op1 = Decrement(2);
        let op2 = Increment(10);
        let res = Decrement(12);

        assert_eq!(op1 - op2, res);
        assert_eq!(op1, res + op2);
    }

    #[test]
    fn test_neg() {
        assert_eq!(-Increment(1), Decrement(1));

        assert_eq!(-Decrement(1), Increment(1));
    }
}
