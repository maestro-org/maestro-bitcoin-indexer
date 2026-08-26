use crate::{Decode, Encode};

use super::{AggregatedSatoshis, BlockHash, Height, TxIndex};

#[derive(Clone, Debug, Encode, Decode)]
pub struct Key {
    /// Block height.
    pub height: Height,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
pub struct Value {
    /// Block hash. (None if mempool)
    pub block_hash: Option<BlockHash>,

    /// Block size.
    pub block_size: u64,

    /// Number of weight units (WU) of the block.
    pub block_weight_units: u64,

    /// The timestamp of the block, as claimed by the miner.
    pub timestamp: Option<u32>,

    /// Total fees paid by all transactions in the block.
    pub total_fees: AggregatedSatoshis,

    /// Total number of satoshis that went through this block, minus fees.
    pub total_volume: AggregatedSatoshis,

    /// Total number of transactions.
    pub total_txs: TxIndex,

    /// Whether any of the inputs or outputs of any of the transactions in the block contains inscriptions.
    pub involves_inscriptions: bool,

    /// Whether any of the inputs or outputs of any of the transactions in the block contains runes.
    pub involves_runes: bool,

    /// Whether any of the inputs or outputs of any of the transactions in the block contains BRC-20 messages.
    pub involves_brc20: bool,

    /// Miner tag.
    pub coinbase_script_sig: Vec<u8>,
}
