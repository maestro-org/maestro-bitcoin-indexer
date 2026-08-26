use crate::{Decode, Encode};

use super::{
    AggregatedSatoshis, BlockHash, Height, InscriptionId, RuneId, RuneQuantity, SatoshiOffset,
    SatoshiQuantity, ScriptHash, TxHash, TxoIndex,
};

#[derive(Clone, Debug, Encode, Decode)]
pub struct Key {
    pub tx_hash: TxHash,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
pub struct Value {
    /// Block height.
    pub block_height: Height,

    /// Block hash. (None if mempool)
    pub block_hash: Option<BlockHash>,

    /// The timestamp of the block, as claimed by the miner. (None if mempool)
    pub timestamp: Option<u32>,

    /// Total number of satoshis that went through this transaction, minus fees.
    pub volume: AggregatedSatoshis,

    /// Fees paid to the miner.
    pub fees: SatoshiQuantity,

    /// Satoshis per vB of the transaction.
    pub sats_per_vb: u64,

    /// Whether any of the inputs or outputs of the transaction contains inscriptions.
    pub involves_inscriptions: bool,

    /// Whether any of the inputs or outputs of the transaction contains runes.
    pub involves_runes: bool,

    /// Whether the transaction involves BRC-20.
    pub involves_brc20: bool,

    /// List of inputs, in the same order as the transaction.
    pub inputs: Vec<TxIn>,

    /// List of outputs, in the same order as the transaction.
    pub outputs: Vec<TxOut>,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
pub struct TxIn {
    /// Hash of the transaction that produced this UTxO.
    pub utxo_hash: TxHash,

    /// Output index in the transaction that produced this UTxO.
    pub utxo_vout: TxoIndex,

    /// Script hash controlling this UTxO.
    pub script_hash: ScriptHash,

    /// Satoshis in this UTxO.
    pub satoshis: SatoshiQuantity,

    /// Inscriptions in the input, each one represented as (offset, (reveal tx hash, inscription index in reveal tx)).
    pub inscriptions: Vec<(SatoshiOffset, InscriptionId)>,

    /// Runes in the input, each one represented as (rune ID, amount of runes in the input), where rune ID is (block of etching, tx of etching).
    pub runes: Vec<(RuneId, RuneQuantity)>,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
pub struct TxOut {
    /// Script hash controlling this UTxO.
    pub script_hash: ScriptHash,

    /// Satoshis in this UTxO.
    pub satoshis: SatoshiQuantity,

    /// Inscriptions in the output, each one represented as (offset, (reveal tx hash, inscription index in reveal tx)).
    pub inscriptions: Vec<(SatoshiOffset, InscriptionId)>,

    /// Runes in the output, each one represented as (rune ID, amount of runes), where rune ID is (block of etching, tx of etching).
    pub runes: Vec<(RuneId, RuneQuantity)>,
}
