use crate::{Decode, Encode};

use super::{Height, RuneId, RuneQuantity, ScriptHash, TxHash, TxIndex};

// max size: 20 + 1 + 8 + 1 + 4 + 1 + 32
#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq, Hash)]
pub struct Key {
    // Script hash.
    pub script_hash: ScriptHash,

    // Block height.
    pub height: Height,

    // Index of tx with rune activity involving this script hash in the block.
    pub activity_tx_index: TxIndex,

    // Transaction hash.
    pub tx_hash: TxHash,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
// max size: (1 + 8 + 4 + (1 + 16)) + 1 + (1 + 8 + 4) + 1 + (4 + number of runes with self transfers for this script hash * (8 + 4 + 16)) + 1 + (4 + number of runes with increased balance for this script hash * (8 + 4 + 16)) + 1 + (4 + number of runes with decreased balance for this script hash * (8 + 4 + 16))
pub struct Value {
    // Etched runes, as rune ID and premined runes amount.
    pub etched: Option<(RuneId, Option<RuneQuantity>)>,

    // Minted runes, if any, as rune ID. Minted amount should be taken from etching terms for this
    // specific rune kind.
    pub minted: Option<RuneId>,

    // Rune balances that remained unchanged but were involved in self-transfers.
    pub self_transfers: Vec<(RuneId, RuneQuantity)>,

    // Increased rune balance after this tx, as rune ID and amount of received runes.
    pub increased_balances: Vec<(RuneId, RuneQuantity)>,

    // Decreased rune balances after this tx, as rune ID and amount of sent runes.
    pub decreased_balances: Vec<(RuneId, RuneQuantity)>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    // Block height.
    pub height: Height,

    // Index of tx with rune activity in the block.
    pub activity_tx_index: TxIndex,

    // Transaction hash.
    pub tx_hash: TxHash,
}
