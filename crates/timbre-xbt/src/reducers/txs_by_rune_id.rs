use crate::{Decode, Encode};

use super::{Height, RuneId, RuneQuantity, ScriptHash, TxHash, TxIndex};

// max size: (8 + 4) + 1 + 8 + 1 + 4 + 1 + 32
#[derive(Clone, Debug, Encode, Decode)]
pub struct Key {
    // Rune ID.
    pub rune_id: RuneId,

    // Block height.
    pub height: Height,

    // Index of tx with rune activity in the block.
    pub activity_tx_index: TxIndex,

    // Transaction hash.
    pub tx_hash: TxHash,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
// max size: 1 + 1 + 1 + 1 + (4 + (number of self-transfers * (20  + 16))) + 1 + (4 + (number of distinct senders * (20 + 16))) + 1 + (4 + (number of distinct receivers * (20 + 16)))
pub struct Value {
    // Etching tx.
    pub etched: bool,

    // Minting tx.
    pub minted: bool,

    // List of self-transfers.
    pub self_transfers: Vec<(ScriptHash, RuneQuantity)>,

    // List of addresses that see their rune balances decreased after the tx, and the decreased amount.
    pub senders: Vec<(ScriptHash, RuneQuantity)>,

    // List of addresses that see their rune balances increased after the tx, and the increased amount.
    pub receivers: Vec<(ScriptHash, RuneQuantity)>,
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
