use crate::{Decode, Encode, TimbreError};

use super::{Height, SatoshiQuantity, ScriptHash, TxHash, TxIndex};

// max size: 20 + 1 + 8 + 1 + 4 + 1 + 32
#[derive(Clone, Debug, Encode, Decode)]
pub struct Key {
    // Script hash.
    pub script_hash: ScriptHash,

    // Block height.
    pub height: Height,

    // Index of tx in the block.
    pub activity_tx_index: TxIndex,

    // Transaction hash.
    pub tx_hash: TxHash,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
// max size: 8 + 1 + 1
pub struct Value {
    pub amount: SatoshiQuantity,

    pub activity_type: SatActivityType,
}

//
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub enum SatActivityType {
    Increased,
    Decreased,
    SelfTransferred,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    // Block height.
    pub height: Height,

    // Index of tx in the block.
    pub activity_tx_index: TxIndex,

    // Transaction hash.
    pub tx_hash: TxHash,
}
