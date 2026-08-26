use crate::{Decode, Encode};

use super::{Height, RuneId, RuneQuantity, SatoshiQuantity, ScriptHash, TxHash, TxoIndex};

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
/// size 59 (including breaks)
pub struct Key {
    // (block, tx) of etching
    pub rune_id: RuneId,
    // block height
    pub height: Height,
    // utxo tx id
    pub utxo_hash: TxHash,
    // utxo tx vout
    pub utxo_index: TxoIndex,
}

#[derive(Encode, Decode, Clone, Debug)]
/// size 46 (including breaks)
pub struct Value {
    pub script_hash: ScriptHash,
    pub rune_quantity: RuneQuantity,
    pub satoshis: SatoshiQuantity,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    pub height: Height,
    pub utxo_hash: TxHash,
    pub utxo_index: TxoIndex,
}

impl Cursor {
    pub fn new(height: u64, utxo_hash: [u8; 32], utxo_index: u32) -> Self {
        Self {
            height,
            utxo_hash,
            utxo_index,
        }
    }
}
