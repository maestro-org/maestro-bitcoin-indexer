use crate::{Decode, Encode};

use super::{Height, SatoshiQuantity, ScriptHash, TxHash, TxoIndex};

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
/// size 67 (including breaks)
pub struct Key {
    // hash of utxo scriptbuf
    pub script_hash: ScriptHash,
    // block height
    pub height: Height,
    // utxo tx id
    pub utxo_hash: TxHash,
    // utxo tx vout
    pub utxo_index: TxoIndex,
}

#[derive(Encode, Decode, Clone, Debug)]
/// size 8
pub struct Value {
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
