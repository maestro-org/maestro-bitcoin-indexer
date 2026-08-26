use crate::Decode;
use timbre_xbt_macros::Encode;

use super::{Height, ScriptHash, TxHash, TxIndex};

#[derive(Clone, Debug, Encode, Decode)]
/// size 65 (including breaks)
pub struct Key {
    pub script_hash: ScriptHash,
    pub height: Height,
    pub address_tx_index: TxIndex,
    pub tx_hash: TxHash,
}

#[derive(Clone, Debug, Encode, Decode)]
/// size 3 (including breaks)
pub struct Value {
    pub input: bool,
    pub output: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    pub height: Height,
    pub address_tx_index: TxIndex,
    pub tx_hash: TxHash,
}

impl Cursor {
    pub fn new(height: u64, address_tx_index: u32, tx_hash: [u8; 32]) -> Self {
        Self {
            height,
            address_tx_index,
            tx_hash,
        }
    }
}
