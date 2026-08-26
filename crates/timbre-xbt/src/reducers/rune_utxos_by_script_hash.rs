use crate::{Decode, Encode, TimbreError};

use super::{Height, RuneId, RuneQuantity, SatoshiQuantity, ScriptHash, TxHash, TxoIndex};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Key {
    // Script hash.
    pub script_hash: ScriptHash,

    // Block height.
    pub height: Height,

    // Tx hash of the UTxO containing the runes.
    pub utxo_hash: TxHash,

    // Tx output index of the UTxO containing the runes.
    pub utxo_index: TxoIndex,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Value {
    // Satoshis locked in the UTxO.
    pub satoshis: SatoshiQuantity,

    // Runes contained in the UTxO, in the form of ((etching block, etching tx), amount).
    pub runes: Vec<(RuneId, RuneQuantity)>,
}

// Pagination by height or by rune amount.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub enum Cursor {
    ByHeight {
        // Block height.
        height: Height,

        // Tx hash of the UTxO containing the runes.
        utxo_hash: TxHash,

        // Tx output index of the UTxO containing the runes.
        utxo_index: TxoIndex,
    },
    ByAmount {
        // Amount of relevant runes.
        amount: RuneQuantity,

        // Tx hash of the UTxO containing the runes.
        utxo_hash: TxHash,

        // Tx output index of the UTxO containing the runes.
        utxo_index: TxoIndex,
    },
}
