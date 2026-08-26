use crate::{Decode, Encode};

use super::{Height, InscriptionId, SatoshiOffset, ScriptHash, TxHash, TxIndex, TxoIndex};

#[derive(Clone, Debug, Encode, Decode)]
/// max size: 35
pub struct Key {
    // block height
    pub height: Height,

    // index of transaction in block
    pub tx_index: TxIndex,
}

// max size: 32 + 1 + 4 + num of inscriptions * ((32 + 4) + (1 + 20 + 4 + 8) + (1 + 20 + 4 + 8))
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Value {
    pub tx_hash: TxHash,

    pub inscriptions_activity: Vec<(
        // (reveal tx hash, index of inscription in reveal tx)
        InscriptionId,
        (
            // (from address, tx input index, inscribed sat offset)
            // NOTE: this is defined as optional to account for new inscriptions
            Option<(ScriptHash, TxoIndex, SatoshiOffset)>,
            // (to address, tx output index, inscribed sat offset)
            // NOTE: this is defined as optional to account for inscriptions spent as fee
            Option<(ScriptHash, TxoIndex, SatoshiOffset)>,
        ),
    )>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    // tx index in block
    pub tx_index: TxIndex,
    // activity within the tx
    pub activity_index: u32,
}
