use crate::{Decode, Encode};

use super::{
    Height, InscriptionId, InscriptionIndex, SatoshiOffset, SatoshiQuantity, ScriptHash, TxHash,
    TxoIndex,
};

#[derive(Clone, Debug, Encode, Decode)]
pub struct Key {
    // script hash
    pub script_hash: ScriptHash,
    // block height
    pub height: Height,
    // tx hash of the UTxO containing the inscribed sat
    pub utxo_hash: TxHash,
    // tx output index of the UTxO containing the inscribed sat
    pub utxo_index: TxoIndex,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Value {
    pub satoshis: SatoshiQuantity,
    // list of inscriptions held in the UTxO, each one consisting in:
    //      - offset in the UTxO of the inscribed sat,
    //      - the inscription ID (reveal tx hash, index of new inscription in reveal tx)
    pub inscriptions: Vec<(SatoshiOffset, InscriptionId)>,
}

// User-facing endpoint paginates according to lexicographical order of inscriptions
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    // reveal tx hash
    pub tx_id: TxHash,
    // index of new inscription in reveal tx
    pub index: InscriptionIndex,
}
