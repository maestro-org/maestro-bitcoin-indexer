use crate::{Decode, Encode, ShortByteString};

use super::{
    Brc20Quantity, Height, InscriptionId, SatoshiOffset, SatoshiQuantity, ScriptHash, TxHash,
    TxoIndex,
};

#[derive(Clone, Debug, Encode, Decode)]
/// size 63 or 64 (including breaks)
pub struct Key {
    // Script hash.
    pub script_hash: ScriptHash,
    // Ticker of the transfer inscription.
    pub ticker: ShortByteString,
    // Inscription ID.
    pub inscription_id: InscriptionId,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
/// size 77 (including breaks)
pub struct Value {
    // Amount of BRC20 token locked in the transfer inscription.
    pub token_amount: Brc20Quantity,
    // Amount of sat locked in the UTxO.
    pub sat_amount: SatoshiQuantity,
    // Tx hash of the UTxO containing the inscribed sat.
    pub utxo_hash: TxHash,
    // Tx output index of the UTxO containing the inscribed sat.
    pub utxo_index: TxoIndex,
    // Offset of the inscribed sat within the UTxO.
    pub offset: SatoshiOffset,
    // Block height of the transfer inscription.
    pub block_height: Height,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    // Ticker of the transfer inscription.
    pub ticker: ShortByteString,
    // Inscription ID.
    pub inscription_id: InscriptionId,
}
