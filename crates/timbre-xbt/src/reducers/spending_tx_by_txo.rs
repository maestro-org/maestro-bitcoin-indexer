use crate::{Decode, Encode};

use super::{TxHash, TxoIndex};

#[derive(Clone, Debug, Encode, Decode)]
pub struct Key {
    /// Hash of the transaction that produced this output.
    pub utxo_tx_hash: TxHash,

    /// Output index.
    pub utxo_vout: TxoIndex,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct Value {
    pub tx_hash: TxHash,
}
