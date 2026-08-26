use crate::{Decode, Encode};

use super::TxHash;

#[derive(Clone, Debug, Encode, Decode)]
// max size: 32
pub struct Key {
    pub tx_hash: TxHash,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
// max size: 17
pub struct Value {
    /// The timestamp of the first time the tx was seen.
    pub timestamp: u64,
}
