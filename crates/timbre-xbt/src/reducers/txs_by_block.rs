use crate::{Decode, Encode};

use super::{Height, TxHash, TxIndex};

#[derive(Clone, Debug, Encode, Decode)]
pub struct Key {
    /// Block height.
    pub height: Height,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
/// size: 32
pub struct Value {
    /// Transaction hashes.
    pub tx_hashes: Vec<TxHash>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    /// Index of transaction in block.
    pub tx_index: TxIndex,
}
