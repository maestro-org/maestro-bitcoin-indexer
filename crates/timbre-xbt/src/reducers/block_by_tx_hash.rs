use crate::{Decode, Encode};

use super::{Height, TxHash};

#[derive(Clone, Debug, Encode, Decode)]
/// size 32
pub struct Key {
    pub tx_hash: TxHash,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Value {
    // block height
    pub height: Height,
}
