use crate::{Decode, Encode};

use super::{BlockHash, Height};

#[derive(Clone, Debug, Encode, Decode)]
/// size 32
pub struct Key {
    pub block_hash: BlockHash,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Value {
    pub block_height: Height,
}
