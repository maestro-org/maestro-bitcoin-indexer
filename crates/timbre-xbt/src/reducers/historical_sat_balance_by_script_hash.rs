use crate::{Decode, Encode};

use super::{Height, SatoshiQuantity, ScriptHash};

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
/// size: 20 + 8
pub struct Key {
    // Script hash.
    pub script_hash: ScriptHash,

    // Block height.
    pub height: Height,
}

/// size: 8
pub type Value = SatoshiQuantity;

pub type Cursor = Height;
