use crate::{Decode, Encode};

use super::{SatoshiQuantity, ScriptHash};

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
/// size: 20
pub struct Key {
    // Script hash.
    pub script_hash: ScriptHash,
}

#[derive(Encode, Decode, Clone, Debug)]
/// size: 8
pub struct Value {
    pub satoshis: SatoshiQuantity,
}
