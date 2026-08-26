use crate::Decode;
use timbre_xbt_macros::Encode;

use super::ScriptHash;

#[derive(Clone, Debug, Encode, Decode)]
/// size 20
pub struct Key {
    pub script_hash: ScriptHash,
}

#[derive(Clone, Debug, Encode, Decode)]
/// size 4 + script length
pub struct Value {
    pub script: Vec<u8>,
}
