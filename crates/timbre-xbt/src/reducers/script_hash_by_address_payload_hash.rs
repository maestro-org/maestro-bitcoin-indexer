use crate::Decode;
use timbre_xbt_macros::Encode;

use super::ScriptHash;

#[derive(Clone, Debug, Encode, Decode)]
/// size 20
pub struct Key {
    pub payload_hash: [u8; 20],
}

#[derive(Clone, Debug, Encode, Decode)]
/// size 20
pub struct Value {
    pub script_hash: ScriptHash,
}
