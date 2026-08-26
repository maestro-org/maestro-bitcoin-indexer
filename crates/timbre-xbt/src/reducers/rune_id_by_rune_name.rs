use crate::Decode;
use timbre_xbt_macros::Encode;

use super::RuneId;

#[derive(Clone, Debug, Encode, Decode)]
/// size 16
pub struct Key {
    pub rune_name: u128,
}

#[derive(Encode, Decode, Debug)]
/// size 12
pub struct Value {
    pub rune_id: RuneId,
}
