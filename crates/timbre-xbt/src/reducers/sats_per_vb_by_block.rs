use crate::Decode;
use timbre_xbt_macros::Encode;

use super::Height;

#[derive(Clone, Debug, Encode, Decode)]
/// size 8
pub struct Key {
    // block height
    pub height: Height,
}

#[derive(Clone, Debug, Encode, Decode)]
/// size 26 (including breaks)
pub struct Value {
    pub min: u64,
    pub median: u64,
    pub max: u64,
}
