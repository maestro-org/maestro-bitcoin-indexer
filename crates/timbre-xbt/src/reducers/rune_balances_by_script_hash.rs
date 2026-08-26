use crate::Decode;
use timbre_xbt_macros::Encode;

use super::{RuneId, RuneQuantity, ScriptHash};

#[derive(Clone, Debug, Encode, Decode)]
pub struct Key {
    pub script_hash: ScriptHash,
    pub rune_id: RuneId,
}

pub type Value = RuneQuantity;
