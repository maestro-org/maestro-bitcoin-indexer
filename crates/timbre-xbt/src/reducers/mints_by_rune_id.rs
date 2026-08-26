use crate::Decode;
use timbre_xbt_macros::Encode;

use super::{RuneId, RuneQuantity};

#[derive(Clone, Debug, Encode, Decode)]
pub struct Key {
    pub rune_id: RuneId,
}

pub type Value = RuneQuantity;
