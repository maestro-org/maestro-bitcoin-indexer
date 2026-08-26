use crate::{Decode, ShortByteString};
use timbre_xbt_macros::Encode;

use super::{Brc20Quantity, ScriptHash};

#[derive(Clone, Debug, Encode, Decode)]
/// size 26 or 27 (including breaks)
pub struct Key {
    pub ticker: ShortByteString,
    pub script_hash: ScriptHash,
}

pub type Value = Brc20Quantity;
