use crate::{Decode, ShortByteString};
use timbre_xbt_macros::Encode;

use super::{Brc20Quantity, InscriptionId};

#[derive(Clone, Debug, Encode, Decode)]
/// size 5 or 6
pub struct Key {
    pub ticker: ShortByteString,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
/// size 74 (including breaks)
pub struct Value {
    pub max: Brc20Quantity,
    pub limit: Brc20Quantity,
    pub dec: u8,
    pub self_mint: bool,
    pub deploy_id: InscriptionId,
}
