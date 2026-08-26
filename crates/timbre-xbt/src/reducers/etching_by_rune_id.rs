use crate::Decode;
use timbre_xbt_macros::Encode;

use super::{RuneId, TxHash};

#[derive(Clone, Debug, Encode, Decode)]
/// size 12
pub struct Key {
    pub rune_id: RuneId,
}

#[derive(Encode, Decode, Debug)]
/// max size 163 (including breaks)
pub struct Value {
    pub tx_hash: TxHash,
    pub name: Option<u128>,
    pub spacers: Option<u32>,
    pub symbol: Option<u32>, // char as u32
    pub divisibility: Option<u8>,
    pub premine: Option<u128>,
    pub max_mint_txs: Option<u128>,
    pub amount_per_mint: Option<u128>,
    pub start_height: Option<u64>,
    pub end_height: Option<u64>,
    pub start_offset: Option<u64>,
    pub end_offset: Option<u64>,
    pub turbo: bool,
    pub cenotaph: bool,
}
