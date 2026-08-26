use crate::{Decode, Encode};

use super::{Height, InscriptionId};

#[derive(Clone, Debug, Encode, Decode)]
/// size 36
pub struct Key {
    // (reveal tx hash, index of inscriptions in reveal tx)
    pub inscription_id: InscriptionId,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
/// 27 + content type length + content body length (including breaks)
pub struct Value {
    // block height of the reveal tx
    pub created_at: Height,
    // global inscription number
    pub inscription_num: u64,
    // type of the content body
    pub content_type: Vec<u8>,
    // inscription content body raw data
    pub content_body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    // offset of last read byte in content_body
    pub offset: u64,
}
