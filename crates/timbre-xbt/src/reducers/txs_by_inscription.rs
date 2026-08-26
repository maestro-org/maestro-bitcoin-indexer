use crate::{Decode, Encode};
use indexmap::IndexMap;

use super::{Height, InscriptionIndex, TxIndex};

pub const PREFIX_LENGTH: usize = 2;
const SUFFIX_LENGTH: usize = 32 - PREFIX_LENGTH;

pub type PREFIX = [u8; PREFIX_LENGTH];
pub type SUFFIX = [u8; SUFFIX_LENGTH];

// The reducer will store the following KV pair:
//      (
//          Key {
//              bucket_id: bid,
//              height: h,
//          },
//          Value {
//              inscriptions_by_block: [
//                  (
//                      (reveal_tx_hash_suffix_0, index_of_inscription_0),
//                      [(tx_height_00, activity_00), (tx_height_01, activity_01)],
//                  ),
//                  (
//                      (reveal_tx_hash_suffix_1, index_of_inscription_1),
//                      [(tx_height_1, activity_1)],
//                  ),
//              ]
//          }
//      )
// if and only if there exist:
//      1. inscription inscription_0 such that
//          - its reveal transaction hash is <bid><reveal_tx_hash_suffix_0>, and its inscription
//              index in that reveal tx is index_of_inscription_0, meaning its inscription ID is:
//                  <bid><reveal_tx_hash_suffix_0>i<index_of_inscription_0>,
//          - it has activity at:
//              * block height h, transaction index tx_height_00 and activity index activity_00
//                  in that transaction, and
//              * block height h, transaction index tx_height_01 and activity index activity_01
//                  in that transaction.
//      2. inscription inscription_1 such that
//          - its reveal transaction hash is <bid><reveal_tx_hash_suffix_1>, and its inscription
//              index in that reveal tx is index_of_inscription_1, meaning its inscription ID is:
//                  <bid><reveal_tx_hash_suffix_1>i<index_of_inscription_1>,
//          - it has activity at:
//              * block height h, transaction index tx_height_1 and activity index activity_1
//                  in that transaction,.

// max size 20
#[derive(Clone, Debug, Encode, Decode)]
pub struct Key {
    // 65535 buckets
    pub bucket_id: PREFIX,
    // block height
    pub height: Height,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct Value {
    // a map from
    // (
    //      reveal tx hash suffix after removing prefix with which we compute the bucket ID,
    //      index of inscription in that reveal tx,
    // )
    // to a list of activity entries, represented by a list of transaction indices in this block
    // and the activity index of the inscription in question within that transaction
    pub activity: IndexMap<(SUFFIX, InscriptionIndex), Vec<(TxIndex, u32)>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    // block height
    pub height: Height,
    // index of transaction in block
    pub tx_index: TxIndex,
}
