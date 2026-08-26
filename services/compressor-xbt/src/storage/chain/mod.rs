use crate::storage::kvtable::*;

use super::BlockValue;

// block height -> block hash, height, body bytes
pub struct BlockByHeightKV;

impl KVTable<DBInt, DBSerde<BlockValue>> for BlockByHeightKV {
    const CF_NAME: &'static str = "BlockByHeightKV";
}
