use super::ScriptHash;
use crate::{Decode, Encode};

#[derive(Clone, Debug, Encode, Decode)]
/// max size 20
pub struct Key {
    // Script hash.
    pub script_hash: ScriptHash,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
/// max size 8
pub struct Value {
    // Total number of txs the script was ever involved in.
    pub total_txs: u64,
}
