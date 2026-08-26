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
    // Total number of utxos currently controlled by the script.
    pub total_utxos: u64,
}
