use super::ScriptHash;
use crate::{Decode, Encode};

#[derive(Clone, Debug, Encode, Decode)]
/// max size 20
pub struct Key {
    // Script hash.
    pub script_hash: ScriptHash,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
/// max size 16
pub struct Value {
    // Total number of sats in all the transaction outputs (spent or unspent) controlled by the script.
    pub total_sat_in_outputs: u128,
}
