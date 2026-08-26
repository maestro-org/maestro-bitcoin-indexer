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
    // Total number of sats in all transaction inputs (i.e., spent transaction outputs).
    pub total_sat_in_inputs: u128,
}
