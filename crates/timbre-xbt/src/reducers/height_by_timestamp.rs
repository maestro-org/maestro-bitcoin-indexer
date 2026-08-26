use crate::{Decode, Encode};

use super::{Height, Timestamp};

#[derive(Clone, Debug, Encode, Decode)]
/// max size: 4
pub struct Key {
    pub timestamp: Timestamp,
}

// max size: 8
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Value {
    pub height: Height,
}
