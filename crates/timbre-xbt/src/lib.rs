use std::ops::Range;

use reducers::{BlockHash, Height};
use thiserror::Error;

mod encdec;
pub mod ingestors;
pub mod reducers;
pub mod rollback;

pub use encdec::*;
use timbre_xbt_macros::{Decode, Encode};

#[derive(Debug, Clone, Copy)]
#[repr(u8)] // Enforces unique representation as u8 for numeric casting
pub enum Reducer {
    EtchingByRuneId = 97,                    // 'a'
    MintsByRuneId = 98,                      // 'b'
    UtxosByRuneId = 99,                      // 'c'
    UtxosByScriptHash = 100,                 // 'd'
    RunesByUtxo = 101,                       // 'e'
    RuneInfoByRuneId = 102,                  // 'f'
    ScriptByScriptHash = 103,                // 'g'
    ScriptHashByAddressPayloadHash = 104,    // 'h'
    RuneIdByRuneName = 105,                  // 'i'
    InscriptionsByUtxo = 106,                // 'j'
    Brc20TotalBalanceByScriptHash = 107,     // 'k'
    Brc20AvailableBalanceByScriptHash = 108, // 'l'
    Brc20TermsByTicker = 109,                // 'm'
    BalancesByBrc20 = 110,                   // 'n'
    TxsByScriptHash = 111,                   // 'o'
    RuneBalancesByScriptHash = 112,          // 'p'
    InscriptionsByScriptHash = 113,          // 'q'
    ContentByInscriptionId = 114,            // 'r'
    TransferInscriptionsByScriptHash = 115,  // 's'
    SatsPerVbByBlock = 116,                  // 't'
    InscriptionUtxosByScriptHash = 117,      // 'u'
    InscriptionActivityByBlock = 118,        // 'v'
    BlockByTxHash = 119,                     // 'w'
    HeightByBlockHash = 120,                 // 'x'
    InscriptionActivityByTx = 121,           // 'y'
    TxsByInscription = 122,                  // 'z'
    TxInfo = 123,
    BlockInfo = 124,
    TxsByBlock = 125,
    BalancesByRuneId = 126,
    SpendingTxByTxo = 127,
    RuneUtxosByScriptHash = 129,
    TxsByRuneId = 130,
    TxFirstSeenTimestamp = 131,
    RuneTxsByScriptHash = 132,
    SatBalanceByScriptHash = 133,
    SatTxsByScriptHash = 134,
    InscriptionActivityByScriptHash = 135,
    InscriptionActivityByTxV2 = 136,
    TotalTxsByScriptHash = 137,
    TotalUtxosByScriptHash = 138,
    TotalInscriptionsByScriptHash = 139,
    HeightByTimestamp = 140,
    HistoricalSatBalanceByScriptHash = 141,
    TotalOutputsByScriptHash = 142,
    TotalSatInOutputsByScriptHash = 143,
    TotalSatInInputsByScriptHash = 144,
}

impl Encode for Reducer {
    fn encode(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)] // Enforces unique representation as u8 for numeric casting
pub enum CollectionIngestor {
    MetadataByInscription = 67,      // 'C'
    InscriptionsBySymbol = 73,       // 'I'
    TokenMetadataByInscription = 77, // 'M'
    MetadataBySymbol = 83,           // 'S'
    StatsBySymbol = 85,              // 'U'
    OmbColorGroupByInscription = 66, // 'B'
}

impl Encode for CollectionIngestor {
    fn encode(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MinerIngestor;

impl Encode for MinerIngestor {
    fn encode(&self) -> Vec<u8> {
        vec![77u8] // 'M'
    }
}

#[derive(Debug, Clone, Error, PartialEq)]
pub enum TimbreError {
    #[error("Malformed input: {0}")]
    MalformedInput(String),
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("Invalid UTF-8 str: {0}")]
    InvalidUtf8Str(#[from] std::str::Utf8Error),
    #[error("Malformed base64: {0}")]
    MalformedBase64(#[from] base64::DecodeError),
    #[error("Invalid range")]
    InvalidRange,
    #[error("Unable to cast into target type: {0}")]
    VarUIntCasting(u128),
}

#[derive(Debug, PartialEq, Eq)]
pub struct CursorValue {
    pub height: Height,
    pub hash: BlockHash,
    pub was_mempool: bool,
    pub timestamp: u64,
    // chain tip and mempool view ts
    pub mempool_info: Option<((Height, BlockHash), u64)>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SplitCommitLockValue {
    pub safe_point: Option<(Height, BlockHash)>,
    pub total_actions: u64,
    pub mutable: bool,
}

/// A bytestring that is at most 255 bytes long
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ShortByteString(pub Vec<u8>);

impl TryFrom<Vec<u8>> for ShortByteString {
    type Error = Vec<u8>;

    fn try_from(bs: Vec<u8>) -> Result<Self, Self::Error> {
        if bs.len() > u8::MAX.into() {
            Err(bs)
        } else {
            Ok(Self(bs))
        }
    }
}

impl From<ShortByteString> for Vec<u8> {
    fn from(val: ShortByteString) -> Self {
        val.0
    }
}

/// Unsigned integer that is encoded efficiently while mantaining expected ordering
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
struct VarUInt(pub u128);

macro_rules! impl_to_varuint {
    ($type:ty) => {
        impl From<$type> for VarUInt {
            fn from(val: $type) -> Self {
                VarUInt(val.try_into().unwrap())
            }
        }
    };
}

impl_to_varuint!(usize);
impl_to_varuint!(u8);
impl_to_varuint!(u16);
impl_to_varuint!(u32);
impl_to_varuint!(u64);
impl_to_varuint!(u128);

macro_rules! impl_try_from_varuint {
    ($type:ty) => {
        impl TryFrom<VarUInt> for $type {
            type Error = TimbreError;

            fn try_from(val: VarUInt) -> Result<$type, Self::Error> {
                let inner_val = val.inner();
                inner_val
                    .try_into()
                    .map_err(|_| TimbreError::VarUIntCasting(inner_val))
            }
        }
    };
}

impl_try_from_varuint!(usize);
impl_try_from_varuint!(u8);
impl_try_from_varuint!(u16);
impl_try_from_varuint!(u32);
impl_try_from_varuint!(u64);
impl_try_from_varuint!(u128);

impl VarUInt {
    pub fn inner(self) -> u128 {
        self.0
    }
}

pub fn prefix_key_range(prefix: &[u8]) -> Range<Vec<u8>> {
    let start = prefix.to_vec();
    let mut end = prefix.to_vec();

    // Work backwards to handle the case where the last byte(s) are 255
    for i in (0..end.len()).rev() {
        if end[i] != 255 {
            end[i] += 1;
            end.truncate(i + 1);
            return start..end;
        }
    }

    // If all bytes are 255, the range is unbounded at the upper end
    start..vec![]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_key_range() {
        let prefix = vec![0x01, 0x02, 0x03];
        let range = prefix_key_range(&prefix);

        assert_eq!(range.start, vec![0x01, 0x02, 0x03]);
        assert_eq!(range.end, vec![0x01, 0x02, 0x04]);

        let prefix = vec![0xFF, 0xFF];
        let range = prefix_key_range(&prefix);

        assert_eq!(range.start, vec![0xFF, 0xFF]);
        assert_eq!(range.end, vec![]);

        let prefix = vec![0xFE, 0xFF];
        let range = prefix_key_range(&prefix);

        assert_eq!(range.start, vec![0xFE, 0xFF]);
        assert_eq!(range.end, vec![0xFF]);

        let prefix = vec![];
        let range = prefix_key_range(&prefix);

        assert_eq!(range.start, vec![]);
        assert_eq!(range.end, vec![]);
    }
}
