use crate::{Decode, Encode};

use super::{Height, InscriptionId, SatoshiOffset, ScriptHash, TxHash, TxIndex, TxoIndex};

#[derive(Clone, Debug, Encode, Decode)]
/// max size 20 + 1 + 8 + 1 + 4 + 1 + 32
pub struct Key {
    // Script hash.
    pub script_hash: ScriptHash,

    // Block height.
    pub height: Height,

    // Index of tx in the block.
    pub activity_tx_index: TxIndex,

    // Transaction hash.
    pub tx_hash: TxHash,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
/// max size (4 + number of self-transfers * 56) + 1 + (4 + number of sent inscriptions * 80) + 1 + (4 + number of received inscriptions * 80)
pub struct Value {
    // List of self-transferred inscriptions.
    pub self_transfers: Vec<SelfTransferredInscription>,

    // List of sent inscriptions.
    pub sent: Vec<SentInscription>,

    // List of received inscriptions.
    pub received: Vec<ReceivedInscription>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
/// max size: (32 + 4) + 1 + 4 + 1 + 4 + 1 + 4 + 1 + 4
pub struct SelfTransferredInscription {
    pub inscription_id: InscriptionId,

    pub input_index: TxoIndex,

    pub input_sat_offset: SatoshiOffset,

    pub output_index: TxoIndex,

    pub output_sat_offset: SatoshiOffset,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
/// max size: (32 + 4) + 1 + 8 + 1 + 4 + 1 + (1 + 8) + 1 + (1 + 4) + 1 + (1 + 20)
pub struct SentInscription {
    pub inscription_id: InscriptionId,

    pub input_index: TxoIndex,

    pub input_sat_offset: SatoshiOffset,

    pub output_index: Option<TxoIndex>,

    pub output_sat_offset: Option<SatoshiOffset>,

    pub output_script_hash: Option<ScriptHash>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
/// max size: (32 + 4) + 1 + (1 + 8) + 1 + (1 + 4) + 1 + (1 + 20) + 1 + 8 + 1 + 4
pub struct ReceivedInscription {
    pub inscription_id: InscriptionId,

    pub input_index: Option<TxoIndex>,

    pub input_sat_offset: Option<SatoshiOffset>,

    pub input_script_hash: Option<ScriptHash>,

    pub output_index: TxoIndex,

    pub output_sat_offset: SatoshiOffset,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct Cursor {
    // Block height.
    pub height: Height,

    // Index of tx in the block.
    pub activity_tx_index: TxIndex,

    // Transaction hash.
    pub tx_hash: TxHash,
}
