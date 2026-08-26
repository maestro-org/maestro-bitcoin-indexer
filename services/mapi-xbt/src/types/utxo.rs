use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct Utxo {
    pub txid: String,
    pub vout: u32,
    pub address: Option<String>,
    pub script_pubkey: String,
    pub satoshis: String,
    pub confirmations: u64,
    pub height: u64,
    pub runes: Vec<RuneAndAmount>,
    pub inscriptions: Vec<InscriptionAndOffset>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct MempoolUtxo {
    pub txid: String,
    pub vout: u32,
    pub address: Option<String>,
    pub script_pubkey: String,
    pub satoshis: String,
    pub height: u64,
    pub mempool: bool,
    pub runes: Vec<RuneAndAmount>,
    pub inscriptions: Vec<InscriptionAndOffset>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct RuneAndAmount {
    pub rune_id: String,
    pub amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct InscriptionAndOffset {
    pub offset: u64,
    pub inscription_id: String,
}
