use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct Brc20TickerAndBalance {
    pub ticker: String,
    pub ticker_hex: String,
    pub balances: Brc20Balances,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct Brc20Balances {
    pub total: String,
    pub available: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct Brc20Holder {
    pub address: Option<String>,
    pub script_pubkey: String,
    pub balance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct Brc20Ticker(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct Brc20Info {
    pub ticker: String,
    pub ticker_hex: String,
    pub deploy_inscription: String,
    pub holders: u64,
    pub minted_supply: String,
    pub terms: Brc20Terms,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct Brc20Terms {
    pub max: String,
    pub limit: String,
    pub dec: u8,
    pub self_mint: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct InscriptionInfo {
    /// String representation of the inscription ID, whose first coordinate is the reveal
    /// transaction hash, and the second coordinate is the index of inscription in the reveal
    /// transaction.
    pub inscription_id: String,
    /// Global inscription number.
    pub inscription_number: Option<u64>,
    /// Block height of the reveal transaction.
    pub created_at: u64,
    /// Current location.
    pub current_location: InscriptionLocation,
    /// Type of the content body.
    pub content_type: Option<String>,
    /// Preview of inscription content body raw data. Max: 100 bytes.
    /// Supported types: "text/plain", "text/plain;charset=utf-8", "application/json".
    pub content_body_preview: Option<String>,
    /// Length of entire inscription content body bytes array.
    pub content_length: u64,
    /// Symbol of collection that the inscription belongs to.
    pub collection_symbol: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct InscriptionLocation {
    /// Address-encoding of the script pubkey currenty controlling the inscription.
    pub address: Option<String>,
    /// Script pubkey currently controlling the inscription.
    pub script_pubkey: String,
    /// Inscribed sat offset in the UTxO containing the inscription.
    pub utxo_sat_offset: u64,
    /// Transaction ID of the UTxO containing the inscription.
    pub utxo_txid: String,
    /// Output index of the UTxO containing the inscription.
    pub utxo_vout: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct InscriptionByAddress {
    /// String representation of the inscription ID, whose first coordinate is the reveal
    /// transaction hash, and the second coordinate is the index of inscription in the reveal
    /// transaction.
    pub inscription_id: String,
    /// Total number of satoshis in the UTxO containing the inscription.
    pub satoshis: String,
    /// Inscribed sat offset in the UTxO containing it.
    pub utxo_sat_offset: u64,
    /// Transaction ID of the UTxO containing the inscription.
    pub utxo_txid: String,
    /// Output index of the UTxO containing the inscription.
    pub utxo_vout: u32,
    /// Block height of the UTxO containing the inscription.
    pub utxo_block_height: u64,
    /// Number of confirmations of the block where the UTxO containing the inscription was created.
    pub utxo_confirmations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct ContentBody {
    /// Base64-encoded representation of a slice of the inscription content body. All types supported.
    pub content_body_page: String,
    /// Number of bytes in entire inscription content body.
    pub total_length: u64,
    /// Number of bytes remaining in the inscription content body.
    pub remaining_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct InscriptionActivityByBlock {
    /// Transaction in which the activity occurred
    pub tx_hash: String,
    /// String representation of the inscription ID, whose first coordinate is the reveal transaction hash, and the second coordinate is the index of inscription in the reveal transaction.
    pub inscription_id: String,
    /// Information about the inscription prior to this transaction. If it is inscribed in this transaction, then this field is null.
    pub from: Option<FromInscriptionLocation>,
    /// Information about the inscription after this transaction. If the inscribed satoshi was paid as fee, then location must be output controlled by the block miner in the coinbase tx.
    pub to: ToInscriptionLocation,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct InscriptionActivityByTx {
    /// String representation of the inscription ID, whose first coordinate is the reveal transaction hash, and the second coordinate is the index of inscription in the reveal transaction.
    pub inscription_id: String,
    /// Information about the inscription prior to this transaction. If it is inscribed in this transaction, then this field is null.
    pub from: Option<FromInscriptionLocation>,
    /// Location of the inscription after this transaction. If the inscribed satoshi was paid as fee, then location must be output controlled by the block miner in the coinbase tx.
    pub to: ToInscriptionLocation,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct FromInscriptionLocation {
    /// Address-encoding of the script pubkey at which the input containing the inscription resides.
    pub address: Option<String>,
    /// Script pubkey at which the input containing the inscription resides.
    pub script_pubkey: String,
    /// Index of the input containing the inscription.
    pub input_index: u32,
    /// Offset of the inscribed satoshi within the input.
    pub sat_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct ToInscriptionLocation {
    /// Address-encoding of the script pubkey at which the output containing the inscription resides.
    pub address: Option<String>,
    /// Script pubkey at which the output containing the inscription resides.
    pub script_pubkey: String,
    /// Index of the output containing the inscription.
    pub output_vout: u32,
    /// Offset of the inscribed satoshi within the output.
    pub sat_offset: u64,
    /// Hash of tx producing the output containing the inscription. If this is the hash of the coinbase tx of the block, then the inscription was spent as fee in a tx in the block and therefore sent to the output of the coinbase tx controlled by the block miner.
    pub output_txid: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxByInscription {
    /// Height of block containing transaction that the inscription took part in.
    pub height: u64,
    /// Index of transaction (in block) that the inscription took part in.
    pub tx_index: u32,
    /// Transaction hash.
    pub tx_hash: String,
    /// Whether the transaction was inscribed or transferred.
    pub r#type: InscriptionTxKind,
    /// Information about the inscription prior to this transaction. If it is inscribed in this transaction, then this field is null.
    pub from: Option<FromInscriptionLocation>,
    /// Information about the inscription after this transaction. If the inscribed satoshi was paid as fee, then location must be output controlled by the block miner in the coinbase tx.
    pub to: ToInscriptionLocation,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
#[serde(rename_all = "snake_case")]
pub enum InscriptionTxKind {
    /// The satoshi was inscribed and set to an output.
    Inscribe,

    /// The inscription was spent from an input and sent to an output.
    Transfer,

    /// The satoshi was inscribed and immediately spent as fee.
    InscribeAndSpentAsFee,

    /// The inscription was spent from an input and spent as fee.
    SpentAsFee,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TransferInscriptionByAddress {
    // Ticker of the inscribed BRC20 token.
    pub ticker: String,
    /// String representation of the inscription ID, whose first coordinate is the reveal transaction hash, and the second coordinate is the index of inscription in the reveal transaction.
    pub inscription_id: String,
    /// Number of tokens locked in the UTxO.
    pub token_amount: String,
    /// Number of sats locked in the UTxO.
    pub satoshis: String,
    /// Transaction ID of the UTxO containing the inscription.
    pub utxo_txid: String,
    /// Output index of the UTxO containing the inscription.
    pub utxo_vout: u32,
    /// Offset of inscribed sat in the UTxO containing it.
    pub utxo_sat_offset: u64,
    /// Block height of the UTxO containing the inscription.
    pub utxo_block_height: u64,
    /// Number of confirmations of the block where the UTxO containing the inscription was created.
    pub utxo_confirmations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct InscriptionActivityByAddress {
    /// Height of block containing the inscription activity.
    pub height: u64,

    /// Number of confirmation blocks.
    pub confirmations: u64,

    /// Hash of transaction containing the inscription activity.
    pub tx_hash: String,

    /// Inscription activity, as lists of self-transferred, sent and received inscriptions.
    pub inscription_activity: InscriptionActivity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct WalletInscriptionActivityByAddress {
    /// Height of block containing the inscription activity.
    pub height: u64,

    /// Number of confirmation blocks.
    pub confirmations: u64,

    /// Whether the data is pending (true) or confirmed (false).
    pub mempool: bool,

    /// Hash of transaction containing the inscription activity.
    pub tx_hash: String,

    /// Inscription activity, as lists of self-transferred, sent and received inscriptions.
    pub inscription_activity: InscriptionActivity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct InscriptionActivity {
    /// List of inscriptions that were self-transferred in the transaction.
    pub self_transferred: Vec<InscriptionActivityByTx>,

    /// List of inscriptions which the script lost control of.
    pub sent: Vec<InscriptionActivityByTx>,

    /// List of inscriptions which the script gained control over.
    pub received: Vec<InscriptionActivityByTx>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
#[serde(rename_all = "snake_case")]
pub enum InscriptionActivityKindByAddress {
    SelfTransfer,
    Send,
    Receive,
}
