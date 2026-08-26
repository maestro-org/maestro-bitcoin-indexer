use crate::types::utxo::RuneAndAmount;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct RuneInfo {
    pub id: String,
    pub etching_cenotaph: bool,
    pub etching_tx: String,
    pub etching_height: u64,
    pub name: String,
    pub spaced_name: String,
    pub symbol: Option<char>,
    /// If no divisibility was specified, then this equals 0
    pub divisibility: u8,
    pub premine: Option<String>,
    pub terms: Terms,
    pub max_supply: String,
    pub circulating_supply: String,
    pub mints: u64,
    // pub burned: String, // TODO
    pub unique_holders: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct RuneInfoBrief {
    pub id: String,
    pub etching_cenotaph: bool,
    pub etching_tx: String,
    pub etching_height: u64,
    pub name: String,
    pub spaced_name: String,
    pub symbol: Option<char>,
    /// If no divisibility was specified, then this equals 0
    pub divisibility: u8,
    pub premine: Option<String>,
    pub terms: Terms,
}

// TODO: default these if they are ommitted?
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct Terms {
    pub mint_txs_cap: Option<String>,
    pub amount_per_mint: Option<String>,
    pub start_height: Option<String>,
    pub end_height: Option<String>,
    pub start_offset: Option<String>,
    pub end_offset: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct RuneUtxo {
    pub txid: String,
    pub vout: u32,
    pub address: Option<String>,
    pub script_pubkey: String,
    pub satoshis: String,
    pub confirmations: u64,
    pub height: u64,
    pub rune_amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct DeprecatedRuneUtxoByAddress {
    pub txid: String,
    pub vout: u32,
    pub satoshis: String,
    pub confirmations: u64,
    pub height: u64,
    pub rune_amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct RuneUtxoByAddress {
    pub txid: String,
    pub vout: u32,
    pub satoshis: String,
    pub confirmations: u64,
    pub height: u64,
    pub runes: Vec<RuneAndAmount>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct RuneIdAndName {
    pub id: String,
    pub spaced_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct RuneHolder {
    pub address: Option<String>,
    pub script_pubkey: String,
    pub balance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxByRune {
    /// Height of block containing the rune activity.
    pub height: u64,
    /// Number of confirmation blocks.
    pub confirmations: u64,
    /// Hash of transaction containing the rune activity.
    pub tx_hash: String,
    /// Whether this is the etching transaction of the rune.
    pub etching_tx: bool,
    /// Number of runes minted in this transaction.
    pub minted: Option<String>,
    /// Number of runes burned in this transaction.
    pub burned: Option<String>,
    /// List of addresses and the corresponding amount, of addresses whose rune balances do not change after the tx, as they are only involved in self-transfers.
    pub self_transfers: Vec<AddressAndRuneAmount>,
    /// List of addresses that see their rune balances decrease after the tx, and the corresponding amount.
    pub senders: Vec<AddressAndRuneAmount>,
    /// List of addresses that see their rune balances increase after the tx, and the corresponding amount.
    pub receivers: Vec<AddressAndRuneAmount>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct AddressAndRuneAmount {
    /// Address-encoding of the script pubkey.
    pub address: Option<String>,
    /// Script pubkey.
    pub script_pubkey: String,
    /// Amount of runes.
    pub amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct RuneActivityByAddress {
    /// Height of block containing the rune activity.
    pub height: u64,

    /// Number of confirmation blocks.
    pub confirmations: u64,

    /// Hash of transaction containing the rune activity.
    pub tx_hash: String,

    /// Rune activity, as etched runes, minted runes, self-transferred runes, runes for which the balance increased, and runes for which the balance decreased.
    pub rune_activity: RuneActivity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct WalletRuneActivityByAddress {
    /// Height of block containing the rune activity.
    pub height: u64,

    /// Number of confirmation blocks.
    pub confirmations: u64,

    /// Whether the data is pending (true) or confirmed (false).
    pub mempool: bool,

    /// Hash of transaction containing the rune activity.
    pub tx_hash: String,

    /// Rune activity, as etched runes, minted runes, self-transferred runes, runes for which the balance increased, and runes for which the balance decreased.
    pub rune_activity: WalletRuneActivity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct RuneActivity {
    /// Etched runes, as rune ID and amount of premined runes.
    pub etched_rune: Option<EtchAndPremine>,

    /// Minted runes, as rune ID.
    pub minted: Option<RuneAndAmount>,

    /// List of runes that were self-transferred.
    pub self_transfers: Vec<RuneAndAmount>,

    /// List of runes and amounts, corresponding to increased balances for this address.
    pub increased_balances: Vec<RuneAndAmount>,

    /// List of runes and amounts, corresponding to decreased balances for this address.
    pub decreased_balances: Vec<RuneAndAmount>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct WalletRuneActivity {
    /// Etched runes, as rune ID and amount of premined runes.
    pub etched_rune: Option<EtchAndPremine>,

    /// Minted runes, as rune ID.
    pub minted: Option<WalletRuneAndAmount>,

    /// List of runes that were self-transferred.
    pub self_transfers: Vec<WalletRuneAndAmount>,

    /// List of runes and amounts, corresponding to increased balances for this address.
    pub increased_balances: Vec<WalletRuneAndAmount>,

    /// List of runes and amounts, corresponding to decreased balances for this address.
    pub decreased_balances: Vec<WalletRuneAndAmount>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct WalletRuneAndAmount {
    pub rune_id: String,

    pub amount: String,

    /// USD price for amounts of runes at the time the block containing this activity was mined. Null if no external price service is configured.
    pub usd_amount: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RuneActivityKindByAddress {
    SelfTransfer,
    Increase,
    Decrease,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct EtchAndPremine {
    /// Rune ID.
    pub rune_id: String,

    /// Amount of premined runes.
    pub premined_amount: Option<String>,
}
