use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::types::{inscriptions::InscriptionActivity, runes::WalletRuneActivity};

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ActivityByAddress {
    /// Height of block containing the satoshi activity.
    pub height: u64,

    /// Number of confirmation blocks.
    pub confirmations: u64,

    /// Hash of transaction containing the satoshi activity.
    pub tx_hash: String,

    /// Bitcoin activity as increased or decreased satoshi balance, or as self-transferred amount.
    pub sat_activity: SatActivity,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct SatActivity {
    /// Kind of activity involving the address. Options: "increase", "decrease", "self_transfer".
    pub kind: ActivityKindByAddress,

    /// Amount of satoshis involved in the activity.
    pub amount: String,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct WalletActivityByAddress {
    /// Height of block containing the satoshi activity.
    pub height: u64,

    /// Number of confirmation blocks.
    pub confirmations: u64,

    /// Whether the data is pending (true) or confirmed (false).
    pub mempool: bool,

    /// Hash of transaction containing the satoshi activity.
    pub tx_hash: String,

    /// Bitcoin activity as increased or decreased satoshi balance, or as self-transferred amount.
    pub sat_activity: WalletSatActivity,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct WalletSatActivity {
    /// Kind of activity involving the address. Options: "increase", "decrease", "self_transfer".
    pub kind: ActivityKindByAddress,

    /// Amount of satoshis involved in the activity.
    pub amount: String,

    /// USD amount if sat amount was exchanged to USD. If the block is confirmed, the exchange rate is that between USD and BTC at the time the block was mined. If the block is pending (mempool transaction), then the exchange rate is that between USD and BTC at the time the block at the tip of the chain was mined. Null if no external price service is configured.
    pub usd_amount: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ActivityKindByAddress {
    SelfTransfer,
    Increase,
    Decrease,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct WalletActivityByAddressWithMetaprotocols {
    /// Height of block containing the satoshi activity.
    pub height: u64,

    /// Number of confirmation blocks.
    pub confirmations: u64,

    /// Whether the data is pending (true) or confirmed (false).
    pub mempool: bool,

    /// Hash of transaction containing the satoshi activity.
    pub tx_hash: String,

    /// Bitcoin activity as increased or decreased satoshi balance, or as self-transferred amount.
    pub sat_activity: WalletSatActivity,

    // /// Inscription activity, as lists of self-transferred, sent and received inscriptions.
    pub inscription_activity: Option<InscriptionActivity>,

    /// Rune activity, as etched runes, minted runes, self-transferred runes, runes for which the balance increased, and runes for which the balance decreased.
    pub rune_activity: Option<WalletRuneActivity>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddressStatistics {
    /// Total number of transactions where the address controlled at least an input or an output.
    pub total_txs: u64,

    /// Total number of inputs (spent outputs) controlled by the address.
    pub total_inputs: u64,

    /// Total number of sats in inputs controlled by the address.
    pub total_sat_in_inputs: u128,

    /// Total number of spent or unspent outputs controlled by the address.
    pub total_outputs: u64,

    /// Total number of sats in spent or unspent outputs controlled by the address.
    pub total_sat_in_outputs: u128,

    /// Total number of unspent outputs (UTxOs) controlled by the address.
    pub total_utxos: u64,

    /// Existence of runes controlled by the address.
    pub runes: bool,

    /// Total number of inscriptions currently controlled by the address.
    pub total_inscriptions: u64,

    /// Current satoshi balance controlled by the address.
    pub sat_balance: String,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct WalletAddressStatistics {
    /// Total number of confirmed transactions where the address controlled at least an input or an output.
    pub total_txs: u64,

    /// Total number of confirmed inputs (i.e., number of all confirmed spending of outputs).
    pub total_inputs: u64,

    /// Total number of sats in confirmed inputs.
    pub total_sat_in_inputs: u128,

    /// Total number of confirmed spent or unspent outputs.
    pub total_outputs: u64,

    /// Total number of sats in confirmed spent or unspent outputs.
    pub total_sat_in_outputs: u128,

    /// Total number of confirmed unspent outputs.
    pub total_utxos: u64,

    /// Existence of runes controlled by the address.
    pub runes: bool,

    /// Total number of inscriptions currently controlled by the address.
    pub total_inscriptions: u64,

    /// Current confirmed sat balance (sat in unspent outputs) controlled by the address.
    pub sat_balance: String,

    /// Confirmed USD balance if sat balance was exchanged. The exchange rate is that between USD and BTC at the time the block at the tip of the chain was mined. Null if no external price service is configured.
    pub usd_balance: Option<String>,

    /// Updates in mempool related to this address.
    pub pending: PendingAddressStatistics,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct PendingAddressStatistics {
    /// Estimated number of new txs in the mempool where the address controls at least an input or an output.
    pub txs: u64,

    /// Estimated number of outputs spent in the mempool.
    pub inputs: u64,

    /// Estimated number of sats in outputs spent in the mempool.
    pub sat_in_inputs: u128,

    /// Estimated number of new outputs (spent or unspent) in the mempool.
    pub outputs: u64,

    /// Estimated number of sats in new outputs (spent or unspent) in the mempool.
    pub sat_in_outputs: u128,

    /// Estimated number of new unspent outputs in the mempool.
    pub utxos: i64,

    /// Estimated sat balance difference between mempool and confirmed data.
    pub sat_balance: String,

    /// Estimated USD balance difference between mempool and confirmed data, if sat balance was exchanged. The exchange rate is that between USD and BTC at the time the block at the tip of the chain was mined. Null if no external price service is configured.
    pub usd_balance: Option<String>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct HistoricalSatBalanceByAddress {
    /// Block height.
    pub height: u64,

    /// Number of confirmation blocks.
    pub confirmations: u64,

    /// The timestamp of the block, as claimed by the miner, in UNIX format.
    pub unix_timestamp: u32,

    /// The timestamp of the block, as claimed by the miner, in UTC format.
    pub timestamp: String,

    /// Satoshi balance of the address at the end of this block.
    pub sat_balance: String,

    /// USD balance if sat balance was exchanged. The exchange rate is that between USD and BTC at the time the block was mined. Null if no external price service is configured.
    pub usd_balance: Option<String>,
}
