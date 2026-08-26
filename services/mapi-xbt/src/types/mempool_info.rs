use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr, PickFirst};
use utoipa::ToSchema;

#[serde_as]
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct MempoolInfo {
    /// Total memory usage for the mempool (in bytes)
    #[schema(example = 47278798)]
    pub bytes: u64,

    /// Whether full replace-by-fee (RBF) is enabled
    #[serde(alias = "fullrbf")]
    #[schema(example = false)]
    pub full_rbf: bool,

    /// The incremental relay fee setting (in BTC)
    #[serde(alias = "incrementalrelayfee")]
    #[serde_as(as = "PickFirst<(DisplayFromStr, _)>")]
    #[schema(example = "0.00001", value_type = String)]
    pub incremental_relay_fee: f64,

    /// Whether the mempool is fully loaded
    #[schema(example = true)]
    pub loaded: bool,

    /// Maximum memory usage for the mempool (in bytes)
    #[serde(alias = "maxmempool")]
    #[schema(example = 300000000)]
    pub max_mempool: u64,

    /// The minimum fee rate (in BTC/kB) for mempool transactions
    #[serde_as(as = "PickFirst<(DisplayFromStr, _)>")]
    #[serde(alias = "mempoolminfee")]
    #[schema(example = "0.00002504", value_type = String)]
    pub mempool_min_fee: f64,

    /// The minimum fee rate (in BTC/kB) for relaying transactions
    #[serde(alias = "minrelaytxfee")]
    #[serde_as(as = "PickFirst<(DisplayFromStr, _)>")]
    #[schema(example = "0.00001", value_type = String)]
    #[schema(example = 0.00001)]
    pub min_relay_tx_fee: f64,

    /// Number of transactions in the mempool
    #[schema(example = 76582)]
    pub size: u64,

    /// The total fees (in BTC) in the mempool
    #[schema(example = 0.90122894)]
    #[serde_as(as = "PickFirst<(DisplayFromStr, _)>")]
    #[schema(example = "0.96076106", value_type = String)]
    pub total_fee: f64,

    /// Number of transactions that have not been broadcast
    #[serde(alias = "unbroadcastcount")]
    #[schema(example = 0)]
    pub unbroadcast_count: u64,

    /// Total usage of the mempool (in bytes)
    #[schema(example = 288872432)]
    pub usage: u64,
}
