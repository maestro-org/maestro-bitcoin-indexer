use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct MempoolTransactions {
    /// Sequence number of the mempool
    #[serde(alias = "mempool_sequence")]
    #[schema(example = json!(13585464))]
    pub mempool_sequence: u64,
    /// List of transaction IDs
    #[serde(alias = "txids")]
    #[schema(example = json!(["afaaab7ce7b00c301dd72ec10ac550645e3984c6e355cc5109653e6635cdadb1", "2d1ac2a6602e4b7b8b1331b637b16736f7a60ae08ad934fe464934ff14050419"]))]
    pub tx_ids: Vec<String>,
}
