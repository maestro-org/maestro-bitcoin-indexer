use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use utoipa::ToSchema;

use super::mempool_transaction_fees::TransactionFees;

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MempoolTransactionDetails {
    /// Number of ancestors
    #[serde(alias = "ancestorcount")]
    #[schema(example = 1)]
    pub ancestor_count: u64,

    /// Size of ancestors
    #[serde(alias = "ancestorsize")]
    #[schema(example = 189)]
    pub ancestor_size: u64,

    /// Whether the transaction is BIP 125 replaceable
    #[serde(alias = "bip125-replaceable")]
    #[schema(example = true)]
    pub bip125_replaceable: bool,

    /// Dependencies of the transaction
    #[schema(example = json!(["3472adf6e7d8c2b2a7cca9db9740a96a1550ba98bb9e20c7e84aa24b620158f1"]))]
    pub depends: Vec<String>,

    /// Number of descendants
    #[serde(alias = "descendantcount")]
    #[schema(example = 1)]
    pub descendant_count: u64,

    /// Size of descendants
    #[serde(alias = "descendantsize")]
    #[schema(example = 189)]
    pub descendant_size: u64,

    /// Fees associated with the transaction
    #[schema(example = json!(TransactionFees {
        ancestor: "0.00025785".to_string(),
        base: "0.00025785".to_string(),
        descendant: "0.00025785".to_string(),
        modified: "0.00025785".to_string(),
    }))]
    pub fees: TransactionFees,

    /// Block height
    #[schema(example = 856470)]
    pub height: u64,

    /// Transactions that spend this one
    #[serde(alias = "spentby")]
    #[schema(example = json!(["5f1a1bbfc7a95c9ab8aeb7145d8f03a6c94a35e25bbaad9c1de24b34589d801f"]))]
    pub spent_by: Vec<String>,

    /// Time of the transaction
    #[schema(example = 1723471064)]
    pub time: u64,

    /// Whether the transaction is unbroadcast
    #[schema(example = false)]
    pub unbroadcast: bool,

    /// Virtual size of the transaction
    #[schema(example = 189)]
    pub vsize: u64,

    /// Weight of the transaction
    #[schema(example = 756)]
    pub weight: u64,

    /// Witness transaction ID
    #[serde(alias = "wtxid")]
    #[schema(example = "afaaab7ce7b00c301dd72ec10ac550645e3984c6e355cc5109653e6635cdadb1")]
    pub wtx_id: String,
}
