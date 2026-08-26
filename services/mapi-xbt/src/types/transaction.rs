use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct InvolvedTransaction {
    /// The transaction's txid
    pub tx_hash: String,
    /// Height of the block which included the transaction
    pub height: u64,
    /// Address/pubkey controlled an input UTxO
    pub input: bool,
    /// Address/pubkey controlled an output UTxO
    pub output: bool,
}
