use crate::types::Metaprotocol;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct BlockInfo {
    /// Block height.
    #[schema(example = 875075, value_type = u64)]
    pub height: u64,

    /// Block hash.
    #[schema(example = "0000000000000000000290db65621592a96224ecbe92ae22532a35dc40213471", value_type = String)]
    pub hash: String,

    /// Block size in bytes.
    #[schema(example = 1865672, value_type = u64)]
    pub size: u64,

    /// Number of weight units (WU) of the block.
    #[schema(example = 3993329, value_type = u64)]
    pub weight_units: u64,

    /// The timestamp of the block, as claimed by the miner.
    #[schema(example = 1734389286, value_type = u32)]
    pub unix_timestamp: u32,

    /// The timestamp of the block, as claimed by the miner, in UTC format.
    #[schema(example = "2024-12-16 22:48:06", value_type = String)]
    pub timestamp: String,

    /// Total fees paid by all transactions in the block, in satoshis.
    #[schema(example = "2110512", value_type = String)]
    pub total_fees: String,

    /// Total number of satoshis that went through this block, minus fees.
    #[schema(example = "240371600038", value_type = String)]
    pub total_volume: String,

    /// Total number of transactions.
    #[schema(example = "1849", value_type = u32)]
    pub total_txs: u32,

    /// Whether any of the transactions in the block involved metaprotocols.
    pub metaprotocols: Vec<Metaprotocol>,

    /// Miner name.
    #[schema(example = "ViaBTC", value_type = Option<String>)]
    pub miner_name: Option<String>,

    /// Base64-encoding of script pubkey used in coinbase transaction input.
    #[schema(example = "A8tlDQgvVmlhQlRDLyz6vm1tPXaDOuJgXZs6zuF9J7o+V55Fu/UWyF9S+hZG5d/z+c8QAAAAAAAAABDThC4BY8HxNM7fc9SQsQ8AAAAAAA==", value_type = String)]
    pub coinbase_tag: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MiningPool {
    pub id: i32,
    pub name: String,
    pub addresses: Vec<String>,
    pub tags: Vec<String>,
    pub link: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxByBlock {
    /// Transaction hash.
    #[schema(example = "2ca28d42583fc5bace84fe024d3697969e06dd1cf769a2141286825b81773fd5", value_type = String)]
    pub tx_hash: String,

    /// Transaction index in block.
    #[schema(example = 0, value_type = u32)]
    pub tx_index: u32,

    /// Total number of satoshis that went through this transaction, minus fees.
    #[schema(example = "12345678", value_type = String)]
    pub volume: String,

    /// Fees paid to the miner.
    #[schema(example = "2504", value_type = String)]
    pub fees: String,

    /// sats per vB of the transaction.
    #[schema(example = 15, value_type = u64)]
    pub sats_per_vb: u64,

    /// List of supported metaprotocols involved in one or more transactions within the block.
    #[schema(example = "[\"inscriptions\", \"runes\"]", value_type = String)]
    pub metaprotocols: Vec<Metaprotocol>,

    /// Total number of inputs. No inputs means this is the coinbase transaction.
    #[schema(example = 5, value_type = u64)]
    pub total_inputs: u64,

    /// Summary of the inputs of the transaction. Maximum 10 returned inputs.
    pub inputs: Vec<TxInByBlock>,

    /// Total number of outputs.
    #[schema(example = 2, value_type = u64)]
    pub total_outputs: u64,

    /// Summary of the outputs of the transaction. Maximum 10 returned outputs.
    pub outputs: Vec<TxOutByBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxInByBlock {
    /// Transaction hash of the UTxO.
    #[schema(example = "2ca28d42583fc5bace84fe024d3697969e06dd1cf769a2141286825b81773fd5", value_type = String)]
    pub txid: String,

    /// Output index of the UTxO.
    #[schema(example = 0, value_type = u32)]
    pub vout: u32,

    /// Address-encoding of the script pubkey at which the input resides.
    #[schema(example = "bc1p5u4y8vdhn46adxhfv5scfv4c8myykw6r5uyzlavm42k4wgjewktq7xqcyr", value_type = Option<String>)]
    pub address: Option<String>,

    /// Script pubkey at which the input resides.
    #[schema(example = "5120a72a43b1b79d75d69ae9652184b2b83ec84b3b43a7082ff59baaad5722597596", value_type = String)]
    pub script_pubkey: String,

    /// Total number of satoshis in the UTxO.
    #[schema(example = "1234567", value_type = String)]
    pub satoshis: String,

    /// Number of inscriptions in the UTxO.
    #[schema(example = 1234567, value_type = u128)]
    pub inscriptions: u128,

    /// Number of rune kinds in the UTxO.
    #[schema(example = 1234567, value_type = u128)]
    pub runes: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxOutByBlock {
    /// Output index of the UTxO.
    #[schema(example = 0, value_type = u32)]
    pub vout: u32,

    /// Address-encoding of the script pubkey at which the input resides.
    #[schema(example = "bc1p5u4y8vdhn46adxhfv5scfv4c8myykw6r5uyzlavm42k4wgjewktq7xqcyr", value_type = Option<String>)]
    pub address: Option<String>,

    /// Script pubkey at which the input resides.
    #[schema(example = "5120a72a43b1b79d75d69ae9652184b2b83ec84b3b43a7082ff59baaad5722597596", value_type = String)]
    pub script_pubkey: String,

    /// If this output is known to have been spent, hash of the transaction that spent it.
    #[schema(example = "2ca28d42583fc5bace84fe024d3697969e06dd1cf769a2141286825b81773fd5", value_type = Option<String>)]
    pub spending_tx: Option<String>,

    /// Total number of satoshis in the UTxO.
    #[schema(example = "1234567", value_type = String)]
    pub satoshis: String,

    /// Number of inscriptions in the UTxO.
    #[schema(example = 1234567, value_type = u128)]
    pub inscriptions: u128,

    /// Number of rune kinds in the UTxO.
    #[schema(example = 1234567, value_type = u128)]
    pub runes: u128,
}
