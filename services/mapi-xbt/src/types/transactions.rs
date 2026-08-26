use crate::types::{
    utxo::{InscriptionAndOffset, RuneAndAmount},
    Metaprotocol,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxInfo {
    /// Block height.
    #[schema(example = 875075, value_type = u64)]
    pub height: u64,

    /// Block hash.
    #[schema(example = "0000000000000000000290db65621592a96224ecbe92ae22532a35dc40213471", value_type = String)]
    pub block_hash: String,

    /// Number of confirmations of the block.
    #[schema(example = 3, value_type = u64)]
    pub confirmations: u64,

    /// The timestamp of the block, as claimed by the miner.
    #[schema(example = 1734389286, value_type = u32)]
    pub unix_timestamp: u32,

    /// The timestamp of the block, as claimed by the miner, in UTC format.
    #[schema(example = "2024-12-16 22:48:06", value_type = String)]
    pub timestamp: String,

    /// Index of transaction in block.
    #[schema(example = 123, value_type = u32)]
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

    /// List of supported metaprotocols involved in the transaction. Runes: etching, mint or edicts. Inscriptions: create or transfer. BRC-20 tokens: deploy, mint, transfer init or transfer.
    pub metaprotocols: Vec<Metaprotocol>,

    /// List of inputs, in the same order as the transaction.
    pub inputs: Vec<TxIn>,

    /// List of outputs, in the same order as the transaction.
    pub outputs: Vec<TxOut>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxIn {
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
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxOut {
    /// Address-encoding of the script pubkey at which the output containing the inscription resides.
    #[schema(example = "bc1ppth27qnr74qhusy9pmcyeaelgvsfky6qzquv9nf56gqmte59vfhqwkqguh", value_type = Option<String>)]
    pub address: Option<String>,

    /// Script pubkey at which the output containing the inscription resides.
    #[schema(example = "51200aeeaf0263f5417e40850ef04cf73f43209b13401038c2cd34d201b5e685626e", value_type = String)]
    pub script_pubkey: String,

    /// Total number of satoshis in the UTxO.
    #[schema(example = "1234567", value_type = String)]
    pub satoshis: String,

    /// If this output is known to have been spent, hash of the transaction that spent it.
    #[schema(example = "2ca28d42583fc5bace84fe024d3697969e06dd1cf769a2141286825b81773fd5", value_type = Option<String>)]
    pub spending_tx: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxInfoMetaprotocols {
    /// Block height.
    #[schema(example = 875075)]
    pub height: u64,

    /// Block hash.
    #[schema(example = "0000000000000000000290db65621592a96224ecbe92ae22532a35dc40213471", value_type = String)]
    pub block_hash: String,

    /// Number of confirmations of the block.
    #[schema(example = 3)]
    pub confirmations: u64,

    /// The timestamp of the block, as claimed by the miner.
    #[schema(example = 1734389286, value_type = u32)]
    pub unix_timestamp: u32,

    /// The timestamp of the block, as claimed by the miner, in UTC format.
    #[schema(example = "2024-12-16 22:48:06", value_type = String)]
    pub timestamp: String,

    /// Index of transaction in block.
    #[schema(example = 123, value_type = u32)]
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

    /// Whether any of the transactions in the block involved metaprotocols.
    pub metaprotocols: Vec<Metaprotocol>,

    /// List of inputs, in the same order as the transaction.
    pub inputs: Vec<TxInMetaprotocols>,

    /// List of outputs, in the same order as the transaction.
    pub outputs: Vec<TxOutMetaprotocols>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxInMetaprotocols {
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

    /// List of inscription IDs and their offsets in this input.
    pub inscriptions: Vec<InscriptionAndOffset>,

    /// List of rune IDs and their amount in this input.
    pub runes: Vec<RuneAndAmount>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct TxOutMetaprotocols {
    /// Address-encoding of the script pubkey at which the output containing the inscription resides.
    #[schema(example = "bc1ppth27qnr74qhusy9pmcyeaelgvsfky6qzquv9nf56gqmte59vfhqwkqguh", value_type = Option<String>)]
    pub address: Option<String>,

    /// Script pubkey at which the output containing the inscription resides.
    #[schema(example = "51200aeeaf0263f5417e40850ef04cf73f43209b13401038c2cd34d201b5e685626e", value_type = String)]
    pub script_pubkey: String,

    /// Total number of satoshis in the UTxO.
    #[schema(example = "1234567", value_type = String)]
    pub satoshis: String,

    /// If this output is known to have been spent, hash of the transaction that spent it.
    #[schema(example = "2ca28d42583fc5bace84fe024d3697969e06dd1cf769a2141286825b81773fd5", value_type = Option<String>)]
    pub spending_tx: Option<String>,

    /// List of inscription IDs and their offsets in this input.
    pub inscriptions: Vec<InscriptionAndOffset>,

    /// List of rune IDs and their amount in this input.
    pub runes: Vec<RuneAndAmount>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
pub struct MempoolTxInfoMetaprotocols {
    /// Block height.
    #[schema(example = 875075)]
    pub height: u64,

    /// Total number of satoshis that went through this transaction, minus fees.
    #[schema(example = "12345678", value_type = String)]
    pub volume: String,

    /// Fees paid to the miner.
    #[schema(example = "2504", value_type = String)]
    pub fees: String,

    /// sats per vB of the transaction.
    #[schema(example = 15, value_type = u64)]
    pub sats_per_vb: u64,

    /// Whether any of the transactions in the block involved metaprotocols.
    pub metaprotocols: Vec<Metaprotocol>,

    /// List of inputs, in the same order as the transaction.
    pub inputs: Vec<TxInMetaprotocols>,

    /// List of outputs, in the same order as the transaction.
    pub outputs: Vec<TxOutMetaprotocols>,
}
