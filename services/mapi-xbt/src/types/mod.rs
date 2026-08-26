pub mod address;
pub mod blocks;
pub mod collections;
mod info;
pub mod inscriptions;
pub mod mempool_info;
pub mod mempool_transaction_ancestors;
pub mod mempool_transaction_descendants;
pub mod mempool_transaction_details;
pub mod mempool_transaction_fees;
pub mod mempool_transactions;
pub mod rpc;
pub mod runes;
mod transaction;
pub mod transaction_details;
pub mod transactions;
mod utxo;

use std::{collections::HashMap, fmt::Debug};

use collections::{CollectionMetadata, CollectionStats, TokenMetadata};
use inscriptions::{
    Brc20Holder, Brc20Info, Brc20Ticker, ContentBody, InscriptionActivityByAddress,
    InscriptionActivityByBlock, InscriptionActivityByTx, InscriptionByAddress, InscriptionInfo,
    TransferInscriptionByAddress, TxByInscription, WalletInscriptionActivityByAddress,
};
use runes::{RuneActivityByAddress, RuneIdAndName, TxByRune, WalletRuneActivityByAddress};
use serde::{Deserialize, Serialize};
use timbre_xbt::reducers::{Height, Timestamp};
use transactions::{MempoolTxInfoMetaprotocols, TxInfo, TxInfoMetaprotocols, TxOutMetaprotocols};
use utoipa::ToSchema;

use self::runes::{DeprecatedRuneUtxoByAddress, RuneHolder, RuneInfo, RuneUtxo, RuneUtxoByAddress};
pub use self::{address::*, blocks::*, info::*, transaction::*, utxo::*};

pub enum BlockParam {
    Hash([u8; 32]),
    Height(Height),
    Timestamp(Timestamp),
}

#[derive(Clone, Copy, Debug, Deserialize, ToSchema, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
#[schema(default = "asc")]
pub enum OrderParam {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug, Deserialize, ToSchema, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
#[schema(default = "height")]
#[non_exhaustive]
pub enum OrderBy {
    Height,
    Amount,
}

#[derive(Clone, Copy, Debug, Deserialize, ToSchema, PartialEq, PartialOrd)]
#[schema(default = 100)]
pub struct CountParam(pub usize);

#[derive(Debug, Deserialize, ToSchema)]
pub struct CursorPaginationParams {
    pub count: Option<CountParam>,
    pub order: Option<OrderParam>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct HeightPaginationParams {
    pub count: Option<CountParam>,
    pub order: Option<OrderParam>,
    pub cursor: Option<String>,
    pub from: Option<u64>, // inclusive
    pub to: Option<u64>,   // inclusive
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct ChainTip {
    /// The hash of the block
    #[schema(example = "0000000000000000000a7f3b7b6b6e1d9a18db65a3b4a3f4f3bcb2e1f1b2d3e7")]
    pub block_hash: String,

    /// The height of the block in the blockchain
    #[schema(example = 707000)]
    pub block_height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct EstimatedBlock {
    /// Height of the estimated block
    #[schema(example = "707001")]
    pub block_height: u64,
    /// Minimum, median, and maximum sat/vB values for the estimated block
    pub sats_per_vb: BlockSatsPerVb,
}

/// For transactions within a block, these are the lowest, median and highest
/// satoshis per virtual-byte values.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct BlockSatsPerVb {
    /// Lowest sat/vB value of the transactions within the block
    #[schema(example = 11)]
    pub min: u64,

    /// Median sat/vB value of the transactions within the block
    #[schema(example = 15)]
    pub median: u64,

    /// Highest sat/vB value of the transactions within the block
    #[schema(example = 255)]
    pub max: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
pub struct MempoolLastUpdated {
    /// Most recent *mined* block in the indexed mainchain, any estimated blocks will be descendants of this block
    pub chain_tip: ChainTip,
    /// Timestamp of the indexed mempool snapshot, if any estimated blocks from the mempool have been indexed
    pub mempool_timestamp: Option<String>,
    /// Information about any estimated blocks from the mempool that were indexed in addition to the mainchain
    pub estimated_blocks: Vec<EstimatedBlock>,
}

// PaginatedResponse defines return types specific to the Blockchain Indexer API.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[aliases(
    PaginatedActivityByAddress = PaginatedResponse<ActivityByAddress>,
    PaginatedInscriptionActivityByAddress = PaginatedResponse<InscriptionActivityByAddress>,
    PaginatedInscriptionActivityByBlock = PaginatedResponse<InscriptionActivityByBlock>,
    PaginatedInscriptionActivityByTx = PaginatedResponse<InscriptionActivityByTx>,
    PaginatedInscriptionByAddress = PaginatedResponse<InscriptionByAddress>,
    PaginatedInvolvedTransaction = PaginatedResponse<InvolvedTransaction>,
    PaginatedBrc20Holder = PaginatedResponse<Brc20Holder>,
    PaginatedBrc20Ticker = PaginatedResponse<Brc20Ticker>,
    PaginatedRuneHolder = PaginatedResponse<RuneHolder>,
    PaginatedRuneIdAndName = PaginatedResponse<RuneIdAndName>,
    PaginatedRuneUtxo = PaginatedResponse<RuneUtxo>,
    PaginatedDeprecatedRuneUtxoByAddress = PaginatedResponse<DeprecatedRuneUtxoByAddress>,
    PaginatedRuneUtxoByAddress = PaginatedResponse<RuneUtxoByAddress>,
    PaginatedTransferInscriptionByAddress = PaginatedResponse<TransferInscriptionByAddress>,
    PaginatedTxsByBlock = PaginatedResponse<TxByBlock>,
    PaginatedTxByInscription = PaginatedResponse<TxByInscription>,
    PaginatedTxsByRune = PaginatedResponse<TxByRune>,
    PaginatedUtxo = PaginatedResponse<Utxo>,
    PaginatedInscriptionsByCollectionSymbol = PaginatedResponse<String>,
    PaginatedRuneActivityByAddress = PaginatedResponse<RuneActivityByAddress>,
)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub last_updated: ChainTip,
    pub next_cursor: Option<String>,
}

// CommonPaginatedResponse defines return types that are common to the Blockchain Indexer and Wallet APIs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[aliases(
    PaginatedHistoricalSatBalanceByAddress = CommonPaginatedResponse<HistoricalSatBalanceByAddress>,
)]
pub struct CommonPaginatedResponse<T> {
    pub data: Vec<T>,
    pub last_updated: ChainTip,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[aliases(
    MempoolPaginatedUtxo = MempoolPaginatedResponse<MempoolUtxo>,
    MempoolPaginatedRuneHolder = MempoolPaginatedResponse<RuneHolder>,
    MempoolPaginatedRuneUtxoByAddress = MempoolPaginatedResponse<RuneUtxoByAddress>,
)]
pub struct MempoolPaginatedResponse<T> {
    pub data: Vec<T>,
    pub indexer_info: MempoolLastUpdated,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[aliases(
    MempoolWalletPaginatedActivityByAddress = MempoolWalletPaginatedResponse<WalletActivityByAddress>,
    MempoolWalletPaginatedInscriptionActivityByAddress = MempoolWalletPaginatedResponse<WalletInscriptionActivityByAddress>,
    MempoolWalletPaginatedRuneActivityByAddress = MempoolWalletPaginatedResponse<WalletRuneActivityByAddress>,
    MempoolWalletPaginatedActivityByAddressWithMetaprotocols = MempoolWalletPaginatedResponse<WalletActivityByAddressWithMetaprotocols>,
)]
pub struct MempoolWalletPaginatedResponse<T> {
    pub data: Vec<T>,
    pub indexer_info: MempoolLastUpdated,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[aliases(
    MempoolTimestampedRuneQuantities = MempoolTimestampedResponse<HashMap<String, String>>,
    MempoolTimestampedFeeRates = MempoolTimestampedResponse<Vec<EstimatedBlock>>,
    MempoolTimestampedTxOutMetaprotocols = MempoolTimestampedResponse<TxOutMetaprotocols>,
    MempoolTimestampedSatoshis = MempoolTimestampedResponse<String>,
    MempoolTimestampedTxInfoMetaprotocols = MempoolTimestampedResponse<MempoolTxInfoMetaprotocols>,
)]
pub struct MempoolTimestampedResponse<T> {
    pub data: T,
    pub indexer_info: MempoolLastUpdated,
}

// TimestampedResponse defines return types specific to the Blockchain Indexer API.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[aliases(
    TimestampedAddressStatistics = TimestampedResponse<AddressStatistics>,
    TimestampedBlock = TimestampedResponse<BlockInfo>,
    TimestampedBrc20Quantities = TimestampedResponse<HashMap<String, String>>,
    TimestampedBrc20Info = TimestampedResponse<Brc20Info>,
    TimestampedCollectionMetadataByInscription = TimestampedResponse<CollectionMetadata>,
    TimestampedCollectionMetadataBySymbol = TimestampedResponse<CollectionMetadata>,
    TimestampedCollectionStatsBySymbol = TimestampedResponse<CollectionStats>,
    TimestampedInscriptionInfo = TimestampedResponse<InscriptionInfo>,
    TimestampedRuneInfo = TimestampedResponse<RuneInfo>,
    TimestampedRuneQuantities = TimestampedResponse<HashMap<String, String>>,
    TimestampedTokenMetadataByInscription = TimestampedResponse<TokenMetadata>,
    TimestampedTxInfo = TimestampedResponse<TxInfo>,
    TimestampedTxInfoMetaprotocols = TimestampedResponse<TxInfoMetaprotocols>,
    TimestampedTxOutMetaprotocols = TimestampedResponse<TxOutMetaprotocols>,
    TimestampedSatoshis = TimestampedResponse<String>,
)]
pub struct TimestampedResponse<T> {
    pub data: T,
    pub last_updated: ChainTip,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[aliases(
    MempoolWalletTimestampedAddressStatistics = MempoolWalletTimestampedResponse<WalletAddressStatistics>,
)]
pub struct MempoolWalletTimestampedResponse<T> {
    pub data: T,
    pub indexer_info: MempoolLastUpdated,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[aliases(
    PaginatedContentBody = PaginatedContent<ContentBody>,
)]
pub struct PaginatedContent<T> {
    pub data: T,
    pub last_updated: ChainTip,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Metaprotocol {
    Inscriptions,
    Runes,
    Brc20,
}
