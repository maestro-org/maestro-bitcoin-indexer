use std::sync::Arc;
use tokio::sync::RwLock;

use serde::Serialize;

use crate::serve::grpc::compressor_api::MempoolBlocksWithCtxResponse;
use crate::storage::mempool::MempoolInfoValue;

/// Source of mempool data
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MempoolSource {
    /// Data fetched from Maestro Global Mempool
    Mgm,
    /// Data fetched via node RPC (getblocktemplate)
    Rpc,
}

/// Shared in-memory cache for mempool data
/// Contains pre-converted protobuf responses for fast gRPC serving
#[derive(Debug, Clone)]
pub struct MempoolCache {
    /// The mempool info (chain tip and timestamp) when this cache was generated
    pub mempool_info: MempoolInfoValue,
    /// Source of the mempool data (MGM or RPC)
    pub source: MempoolSource,
    /// Number of mempool blocks in this snapshot
    pub block_count: usize,
    /// Total number of transactions across all blocks
    pub tx_count: usize,
    /// Pre-converted protobuf response for mempool blocks
    pub response: MempoolBlocksWithCtxResponse,
}

impl MempoolCache {
    pub fn new(
        mempool_info: MempoolInfoValue,
        source: MempoolSource,
        block_count: usize,
        tx_count: usize,
        response: MempoolBlocksWithCtxResponse,
    ) -> Self {
        Self {
            mempool_info,
            source,
            block_count,
            tx_count,
            response,
        }
    }

    /// Check if this cache is still valid for the given chain tip and mempool timestamp
    pub fn is_valid(&self, chain_tip: (u64, [u8; 32]), mempool_view_ts: u64) -> bool {
        self.mempool_info.chain_tip == chain_tip
            && self.mempool_info.mempool_view_ts == mempool_view_ts
    }
}

/// Shared mempool cache wrapped in Arc<RwLock> for thread-safe access
pub type SharedMempoolCache = Arc<RwLock<Option<MempoolCache>>>;

/// Create a new shared mempool cache
pub fn create_shared_cache() -> SharedMempoolCache {
    Arc::new(RwLock::new(None))
}
