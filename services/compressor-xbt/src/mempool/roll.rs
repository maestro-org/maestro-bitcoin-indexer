use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bitcoin::consensus::Decodable;
use bitcoin::hashes::Hash;
use bitcoin::{consensus::Encodable, BlockHash, Transaction, Txid};
use bitcoin::{hash_types::TxMerkleNode, merkle_tree};
use bitcoincore_rpc::{
    json::{GetBlockTemplateModes, GetBlockTemplateRules},
    Auth, Client as RpcClient, RpcApi,
};
use gasket::framework::*;
use itertools::izip;
use ordinals::Height;
use tokio::time::Instant;
use tonic::{transport::Channel, Request};
use tracing::{debug, error, info, warn};

use crate::storage::mempool::{MempoolBlockValue, MempoolInfoValue};
use crate::{
    mempool::{
        cache::{MempoolCache, MempoolSource},
        processor::{MempoolProcessor, StorageCache},
        SharedMempoolCache,
    },
    ordinals::OrdinalRanges,
    serve::grpc::{compressor_api::MempoolBlocksWithCtxResponse, convert_mempool_block_to_proto},
    storage::{
        self,
        chain::BlockByHeightKV,
        kvtable::{DBInt, DBSerde, KVTable},
        Error, TxoBody, TxoRef,
    },
    sync::BitcoinCompatibleNetwork,
};

use super::model::{ChainTipHash, Timestamp};

pub mod mgm_api {
    tonic::include_proto!("maestroglobalmempool.mgm.v1");
}

#[derive(Debug, Clone)]
pub struct TxIdAndTx {
    pub id: Txid,
    pub tx: Transaction,
}

#[derive(Debug, Clone)]
pub struct EstimatedBlock {
    pub merkle_root: [u8; 32],
    pub txs: Vec<TxIdAndTx>,
}

#[derive(Debug, Clone)]
pub struct WorkUnit {
    chain_tip_hash: ChainTipHash,
    mempool_view_ts: Timestamp,
    estimated_blocks: Vec<EstimatedBlock>,
    source: MempoolSource,
}

#[derive(Stage)]
#[stage(name = "mproll", unit = "WorkUnit", worker = "Worker")]
pub struct Stage {
    chain_db: storage::ChainDB,
    network: BitcoinCompatibleNetwork,
    _chain_notifier: Arc<tokio::sync::Notify>,
    mgm_address: Option<String>,
    node_rpc: String,
    node_rpc_auth: Auth,
    mempool_refresh_rate: u64,
    max_blocks: usize,
    shared_cache: SharedMempoolCache,
}

pub struct UtxoResolver(BTreeMap<TxoRef, TxoBody>, u64);

impl UtxoResolver {
    pub fn new() -> Self {
        UtxoResolver(BTreeMap::new(), 0)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, txo_ref: &TxoRef) -> Option<&TxoBody> {
        self.0.get(txo_ref)
    }

    pub fn remove(&mut self, txo_ref: &TxoRef) -> Option<TxoBody> {
        if let Some(x) = self.0.remove(txo_ref) {
            self.1 -= x.raw.len() as u64;
            Some(x)
        } else {
            None
        }
    }

    pub fn insert(&mut self, txo_ref: TxoRef, txo_body: TxoBody) -> Option<TxoBody> {
        self.1 += txo_body.raw.len() as u64;
        self.0.insert(txo_ref, txo_body)
    }

    pub fn contains_key(&self, txo_ref: &TxoRef) -> bool {
        self.0.contains_key(txo_ref)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Stage {
    pub fn new(
        chain_db: storage::ChainDB,
        network: BitcoinCompatibleNetwork,
        chain_notifier: Arc<tokio::sync::Notify>,
        mgm_address: Option<String>,
        node_rpc: String,
        node_rpc_auth: Auth,
        mempool_refresh_rate: u64,
        max_blocks: usize,
        shared_cache: SharedMempoolCache,
    ) -> Self {
        Self {
            chain_db,
            network,
            _chain_notifier: chain_notifier,
            mgm_address,
            node_rpc,
            node_rpc_auth,
            mempool_refresh_rate,
            max_blocks,
            shared_cache,
        }
    }
}

pub struct Worker {
    client: Option<mgm_api::service_client::ServiceClient<Channel>>,
    rpc_client: Option<RpcClient>,
    mgm_consecutive_failures: u32,
    last_fetch: Option<Instant>,
    last_processed_mempool_ts: Option<u64>,
    /// Timestamp when MGM was disconnected (to retry reconnection after interval)
    mgm_disconnected_at: Option<Instant>,
    /// Cache of storage lookups from the previous iteration
    storage_cache: Option<StorageCache>,
    /// Transaction cache from the last MGM fetch - maps txid bytes to full transaction
    mgm_tx_cache: std::collections::HashMap<Vec<u8>, Transaction>,
}

impl Worker {
    async fn fetch_from_mgm(&mut self, stage: &mut Stage) -> Result<Option<WorkUnit>, WorkerError> {
        let client = match &mut self.client {
            Some(client) => client,
            None => {
                warn!("MGM client not available");
                return Ok(None);
            }
        };

        let request = Request::new(mgm_api::GetTemplatesV2Request {
            cached_txs: self.mgm_tx_cache.keys().cloned().collect(),
        });

        debug!(
            "fetching templates from MGM V2 with {} cached txs",
            self.mgm_tx_cache.len()
        );

        let response = match client.get_templates_v2(request).await {
            Ok(response) => {
                self.mgm_consecutive_failures = 0;
                self.mgm_disconnected_at = None; // Clear disconnect timestamp on success
                response.into_inner()
            }
            Err(e) => {
                self.mgm_consecutive_failures += 1;
                error!(
                    "MGM request failed (attempt {}): {}",
                    self.mgm_consecutive_failures, e
                );

                if self.mgm_consecutive_failures >= 3 {
                    warn!(
                        "MGM has failed {} times, switching to RPC fallback",
                        self.mgm_consecutive_failures
                    );
                    self.client = None;
                    self.mgm_disconnected_at = Some(Instant::now());
                    self.mgm_tx_cache.clear(); // Clear cache when switching away from MGM
                }
                return Ok(None);
            }
        };

        debug!("fetched templates from MGM");

        let mempool_view_ts = response.timestamp;

        // switch to RPC if timestamp over 2mins old
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if current_time.saturating_sub(mempool_view_ts) > 120 {
            warn!(
                "MGM template timestamp is too old ({} seconds), switching to RPC fallback",
                current_time.saturating_sub(mempool_view_ts)
            );
            self.client = None;
            self.mgm_disconnected_at = Some(Instant::now());
            self.mgm_tx_cache.clear(); // Clear cache when switching away from MGM
            return Ok(None);
        }

        let prev_bh =
            BlockHash::consensus_decode(&mut response.prev_block.unwrap().hash.as_slice()).unwrap();

        // Build a new tx cache for this response
        let mut new_tx_cache = std::collections::HashMap::new();

        let blocks: Vec<Vec<_>> = response
            .blocks
            .into_iter()
            .map(|b| {
                b.txs
                    .into_iter()
                    .map(|package_tx| {
                        let txid = Txid::from_slice(&package_tx.txid).expect("invalid txid from MGM");
                        let txid_bytes = package_tx.txid.clone();

                        // If tx bytes are empty, look it up from our cache
                        let tx = if package_tx.tx.is_empty() {
                            // Look up transaction from cache
                            let tx = self.mgm_tx_cache.get(&txid_bytes)
                                .cloned()
                                .unwrap_or_else(|| panic!("MGM sent cached tx reference for {} but we don't have it in cache", txid));

                            // Store in new cache for next request
                            new_tx_cache.insert(txid_bytes, tx.clone());

                            tx
                        } else {
                            // Parse the transaction bytes
                            let tx = Transaction::consensus_decode(&mut package_tx.tx.as_slice())
                                .expect("invalid transaction bytes from MGM");

                            // Store in new cache for next request
                            new_tx_cache.insert(txid_bytes, tx.clone());

                            tx
                        };

                        TxIdAndTx { id: txid, tx }
                    })
                    .collect()
            })
            .collect();

        // Update cache for next request
        self.mgm_tx_cache = new_tx_cache;

        let total: usize = blocks.iter().map(|x| x.len()).sum();

        // Check if MGM returned empty block template
        if total == 0 {
            warn!("MGM returned empty block template, switching to RPC fallback");
            self.client = None; // Switch to RPC by removing MGM client
            self.mgm_disconnected_at = Some(Instant::now());
            self.mgm_tx_cache.clear(); // Clear cache when switching away from MGM
            return Ok(None);
        }

        let mut estimated_blocks = vec![];

        for block in blocks {
            if block.is_empty() {
                break;
            }

            let hashes = block.iter().map(|tx| tx.id.to_raw_hash());
            let merkle_root: TxMerkleNode = merkle_tree::calculate_root(hashes)
                .map(|h| h.into())
                .unwrap();

            let merkle_root = merkle_root.to_raw_hash().to_byte_array();

            estimated_blocks.push(EstimatedBlock {
                merkle_root,
                txs: block,
            });

            if estimated_blocks.len() == stage.max_blocks {
                break;
            }
        }

        debug!(
            mempool_view_ts,
            "scheduling {} mempool blocks containing {} txs from MGM",
            estimated_blocks.len(),
            total
        );

        let work_unit = WorkUnit {
            chain_tip_hash: prev_bh,
            mempool_view_ts,
            estimated_blocks,
            source: MempoolSource::Mgm,
        };

        Ok(Some(work_unit))
    }

    async fn fetch_from_rpc(
        &mut self,
        _stage: &mut Stage,
    ) -> Result<Option<WorkUnit>, WorkerError> {
        let rpc_client = match &self.rpc_client {
            Some(client) => client,
            None => {
                error!("RPC client not available but fallback was requested");
                return Err(WorkerError::Restart);
            }
        };

        debug!("fetching block template from RPC fallback");

        let template = match rpc_client.get_block_template(
            GetBlockTemplateModes::Template,
            &[GetBlockTemplateRules::SegWit],
            &[],
        ) {
            Ok(template) => template,
            Err(e) => {
                error!(
                    "RPC block template request failed, restarting worker: {}",
                    e
                );
                return Err(WorkerError::Restart);
            }
        };

        debug!("fetched block template from RPC");

        let mempool_view_ts = template.current_time;
        let prev_bh = template.previous_block_hash;

        // Convert transactions from template
        let mut txs = vec![];
        for tx_data in template.transactions {
            match Transaction::consensus_decode(&mut tx_data.raw_tx.as_slice()) {
                Ok(tx) => {
                    txs.push(TxIdAndTx {
                        id: tx.compute_txid(),
                        tx,
                    });
                }
                Err(e) => {
                    warn!("Failed to decode transaction from RPC template: {}", e);
                    continue;
                }
            }
        }

        if txs.is_empty() {
            warn!("RPC returned empty block template, not scheduling mempool");
            return Ok(None);
        }

        let hashes = txs.iter().map(|tx| tx.id.to_raw_hash());
        let merkle_root: TxMerkleNode = merkle_tree::calculate_root(hashes)
            .map(|h| h.into())
            .unwrap();

        let merkle_root = merkle_root.to_raw_hash().to_byte_array();

        let estimated_blocks = vec![EstimatedBlock { merkle_root, txs }];

        debug!(
            mempool_view_ts,
            "scheduling 1 mempool block containing {} txs from RPC",
            estimated_blocks[0].txs.len()
        );

        let work_unit = WorkUnit {
            chain_tip_hash: prev_bh,
            mempool_view_ts,
            estimated_blocks,
            source: MempoolSource::Rpc,
        };

        Ok(Some(work_unit))
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        // Initialize RPC client for fallback (required)
        debug!("initializing RPC fallback client {}", &stage.node_rpc);
        let rpc_client = RpcClient::new(&stage.node_rpc, stage.node_rpc_auth.clone()).or_retry()?;

        // confirm rpc connection working
        rpc_client.get_chain_tips().or_retry()?;

        // Try to connect to MGM if address is provided
        let mut mgm_client = None;

        if let Some(mgm_address) = &stage.mgm_address {
            debug!("attempting to connect to MGM {}", mgm_address);
            let max_mgm_retries = 3;

            for attempt in 1..=max_mgm_retries {
                match mgm_api::service_client::ServiceClient::connect(mgm_address.clone()).await {
                    Ok(client) => {
                        info!("successfully connected to MGM on attempt {}", attempt);
                        mgm_client = Some(
                            client
                                .max_decoding_message_size(usize::MAX)
                                .max_encoding_message_size(usize::MAX),
                        );
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "failed to connect to MGM on attempt {}/{}: {}",
                            attempt, max_mgm_retries, e
                        );
                        if attempt == max_mgm_retries {
                            warn!(
                                "failed to connect to MGM after {} attempts, starting in RPC-only mode",
                                max_mgm_retries
                            );
                        } else {
                            // Wait a bit before retrying
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        } else {
            info!("MGM address not configured, running in RPC-only mode");
        }

        let worker = Self {
            client: mgm_client,
            rpc_client: Some(rpc_client),
            mgm_consecutive_failures: 0,
            last_fetch: None,
            last_processed_mempool_ts: None,
            mgm_disconnected_at: None,
            storage_cache: None,
            mgm_tx_cache: std::collections::HashMap::new(),
        };

        Ok(worker)
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<WorkUnit>, WorkerError> {
        if let Some(last) = self.last_fetch {
            if last.elapsed() <= Duration::from_millis(stage.mempool_refresh_rate) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                return Ok(WorkSchedule::Idle);
            }
        }

        self.last_fetch = Some(Instant::now());

        // Check if we should try to reconnect to MGM (only if MGM address is configured)
        if self.client.is_none() && stage.mgm_address.is_some() {
            if let Some(disconnected_at) = self.mgm_disconnected_at {
                if disconnected_at.elapsed() >= Duration::from_secs(30) {
                    debug!(
                        "MGM has been disconnected for {} seconds, attempting to reconnect",
                        disconnected_at.elapsed().as_secs()
                    );

                    match mgm_api::service_client::ServiceClient::connect(
                        stage.mgm_address.as_ref().unwrap().clone(),
                    )
                    .await
                    {
                        Ok(client) => {
                            info!("successfully reconnected to MGM");
                            self.client = Some(
                                client
                                    .max_decoding_message_size(usize::MAX)
                                    .max_encoding_message_size(usize::MAX),
                            );
                            self.mgm_consecutive_failures = 0;
                            self.mgm_disconnected_at = None;
                        }
                        Err(e) => {
                            warn!(
                                "Failed to reconnect to MGM: {}, will retry in 30 seconds",
                                e
                            );
                            // Reset the timer to try again in another 30 seconds
                            self.mgm_disconnected_at = Some(Instant::now());
                        }
                    }
                }
            }
        }

        // Use MGM if available, otherwise use RPC
        let work_unit = if self.client.is_some() {
            // Try MGM first
            match self.fetch_from_mgm(stage).await? {
                Some(unit) => Some(unit),
                None => {
                    // MGM failed, try RPC fallback
                    debug!("MGM failed, trying RPC fallback");
                    self.fetch_from_rpc(stage).await?
                }
            }
        } else {
            // No MGM client available, use RPC
            self.fetch_from_rpc(stage).await?
        };

        match work_unit {
            Some(unit) => {
                // Check if we've already processed this mempool view timestamp
                if let Some(last_ts) = self.last_processed_mempool_ts {
                    if last_ts == unit.mempool_view_ts {
                        debug!(
                            mempool_view_ts = unit.mempool_view_ts,
                            "skipping mempool snapshot - already processed this timestamp"
                        );
                        return Ok(WorkSchedule::Idle);
                    }
                }

                // Update the last processed timestamp
                self.last_processed_mempool_ts = Some(unit.mempool_view_ts);

                Ok(WorkSchedule::Unit(unit))
            }
            None => {
                if self.client.is_none() {
                    warn!("RPC fallback returned empty data, not scheduling mempool");
                } else {
                    warn!("MGM failed to provide mempool data, not scheduling mempool");
                }
                Ok(WorkSchedule::Idle)
            }
        }
    }

    async fn execute(&mut self, unit: &WorkUnit, stage: &mut Stage) -> Result<(), WorkerError> {
        let tip_hash = &unit.chain_tip_hash;
        let mempool_view_ts = &unit.mempool_view_ts;
        let estimated_blocks = &unit.estimated_blocks;

        let start = Instant::now();

        // Log cache status from previous iteration
        if let Some(cache) = &self.storage_cache {
            debug!(
                "Using storage cache from previous iteration: {} total entries, chain_tip: {}",
                cache.len(),
                cache.chain_tip
            );
        } else {
            debug!("No storage cache from previous iteration");
        }

        // Get the database snapshot first to fetch the chain tip
        let db_clone = stage.chain_db.clone();
        let snapshot = db_clone.db.snapshot();

        // Get tip
        let v = BlockByHeightKV::last_entry_snapshot(&db_clone.db, &snapshot).or_restart()?;

        let (tip, mut next_height) =
            if let Some((hi, ha)) = v.map(|(DBInt(hi), DBSerde((ha, _)))| (hi, ha)) {
                ((hi, ha.to_byte_array()), hi + 1)
            } else {
                warn!("no tip in block by height KV");
                ((0, [0; 32]), 0)
            };

        if tip.1 != tip_hash.to_byte_array() {
            warn!(
                "skipping because mempool snapshot tip does not match our rocksdb tip: {} vs {}",
                BlockHash::from_byte_array(tip.1),
                tip_hash
            );
            return Ok(());
        }

        let tip_hash_for_cache = BlockHash::from_byte_array(tip.1);

        // Pass the cache from the previous iteration to the new processor
        // The processor will validate the cache against the current chain tip
        let mut processor = MempoolProcessor::new(
            stage.chain_db.clone(),
            stage.network,
            tip_hash_for_cache,
            self.storage_cache.take(),
        );

        // initialise inscription counters in processor as of tip
        processor
            .init_inscription_counters(&snapshot)
            .or_restart()?;

        let mut resolver = HashMap::new();

        let mut mpbvs = vec![];

        let mut resolve_time = Duration::from_millis(0);
        let mut runes_total = Duration::from_millis(0);
        let mut inscriptions_total = Duration::from_millis(0);
        let mut encode_time = Duration::from_millis(0);

        let mut total_txs = 0;

        for block in estimated_blocks {
            let txs = block.txs.clone();
            total_txs += txs.len();

            let mut tx_list = Vec::with_capacity(txs.len());

            let mut successful_etches = vec![];
            let mut successful_mints = vec![];

            let mut reward = Height(next_height.try_into().unwrap()).subsidy();
            let mut valid_reinscriptions = Vec::new();
            let mut brc20_resolver = Vec::new();
            let mut output_runes_resolver = Vec::new();
            let mut output_inscriptions_resolver = Vec::new();
            let mut new_inscriptions = Vec::new();

            // ---
            // insert all input txos (excl those produced in the block) into txo resolver for
            // the block we will store in the MempoolKV
            // ---

            let tx_ids = txs.iter().map(|x| x.id).collect::<HashSet<_>>();

            let mut input_refs = txs
                .iter()
                .map(|x| &x.tx.input)
                .flatten()
                .map(|x| x.previous_output)
                .map(|x| TxoRef(x.txid, x.vout))
                .collect::<HashSet<TxoRef>>();

            let all_input_refs = input_refs.clone();

            input_refs.retain(|k| !tx_ids.contains(&k.0));

            let non_chained_input_refs = input_refs.into_iter().collect::<Vec<_>>();

            let resolve_start = Instant::now();

            // resolve all non-chained inputs and add to resolver, we still
            // need to add the chained inputs - to do that we add all outputs
            // to the resolver then retain only the inputs
            resolver.extend(
                processor
                    .resolve_utxos(non_chained_input_refs, &snapshot)
                    .or_restart()?,
            );

            resolve_time += resolve_start.elapsed();

            // --- process each transaction ---

            for (idx, tx) in txs.iter().enumerate() {
                let encode_start = Instant::now();

                let mut tx_body = vec![];
                tx.tx
                    .consensus_encode(&mut tx_body)
                    .map_err(|err| Error::BitcoinEncode(err.into()))
                    .or_restart()?;

                encode_time += encode_start.elapsed();

                tx_list.push((tx.id, tx_body));

                // --- process runes ---

                let output_runes = {
                    let runes_start = Instant::now();

                    let runes_result = processor
                        .index_runes(idx as u32, &tx.tx, next_height, &snapshot)
                        .or_restart()?;

                    runes_total += runes_start.elapsed();

                    if runes_result.successful_etch {
                        successful_etches.push(idx as u32)
                    }

                    if runes_result.successful_mint {
                        successful_mints.push(idx as u32)
                    }

                    runes_result.output_runes
                };

                // --- process inscriptions ---

                let (output_inscriptions, _other_inscriptions, new_brc20_transfers) = {
                    let inscriptions_start = Instant::now();

                    let res = processor
                        .index_inscriptions(
                            &resolver,
                            &snapshot,
                            &tx.tx,
                            tx.id,
                            next_height,
                            processor.chain_db.jubilee_height,
                            &mut reward,
                            &mut vec![],
                            &mut new_inscriptions,
                        )
                        .or_restart()?;

                    inscriptions_total += inscriptions_start.elapsed();

                    valid_reinscriptions
                        .extend(res.valid_reinscriptions.iter().map(|x| (idx as u32, *x)));

                    brc20_resolver.extend(res.brc20_resolver);

                    (
                        res.output_inscriptions,
                        res.lost_or_unbound_inscriptions,
                        res.new_unused_brc20_transfers,
                    )
                };

                // --- process outputs, and insert into resolvers ---

                for (idx, output, runes, inscriptions) in
                    izip!(0.., tx.tx.output.iter(), output_runes, output_inscriptions)
                {
                    let txo_ref = TxoRef(tx.id, idx as u32);

                    // let output_sats = output.value;
                    // let ord_ranges = tx_ords.take(output_sats).map_err(Error::Ordinals)?;

                    let ord_ranges = OrdinalRanges::new();

                    let encode_start = Instant::now();

                    let mut body = vec![];
                    output
                        .consensus_encode(&mut body)
                        .map_err(|err| Error::BitcoinEncode(err.into()))
                        .or_restart()?;

                    encode_time += encode_start.elapsed();

                    let runes = runes
                        .into_iter()
                        .map(|(x, y)| (x.into(), y))
                        .collect::<Vec<_>>();

                    // if the output contains runes, add to the output runes resolver
                    if !runes.is_empty() {
                        output_runes_resolver.push((txo_ref.clone(), runes.clone()))
                    }

                    // if the output contains inscriptons, add to the output inscriptions resolver
                    if !inscriptions.is_empty() {
                        output_inscriptions_resolver.push((txo_ref.clone(), inscriptions.clone()))
                    }

                    let inscids = inscriptions.iter().map(|(_, x)| x).collect::<HashSet<_>>();

                    let mut unused_brc20_transfers = new_brc20_transfers.clone();

                    // filter unused brc20 transfers to inscriptions in this output
                    unused_brc20_transfers.retain(|k, _| inscids.contains(&k));

                    let txo_body = TxoBody {
                        height: next_height,
                        raw: body,
                        ord_ranges,
                        runes,
                        inscriptions,
                        unused_brc20_transfers,
                    };

                    //insert txo into mempool processor resolver
                    processor.insert_utxo(txo_ref.clone(), txo_body.clone());

                    // insert txo into resolver stored in MempoolKV
                    resolver.insert(txo_ref, txo_body);
                }
            }

            // resolver contains all inputs and outputs. remove the inputs and store them in
            // the MPBV, let the outputs remain so we can resolve them in the next mempool
            // blocks

            let (inputs_resolver, outputs_resolver): (Vec<_>, Vec<_>) = resolver
                .into_iter()
                .partition(|(x, _)| all_input_refs.contains(x));

            resolver = outputs_resolver.into_iter().collect();

            mpbvs.push(MempoolBlockValue {
                merkle_root: block.merkle_root,
                // all txids and tx bytes
                txs: tx_list,
                // all resolved inputs
                resolver: inputs_resolver,
                // outputs with runes
                output_runes_resolver,
                // successful rune etches and mints
                successful_etches,
                successful_mints,
                // outputs with inscriptions
                output_inscriptions_resolver,
                // valid reinscriptions
                valid_reinscriptions,
                brc20_resolver,
                new_inscriptions,
            });

            next_height += 1;
        }

        // -----

        let write_start = Instant::now();

        let mempool_info = MempoolInfoValue {
            chain_tip: tip,
            mempool_view_ts: *mempool_view_ts,
        };

        // Prepare data for protobuf conversion
        let blocks_with_heights: Vec<(u64, MempoolBlockValue)> = mpbvs
            .iter()
            .enumerate()
            .map(|(i, mpbv)| (tip.0 + i as u64 + 1, mpbv.clone()))
            .collect();

        // Calculate total transaction count
        let tx_count: usize = mpbvs.iter().map(|b| b.txs.len()).sum();

        let response = convert_to_protobuf_response(&blocks_with_heights, &mempool_info);
        let cache = MempoolCache::new(
            mempool_info.clone(),
            unit.source,
            mpbvs.len(),
            tx_count,
            response,
        );

        let mut cache_guard = stage.shared_cache.write().await;
        *cache_guard = Some(cache);
        drop(cache_guard);

        // Capture the cache for the next iteration
        self.storage_cache = Some(processor.take_output_cache());

        // Log cache statistics
        if let Some(cache) = &self.storage_cache {
            debug!(
                "Captured storage cache for next iteration: {} total entries, chain_tip: {}",
                cache.len(),
                cache.chain_tip
            );
        }

        let total_time = start.elapsed().as_millis();

        if total_time >= 1000 {
            warn!(
                tip_height = tip.0,
                %tip_hash,
                %mempool_view_ts,
                total_time,
                resolve_time = resolve_time.as_millis(),
                runes_time = runes_total.as_millis(),
                inscriptions_time = inscriptions_total.as_millis(),
                write_time = write_start.elapsed().as_millis(),
                encode_time = encode_time.as_millis(),
                blocks = estimated_blocks.len(),
                txs = total_txs,
                "slow processing mempool refresh",
            );
        } else {
            debug!(
                tip_height = tip.0,
                %tip_hash,
                %mempool_view_ts,
                total_time,
                resolve_time = resolve_time.as_millis(),
                runes_time = runes_total.as_millis(),
                inscriptions_time = inscriptions_total.as_millis(),
                write_time = write_start.elapsed().as_millis(),
                encode_time = encode_time.as_millis(),
                blocks = estimated_blocks.len(),
                txs = total_txs,
                "finished processing mempool refresh",
            );
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}

/// Convert processed mempool data to protobuf response format using the shared conversion logic
fn convert_to_protobuf_response(
    mpbvs: &[(u64, MempoolBlockValue)],
    mempool_info: &MempoolInfoValue,
) -> MempoolBlocksWithCtxResponse {
    let blocks = mpbvs
        .iter()
        .map(|(height, mpbv)| convert_mempool_block_to_proto(*height, mpbv.clone(), mempool_info))
        .collect();

    MempoolBlocksWithCtxResponse { blocks }
}
