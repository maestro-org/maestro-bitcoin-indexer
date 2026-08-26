use std::{collections::HashMap, str::FromStr, u64};

use bb8::{Pool, State};
use bb8_redis_cluster::redis_cluster_async::redis::AsyncCommands;
use bb8_redis_cluster::RedisConnectionManager;
use bb8_tikv::TiKVTransactionalConnectionManager;
use bitcoin::{hashes::Hash, Address, BlockHash, Network, ScriptBuf};
use tikv_client::{Snapshot, TransactionOptions};
use timbre_xbt::{
    reducers::{rune_id_by_rune_name, script_by_script_hash, script_hash_by_address_payload_hash},
    CollectionIngestor, Decode, Encode, MinerIngestor, Prefix, Reducer,
};
use tracing::{debug, error, info, warn};

use crate::{
    error::{Error, MapiResult},
    options::Mode,
    types::ChainTip,
};

use super::{
    key_resolver::{self, ReducerType},
    redis_entry::{RedisEntry, Timestamp},
    unify::{find_best_common_block, find_best_common_mempool_view, find_common_block_at_height},
};

#[derive(Debug, Clone, Copy)]
pub struct MempoolView {
    pub mempool_view_ts: u64,
    pub mempool_blocks: usize,
}

pub type Point = (u64, [u8; 32]);

#[derive(Debug, Clone, Copy)]
pub struct SnapshotPoint {
    pub chain_tip: Point,
    pub mempool: Option<MempoolView>,
}

#[derive(Debug, Copy, Clone)]
pub struct IngestorInstances {
    pub collections: (u8, u16),
    pub miners: (u8, u16),
}

pub struct TiKVAdapter {
    pub tikv_pool: Pool<TiKVTransactionalConnectionManager>,
    pub redis_pool: Pool<RedisConnectionManager>,
    pub network: Mode,
    pub instance_redis_pool: Pool<RedisConnectionManager>,
    pub ingestor_instances: IngestorInstances,
    pub snapshot_point: Option<SnapshotPoint>,
    snapshots: HashMap<ReducerType, (Snapshot, Prefix)>,
}

impl Clone for TiKVAdapter {
    fn clone(&self) -> Self {
        Self {
            tikv_pool: self.tikv_pool.clone(),
            redis_pool: self.redis_pool.clone(),
            network: self.network.clone(),
            instance_redis_pool: self.instance_redis_pool.clone(),
            ingestor_instances: self.ingestor_instances.clone(),
            snapshot_point: None,
            snapshots: HashMap::new(),
        }
    }
}

impl TiKVAdapter {
    pub fn new(
        tikv_pool: Pool<TiKVTransactionalConnectionManager>,
        redis_pool: Pool<RedisConnectionManager>,
        network: Mode,
        instance_redis_pool: Pool<RedisConnectionManager>,
        ingestor_instances: IngestorInstances,
    ) -> Self {
        Self {
            tikv_pool,
            redis_pool,
            network,
            instance_redis_pool,
            ingestor_instances,
            snapshot_point: None,
            snapshots: HashMap::new(),
        }
    }

    /// Get bb8 TiKV connection pool stats
    pub async fn get_tikv_pool_state(&self) -> State {
        self.tikv_pool.state()
    }

    /// Get a TiKV connection from the bb7 connection pool
    pub async fn get_tikv_client(
        &self,
    ) -> Result<bb8::PooledConnection<'_, TiKVTransactionalConnectionManager>, Error> {
        self.tikv_pool
            .get()
            .await
            .map_err(|_| Error::Internal("Unable to get TiKV pool connection".into()))
    }

    /// Parse a 3-byte instance entry into (dataplane_id, instance_id)
    fn parse_instance_entry(entry: &[u8]) -> MapiResult<(u8, u16)> {
        if entry.len() >= 3 {
            let dataplane_id = entry[0];
            let instance_id = u16::from_be_bytes(entry[1..3].try_into().unwrap());
            Ok((dataplane_id, instance_id))
        } else {
            Err(Error::KeyResolverMalformed(entry.to_vec()))
        }
    }

    /// Given some required reducer types, find a healthy instance (dataplane ID and instance ID
    /// pair) for each of them
    pub async fn resolve_instances(
        &self,
        reducers: &[ReducerType],
        same_dataplane: bool,
    ) -> MapiResult<HashMap<ReducerType, (u8, u16)>> {
        let mut con = self
            .instance_redis_pool
            .get()
            .await
            .map_err(Error::RedisPoolError)?;

        let network = match self.network {
            Mode::Bitcoin => key_resolver::Network::Mainnet,
            Mode::BitcoinTestnet => key_resolver::Network::Testnet,
            _ => unreachable!(),
        };

        let mut found = HashMap::new();

        if same_dataplane {
            let mut instance_entries = HashMap::<ReducerType, Vec<(Vec<u8>, i64)>>::new();

            // create a map of dataplane id -> lowest chain tip of required instance in dataplane
            // (so the highest block at which we can unify the instances within just that dataplane)
            let mut dataplane_shared_tip = HashMap::<u8, i64>::new();

            for reducer in reducers {
                let key = format!("{{bitcoin:{}:{}}}:scores", network, reducer);

                let entries: Vec<(Vec<u8>, i64)> = con
                    .zrange_withscores(&key, 0, 100)
                    .await
                    .map_err(Error::Redis)?;

                if entries.is_empty() {
                    return Err(Error::KeyResolverNoEntries(key));
                }

                for (entry, score) in entries.iter() {
                    let (dataplane_id, _) = Self::parse_instance_entry(entry)?;

                    // track the lowest tip of required instances in each dataplane
                    if let Some(tip) = dataplane_shared_tip.get(&dataplane_id) {
                        if score < tip {
                            dataplane_shared_tip.insert(dataplane_id, *score);
                        }
                    } else {
                        dataplane_shared_tip.insert(dataplane_id, *score);
                    };
                }

                instance_entries.insert(*reducer, entries);
            }

            // select dataplane with greatest common point
            let Some(best_dataplane) = dataplane_shared_tip.into_iter().max_by_key(|(_, v)| *v)
            else {
                return Ok(found);
            };

            // now select instances from the best dataplane
            for (instance, entries) in instance_entries {
                for (entry, _) in entries {
                    let (dataplane_id, instance_id) = Self::parse_instance_entry(&entry)?;

                    if dataplane_id != best_dataplane.0 {
                        continue;
                    }

                    found.insert(instance, (dataplane_id, instance_id));
                    break;
                }
            }
        } else {
            for reducer in reducers {
                // Construct the Redis key
                let key = format!("{{bitcoin:{}:{}}}:scores", network, reducer);

                debug!("Retrieving key: {} from redis...", key);

                // Fetch the first entry from the sorted set in Redis
                let entries: Vec<Vec<u8>> = con.zrange(&key, 0, 0).await.map_err(Error::Redis)?;

                if let Some(entry) = entries.first() {
                    let (dataplane_id, instance_id) = Self::parse_instance_entry(entry)?;

                    debug!(
                        "{} => dataplane_id: {} instance_id: {}",
                        reducer, dataplane_id, instance_id
                    );

                    found.insert(*reducer, (dataplane_id, instance_id));
                } else {
                    return Err(Error::KeyResolverNoEntries(key));
                }
            }
        }

        Ok(found)
    }

    /// Given some required reducer types, find a healthy instance for each of them by selecting
    /// the dataplane with the best (most recent) mempool view timestamp.
    /// Returns None if any reducer lacks mempool-view entries, signaling fallback to tip-based selection.
    pub async fn resolve_instances_by_mempool_view(
        &self,
        reducers: &[ReducerType],
    ) -> MapiResult<Option<HashMap<ReducerType, (u8, u16)>>> {
        let mut con = self
            .instance_redis_pool
            .get()
            .await
            .map_err(Error::RedisPoolError)?;

        let network = match self.network {
            Mode::Bitcoin => key_resolver::Network::Mainnet,
            Mode::BitcoinTestnet => key_resolver::Network::Testnet,
            _ => unreachable!(),
        };

        let mut instance_entries = HashMap::<ReducerType, Vec<(Vec<u8>, i64)>>::new();

        // create a map of dataplane id -> lowest mempool_view_ts of required instances in dataplane
        // (so the best mempool view at which we can unify the instances within just that dataplane)
        let mut dataplane_shared_mempool_ts = HashMap::<u8, i64>::new();

        for reducer in reducers {
            let key = format!("{{bitcoin:{}:{}}}:mempool-view", network, reducer);

            let entries: Vec<(Vec<u8>, i64)> = con
                .zrange_withscores(&key, 0, 100)
                .await
                .map_err(Error::Redis)?;

            // If any reducer lacks mempool-view entries, signal fallback
            if entries.is_empty() {
                return Ok(None);
            }

            for (entry, score) in entries.iter() {
                let (dataplane_id, _) = Self::parse_instance_entry(entry)?;

                // track the lowest mempool_view_ts of required instances in each dataplane
                if let Some(ts) = dataplane_shared_mempool_ts.get(&dataplane_id) {
                    if score < ts {
                        dataplane_shared_mempool_ts.insert(dataplane_id, *score);
                    }
                } else {
                    dataplane_shared_mempool_ts.insert(dataplane_id, *score);
                };
            }

            instance_entries.insert(*reducer, entries);
        }

        // select dataplane with greatest (most recent) shared mempool_view_ts
        let Some(best_dataplane) = dataplane_shared_mempool_ts
            .into_iter()
            .max_by_key(|(_, v)| *v)
        else {
            return Ok(None);
        };

        let mut found = HashMap::new();

        // now select instances from the best dataplane
        for (reducer, entries) in instance_entries {
            for (entry, _) in entries {
                let (dataplane_id, instance_id) = Self::parse_instance_entry(&entry)?;

                if dataplane_id != best_dataplane.0 {
                    continue;
                }

                found.insert(reducer, (dataplane_id, instance_id));
                break;
            }
        }

        // If not all reducers were found in the best dataplane, fall back to tip-based selection
        if found.len() != reducers.len() {
            return Ok(None);
        }

        Ok(Some(found))
    }

    /// Given a list of reducers, use information from the timestamp entry redis database to start
    /// TiKV snapshots for each required polyphony instance such that we can view data as of the
    /// most recent common block which has been processed by all of the instances.
    pub async fn init_tip(&mut self, reducers: &[ReducerType]) -> MapiResult<()> {
        if reducers.is_empty() {
            return Ok(());
        };

        let mut snapshots = HashMap::new();

        // discover the healthy instances (dataplane ID and instance ID) for the required instance
        // types
        let instances = self.resolve_instances(reducers, false).await?;

        // fetch the timestamp entries for the required instances
        let mut entry_map = HashMap::new();

        let mut redis = self.redis_pool.get().await.map_err(Error::RedisPoolError)?;

        for (instance_type, instance_id) in instances.iter() {
            let key = format!("tikv-timestamps:{}:{}", instance_id.0, instance_id.1);

            // get the timestamp values for the instance
            let block_options: Vec<RedisEntry> = redis
                .zrangebyscore(key, "-inf", "+inf")
                .await
                .map_err(Error::Redis)?;

            if block_options.is_empty() {
                return Err(Error::NoTimestampEntires(instance_id.0, instance_id.1));
            }

            entry_map.insert(*instance_type, block_options);
        }

        // now, find the best non-mempool point shared across the required instances

        let Some(intersection_block) = find_best_common_block(&entry_map) else {
            return Err(Error::NoInstanceIntersect(entry_map));
        };

        // now we have the intersection block, start tikv snapshot at that block for each instance

        for (instance_type, mut entries) in entry_map.into_iter() {
            // get the most recent timestamp for the desired point (there may be many)
            entries.sort_by_key(|b| (b.height, Into::<Timestamp>::into(b.commit_ts.clone())));
            entries.reverse();

            let tikv_ts = entries
                .into_iter()
                .filter(|x| !x.was_mempool)
                .find(|b| (b.height, b.block_hash) == intersection_block)
                .unwrap() // we know there is an entry with the intersect block
                .commit_ts;

            let snapshot = self
                .get_tikv_client()
                .await?
                .snapshot(tikv_ts, TransactionOptions::new_optimistic());

            let (dataplane_id, instance_id) = instances[&instance_type];

            let prefix = Prefix::new(dataplane_id, instance_id);

            snapshots.insert(instance_type.clone(), (snapshot, prefix));
        }

        self.snapshots = snapshots;
        self.snapshot_point = Some(SnapshotPoint {
            chain_tip: intersection_block,
            mempool: None,
        });

        Ok(())
    }

    /// Given a list of reducers, use information from the timestamp entry redis database to start
    /// TiKV snapshots for each required polyphony instance such that we can view data as of the
    /// most recent common mempool snapshot has been processed by all of the instances.
    ///
    /// 1. get best chain tip block across the instances
    /// 2. find the best mempool view ts which every instance has processed for that chain tip
    /// 3. find the minimum number of mempool blocks for that mempool view ts
    /// 4. start snapshot at that mempool view ts, for the number of blocks available or max blocks
    pub async fn init_mempool(
        &mut self,
        reducers: &[ReducerType],
        max_blocks: Option<u8>,
    ) -> MapiResult<()> {
        // if max mempool blocks is 0, then just use the init tip
        if max_blocks == Some(0) {
            return self.init_tip(reducers).await;
        }

        if reducers.is_empty() {
            return Ok(());
        };

        let mut snapshots = HashMap::new();

        // discover the healthy instances (dataplane ID and instance ID) for the required reducers
        // First try to select dataplane by best mempool view timestamp, fall back to tip-based selection
        let instances = match self.resolve_instances_by_mempool_view(reducers).await? {
            Some(instances) => instances,
            None => self.resolve_instances(reducers, true).await?,
        };

        // fetch the timestamp entries for the required instances
        let mut entry_map = HashMap::new();

        let mut redis = self.redis_pool.get().await.map_err(Error::RedisPoolError)?;

        for (instance_type, instance_id) in instances.iter() {
            let key = format!("tikv-timestamps:{}:{}", instance_id.0, instance_id.1);

            // get the timestamp values for the instance
            let block_options: Vec<RedisEntry> = redis
                .zrangebyscore(key, "-inf", "+inf")
                .await
                .map_err(Error::Redis)?;

            if block_options.is_empty() {
                return Err(Error::NoTimestampEntires(instance_id.0, instance_id.1));
            }

            entry_map.insert(*instance_type, block_options);
        }

        // now, find the best non-mempool point shared across the required instances

        let Some(intersection_block) = find_best_common_block(&entry_map) else {
            return Err(Error::NoInstanceIntersect(entry_map));
        };

        // now we have the best common chaintip we need to find the most recent common mempool view
        // which has been seen be all instances

        // 1. limit entries to those who: a. are mempool, b. have chain tip as the intersection block
        // 2. for each instance make a make a map of (mempool view ts -> number of available blocks)
        // 3. now find the best common mempool view ts shared over all instances
        // 4. take the lowest number of available blocks for that mempool view ts over all instances
        // 5. now we have the [chaintip, mempool view ts, number of mempool blocks] required to start snapshots

        let mempool_view_intersection =
            find_best_common_mempool_view(&entry_map, intersection_block);

        // Determine effective tip and mempool view:
        // - If current tip has mempool, use it
        // - If not, try the block at height-1 for mempool
        // - If that also fails, use current tip without mempool
        let (effective_tip, mempool_view_ts) = match mempool_view_intersection {
            Some(ts) => {
                debug!(
                    "Using current tip height={} with mempool view ts={}",
                    intersection_block.0, ts
                );
                (intersection_block, Some(ts))
            }
            None => {
                // Try fallback: check if height-1 has a common block with mempool
                let fallback_height = intersection_block.0.saturating_sub(1);
                let fallback_result = find_common_block_at_height(&entry_map, fallback_height)
                    .and_then(|fallback_tip| {
                        find_best_common_mempool_view(&entry_map, fallback_tip)
                            .map(|ts| (fallback_tip, ts))
                    });

                match fallback_result {
                    Some((fallback_tip, ts)) => {
                        info!(
                            "Tip height={} has no mempool, falling back to height={} with mempool view ts={}",
                            intersection_block.0, fallback_tip.0, ts
                        );
                        (fallback_tip, Some(ts))
                    }
                    None => {
                        warn!(
                            "No mempool at height={} or height={}, serving without mempool",
                            intersection_block.0, fallback_height
                        );
                        (intersection_block, None)
                    }
                }
            }
        };

        // if no mempool view found, we are just reading data from the tip
        match mempool_view_ts {
            None => {
                for (reducer_type, mut entries) in entry_map.into_iter() {
                    // get the most recent timestamp for the desired point (there may be many)
                    entries
                        .sort_by_key(|b| (b.height, Into::<Timestamp>::into(b.commit_ts.clone())));
                    entries.reverse();

                    let tikv_ts = entries
                        .into_iter()
                        .filter(|x| !x.was_mempool)
                        .find(|b| (b.height, b.block_hash) == effective_tip)
                        .unwrap() // we know there is an entry with the effective tip
                        .commit_ts;

                    let snapshot = self
                        .get_tikv_client()
                        .await?
                        .snapshot(tikv_ts, TransactionOptions::new_optimistic());

                    let (dataplane_id, instance_id) = instances[&reducer_type];

                    let prefix = Prefix::new(dataplane_id, instance_id);

                    snapshots.insert(reducer_type, (snapshot, prefix));
                }

                self.snapshots = snapshots;
                self.snapshot_point = Some(SnapshotPoint {
                    chain_tip: effective_tip,
                    mempool: None,
                });
            }
            Some(mempool_view_intersect) => {
                // we found a common mempool view ts, now we need to find the common point
                // (just assert they are all the same?)
                let mut available_blocks = HashMap::with_capacity(reducers.len());

                let mut most_blocks = 0;

                // find how many available mempool blocks we have for each instance for the mempool
                // view
                for (instance_type, mut entries) in entry_map.into_iter() {
                    // get entries relating to this mempool view
                    entries.retain(|x| {
                        x.was_mempool
                            && (x.chain_tip_height, x.chain_tip_hash) == effective_tip
                            && x.mempool_view_ts == mempool_view_intersect
                    });

                    // we should only have a single entry for each mempool view
                    if entries.len() != 1 {
                        error!("expected a single entry, got {entries:?}");
                    }

                    entries.sort_by_key(|b| Into::<Timestamp>::into(b.commit_ts.clone()));

                    let entry = entries.pop().unwrap();

                    let mempool_blocks = entry.height - entry.chain_tip_height;

                    if mempool_blocks > most_blocks {
                        most_blocks = mempool_blocks;
                    }

                    available_blocks.insert(instance_type, entry);
                }

                assert!(available_blocks
                    .iter()
                    .all(|(_, x)| x.height - x.chain_tip_height == most_blocks));

                // start snapshots
                for (instance_type, entry) in available_blocks {
                    let tikv_ts = entry.commit_ts.clone();

                    let snapshot = self
                        .get_tikv_client()
                        .await?
                        .snapshot(tikv_ts, TransactionOptions::new_optimistic());

                    let (dataplane_id, instance_id) = instances[&instance_type];

                    let prefix = Prefix::new(dataplane_id, instance_id);

                    snapshots.insert(instance_type.clone(), (snapshot, prefix));
                }

                self.snapshots = snapshots;
                self.snapshot_point = Some(SnapshotPoint {
                    chain_tip: effective_tip,
                    mempool: Some(MempoolView {
                        mempool_view_ts: mempool_view_intersect,
                        mempool_blocks: most_blocks as usize,
                    }),
                });
            }
        }

        Ok(())
    }

    pub fn take_snapshot_and_encoder(
        &mut self,
        reducer: ReducerType,
    ) -> MapiResult<(Snapshot, Prefix)> {
        let snapshot = self
            .snapshots
            .remove(&reducer)
            .ok_or(Error::AdapterMissingReducer(reducer))?;

        Ok(snapshot)
    }

    pub fn insert_snapshot_and_encoder(
        &mut self,
        reducer: ReducerType,
        snapshot: Snapshot,
        encoder: Prefix,
    ) {
        self.snapshots.insert(reducer, (snapshot, encoder));
    }

    pub fn get_snapshot(&mut self, reducer: ReducerType) -> MapiResult<&mut Snapshot> {
        let (snapshot, _) = self
            .snapshots
            .get_mut(&reducer)
            .ok_or(Error::AdapterMissingReducer(reducer))?;

        Ok(snapshot)
    }

    pub fn get_encoder(&mut self, reducer: ReducerType) -> MapiResult<Prefix> {
        let (_, prefix) = self
            .snapshots
            .get(&reducer)
            .ok_or(Error::AdapterMissingReducer(reducer))?;

        Ok(prefix.clone())
    }

    pub fn get_snapshot_point(&self) -> MapiResult<ChainTip> {
        let Some((block_height, block_hash)) = self.snapshot_point.clone().map(|x| x.chain_tip)
        else {
            return Err(Error::AdapterNotInitialised);
        };

        Ok(ChainTip {
            block_height,
            block_hash: BlockHash::from_byte_array(block_hash).to_string(),
        })
    }

    pub fn get_snapshot_mempool_info(&self) -> MapiResult<Option<MempoolView>> {
        let Some(mempool_view) = self.snapshot_point.clone().map(|x| x.mempool) else {
            return Err(Error::AdapterNotInitialised);
        };

        Ok(mempool_view)
    }

    pub async fn parse_address_or_script_bytes(
        &mut self,
        input: &str,
    ) -> MapiResult<(Option<Address>, Vec<u8>)> {
        let network = match self.network {
            Mode::Bitcoin => Network::Bitcoin,
            Mode::BitcoinTestnet => Network::Testnet,
            _ => unreachable!(),
        };

        let (address, script) = match Address::from_str(input) {
            Ok(addr) => {
                let addr = addr
                    .require_network(network)
                    .map_err(|_| Error::MalformedRequest("Address not valid for network".into()))?;

                let payload_hash = addr.script_pubkey().script_hash().to_byte_array();

                let script_hash = self
                    .get_reducer_key_maybe::<_, script_hash_by_address_payload_hash::Value>(
                        (
                            ReducerType::ScriptHashByAddressPayloadHash,
                            Reducer::ScriptHashByAddressPayloadHash,
                        ),
                        &script_hash_by_address_payload_hash::Key { payload_hash },
                    )
                    .await?
                    .map(|x| x.script_hash)
                    .ok_or_else(|| Error::NotFound)?;

                let script = self
                    .get_reducer_key_maybe::<_, script_by_script_hash::Value>(
                        (ReducerType::ScriptByScriptHash, Reducer::ScriptByScriptHash),
                        &script_by_script_hash::Key { script_hash },
                    )
                    .await?
                    .map(|x| x.script)
                    .ok_or_else(|| Error::Internal("missing script by sh".into()))?;

                (Some(addr), script)
            }
            Err(_) => {
                let script_bytes = hex::decode(input).map_err(|_| {
                    Error::MalformedRequest(
                        "Could not decode as address or hex-encoded script pubkey".into(),
                    )
                })?;

                let script = ScriptBuf::from_bytes(script_bytes.clone());

                let address = bitcoin::Address::from_script(script.as_script(), network).ok();

                (address, script_bytes)
            }
        };

        Ok((address, script))
    }

    /// Note: not mempool compatible currentlys
    pub async fn resolve_script_hash(
        &mut self,
        network: Mode,
        script_hash: [u8; 20],
    ) -> MapiResult<(Option<Address>, Vec<u8>)> {
        let network = match network {
            Mode::Bitcoin => Network::Bitcoin,
            Mode::BitcoinTestnet => Network::Testnet,
            _ => unreachable!(),
        };

        let script_bytes: Vec<u8> = self
            .get_reducer_key(
                (ReducerType::ScriptByScriptHash, Reducer::ScriptByScriptHash),
                &script_by_script_hash::Key { script_hash },
            )
            .await?;

        let script = ScriptBuf::from_bytes(script_bytes.clone());

        let address = bitcoin::Address::from_script(script.as_script(), network).ok();

        Ok((address, script_bytes))
    }

    pub async fn resolve_rune_name(&mut self, rune_name: u128) -> MapiResult<Option<(u64, u32)>> {
        Ok(self
            .get_reducer_key_maybe::<_, rune_id_by_rune_name::Value>(
                (ReducerType::RuneIdByRuneName, Reducer::RuneIdByRuneName),
                &rune_id_by_rune_name::Key { rune_name },
            )
            .await?
            .map(|x| x.rune_id))
    }

    pub async fn get_reducer_key<A: Encode + Decode, B: Decode>(
        &mut self,
        reducer: (ReducerType, timbre_xbt::Reducer), // TODO infer
        key: &A,
    ) -> MapiResult<B> {
        let (_, encoder) = self
            .snapshots
            .get(&reducer.0)
            .ok_or(Error::AdapterMissingReducer(reducer.0))?;

        let data = encoder.data(&reducer.1, key);

        self.get_reducer_key_maybe(reducer, key)
            .await?
            .ok_or_else(|| Error::MissingData(data))
    }

    pub async fn get_reducer_key_maybe<A: Encode + Decode, B: Decode>(
        &mut self,
        reducer: (ReducerType, timbre_xbt::Reducer), // TODO infer
        key: &A,
    ) -> MapiResult<Option<B>> {
        let (snapshot, encoder) = self
            .snapshots
            .get_mut(&reducer.0)
            .ok_or(Error::AdapterMissingReducer(reducer.0))?;

        let encoded_key = encoder.data(&reducer.1, key);

        let encoded_value = snapshot.get(encoded_key).await.map_err(Error::TiKV)?;

        match encoded_value {
            Some(v) => Ok(Some(
                <B>::decode(&v)
                    .map_err(|e| Error::MalformedData(v.clone(), Some(e)))?
                    .0,
            )),
            None => Ok(None),
        }
    }

    pub async fn with_collection_metadata(&mut self) -> MapiResult<()> {
        let instance = self.ingestor_instances.collections;

        let timestamp = self.get_tikv_client().await?.current_timestamp().await?;

        let latest_snapshot = self
            .get_tikv_client()
            .await?
            .snapshot(timestamp, TransactionOptions::new_optimistic());

        let encoder = Prefix::new(instance.0, instance.1);

        self.snapshots.insert(
            ReducerType::InscriptionCollectionsMetadata,
            (latest_snapshot, encoder),
        );

        Ok(())
    }

    pub async fn get_collection_key_maybe<A: Encode>(
        &mut self,
        kind: &CollectionIngestor,
        key: &A,
    ) -> MapiResult<Option<Vec<u8>>> {
        let (snapshot, encoder) = self
            .snapshots
            .get_mut(&ReducerType::InscriptionCollectionsMetadata)
            .ok_or(Error::AdapterMissingReducer(
                ReducerType::InscriptionCollectionsMetadata,
            ))?;

        let encoded_key = encoder.collection_metadata(kind, key);

        snapshot.get(encoded_key).await.map_err(Error::TiKV)
    }

    pub async fn with_miner_metadata(&mut self) -> MapiResult<()> {
        let instance = self.ingestor_instances.miners;

        let timestamp = self.get_tikv_client().await?.current_timestamp().await?;

        let latest_snapshot = self
            .get_tikv_client()
            .await?
            .snapshot(timestamp, TransactionOptions::new_optimistic());

        let encoder = Prefix::new(instance.0, instance.1);

        self.snapshots
            .insert(ReducerType::MinerMetadata, (latest_snapshot, encoder));

        Ok(())
    }

    pub async fn get_miner_metadata_key_maybe<A: Encode>(
        &mut self,
        kind: &MinerIngestor,
        key: &A,
    ) -> MapiResult<Option<Vec<u8>>> {
        let (snapshot, encoder) = self
            .snapshots
            .get_mut(&ReducerType::MinerMetadata)
            .ok_or(Error::AdapterMissingReducer(ReducerType::MinerMetadata))?;

        let encoded_key = encoder.miner_metadata(kind, key);

        snapshot.get(encoded_key).await.map_err(Error::TiKV)
    }
}
