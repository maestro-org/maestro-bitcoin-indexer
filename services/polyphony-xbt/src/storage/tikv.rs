use bitcoin::{BlockHash, hashes::Hash};
use builder::PREFIX_DATA;
use futures::future::join_all;
use gasket::{
    error::AsWorkError,
    runtime::{ScheduleResult, WorkSchedule, spawn_stage},
};
use itertools::Itertools;
use redis::{Commands, cluster::ClusterClient};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
    u32,
};
use tikv_client::{
    CommitTTLParameters, Error, Key, KvPair, Timestamp, TimestampExt, Transaction,
    TransactionClient, TransactionOptions,
};
use timbre_xbt::{
    Decode,
    rollback::{MetadataKey, rollback_metadata_key_range},
};
use tokio::task::JoinHandle;
use tracing::error;
use tracing::{debug, info, warn};

use crate::{
    bootstrap,
    crosscut::{self, Point},
    model::{
        self,
        StorageAction::{self, *},
        StorageActionPayload,
    },
    prelude::AppliesPolicy,
    reducers::{
        IncrOrDecr, ReducerOutput, UtxoAction, balances_by_brc20, balances_by_rune_id,
        block_by_tx_hash, block_info, brc20_balances_by_script_hash, brc20_terms_by_ticker,
        content_by_inscription_id, etching_by_rune_id, height_by_block_hash, height_by_timestamp,
        historical_sat_balance_by_script_hash, inscription_activity_by_script_hash,
        inscription_activity_by_tx, inscription_activity_by_tx_v2,
        inscription_utxos_by_script_hash, mints_by_rune_id, rune_id_by_rune_name,
        rune_txs_by_script_hash, rune_utxos_by_script_hash, sat_balance_by_script_hash,
        sat_txs_by_script_hash, sats_per_vb_by_block, script_by_script_hash,
        script_hash_by_address_payload_hash, spending_tx_by_txo, total_inscriptions_by_script_hash,
        total_outputs_by_script_hash, total_sat_in_inputs_by_script_hash,
        total_sat_in_outputs_by_script_hash, total_txs_by_script_hash, total_utxos_by_script_hash,
        transfer_inscriptions_by_script_hash, tx_first_seen_timestamp, tx_info, txs_by_block,
        txs_by_inscription, txs_by_rune_id, txs_by_script_hash, utxos_by_rune_id,
        utxos_by_script_hash,
    },
    rollback::{
        PersistentBufferValue,
        buffer::{PointWithResult, RollbackBuffer},
    },
    storage::{
        StorageActions,
        action_merger::ActionMerger,
        redis_entry::RedisEntry,
        utils::{commit_txn_or_rollback, compact_actions, delete_mutation, set_mutation},
    },
};

use timbre_xbt::*;

type InputPort = gasket::messaging::tokio::InputPort<model::StorageActionPayload>;

// TODO: use config crate
static DEFAULT_SPLIT_COMMIT_BATCH_SIZE: usize = 500;
static DEFAULT_SPLIT_COMMIT_MAX_CONCURRENT: usize = 100;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    /// TiKV PD
    pub connection_params: String,
    /// Redis address
    pub redis_address: String,
    /// Which network is this dataplane for
    pub network: String,
    /// Polyphony dataplane identifier
    pub dataplane_id: u8,
    /// Polyphony instance identifier so multiple Polyphony can use the same DB
    pub instance_id: u16,
    /// Maximum number of actions within a single split commit batch (default 500)
    pub split_commit_batch_size: Option<usize>,
    /// Maximum number of batches to commit in parallel at once (default 100)
    pub split_commit_max_concurrent: Option<usize>,
    /// TiKV commit min TTL in millis (default 3000)
    pub tikv_commit_min_ttl: Option<u64>,
    /// TiKV commit max TTL in millis (default 20000)
    pub tikv_commit_max_ttl: Option<u64>,
    /// TiKV commit batch size (default 16kb)
    pub tikv_commit_batch_size: Option<u64>,
    /// TiKV commit TTL multiplier (default 6000.0)
    pub tikv_commit_ttl_factor: Option<f64>,
    /// TiKV cleanup locks after every transaction (default true)
    pub tikv_cleanup_locks: Option<bool>,
    /// True if we want to display warnings if attempting to delete non-existent keys or decrement
    /// more than the current value
    pub key_warnings: Option<bool>,
    /// Advertise this instance in the per-reducer instance registry immediately, rather
    /// than waiting until it has caught up with the chain tip (default false). The registry
    /// is how the API layer selects instances, so a backfilling instance normally stays
    /// unadvertised until it reaches the tip — this is what makes zero-downtime reducer
    /// upgrades work: sync a new instance in parallel, and it takes over once caught up.
    /// Only set this for single-instance dev setups with `use_mempool` disabled (catch-up
    /// is detected by the arrival of mempool blocks, which only stream at the tip).
    pub advertise_immediately: Option<bool>,
}

impl Config {
    pub fn bootstrapper(
        self,
        policy: &crosscut::policies::RuntimePolicy,
        reducer_names: Vec<String>,
    ) -> Bootstrapper {
        Bootstrapper {
            config: self,
            policy: policy.clone(),
            input: Default::default(),
            reducer_names,
        }
    }

    fn build_tx_opts(&self) -> TransactionOptions {
        let mut tx_opts = TransactionOptions::new_optimistic();

        let mut ttl_parameters = CommitTTLParameters::default();

        if let Some(x) = self.tikv_commit_min_ttl {
            ttl_parameters = ttl_parameters.min_ttl(x)
        }

        if let Some(x) = self.tikv_commit_max_ttl {
            ttl_parameters = ttl_parameters.max_ttl(x)
        }

        if let Some(x) = self.tikv_commit_batch_size {
            ttl_parameters = ttl_parameters.txn_commit_batch_size(x)
        }

        if let Some(x) = self.tikv_commit_ttl_factor {
            ttl_parameters = ttl_parameters.ttl_factor(x)
        }

        tx_opts = tx_opts.ttl_parameters(ttl_parameters);

        tx_opts = tx_opts.drop_check(tikv_client::CheckLevel::Warn);

        tx_opts
    }
}
pub struct Bootstrapper {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    input: InputPort,
    reducer_names: Vec<String>,
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort {
        &mut self.input
    }

    pub fn build_cursor(&self, buffer_size: usize) -> Cursor {
        Cursor {
            config: self.config.clone(),
            buffer_size,
        }
    }

    pub fn spawn_stages(
        self,
        pipeline: &mut bootstrap::Pipeline,
        intersect: Option<Point>,
        buf: Option<Vec<PersistentBufferValue>>,
        buffer_size: usize,
        timeout: u64,
        safe_mode: bool,
    ) {
        let mut rollback_buffer: RollbackBuffer = RollbackBuffer::new(buffer_size, safe_mode);

        let mutable = buf.is_some(); // OK?

        if let Some(persistent_buf) = buf {
            for entry in persistent_buf {
                if let Some(p) =
                    rollback_buffer.add_block(entry.point.into(), entry.inverse_actions)
                {
                    warn!(
                        "discarding point {:?} ({}) while populating in memory rollback buffer",
                        p.point,
                        p.result.len()
                    )
                }
            }

            tracing::debug!(
                "populated memory rollback buffer for storage stage: {:?}",
                rollback_buffer
            )
        } else {
            tracing::debug!("no persistent rollback buffer found to populate memory buf")
        }

        let cursor = self.build_cursor(buffer_size);

        let worker = Worker {
            config: self.config.clone(),
            policy: self.policy.clone(),
            connection: None,
            redis_connection: None,
            input: self.input,
            ops_count: Default::default(),
            key_encoder: Prefix::new(self.config.dataplane_id, self.config.instance_id),
            tx_options: self.config.build_tx_opts(),
            rollback_buffer,
            work_unit_2pc: vec![],
            mutable,
            last_processed: intersect,
            partial_split_commit: None,
            cursor,
            safe_mode,
            mempool_refresh_cache: None,
            reducer_names: self.reducer_names,
            registered: self.config.advertise_immediately.unwrap_or(false),
        };

        info!(
            "using dataplane id {} with instance id {} and buffer size {buffer_size}",
            self.config.dataplane_id, self.config.instance_id
        );

        pipeline.register_stage(spawn_stage(
            worker,
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(timeout)),
                bootstrap_retry: gasket::retries::Policy {
                    max_retries: 20,
                    backoff_unit: Duration::from_secs(1),
                    backoff_factor: 2,
                    max_backoff: Duration::from_secs(60),
                },
                ..Default::default()
            },
            Some("tikv"),
        ));
    }
}

#[derive(Clone)]
pub struct Cursor {
    config: Config,
    buffer_size: usize,
}

impl Cursor {
    pub async fn last_point(&mut self) -> Result<Option<Point>, crate::Error> {
        // first check if there is a split commit lock, and take that as the last point
        if let Some(lock) = self.split_commit_lock().await?.map(|x| x.lock.safe_point) {
            let Some((lock_height, lock_hash)) = lock else {
                panic!("no last processed in lock")
            };

            info!("found split commit lock in storage plugin: {lock_height} {lock_hash:?}");

            return Ok(Some(Point {
                height: lock_height,
                hash: BlockHash::from_byte_array(lock_hash),
            }));
        }

        let connection = TransactionClient::new(vec![self.config.connection_params.clone()])
            .await
            .map_err(crate::Error::storage)?;

        let encoder = Prefix::new(self.config.dataplane_id, self.config.instance_id);

        let mut snapshot = connection.snapshot(
            connection
                .current_timestamp()
                .await
                .map_err(crate::Error::storage)?,
            TransactionOptions::new_optimistic().drop_check(tikv_client::CheckLevel::Warn),
        );

        let raw = snapshot
            .get(encoder.cursor())
            .await
            .map_err(crate::Error::storage)?;

        let point = match raw {
            Some(x) => {
                let ((height, hash), _) = <_>::decode(&x).unwrap();

                Some(Point {
                    height,
                    hash: BlockHash::from_byte_array(hash),
                })
            }
            None => None,
        };

        Ok(point)
    }

    pub async fn split_commit_lock(&mut self) -> Result<Option<SplitCommitInfo>, crate::Error> {
        let key_encoder = Prefix::new(self.config.dataplane_id, self.config.instance_id);
        let namespace = key_encoder.namespace();

        let lock_key = key_encoder.lock();

        let connection = TransactionClient::new(vec![self.config.connection_params.clone()])
            .await
            .map_err(crate::Error::storage)?;

        let mut snapshot = connection.snapshot(
            connection
                .current_timestamp()
                .await
                .map_err(crate::Error::storage)?,
            TransactionOptions::new_optimistic().drop_check(tikv_client::CheckLevel::Warn),
        );

        if let Some(raw_lock) = snapshot
            .get(lock_key)
            .await
            .map_err(crate::Error::storage)?
        {
            // last block processed (height, hash), number of actions being applied, was mutable
            let (lock_val, _) =
                SplitCommitLockValue::decode(&raw_lock).map_err(crate::Error::storage)?;

            // check for batch completion keys (present when mutable), these signal which batches
            // have been applied/the database tx committed

            let batch_complete_key_range = namespace.batch_complete_key_range();

            let batch_complete_keys = snapshot
                .scan_keys(batch_complete_key_range, u32::MAX)
                .await
                .map_err(crate::Error::storage)?;

            let batch_complete_ids = batch_complete_keys
                .into_iter()
                .map(|x| Into::<Vec<u8>>::into(x))
                .map(|x| <u32>::decode(&x[Namespace::size() + 2..]).unwrap().0)
                .collect::<Vec<_>>();

            Ok(Some(SplitCommitInfo {
                lock: lock_val,
                batches_complete: batch_complete_ids,
            }))
        } else {
            Ok(None)
        }
    }

    /// Fetch the persistent buffer from storage, if it exists, to bootstrap the
    /// stage rollback buffers on start-up.
    ///
    /// TODO: Refactor...
    pub async fn fetch_persistent_buffer(
        &mut self,
    ) -> Result<Option<Vec<PersistentBufferValue>>, crate::Error> {
        let connection = TransactionClient::new(vec![self.config.connection_params.clone()])
            .await
            .map_err(crate::Error::storage)?;

        let mut persistent_buf: Vec<PersistentBufferValue> = Vec::new();

        let mut snapshot = connection.snapshot(
            connection
                .current_timestamp()
                .await
                .map_err(crate::Error::storage)?,
            TransactionOptions::new_optimistic().drop_check(tikv_client::CheckLevel::Warn),
        );

        // find rollback buffer height range instead of scanning entire range (which might have
        // mvcc tombstones, missed gc'd points, ...)

        // note cursor height might be less than height of some entries in rollback buffer
        let cursor_height = self
            .last_point()
            .await
            .map_err(crate::Error::storage)?
            .map(|x| x.height)
            .unwrap_or_default();

        let min_height = cursor_height.saturating_sub(self.buffer_size as u64 + 1);

        // scan all metadata keys, for each point create a vec of inverse storage ops

        info!("scanning persistent rb buf from {}", min_height);

        let range = rollback_metadata_key_range(
            &Namespace::new(self.config.dataplane_id, self.config.instance_id),
            Some(min_height),
            None::<u64>,
        );

        // --

        let mut range = range.0..range.1;

        let mut current_point = Point {
            height: 0,
            hash: BlockHash::from_byte_array([0; 32]), // TODO
        };

        let mut current_point_height = 0;

        let mut actions = Vec::new();

        let mut scan_size = 2000;
        let mut kvs = Vec::new();

        loop {
            match snapshot.scan(range.clone(), scan_size).await {
                Ok(kvs_iter) => {
                    let kvs_vec: Vec<KvPair> = kvs_iter.collect();

                    kvs.extend(kvs_vec.clone());

                    if kvs_vec.is_empty() {
                        break;
                    }

                    let mut last_key =
                        Into::<Vec<u8>>::into(kvs_vec.last().unwrap().clone().into_key());

                    debug!(
                        "scanned {} rb metadata, {} total, last key {}",
                        kvs_vec.len(),
                        kvs.len(),
                        hex::encode(&last_key)
                    );

                    last_key.push(0);
                    range = last_key..range.end;
                }
                Err(Error::Grpc(e))
                    if e.to_string().contains("Received message larger than max") =>
                {
                    warn!("halving scan size because got error {e:?}");
                    scan_size /= 2
                }
                Err(e) => {
                    error!("error when trying to fetch persistent buffer {e:?}");
                    return Err(e).map_err(crate::Error::storage);
                }
            }
        }

        info!("scanned {} total rb metadata keys", kvs.len());

        for KvPair(k, v) in kvs.into_iter() {
            let k: Vec<u8> = k.into();
            let k = k.get((Namespace::size() + 2)..).unwrap(); // TODO

            let (metadata_key, _) = MetadataKey::decode(&k).unwrap(); // TODO

            debug!("rollback metadata key found: {metadata_key:?}");

            // we are now looking at metadata for a new point, push info for
            // previous point to buffer
            if metadata_key.height != current_point_height {
                // don't include the init point
                // TODO: cleaner
                if current_point_height != 0 {
                    let bufval = PersistentBufferValue {
                        point: current_point.clone(),
                        inverse_actions: actions.clone(),
                    };

                    debug!("finished parsing point rb metadata: {bufval:?}");

                    persistent_buf.push(bufval);
                }

                // reset accumulators and set point to next
                current_point = Point {
                    height: metadata_key.height,
                    hash: BlockHash::from_byte_array(metadata_key.hash),
                };

                current_point_height = current_point.height;
                actions.clear();
            } else if metadata_key.hash != *current_point.hash.as_byte_array() {
                error!(
                    "multiple block hashes for same height in rb buf: {} vs {}",
                    BlockHash::from_byte_array(metadata_key.hash),
                    current_point.hash
                );

                // don't include the init point
                // TODO: cleaner
                if current_point_height != 0 {
                    let bufval = PersistentBufferValue {
                        point: current_point.clone(),
                        inverse_actions: actions.clone(),
                    };

                    debug!("finished parsing point rb metadata: {bufval:?}");

                    persistent_buf.push(bufval);
                }

                // reset accumulators and set point to next
                current_point = Point {
                    height: metadata_key.height,
                    hash: BlockHash::from_byte_array(metadata_key.hash),
                };

                current_point_height = current_point.height;
                actions.clear();

                current_point = Point {
                    height: metadata_key.height,
                    hash: BlockHash::from_byte_array(metadata_key.hash),
                };
            }

            if v.is_empty() {
                // empty value means we need to delete the key
                actions.push(StorageAction::Delete(metadata_key.modified_key))
            } else {
                // non-empty value means we need to create or overwrite key with previous value
                actions.push(StorageAction::Set(metadata_key.modified_key, v))
            }
        }

        // we have finished building the buf val for the most recent point as we
        // have exhausted all the rb metadata keys, push the final point. don't push
        // if it is the origin point though.
        if current_point_height != 0 {
            let bufval = PersistentBufferValue {
                point: current_point.clone(),
                inverse_actions: actions.clone(),
            };

            debug!("finished parsing final point rb metadata: {bufval:?}");

            persistent_buf.push(bufval);
        }

        if persistent_buf.is_empty() {
            info!("no persistent buffer");

            Ok(None)
        } else {
            info!("persistent buffer found ({})", persistent_buf.len());

            Ok(Some(persistent_buf))
        }
    }
}

#[derive(Debug, Clone)]
pub struct SplitCommitInfo {
    pub lock: SplitCommitLockValue,
    pub batches_complete: Vec<u32>,
}

pub struct Worker {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    connection: Option<tikv_client::TransactionClient>,
    redis_connection: Option<redis::cluster::ClusterClient>,
    ops_count: gasket::metrics::Counter,
    input: InputPort,
    key_encoder: Prefix,
    tx_options: TransactionOptions,
    rollback_buffer: RollbackBuffer,
    mutable: bool,
    work_unit_2pc: Vec<StorageActionPayload>,
    last_processed: Option<Point>,
    partial_split_commit: Option<SplitCommitInfo>,
    cursor: Cursor,
    /// Perform more integrity checks at some expense
    safe_mode: bool, // todo
    /// Note storage actions made by last mempool refresh, so we can just apply the difference
    mempool_refresh_cache: Option<(Point, HashMap<model::Key, StorageAction>)>,
    /// Kebab-case names of the reducers this instance runs, advertised in the
    /// Redis instance registry so the API layer can resolve instances
    reducer_names: Vec<String>,
    /// Whether this instance has started advertising itself in the instance registry.
    /// Flips to true on the first mempool block (mempool blocks only stream once the
    /// instance is at the chain tip), so backfilling instances stay unadvertised.
    registered: bool,
}

// TODO: Refactor
impl Worker {
    /// Check for lock key and batch completion keys used as part of split database tx commit
    async fn check_partial_split_commit(&mut self) -> Result<(), gasket::error::Error> {
        let current_ts = self
            .connection
            .as_mut()
            .unwrap()
            .current_timestamp()
            .await
            .or_restart()?;

        let mut snapshot = self
            .connection
            .as_mut()
            .unwrap()
            .snapshot(current_ts.clone(), self.tx_options.clone());

        let namespace = self.key_encoder.namespace();

        // check if lock key exists

        let lock_key = self.key_encoder.lock();

        if let Some(raw_lock) = snapshot.get(lock_key).await.or_restart()? {
            // last block processed (height, hash), number of actions being applied, was mutable
            let (lock_val, _) = SplitCommitLockValue::decode(&raw_lock).or_panic()?;

            // check for batch completion keys (present when immutable), these signal which batches
            // have been applied/the database tx committed

            let batch_complete_key_range = namespace.batch_complete_key_range();

            let batch_complete_keys = snapshot
                .scan_keys(batch_complete_key_range, u32::MAX)
                .await
                .or_restart()?;

            let batch_complete_ids = batch_complete_keys
                .into_iter()
                .map(|x| Into::<Vec<u8>>::into(x))
                .map(|x| <u32>::decode(&x[Namespace::size() + 2..]).unwrap().0)
                .collect::<Vec<_>>();

            self.partial_split_commit = Some(SplitCommitInfo {
                lock: lock_val,
                batches_complete: batch_complete_ids,
            })
        }

        Ok(())
    }

    /// Start an TiKV database transaction
    async fn begin_tikv_transaction(&mut self) -> Result<Transaction, tikv_client::Error> {
        let txn = self
            .connection
            .as_mut()
            .unwrap()
            .begin_with_options(self.tx_options.clone())
            .await?;

        let txn_start_ts = txn.start_timestamp();

        debug!(
            "started a tikv transaction at {txn_start_ts:?} ({})",
            txn_start_ts.version()
        );

        Ok(txn)
    }

    async fn insert_timestamp_entry(
        &mut self,
        point: &Point,
        ts: Timestamp,
        mempool_info: Option<((u64, [u8; 32]), u64)>,
    ) -> Result<(), gasket::error::Error> {
        let ((chain_tip_height, chain_tip_hash), mempool_view_ts) =
            mempool_info.unwrap_or_default();

        let mut redis_conn = self
            .redis_connection
            .as_mut()
            .unwrap()
            .get_connection()
            .or_restart()?;

        let key = format!(
            "tikv-timestamps:{}:{}",
            self.config.dataplane_id, self.config.instance_id
        );

        // update most recent timestamp
        let _: redis::Value = redis_conn
            .zadd("tikv-timestamps-keys", &key, ts.physical)
            .or_restart()?;

        // add new timestamp entry for instance
        let _: redis::Value = redis_conn
            .zadd(
                key,
                RedisEntry {
                    height: point.height,
                    // merkle root if mempool block
                    block_hash: *point.hash.as_byte_array(),
                    was_mempool: mempool_info.is_some(),
                    commit_ts: ts.clone(),
                    network: self.config.network.clone(),
                    chain_tip_height,
                    chain_tip_hash,
                    mempool_view_ts,
                },
                ts.physical,
            )
            .or_restart()?;

        // Advertise this instance in the per-reducer instance registry, which the API
        // layer uses to pick a (dataplane, instance) pair per reducer. Members are the
        // 3-byte instance identity; scores are the indexed chain tip (`:scores`) or the
        // mempool view timestamp (`:mempool-view`).
        //
        // Registration is gated on catching up with the chain tip (signalled by the first
        // mempool block): a backfilling instance must not appear in the registry, or the
        // API would unify its snapshot selection down to the backfiller's height. This
        // gate is the swap-over mechanism for zero-downtime reducer upgrades.
        if mempool_info.is_some() && !self.registered {
            info!("instance reached chain tip, advertising in instance registry");
            self.registered = true;
        }

        if !self.registered {
            return Ok(());
        }

        let member: [u8; 3] = {
            let iid = self.config.instance_id.to_be_bytes();
            [self.config.dataplane_id, iid[0], iid[1]]
        };

        let network = self.config.network.to_lowercase();

        for reducer in &self.reducer_names {
            if mempool_info.is_some() {
                let key = format!("{{bitcoin:{}:{}}}:mempool-view", network, reducer);
                let _: redis::Value = redis_conn
                    .zadd(key, &member[..], mempool_view_ts)
                    .or_restart()?;

                // mempool entries know the chain tip, so keep the tip score fresh too
                // (this also seeds `:scores` at the moment of registration, rather than
                // waiting for the next chain block)
                let key = format!("{{bitcoin:{}:{}}}:scores", network, reducer);
                let _: redis::Value = redis_conn
                    .zadd(key, &member[..], chain_tip_height)
                    .or_restart()?;
            } else {
                let key = format!("{{bitcoin:{}:{}}}:scores", network, reducer);
                let _: redis::Value = redis_conn
                    .zadd(key, &member[..], point.height)
                    .or_restart()?;
            }
        }

        Ok(())
    }

    /// Clean up any TiKV locks on data keys for this instance
    async fn cleanup_tikv_locks(
        &mut self,
        safepoint: &Timestamp,
        just_data: bool,
    ) -> Result<usize, tikv_client::Error> {
        // range will be all reducer data keys for this instance
        let namespace = self.key_encoder.namespace();

        let range = if just_data {
            // create range for just data keys relating to this instance
            let instance_data_key_range_start = [namespace.encode(), vec![PREFIX_DATA]].concat();

            let instance_data_key_range_end = [namespace.encode(), vec![PREFIX_DATA + 1]].concat();

            instance_data_key_range_start..instance_data_key_range_end
        } else {
            namespace.instance_key_range()
        };

        let resolved_locks = self
            .connection
            .as_mut()
            .unwrap()
            .legacy_cleanup_locks(range.into(), safepoint)
            .await?;

        if resolved_locks > 0 {
            debug!("cleaned up {resolved_locks} tikv locks")
        };

        Ok(resolved_locks)
    }

    /// Convert a ReducerOutput to a StorageAction, by specifying what key and values
    /// should be written to, or which keys should be deleted from, the database. This
    /// means the key and value encoding logic for a ReducerOutput can be different
    /// for different storage backends.
    fn reducer_output_to_storage_ops(&self, output: ReducerOutput) -> Vec<StorageAction> {
        match output {
            ReducerOutput::BalancesByBrc20(balances_by_brc20::Output {
                brc_ticker,
                script_hash,
                total_delta,
            }) => {
                let key = timbre_xbt::reducers::balances_by_brc20::Key {
                    ticker: brc_ticker.try_into().unwrap(),
                    script_hash,
                };

                let total_balance_key = self.key_encoder.data(&Reducer::BalancesByBrc20, &key);

                match total_delta {
                    IncrOrDecr::Increment(x) => {
                        vec![StorageAction::Increment(total_balance_key, x)]
                    }
                    IncrOrDecr::Decrement(x) => {
                        vec![StorageAction::Decrement(total_balance_key, x)]
                    }
                }
            }
            ReducerOutput::BalancesByRuneId(balances_by_rune_id::Output {
                rune_id,
                script_hash,
                action,
            }) => {
                let key = timbre_xbt::reducers::balances_by_rune_id::Key {
                    rune_id,
                    script_hash,
                };

                let balance_key = self.key_encoder.data(&Reducer::BalancesByRuneId, &key);

                match action {
                    IncrOrDecr::Increment(x) => {
                        vec![StorageAction::Increment(balance_key, x)]
                    }
                    IncrOrDecr::Decrement(x) => {
                        vec![StorageAction::Decrement(balance_key, x)]
                    }
                }
            }
            ReducerOutput::BlockByTxHash(block_by_tx_hash::Output { tx_hash, height }) => {
                let key = timbre_xbt::reducers::block_by_tx_hash::Key { tx_hash };
                let encoded_key = self.key_encoder.data(&Reducer::BlockByTxHash, &key);

                let value = timbre_xbt::reducers::block_by_tx_hash::Value { height };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::BlockInfo(block_info::Output {
                height,
                block_hash,
                block_size,
                block_weight_units,
                timestamp,
                total_fees,
                total_volume,
                total_txs,
                involves_inscriptions,
                involves_runes,
                involves_brc20,
                coinbase_script_sig,
            }) => {
                let key = timbre_xbt::reducers::block_info::Key { height };
                let encoded_key = self.key_encoder.data(&Reducer::BlockInfo, &key);

                let value = timbre_xbt::reducers::block_info::Value {
                    block_hash,
                    block_size,
                    block_weight_units,
                    timestamp,
                    total_fees,
                    total_volume,
                    total_txs,
                    involves_inscriptions,
                    involves_runes,
                    involves_brc20,
                    coinbase_script_sig,
                };
                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::Brc20BalancesByScriptHash(brc20_balances_by_script_hash::Output {
                script_hash,
                brc_ticker,
                total_delta,
                available_delta,
            }) => {
                let key = timbre_xbt::reducers::brc20_balances_by_script_hash::Key {
                    script_hash,
                    ticker: brc_ticker.try_into().unwrap(),
                };

                let mut actions = vec![];

                let total_balance_key = self
                    .key_encoder
                    .data(&Reducer::Brc20TotalBalanceByScriptHash, &key);

                match total_delta {
                    IncrOrDecr::Increment(x) => {
                        actions.push(StorageAction::Increment(total_balance_key, x))
                    }
                    IncrOrDecr::Decrement(x) => {
                        actions.push(StorageAction::DecrementNoDelete(total_balance_key, x))
                    }
                }

                let available_balance_key = self
                    .key_encoder
                    .data(&Reducer::Brc20AvailableBalanceByScriptHash, &key);

                match available_delta {
                    IncrOrDecr::Increment(x) => {
                        actions.push(StorageAction::Increment(available_balance_key, x))
                    }
                    IncrOrDecr::Decrement(x) => {
                        actions.push(StorageAction::DecrementNoDelete(available_balance_key, x))
                    }
                }

                actions
            }
            ReducerOutput::Brc20TermsByTicker(brc20_terms_by_ticker::Output {
                ticker,
                max,
                limit,
                dec,
                self_mint,
                deploy_id,
            }) => {
                let key = timbre_xbt::reducers::brc20_terms_by_ticker::Key {
                    ticker: ticker.try_into().unwrap(),
                };

                let encoded_key = self.key_encoder.data(&Reducer::Brc20TermsByTicker, &key); // TODO: implicit reducer tag byte

                let value = timbre_xbt::reducers::brc20_terms_by_ticker::Value {
                    max,
                    limit,
                    dec,
                    self_mint,
                    deploy_id,
                };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::ContentByInscriptionId(content_by_inscription_id::Output {
                inscription_id,
                created_at,
                inscription_num,
                content_type,
                content_body,
            }) => {
                let key = timbre_xbt::reducers::content_by_inscription_id::Key { inscription_id };
                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::ContentByInscriptionId, &key);
                let value = timbre_xbt::reducers::content_by_inscription_id::Value {
                    created_at,
                    inscription_num,
                    content_type,
                    content_body,
                };
                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::EtchingByRuneId(etching_by_rune_id::Output {
                rune_id,
                tx_hash,
                etching,
                cenotaph,
                is_bootstrap,
            }) => {
                let key = timbre_xbt::reducers::etching_by_rune_id::Key { rune_id };

                let encoded_key = self.key_encoder.data(&Reducer::EtchingByRuneId, &key); // TODO: implicit reducer tag byte

                let value = timbre_xbt::reducers::etching_by_rune_id::Value {
                    tx_hash,
                    name: etching.name,
                    spacers: etching.spacers,
                    symbol: etching.symbol,
                    divisibility: etching.divisibility,
                    premine: etching.premine,
                    max_mint_txs: etching.max_mint_txs,
                    amount_per_mint: etching.amount_per_mint,
                    start_height: etching.start_height,
                    end_height: etching.end_height,
                    start_offset: etching.start_offset,
                    end_offset: etching.end_offset,
                    turbo: etching.turbo,
                    cenotaph,
                };

                // Use SetPermanent for bootstrap (genesis rune) - immutable data, idempotent writes
                // Only write genesis rune on mainnet
                if is_bootstrap && self.config.network == "bitcoin" {
                    vec![StorageAction::SetPermanent(encoded_key, value.encode())]
                } else if is_bootstrap {
                    // Skip bootstrap on non-mainnet networks
                    vec![]
                } else {
                    vec![StorageAction::SetOnce(encoded_key, value.encode())]
                }
            }
            ReducerOutput::HeightByTimestamp(height_by_timestamp::Output { timestamp, height }) => {
                let key = timbre_xbt::reducers::height_by_timestamp::Key { timestamp };
                let encoded_key = self.key_encoder.data(&Reducer::HeightByTimestamp, &key);

                let value = timbre_xbt::reducers::height_by_timestamp::Value { height };

                vec![StorageAction::Insert(encoded_key, value.encode())]
            }
            ReducerOutput::HeightByBlockHash(height_by_block_hash::Output {
                block_hash,
                block_height,
            }) => {
                let key = timbre_xbt::reducers::height_by_block_hash::Key { block_hash };
                let encoded_key = self.key_encoder.data(&Reducer::HeightByBlockHash, &key);

                let value = timbre_xbt::reducers::height_by_block_hash::Value { block_height };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::HistoricalSatBalanceByScriptHash(
                historical_sat_balance_by_script_hash::Output {
                    script_hash,
                    height,
                    satoshi_delta,
                },
            ) => {
                let encoded_key_prefix = self
                    .key_encoder
                    .data(&Reducer::HistoricalSatBalanceByScriptHash, &script_hash);

                vec![StorageAction::PointAggregate(
                    encoded_key_prefix,
                    vec![(height.into(), satoshi_delta)],
                )]
            }
            ReducerOutput::InscriptionActivityByScriptHash(
                inscription_activity_by_script_hash::Output {
                    script_hash,
                    height,
                    activity_tx_index,
                    tx_hash,
                    self_transfers,
                    sent,
                    received,
                },
            ) => {
                let key = timbre_xbt::reducers::inscription_activity_by_script_hash::Key {
                    script_hash,
                    height,
                    activity_tx_index,
                    tx_hash,
                };
                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::InscriptionActivityByScriptHash, &key);

                let self_transfers = self_transfers
                    .into_iter()
                    .map(|(inscription_id, (input_index, input_sat_offset), (output_index, output_sat_offset))|
                        timbre_xbt::reducers::inscription_activity_by_script_hash::SelfTransferredInscription {
                            inscription_id,
                            input_index,
                            input_sat_offset,
                            output_index,
                            output_sat_offset,
                        }).collect::<Vec<_>>();

                let sent = sent
                    .into_iter()
                    .map(|(inscription_id, (input_index, input_sat_offset), output_info)| {
                        let (output_index, output_sat_offset, output_script_hash) = if let Some((output_index, output_sat_offset, output_script_hash)) = output_info {
                            (Some(output_index), Some(output_sat_offset), Some(output_script_hash))
                        } else {
                            (None, None, None)
                        };

                        timbre_xbt::reducers::inscription_activity_by_script_hash::SentInscription {
                            inscription_id,
                            input_index,
                            input_sat_offset,
                            output_script_hash,
                            output_index,
                            output_sat_offset,
                        }
                    }).collect::<Vec<_>>();

                let received = received
                    .into_iter()
                    .map(|(
                        inscription_id,
                        input_data,
                        (output_index, output_sat_offset)
                    )| timbre_xbt::reducers::inscription_activity_by_script_hash::ReceivedInscription {
                        inscription_id,
                        input_index: input_data.map(|(input_index, _, _)| input_index),
                        input_sat_offset:
                            input_data.map(|(_, input_sat_offset, _)| input_sat_offset),
                        input_script_hash:
                            input_data.map(|(_, _, input_script_hash)| input_script_hash),
                        output_index,
                        output_sat_offset,
                    }).collect::<Vec<_>>();

                let value = timbre_xbt::reducers::inscription_activity_by_script_hash::Value {
                    self_transfers,
                    sent,
                    received,
                };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::InscriptionActivityByTx(inscription_activity_by_tx::Output {
                height,
                tx_index,
                tx_hash,
                inscriptions_activity,
            }) => {
                let key =
                    timbre_xbt::reducers::inscription_activity_by_tx::Key { height, tx_index };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::InscriptionActivityByTx, &key);

                let value = timbre_xbt::reducers::inscription_activity_by_tx::Value {
                    tx_hash,
                    inscriptions_activity,
                };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::InscriptionActivityByTxV2(inscription_activity_by_tx_v2::Output {
                height,
                tx_index,
                tx_hash,
                inscriptions_activity,
            }) => {
                let key =
                    timbre_xbt::reducers::inscription_activity_by_tx_v2::Key { height, tx_index };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::InscriptionActivityByTxV2, &key);

                let value = timbre_xbt::reducers::inscription_activity_by_tx_v2::Value {
                    tx_hash,
                    inscriptions_activity,
                };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::InscriptionUtxosByScriptHash(
                inscription_utxos_by_script_hash::Output {
                    script_hash,
                    height,
                    utxo_hash,
                    utxo_index,
                    action,
                },
            ) => {
                let key = timbre_xbt::reducers::inscription_utxos_by_script_hash::Key {
                    script_hash,
                    height,
                    utxo_hash,
                    utxo_index,
                };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::InscriptionUtxosByScriptHash, &key); // TODO: implicit reducer tag byte

                match action {
                    UtxoAction::Consumed => vec![StorageAction::Delete(encoded_key)],
                    UtxoAction::Produced((satoshis, inscriptions)) => {
                        let value = timbre_xbt::reducers::inscription_utxos_by_script_hash::Value {
                            satoshis,
                            inscriptions,
                        };

                        vec![StorageAction::SetOnce(encoded_key, value.encode())]
                    }
                }
            }
            ReducerOutput::MintsByRuneId(mints_by_rune_id::Output { rune_id }) => {
                let key = timbre_xbt::reducers::mints_by_rune_id::Key { rune_id };

                let encoded_key = self.key_encoder.data(&Reducer::MintsByRuneId, &key); // TODO: implicit reducer tag byte

                vec![StorageAction::Increment(encoded_key, 1)]
            }
            ReducerOutput::RuneUtxosByScriptHash(rune_utxos_by_script_hash::Output {
                script_hash,
                height,
                utxo_hash,
                utxo_index,
                action,
            }) => {
                let key = timbre_xbt::reducers::rune_utxos_by_script_hash::Key {
                    script_hash,
                    height,
                    utxo_hash,
                    utxo_index,
                };

                let encoded_key = self.key_encoder.data(&Reducer::RuneUtxosByScriptHash, &key);

                match action {
                    UtxoAction::Consumed => vec![StorageAction::Delete(encoded_key)],
                    UtxoAction::Produced((satoshis, runes)) => {
                        let value = timbre_xbt::reducers::rune_utxos_by_script_hash::Value {
                            satoshis,
                            runes,
                        };

                        vec![StorageAction::SetOnce(encoded_key, value.encode())]
                    }
                }
            }
            ReducerOutput::RuneTxsByScriptHash(rune_txs_by_script_hash::Output {
                script_hash,
                height,
                activity_tx_index,
                tx_hash,
                etched,
                minted,
                self_transfers,
                increased_balances,
                decreased_balances,
            }) => {
                let key = timbre_xbt::reducers::rune_txs_by_script_hash::Key {
                    script_hash,
                    height,
                    activity_tx_index,
                    tx_hash,
                };

                let encoded_key = self.key_encoder.data(&Reducer::RuneTxsByScriptHash, &key);

                let value = timbre_xbt::reducers::rune_txs_by_script_hash::Value {
                    etched,
                    minted,
                    self_transfers,
                    increased_balances,
                    decreased_balances,
                };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::RuneIdByRuneName(rune_id_by_rune_name::Output {
                rune_id,
                rune_name,
                is_bootstrap,
            }) => {
                let key = timbre_xbt::reducers::rune_id_by_rune_name::Key { rune_name };

                let encoded_key = self.key_encoder.data(&Reducer::RuneIdByRuneName, &key); // TODO: implicit reducer tag byte

                let value = timbre_xbt::reducers::rune_id_by_rune_name::Value { rune_id };

                // Use SetPermanent for bootstrap (genesis rune) - immutable data, idempotent writes
                // Only write genesis rune on mainnet
                if is_bootstrap && self.config.network == "bitcoin" {
                    vec![StorageAction::SetPermanent(encoded_key, value.encode())]
                } else if is_bootstrap {
                    // Skip bootstrap on non-mainnet networks
                    vec![]
                } else {
                    vec![StorageAction::SetOnce(encoded_key, value.encode())]
                }
            }
            ReducerOutput::SatBalanceByScriptHash(sat_balance_by_script_hash::Output {
                script_hash,
                delta,
            }) => {
                let key = timbre_xbt::reducers::sat_balance_by_script_hash::Key { script_hash };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::SatBalanceByScriptHash, &key);

                match delta {
                    IncrOrDecr::Increment(x) => vec![StorageAction::Increment(encoded_key, x)],
                    IncrOrDecr::Decrement(x) => vec![StorageAction::Decrement(encoded_key, x)],
                }
            }
            ReducerOutput::SatTxsByScriptHash(sat_txs_by_script_hash::Output {
                script_hash,
                height,
                activity_tx_index,
                tx_hash,
                amount,
                activity_type,
            }) => {
                let key = timbre_xbt::reducers::sat_txs_by_script_hash::Key {
                    script_hash,
                    height,
                    activity_tx_index,
                    tx_hash,
                };

                let encoded_key = self.key_encoder.data(&Reducer::SatTxsByScriptHash, &key);

                let value = timbre_xbt::reducers::sat_txs_by_script_hash::Value {
                    amount,
                    activity_type,
                };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::SatsPerVbByBlock(sats_per_vb_by_block::Output {
                height,
                min,
                median,
                max,
            }) => {
                let key = timbre_xbt::reducers::sats_per_vb_by_block::Key { height };

                let encoded_key = self.key_encoder.data(&Reducer::SatsPerVbByBlock, &key); // TODO: implicit reducer tag byte

                let value = timbre_xbt::reducers::sats_per_vb_by_block::Value { min, median, max };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::ScriptByScriptHash(script_by_script_hash::Output {
                script_hash,
                script,
            }) => {
                let key = timbre_xbt::reducers::script_by_script_hash::Key { script_hash };

                let encoded_key = self.key_encoder.data(&Reducer::ScriptByScriptHash, &key); // TODO: implicit reducer tag byte

                let value = timbre_xbt::reducers::script_by_script_hash::Value { script };

                vec![StorageAction::SetPermanent(encoded_key, value.encode())]
            }
            ReducerOutput::ScriptHashByAddressPayloadHash(
                script_hash_by_address_payload_hash::Output {
                    payload_hash,
                    script_hash,
                },
            ) => {
                let key =
                    timbre_xbt::reducers::script_hash_by_address_payload_hash::Key { payload_hash };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::ScriptHashByAddressPayloadHash, &key); // TODO: implicit reducer tag byte

                let value = timbre_xbt::reducers::script_hash_by_address_payload_hash::Value {
                    script_hash,
                };

                vec![StorageAction::SetPermanent(encoded_key, value.encode())]
            }
            ReducerOutput::SpendingTxByTxo(spending_tx_by_txo::Output {
                utxo_tx_hash,
                utxo_vout,
                tx_hash,
            }) => {
                let key = timbre_xbt::reducers::spending_tx_by_txo::Key {
                    utxo_tx_hash,
                    utxo_vout,
                };

                let encoded_key = self.key_encoder.data(&Reducer::SpendingTxByTxo, &key);

                let encoded_value =
                    timbre_xbt::reducers::spending_tx_by_txo::Value { tx_hash }.encode();

                vec![StorageAction::SetOnce(encoded_key, encoded_value)]
            }
            ReducerOutput::TotalInscriptionsByScriptHash(
                total_inscriptions_by_script_hash::Output { script_hash, delta },
            ) => {
                let key =
                    timbre_xbt::reducers::total_inscriptions_by_script_hash::Key { script_hash };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::TotalInscriptionsByScriptHash, &key);

                match delta {
                    IncrOrDecr::Increment(new_inscriptions) => {
                        vec![StorageAction::Increment(encoded_key, new_inscriptions)]
                    }
                    IncrOrDecr::Decrement(spent_inscriptions) => {
                        vec![StorageAction::Decrement(encoded_key, spent_inscriptions)]
                    }
                }
            }
            ReducerOutput::TotalOutputsByScriptHash(total_outputs_by_script_hash::Output {
                script_hash,
                new_outputs,
            }) => {
                let key = timbre_xbt::reducers::total_outputs_by_script_hash::Key { script_hash };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::TotalOutputsByScriptHash, &key);

                vec![StorageAction::Increment(encoded_key, new_outputs as u128)]
            }
            ReducerOutput::TotalSatInInputsByScriptHash(
                total_sat_in_inputs_by_script_hash::Output {
                    script_hash,
                    new_sats,
                },
            ) => {
                let key =
                    timbre_xbt::reducers::total_sat_in_inputs_by_script_hash::Key { script_hash };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::TotalSatInInputsByScriptHash, &key);

                vec![StorageAction::Increment(encoded_key, new_sats as u128)]
            }
            ReducerOutput::TotalSatInOutputsByScriptHash(
                total_sat_in_outputs_by_script_hash::Output {
                    script_hash,
                    new_sats,
                },
            ) => {
                let key =
                    timbre_xbt::reducers::total_sat_in_outputs_by_script_hash::Key { script_hash };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::TotalSatInOutputsByScriptHash, &key);

                vec![StorageAction::Increment(encoded_key, new_sats as u128)]
            }
            ReducerOutput::TotalTxsByScriptHash(total_txs_by_script_hash::Output {
                script_hash,
            }) => {
                let key = timbre_xbt::reducers::total_txs_by_script_hash::Key { script_hash };

                let encoded_key = self.key_encoder.data(&Reducer::TotalTxsByScriptHash, &key);

                vec![StorageAction::Increment(encoded_key, 1u128)]
            }
            ReducerOutput::TotalUtxosByScriptHash(total_utxos_by_script_hash::Output {
                script_hash,
                is_new,
            }) => {
                let key = timbre_xbt::reducers::total_utxos_by_script_hash::Key { script_hash };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::TotalUtxosByScriptHash, &key);

                if is_new {
                    vec![StorageAction::Increment(encoded_key, 1u128)]
                } else {
                    vec![StorageAction::Decrement(encoded_key, 1u128)]
                }
            }
            ReducerOutput::TransferInscriptionsByScriptHash(
                transfer_inscriptions_by_script_hash::Output {
                    script_hash,
                    ticker,
                    inscription_id,
                    action,
                },
            ) => {
                let key = timbre_xbt::reducers::transfer_inscriptions_by_script_hash::Key {
                    script_hash,
                    ticker: timbre_xbt::ShortByteString(ticker),
                    inscription_id,
                };

                let encoded_key = self
                    .key_encoder
                    .data(&Reducer::TransferInscriptionsByScriptHash, &key);

                match action {
                    UtxoAction::Consumed => vec![StorageAction::Delete(encoded_key)],
                    UtxoAction::Produced((
                        token_amount,
                        sat_amount,
                        (utxo_hash, utxo_index),
                        offset,
                        block_height,
                    )) => {
                        let value =
                            timbre_xbt::reducers::transfer_inscriptions_by_script_hash::Value {
                                token_amount,
                                sat_amount,
                                utxo_hash,
                                utxo_index,
                                offset,
                                block_height,
                            };

                        vec![StorageAction::SetOnce(encoded_key, value.encode())]
                    }
                }
            }
            ReducerOutput::TxFirstSeenTimestamp(tx_first_seen_timestamp::Output {
                tx_hash,
                timestamp,
            }) => {
                let key = timbre_xbt::reducers::tx_first_seen_timestamp::Key { tx_hash };

                let encoded_key = self.key_encoder.data(&Reducer::TxFirstSeenTimestamp, &key);

                let encoded_value =
                    timbre_xbt::reducers::tx_first_seen_timestamp::Value { timestamp }.encode();

                vec![StorageAction::InsertPermanent(encoded_key, encoded_value)]
            }
            ReducerOutput::TxInfo(tx_info::Output {
                tx_hash,
                block_height,
                block_hash,
                timestamp,
                volume,
                fees,
                sats_per_vb,
                involves_inscriptions,
                involves_runes,
                involves_brc20,
                ins,
                outs,
            }) => {
                let key = timbre_xbt::reducers::tx_info::Key { tx_hash };
                let encoded_key = self.key_encoder.data(&Reducer::TxInfo, &key);

                let value = timbre_xbt::reducers::tx_info::Value {
                    block_height,
                    block_hash,
                    timestamp,
                    volume,
                    fees,
                    sats_per_vb,
                    involves_inscriptions,
                    involves_runes,
                    involves_brc20,
                    inputs: ins
                        .into_iter()
                        .map(
                            |(utxo_hash, utxo_vout, script_hash, satoshis, inscriptions, runes)| {
                                timbre_xbt::reducers::tx_info::TxIn {
                                    utxo_hash,
                                    utxo_vout,
                                    script_hash,
                                    satoshis,
                                    inscriptions,
                                    runes,
                                }
                            },
                        )
                        .collect(),
                    outputs: outs
                        .into_iter()
                        .map(|(script_hash, satoshis, inscriptions, runes)| {
                            timbre_xbt::reducers::tx_info::TxOut {
                                script_hash,
                                satoshis,
                                inscriptions,
                                runes,
                            }
                        })
                        .collect(),
                };
                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::TxsByBlock(txs_by_block::Output { height, tx_hashes }) => {
                let key = timbre_xbt::reducers::txs_by_block::Key { height };
                let encoded_key = self.key_encoder.data(&Reducer::TxsByBlock, &key);
                let encoded_value =
                    timbre_xbt::reducers::txs_by_block::Value { tx_hashes }.encode();

                vec![StorageAction::SetOnce(encoded_key, encoded_value)]
            }
            ReducerOutput::TxsByInscription(txs_by_inscription::Output {
                bucket_id,
                height,
                activity,
            }) => {
                let key = timbre_xbt::reducers::txs_by_inscription::Key { bucket_id, height };
                let encoded_key = self.key_encoder.data(&Reducer::TxsByInscription, &key);

                let value = timbre_xbt::reducers::txs_by_inscription::Value { activity };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::TxsByRuneId(txs_by_rune_id::Output {
                rune_id,
                height,
                activity_tx_index,
                tx_hash,
                etched,
                minted,
                self_transfers,
                senders,
                receivers,
            }) => {
                let key = timbre_xbt::reducers::txs_by_rune_id::Key {
                    rune_id,
                    height,
                    activity_tx_index,
                    tx_hash,
                };

                let encoded_key = self.key_encoder.data(&Reducer::TxsByRuneId, &key);

                let value = timbre_xbt::reducers::txs_by_rune_id::Value {
                    etched,
                    minted,
                    self_transfers,
                    senders,
                    receivers,
                };

                vec![StorageAction::SetOnce(encoded_key, value.encode())]
            }
            ReducerOutput::TxsByScriptHash(txs_by_script_hash::Output {
                script_hash,
                height,
                tx_hash,
                address_tx_index,
                input,
                output,
            }) => {
                let key = timbre_xbt::reducers::txs_by_script_hash::Key {
                    script_hash,
                    height,
                    tx_hash,
                    address_tx_index,
                };

                let encoded_key = self.key_encoder.data(&Reducer::TxsByScriptHash, &key);

                let value =
                    timbre_xbt::reducers::txs_by_script_hash::Value { input, output }.encode();

                vec![StorageAction::SetOnce(encoded_key, value)]
            }
            ReducerOutput::UtxosByRuneId(utxos_by_rune_id::Output {
                rune_id,
                height,
                utxo_hash,
                utxo_index,
                action,
            }) => {
                let key = timbre_xbt::reducers::utxos_by_rune_id::Key {
                    rune_id,
                    height,
                    utxo_hash,
                    utxo_index,
                };

                let encoded_key = self.key_encoder.data(&Reducer::UtxosByRuneId, &key);

                match action {
                    UtxoAction::Consumed => vec![StorageAction::Delete(encoded_key)],
                    UtxoAction::Produced((script_hash, rune_quantity, satoshis)) => {
                        let value = timbre_xbt::reducers::utxos_by_rune_id::Value {
                            script_hash,
                            rune_quantity,
                            satoshis,
                        };

                        vec![StorageAction::SetOnce(encoded_key, value.encode())]
                    }
                }
            }
            ReducerOutput::UtxosByScriptHash(utxos_by_script_hash::Output {
                script_hash,
                height,
                utxo_hash,
                utxo_index,
                action,
            }) => {
                let key = timbre_xbt::reducers::utxos_by_script_hash::Key {
                    script_hash,
                    height,
                    utxo_hash,
                    utxo_index,
                };

                let encoded_key = self.key_encoder.data(&Reducer::UtxosByScriptHash, &key);

                match action {
                    UtxoAction::Consumed => vec![StorageAction::Delete(encoded_key)],
                    UtxoAction::Produced(satoshis) => {
                        let value = timbre_xbt::reducers::utxos_by_script_hash::Value { satoshis };

                        vec![StorageAction::SetOnce(encoded_key, value.encode())]
                    }
                }
            }
            ReducerOutput::Cursor(point, was_mempool, timestamp, mempool_info) => {
                let value = CursorValue {
                    height: point.height,
                    hash: point.hash.to_byte_array(),
                    was_mempool,
                    timestamp,
                    mempool_info,
                };

                vec![
                    StorageAction::Set(self.key_encoder.cursor(), value.encode()),
                    StorageAction::Set(self.key_encoder.info(), value.encode()),
                ]
            }
        }
    }

    fn spawn_garbage_collect_rollback_metadata(&self, evicted_block: PointWithResult) {
        let connection = self.connection.clone().unwrap();
        let tx_options = self.tx_options.clone();
        let key_encoder = self.key_encoder.clone();

        tokio::spawn(async move {
            let max_retries = 5;
            let mut retry_count = 0;
            let mut backoff_ms = 100;

            loop {
                let start = Instant::now();
                let mut total = 0;

                let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
                    let mut txn = connection.begin_with_options(tx_options.clone()).await?;
                    let point = &evicted_block.point;

                    // Delete rollback metadata keys for all actions in this block
                    for action in evicted_block.result.iter() {
                        let rb_key = key_encoder.rollback(MetadataKey {
                            height: point.height,
                            hash: point.hash.to_byte_array(),
                            modified_key: action.key().clone(),
                        });

                        txn.delete(Key::from(rb_key)).await?;
                        total += 1;
                    }

                    commit_txn_or_rollback(&mut txn).await?;

                    let elapsed = start.elapsed();

                    if elapsed >= Duration::from_millis(2000) {
                        warn!(
                            "gc'd {} actions for point {} in {:?}",
                            total, point.height, elapsed
                        )
                    } else {
                        debug!(
                            "gc'd {} actions for point {} in {:?}",
                            total, point.height, elapsed
                        )
                    }

                    Ok(())
                }
                .await;

                match result {
                    Ok(()) => break,
                    Err(e) => {
                        retry_count += 1;
                        if retry_count >= max_retries {
                            error!(
                                "failed to gc rollback metadata for point {} after {} retries: {:?}",
                                evicted_block.point.height, max_retries, e
                            );
                            break;
                        }

                        warn!(
                            "gc rollback metadata failed for point {} (attempt {}/{}), retrying in {}ms: {:?}",
                            evicted_block.point.height, retry_count, max_retries, backoff_ms, e
                        );

                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms *= 2; // exponential backoff
                    }
                }
            }
        });
    }

    /// Apply actions, by splitting them into batches and committing them in parallel
    async fn apply_actions_split_commit_immutable(
        &mut self,
        mut actions: Vec<StorageAction>,
    ) -> Result<
        (
            Option<Timestamp>,
            std::time::Duration,
            Vec<std::time::Duration>,
        ),
        gasket::error::Error,
    > {
        let key_warning = self.config.key_warnings.unwrap_or_default();
        let safe_mode = self.safe_mode.clone();

        // sort the actions to ensure the batches are deterministic
        actions.sort_by_key(|x| x.key().clone());

        let total_actions = actions.len() as u64;

        let mut action_batches = actions
            .chunks(
                self.config
                    .split_commit_batch_size
                    .unwrap_or(DEFAULT_SPLIT_COMMIT_BATCH_SIZE),
            )
            .enumerate()
            .collect::<Vec<_>>();

        let num_of_batches = action_batches.len();

        // -- check if we have already applied some batches for this point, skip those batches

        if let Some(partial_commit_info) = self.partial_split_commit.clone() {
            if total_actions != partial_commit_info.lock.total_actions {
                panic!("found split commit lock, but number of actions does not match")
            }

            info!(
                "found partial commit for point {:?}, skipping batches: {:?}",
                partial_commit_info.lock, partial_commit_info.batches_complete
            );

            // do not apply batches we have already applied
            action_batches.retain(|(idx, _)| {
                !partial_commit_info
                    .batches_complete
                    .contains(&(*idx as u32))
            });

            // lock should also specify non-mutable
            assert!(!partial_commit_info.lock.mutable);
        }

        // --  write split commit lock key

        let put_lock_start = tokio::time::Instant::now();

        let mut txn = self.begin_tikv_transaction().await.or_restart()?;

        let last_processed = self
            .last_processed
            .clone()
            .map(|x| (x.height, x.hash.to_byte_array()));

        let lock_val = SplitCommitLockValue {
            safe_point: last_processed,
            total_actions,
            mutable: false,
        };

        txn.put(self.key_encoder.lock(), lock_val.encode())
            .await
            .or_restart()?;

        commit_txn_or_rollback(&mut txn).await.or_restart()?;

        debug!("{:?} to store lock", put_lock_start.elapsed());

        // -- create parallel task for each batch of actions

        let parallel_commit_start = tokio::time::Instant::now();
        let mut batch_timings = Vec::new();

        // commit the action batches in parallel, but in groups of `split_commit_max_concurrent`
        // batches. for example, if we have 100 batches but max concurrent is 50, we will first
        // commit 50 in parallel, then the next 50 in parallel once the first has finished.
        let concurrent_batches = action_batches
            .chunks(
                self.config
                    .split_commit_max_concurrent
                    .unwrap_or(DEFAULT_SPLIT_COMMIT_MAX_CONCURRENT),
            )
            .map(|x| x.to_vec())
            .collect_vec();

        for concurrent_batch in concurrent_batches {
            let mut tasks: Vec<JoinHandle<Result<std::time::Duration, gasket::error::Error>>> =
                Vec::new();

            for (batch_id, batch) in concurrent_batch {
                let batch_start = tokio::time::Instant::now();

                let batch: Vec<_> = batch.to_vec();
                let mut txn = self.begin_tikv_transaction().await.or_restart()?;
                let batch_complete_key = self.key_encoder.batch_complete(batch_id as u32);

                let task = tokio::spawn(async move {
                    let mut kv_map =
                        batch_get_required_kvs_for_batch(&mut txn, &batch, false, safe_mode)
                            .await?;

                    for action in batch {
                        execute_storage_op_in_txn(&mut txn, action, &mut kv_map, key_warning)
                            .await
                            .or_restart()?;
                    }

                    // write a key which will signal we successfully committed this batch
                    txn.put(batch_complete_key, vec![]).await.or_restart()?;

                    commit_txn_or_rollback(&mut txn).await.or_restart()?;

                    let elapsed = batch_start.elapsed();
                    debug!("{batch_id} finished committing, {:?} elapsed", elapsed);

                    Ok(elapsed)
                });

                tasks.push(task);
            }

            // Wait for all transactions to complete
            let results = join_all(tasks).await;

            debug!(
                "all batches completed, {:?} elapsed",
                parallel_commit_start.elapsed()
            );

            // Check if any transaction failed, restart if so
            for result in results {
                if let Ok(x) = result {
                    batch_timings.push(x?);
                } else {
                    error!("join error in split commit: {result:?}");
                    return Err(gasket::error::Error::ShouldRestart);
                }
            }
        }

        // -- all batches committed successfully, delete lock key and batch complete keys

        let parallel_commit_cleanup = tokio::time::Instant::now();

        let mut txn = self.begin_tikv_transaction().await.or_restart()?;

        // delete lock key
        let mut mutations = vec![delete_mutation(self.key_encoder.lock())];

        // delete batch complete keys
        for batch_id in 0..num_of_batches {
            mutations.push(delete_mutation(
                self.key_encoder.batch_complete(batch_id as u32),
            ));
        }

        // submit lock key and batch complete key deletions
        txn.batch_mutate(mutations).await.or_restart()?;

        let final_ts = commit_txn_or_rollback(&mut txn).await.or_restart()?;

        // wipe split commit lock (if present)
        self.partial_split_commit = None;

        debug!(
            "parallel commit cleanup finished, {:?} elapsed. total: {:?}",
            parallel_commit_cleanup.elapsed(),
            put_lock_start.elapsed()
        );

        let total_duration = put_lock_start.elapsed();
        Ok((final_ts, total_duration, batch_timings))
    }

    /// Apply actions, by splitting them into batches and committing them in parallel
    ///
    /// If a batch succeeds, then the inverse actions will be in the persistent rollback buffer. So
    /// if we detect a lock key, we can use the inverse actions from the persistent rollback buffer
    /// to undo any batches which succeeded and return the data in TiKV to a correct state (block
    /// before block we failed to process).
    async fn apply_actions_split_commit_mutable(
        &mut self,
        mut actions: Vec<StorageAction>,
        point: &Point,
    ) -> Result<
        (
            Option<Timestamp>,
            Vec<StorageAction>,
            std::time::Duration,
            Vec<std::time::Duration>,
        ),
        gasket::error::Error,
    > {
        let key_warning = self.config.key_warnings.unwrap_or_default();
        let safe_mode = self.safe_mode.clone();

        actions.sort_by_key(|x| x.key().clone());

        let total_actions = actions.len() as u64;

        let action_batches = actions
            .chunks(
                self.config
                    .split_commit_batch_size
                    .unwrap_or(DEFAULT_SPLIT_COMMIT_BATCH_SIZE),
            )
            .map(|chunk| chunk.to_vec())
            .enumerate()
            .collect_vec();

        // --  write split commit lock key

        let put_lock_start = tokio::time::Instant::now();

        let mut txn = self.begin_tikv_transaction().await.or_restart()?;

        let last_processed = self
            .last_processed
            .clone()
            .map(|x| (x.height, x.hash.to_byte_array()));

        let lock_val = SplitCommitLockValue {
            safe_point: last_processed,
            total_actions,
            mutable: true,
        };

        txn.put(self.key_encoder.lock(), lock_val.encode())
            .await
            .or_restart()?;

        commit_txn_or_rollback(&mut txn).await.or_restart()?;

        debug!("{:?} to store lock", put_lock_start.elapsed());

        // -- create parallel task for each batch of actions

        let parallel_commit_start = tokio::time::Instant::now();

        let mut all_inverses = vec![];
        let mut batch_timings = Vec::new();

        // commit the action batches in parallel, but in groups of `split_commit_max_concurrent`
        // batches. for example, if we have 100 batches but max concurrent is 50, we will first
        // commit 50 in parallel, then the next 50 in parallel once the first has finished.
        let concurrent_batches = action_batches
            .chunks(
                self.config
                    .split_commit_max_concurrent
                    .unwrap_or(DEFAULT_SPLIT_COMMIT_MAX_CONCURRENT),
            )
            .map(|x| x.to_vec())
            .collect_vec();

        for concurrent_batch in concurrent_batches {
            // vector to store async tasks, returns inverseops
            let mut tasks: Vec<
                JoinHandle<Result<(Vec<StorageAction>, std::time::Duration), gasket::error::Error>>,
            > = Vec::new();

            for (batch_id, batch) in concurrent_batch {
                let batch_start = tokio::time::Instant::now();

                let key_encoder = self.key_encoder.clone();
                let point = point.clone();
                let batch: Vec<_> = batch.to_vec();

                let mut txn = self.begin_tikv_transaction().await.or_restart()?;

                let task = tokio::spawn(async move {
                    let mut kv_map =
                        batch_get_required_kvs_for_batch(&mut txn, &batch, true, safe_mode).await?;

                    let mut inverses: Vec<StorageAction> = Vec::new();

                    for action in batch {
                        debug!("executing storage op with inverse: {action:?}");

                        if let Some(inverse_op) = execute_storage_op_in_txn_with_inverse(
                            &mut txn,
                            action,
                            &mut kv_map,
                            key_warning,
                            safe_mode,
                        )
                        .await
                        .or_restart()?
                        {
                            debug!("storing inverse storage op: {inverse_op:?}");
                            inverses.extend(inverse_op)
                        }
                    }

                    apply_storage_rb_actions(&mut txn, &inverses, &point, &key_encoder).await?;

                    commit_txn_or_rollback(&mut txn).await.or_restart()?;

                    let elapsed = batch_start.elapsed();
                    debug!("{batch_id} finished committing, {:?} elapsed", elapsed);

                    Ok((inverses, elapsed))
                });

                tasks.push(task);
            }

            // Wait for all transactions to complete
            let results = join_all(tasks).await;

            debug!(
                "all batches completed, {:?} elapsed",
                parallel_commit_start.elapsed()
            );

            // Check if any transaction failed, restart if so
            for result in results {
                if let Ok(x) = result {
                    let (inverses, timing) = x?;
                    all_inverses.extend(inverses);
                    batch_timings.push(timing);
                } else {
                    error!("join error in split commit: {result:?}");
                    return Err(gasket::error::Error::ShouldRestart);
                }
            }
        }

        // -- all batches committed successfully, delete lock key

        let parallel_commit_cleanup = tokio::time::Instant::now();

        let mut txn = self.begin_tikv_transaction().await.or_restart()?;

        txn.delete(self.key_encoder.lock()).await.or_restart()?;

        let final_ts = commit_txn_or_rollback(&mut txn).await.or_restart()?;

        debug!(
            "parallel commit cleanup finished, {:?} elapsed. total {:?}",
            parallel_commit_cleanup.elapsed(),
            put_lock_start.elapsed()
        );

        let total_duration = put_lock_start.elapsed();
        Ok((final_ts, all_inverses, total_duration, batch_timings))
    }

    /// Apply mempool refresh diff actions, by splitting them into batches and committing them in
    /// parallel
    async fn apply_actions_split_commit_mempool(
        &mut self,
        mut actions: Vec<(StorageAction, bool)>,
        mempool_point: Point,
        chain_tip: Point,
    ) -> Result<(Option<Timestamp>, Vec<StorageAction>), gasket::error::Error> {
        let key_warning = self.config.key_warnings.unwrap_or_default();
        let safe_mode = self.safe_mode.clone();

        // sort the actions to ensure the batches are deterministic
        actions.sort_by_key(|(x, _)| x.key().clone());

        let total_actions = actions.len() as u64;

        let action_batches = actions
            .chunks(
                self.config
                    .split_commit_batch_size
                    .unwrap_or(DEFAULT_SPLIT_COMMIT_BATCH_SIZE),
            )
            .map(|chunk| chunk.to_vec())
            .enumerate()
            .collect::<Vec<_>>();

        // --  write split commit lock key

        let put_lock_start = tokio::time::Instant::now();

        // write split commit lock key
        let mut txn = self.begin_tikv_transaction().await.or_restart()?;

        let chain_tip = (chain_tip.height, chain_tip.hash.to_byte_array());

        let lock_val = SplitCommitLockValue {
            safe_point: Some(chain_tip),
            total_actions,
            mutable: true,
        };

        txn.put(self.key_encoder.lock(), lock_val.encode())
            .await
            .or_restart()?;

        commit_txn_or_rollback(&mut txn).await.or_restart()?;

        debug!("{:?} to store lock", put_lock_start.elapsed());

        // -- create parallel task for each batch of actions

        let parallel_commit_start = tokio::time::Instant::now();

        let mut all_inverses = vec![];

        // commit the action batches in parallel, but in groups of `split_commit_max_concurrent`
        // batches. for example, if we have 100 batches but max concurrent is 50, we will first
        // commit 50 in parallel, then the next 50 in parallel once the first has finished.
        let concurrent_batches = action_batches
            .chunks(
                self.config
                    .split_commit_max_concurrent
                    .unwrap_or(DEFAULT_SPLIT_COMMIT_MAX_CONCURRENT),
            )
            .map(|x| x.to_vec())
            .collect_vec();

        for concurrent_batch in concurrent_batches {
            // vector to store async tasks, returns inverseops
            let mut tasks: Vec<JoinHandle<Result<Vec<StorageAction>, gasket::error::Error>>> =
                Vec::new();

            for (batch_id, batch) in concurrent_batch {
                let batch_start = tokio::time::Instant::now();

                let key_encoder = self.key_encoder.clone();

                let batch: Vec<_> = batch.into_iter().collect();
                let batch_actions: Vec<_> = batch.iter().map(|(x, _)| x.clone()).collect();

                let mut txn = self.begin_tikv_transaction().await.or_restart()?;

                let task = tokio::spawn(async move {
                    let mut kv_map =
                        batch_get_required_kvs_for_batch(&mut txn, &batch_actions, true, safe_mode)
                            .await?;

                    let mut inverses: Vec<StorageAction> = Vec::new();

                    let batch: Vec<_> = batch.into_iter().collect();

                    for (action, with_inverse) in batch {
                        debug!("executing storage op with inverse: {action:?}");

                        if let Some(inverse_op) = execute_storage_op_in_txn_with_inverse(
                            &mut txn,
                            action.clone(),
                            &mut kv_map,
                            key_warning,
                            safe_mode,
                        )
                        .await
                        .or_restart()?
                        {
                            if with_inverse {
                                debug!("storing inverse storage op: {inverse_op:?}");
                                inverses.extend(inverse_op)
                            }
                        }
                    }

                    apply_storage_rb_actions(&mut txn, &inverses, &mempool_point, &key_encoder)
                        .await?;

                    commit_txn_or_rollback(&mut txn).await.or_restart()?;

                    debug!(
                        "{batch_id} finished committing, {:?} elapsed",
                        batch_start.elapsed()
                    );

                    Ok(inverses)
                });

                tasks.push(task);
            }

            // Wait for all transactions to complete
            let results = join_all(tasks).await;

            debug!(
                "all batches completed, {:?} elapsed",
                parallel_commit_start.elapsed()
            );

            // Check if any transaction failed, restart if so
            for result in results {
                if let Ok(x) = result {
                    all_inverses.extend(x?)
                } else {
                    error!("join error in split commit: {result:?}");
                    return Err(gasket::error::Error::ShouldRestart);
                }
            }
        }

        // -- all batches committed successfully, delete lock key and batch complete keys

        let parallel_commit_cleanup = tokio::time::Instant::now();

        let mut txn = self.begin_tikv_transaction().await.or_restart()?;

        txn.delete(self.key_encoder.lock()).await.or_restart()?;

        let final_ts = commit_txn_or_rollback(&mut txn).await.or_restart()?;

        debug!(
            "parallel commit cleanup finished, {:?} elapsed. total {:?}",
            parallel_commit_cleanup.elapsed(),
            put_lock_start.elapsed()
        );

        Ok((final_ts, all_inverses))
    }

    /// Apply inverse actions (undo a block) and remove corresponding actions
    /// from rollback buffer. Do so by splitting the actions into batches and
    /// processing them in parallel TiKV transactions.
    async fn apply_inverse_actions_split_commit(
        &mut self,
        reversing_blocks: Vec<(Point, StorageActions)>, // most recent first
        rb_point: &Point,
    ) -> Result<Option<Timestamp>, gasket::error::Error> {
        // --  write split commit lock key

        let put_lock_start = tokio::time::Instant::now();

        let mut txn = self.begin_tikv_transaction().await.or_restart()?;

        let safe_point = (rb_point.height, rb_point.hash.to_byte_array());

        let lock_val = SplitCommitLockValue {
            safe_point: Some(safe_point),
            total_actions: 0u64,
            mutable: true,
        };

        // number of actions only relevant when immutable (hence 0u64)
        txn.put(self.key_encoder.lock(), lock_val.encode())
            .await
            .or_restart()?;

        commit_txn_or_rollback(&mut txn).await.or_restart()?;

        debug!("{:?} to store lock", put_lock_start.elapsed());

        // -- for each block we need to undo, split the inverse actions into batches and apply in
        // parallel

        for (reverse_point, mut actions) in reversing_blocks {
            debug!("undoing {reverse_point} using split commit");

            actions.sort_by_key(|x| x.key().clone());

            let action_batches = actions
                .chunks(
                    self.config
                        .split_commit_batch_size
                        .unwrap_or(DEFAULT_SPLIT_COMMIT_BATCH_SIZE),
                )
                .enumerate()
                .collect_vec();

            // -- create parallel task for each batch of actions

            let parallel_commit_start = tokio::time::Instant::now();

            let concurrent_batches = action_batches
                .chunks(
                    self.config
                        .split_commit_max_concurrent
                        .unwrap_or(DEFAULT_SPLIT_COMMIT_MAX_CONCURRENT),
                )
                .map(|x| x.to_vec())
                .collect_vec();

            for concurrent_batch in concurrent_batches {
                let mut tasks: Vec<JoinHandle<Result<(), gasket::error::Error>>> = Vec::new();

                for (batch_id, batch) in concurrent_batch {
                    let batch_start = tokio::time::Instant::now();

                    let key_encoder = self.key_encoder.clone();
                    let batch: Vec<_> = batch.to_vec();

                    let mut txn = self.begin_tikv_transaction().await.or_restart()?;

                    let task = tokio::spawn(async move {
                        let mut mutations = Vec::with_capacity(batch.len());

                        for action in batch {
                            debug!("applying inverse action and deleting from rb buf {action:?}");

                            let action_key = action.key().clone();

                            match action {
                                StorageAction::Set(k, v) => mutations.push(set_mutation(k, v)),
                                StorageAction::Delete(k) => mutations.push(delete_mutation(k)),
                                _ => unreachable!("inverse actions are always set or delete"),
                            }

                            // delete the inverse action from the rollback buffer
                            let rb_buf_key = key_encoder.rollback(MetadataKey {
                                height: reverse_point.height,
                                hash: reverse_point.hash.to_byte_array(),
                                modified_key: action_key,
                            });

                            mutations.push(delete_mutation(rb_buf_key));
                        }

                        // submit lock key and batch complete key deletions
                        txn.batch_mutate(mutations).await.or_restart()?;

                        commit_txn_or_rollback(&mut txn).await.or_restart()?;

                        debug!(
                            "{batch_id} finished committing, {:?} elapsed",
                            batch_start.elapsed()
                        );

                        Ok(())
                    });

                    tasks.push(task);
                }

                // Wait for all transactions to complete
                let results = join_all(tasks).await;

                debug!(
                    "all batches completed, {:?} elapsed",
                    parallel_commit_start.elapsed()
                );

                // Check if any transaction failed, restart if so
                for result in results {
                    if let Ok(x) = result {
                        x?
                    } else {
                        error!("join error in split commit: {result:?}");
                        return Err(gasket::error::Error::ShouldRestart);
                    }
                }
            }
        }

        // -- all blocks reversed successfully, delete lock key

        let parallel_commit_cleanup = tokio::time::Instant::now();

        let mut txn = self.begin_tikv_transaction().await.or_restart()?;

        txn.delete(self.key_encoder.lock()).await.or_restart()?;

        // at this ts, all the blocks have been undone
        let final_ts = commit_txn_or_rollback(&mut txn).await.or_restart()?;

        debug!(
            "parallel commit cleanup finished, {:?} elapsed. total {:?}",
            parallel_commit_cleanup.elapsed(),
            put_lock_start.elapsed()
        );

        Ok(final_ts)
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::runtime::Worker for Worker {
    type WorkUnit = Vec<StorageActionPayload>;

    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("storage_ops", &self.ops_count)
            .build()
    }

    async fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        self.connection = TransactionClient::new_with_config(
            vec![self.config.connection_params.clone()],
            tikv_client::Config::default(),
        )
        .await
        .or_restart()?
        .into();

        self.redis_connection = ClusterClient::new(vec![self.config.redis_address.clone()])
            .or_restart()?
            .into();

        /*
            check if we have partially processed a block - we restarted during processing a block
            and may have applied some of the batches to the database but not all.

            if we find a lock and we were mutable, we can use the rollback buffer to undo any
            actions for blocks which may have been partially applied. if we were not mutable then
            recovery will be handled by `apply_actions_split_commit_immutable`, which will not apply
            batches which were successfuly committed.
        */

        self.check_partial_split_commit().await?;

        if let Some(pcinfo) = self.partial_split_commit.clone() {
            // if we were mutable, we will use the rollback buffer to rollback to a safepoint
            if pcinfo.lock.mutable == true {
                info!(
                    "detected partial split commit while mutable, undoing any partial changes using rollback buffer: {pcinfo:?}"
                );

                // we need to fetch the current persistent rollback buffer
                if let Some(persistent_rb) =
                    self.cursor.fetch_persistent_buffer().await.or_restart()?
                {
                    let mut refreshed_rollback_buffer =
                        RollbackBuffer::new(self.rollback_buffer.capacity(), self.safe_mode);

                    for entry in persistent_rb {
                        let _ = refreshed_rollback_buffer
                            .add_block(entry.point.into(), entry.inverse_actions);
                    }

                    let rb_point = pcinfo.lock.safe_point.unwrap();

                    let rb_point = Point {
                        height: rb_point.0,
                        hash: BlockHash::from_byte_array(rb_point.1),
                    };

                    // fetch the required points and inverse storage actions from
                    // the memory rollback buffer
                    let points_and_results: Vec<_> = refreshed_rollback_buffer
                        .points_since(&rb_point)
                        .expect("could not handle rollback")
                        .into_iter()
                        .map(|x| (x.point, x.result))
                        .collect();

                    info!(
                        "found {} potentially partially processed blocks in rollback buffer: {:?}",
                        points_and_results.len(),
                        points_and_results.iter().map(|x| x.0).join(", ")
                    );

                    self.apply_inverse_actions_split_commit(points_and_results, &rb_point)
                        .await?;

                    info!("rolledback to safepoint and cleared split commit lock");

                    refreshed_rollback_buffer
                        .rollback_to_point(&rb_point)
                        .or_panic()?;

                    self.rollback_buffer = refreshed_rollback_buffer;

                    self.partial_split_commit = None;

                    self.last_processed = Some(rb_point);
                } else {
                    panic!("mutable split commit lock but no persistent buffer")
                }
            }
        }

        // -- try clean up any lingering locks

        let current_ts = self
            .connection
            .as_mut()
            .unwrap()
            .current_timestamp()
            .await
            .or_restart()?;

        if self.config.tikv_cleanup_locks.unwrap_or(true) {
            // clean up any lingering locks on any key for this instance
            self.cleanup_tikv_locks(&current_ts, false)
                .await
                .or_restart()?;
        }

        /*
            There can be a situation where polyphony ends after committing the database transaction
            processing a block(s) BUT BEFORE writing the corresponding entry to the Redis. Here we
            try to detect and resolve that by creating the Redis entry now with the current ts.
        */

        let mut snapshot = self.connection.as_mut().unwrap().snapshot(
            current_ts.clone(),
            TransactionOptions::new_optimistic().drop_check(tikv_client::CheckLevel::Warn),
        );

        let raw_cursor = snapshot.get(self.key_encoder.cursor()).await.or_restart()?;

        if let Some(cursor_bytes) = raw_cursor {
            let ((height, hash), _) = <_>::decode(&cursor_bytes).unwrap();

            let key = format!(
                "tikv-timestamps:{}:{}",
                self.config.dataplane_id, self.config.instance_id
            );

            let redis_entries: Vec<RedisEntry> = self
                .redis_connection
                .as_mut()
                .unwrap()
                .get_connection()
                .or_restart()?
                .zrangebyscore(key, "-inf", "+inf")
                .or_restart()?;

            let have_redis_entries = !redis_entries.is_empty();

            let redis_entries_contains_cursor = redis_entries
                .iter()
                .position(|r| r.height == height && r.block_hash == hash)
                .is_some();

            // if we have redis entries, but we can't find a redis entry for the current cursor in
            // tikv, and we have not partially applied the point, then insert a new entry with the
            // current timestamp
            let should_insert = have_redis_entries
                && !redis_entries_contains_cursor
                && self.partial_split_commit.is_none();

            if should_insert {
                warn!(
                    "no redis entry found for current tikv cursor: {:?}",
                    (height, hash)
                );

                info!("inserting a new redis entry for the tikv cursor with ts {current_ts:?}");

                let (cursor_val, _) = CursorValue::decode(&cursor_bytes).unwrap();

                let point = Point {
                    height,
                    hash: BlockHash::from_byte_array(hash),
                };

                self.insert_timestamp_entry(&point, current_ts.clone(), cursor_val.mempool_info)
                    .await?;
            }
        }

        Ok(())
    }

    async fn schedule(&mut self) -> ScheduleResult<Self::WorkUnit> {
        // if we haven't committed the last work unit then reschedule that, else
        // pull the next message from the upstream stage
        if self.work_unit_2pc.is_empty() {
            debug!("scheduling");

            // -- pull all actions from queue

            let actions = self
                .input
                .recv_many(100)
                .await?
                .into_iter()
                .map(|x| x.payload)
                .collect::<Vec<_>>();

            if actions.is_empty() {
                return Ok(WorkSchedule::Idle);
            }

            // -- try compact actions to avoid unnecessarily processing blocks which are immediately
            // rolled back, or stale mempool refreshes (only when mutable)

            let actions = if self.mutable {
                let pre_compact_len = actions.len();

                let compacted_actions = compact_actions(actions);

                let skipped = pre_compact_len - compacted_actions.len();

                if skipped > 0 {
                    info!("compacted {skipped} chain actions");
                }

                compacted_actions
            } else {
                // dont try compact actions when not mutable, to avoid weirdness at mutable
                // switchover
                actions
            };

            if actions.is_empty() {
                Ok(WorkSchedule::Idle)
            } else {
                Ok(WorkSchedule::Unit(actions))
            }
        } else {
            info!("found uncommitted 2pc work unit, re-scheduling...");
            Ok(WorkSchedule::Unit(self.work_unit_2pc.clone()))
        }
    }

    // TODO check state changes are sound with possible restarts
    async fn execute(&mut self, unit: &Self::WorkUnit) -> Result<(), gasket::error::Error> {
        self.work_unit_2pc = unit.clone();

        // take the first action from our list of actions to process
        for chain_action in unit {
            match chain_action {
                StorageActionPayload::RollForward(point, outputs, mutable) => {
                    // integrity check
                    if let Some(last) = self.last_processed {
                        assert_eq!(
                            last.height + 1,
                            point.height,
                            "block height increment mismatch"
                        )
                    }

                    self.mutable |= mutable;

                    // wipe mempool cache if this is not a mempool refresh
                    self.mempool_refresh_cache = None;

                    // convert the reducer outputs into a list of storage actions
                    // to be executed against the db
                    let reducer_storage_ops: StorageActions = outputs
                        .clone()
                        .into_iter()
                        .map(|x| self.reducer_output_to_storage_ops(x))
                        .flatten()
                        .collect();

                    let original_actions_len = reducer_storage_ops.len();

                    // merge actions so that we have one action per key
                    let mut merger = ActionMerger::new();
                    merger.push_actions(reducer_storage_ops);
                    let actions = merger.into_merged_actions();

                    let merged_actions_len = actions.len();

                    if !self.mutable {
                        let (commit_ts, total_duration, batch_timings) =
                            self.apply_actions_split_commit_immutable(actions).await?;

                        // 2pc unlock: remove the processed action from front of actions list
                        self.work_unit_2pc.drain(..1);

                        // update last processed point
                        self.last_processed = Some(point.clone());

                        // Log every 1000 blocks in immutable mode
                        if point.height % 1000 == 0 {
                            let min_batch = batch_timings
                                .iter()
                                .min()
                                .map(|d| d.as_millis())
                                .unwrap_or(0);
                            let max_batch = batch_timings
                                .iter()
                                .max()
                                .map(|d| d.as_millis())
                                .unwrap_or(0);
                            let avg_batch = if !batch_timings.is_empty() {
                                batch_timings.iter().map(|d| d.as_millis()).sum::<u128>()
                                    / batch_timings.len() as u128
                            } else {
                                0
                            };

                            let commit_ts_str = commit_ts
                                .as_ref()
                                .map(|ts| format!("{:?}", ts))
                                .unwrap_or_else(|| "None".to_string());
                            info!(
                                "processed block {:?} ({} actions, {} post merge) [immutable] - total: {}ms, batches: {} (min: {}ms, max: {}ms, avg: {}ms), commit_ts: {}",
                                point,
                                original_actions_len,
                                merged_actions_len,
                                total_duration.as_millis(),
                                batch_timings.len(),
                                min_batch,
                                max_batch,
                                avg_batch,
                                commit_ts_str
                            );
                        }
                    } else {
                        let first_mutable = self.rollback_buffer.is_empty();

                        // initialise persistent rollback buffer
                        if first_mutable {
                            if let Some(prev_point) = self.last_processed {
                                let mut txn = self.begin_tikv_transaction().await.or_restart()?;

                                let dummy_action = StorageAction::Delete(vec![]);

                                apply_storage_rb_actions(
                                    &mut txn,
                                    &vec![dummy_action.clone()],
                                    &prev_point,
                                    &self.key_encoder,
                                )
                                .await?;

                                commit_txn_or_rollback(&mut txn).await.or_restart()?;

                                let _ = self
                                    .rollback_buffer
                                    .add_block(prev_point, vec![dummy_action]);
                            }
                        }

                        let (commit_ts, inverse_ops, total_duration, batch_timings) = self
                            .apply_actions_split_commit_mutable(actions, &point)
                            .await
                            .or_restart()?;

                        // now we have successfully committed the block we can remove the 2pc unit
                        // 2pc unlock: remove the processed action from front of actions list
                        self.work_unit_2pc.drain(..1);

                        self.last_processed = Some(point.clone());

                        // push the point and the inverse storage actions onto the
                        // front of the in-memory rollback buffer
                        let evicted_block =
                            self.rollback_buffer.add_block(point.clone(), inverse_ops);

                        // store the commit timestamp in redis
                        let commit_ts_for_log = commit_ts.as_ref().map(|ts| format!("{:?}", ts));
                        if let Some(ts) = commit_ts {
                            if self.config.tikv_cleanup_locks.unwrap_or(true) {
                                // clean up any lingering locks on any key for this instance
                                self.cleanup_tikv_locks(&ts, true).await.or_restart()?;
                            }

                            self.insert_timestamp_entry(&point, ts, None)
                                .await
                                .or_restart()?;
                        } else {
                            warn!("no commit ts")
                        }

                        // garbage collect rollback metadata for the evicted block (if any)
                        if let Some(block) = evicted_block {
                            self.spawn_garbage_collect_rollback_metadata(block);
                        }

                        // Log every block in mutable mode
                        let min_batch = batch_timings
                            .iter()
                            .min()
                            .map(|d| d.as_millis())
                            .unwrap_or(0);
                        let max_batch = batch_timings
                            .iter()
                            .max()
                            .map(|d| d.as_millis())
                            .unwrap_or(0);
                        let avg_batch = if !batch_timings.is_empty() {
                            batch_timings.iter().map(|d| d.as_millis()).sum::<u128>()
                                / batch_timings.len() as u128
                        } else {
                            0
                        };

                        info!(
                            "processed block {:?} ({} actions, {} post merge) [mutable] - total: {}ms, batches: {} (min: {}ms, max: {}ms, avg: {}ms), commit_ts: {}",
                            point,
                            original_actions_len,
                            merged_actions_len,
                            total_duration.as_millis(),
                            batch_timings.len(),
                            min_batch,
                            max_batch,
                            avg_batch,
                            commit_ts_for_log.unwrap_or_else(|| "None".to_string())
                        );
                    }
                }
                StorageActionPayload::RollBack(point, mutable) => {
                    info!("processing roll backwards msg for point {point:?}");

                    self.mutable |= mutable;

                    // wipe mempool cache if this is not a mempool refresh
                    self.mempool_refresh_cache = None;

                    // fetch the required points and inverse storage actions from
                    // the memory rollback buffer
                    let points_and_results: Vec<_> = self
                        .rollback_buffer
                        .points_since(&point)
                        .expect("could not handle rollback")
                        .into_iter()
                        .map(|x| (x.point, x.result))
                        .collect();

                    info!(
                        "found {} points in rb buf after rb point",
                        points_and_results.len()
                    );

                    // reverse each block, starting with most recent
                    let rollback_final_ts = self
                        .apply_inverse_actions_split_commit(points_and_results, &point)
                        .await
                        .or_restart()?;

                    // store the commit timestamp in redis
                    if let Some(ts) = rollback_final_ts {
                        if self.config.tikv_cleanup_locks.unwrap_or(true) {
                            // clean up any lingering locks on any key for this instance
                            self.cleanup_tikv_locks(&ts, true).await.or_restart()?;
                        }

                        self.insert_timestamp_entry(&point, ts, None).await?;
                    }

                    // 2pc unlock: remove the processed action from front of actions list
                    self.work_unit_2pc.drain(..1);

                    self.last_processed = Some(point.clone());

                    // Now we have successfully sent a transaction to storage using data from the memory
                    // rollback buffer, we can trim those points from the buffer.
                    self.rollback_buffer
                        .rollback_to_point(&point)
                        .map_err(crate::Error::rollback)
                        .apply_policy(&self.policy)
                        .or_panic()?;
                }
                StorageActionPayload::MempoolRefresh(
                    mempool_info,
                    blocks,
                    fetch_duration_ms,
                    reduce_duration_ms,
                ) => {
                    let start_time = Instant::now();

                    let chain_tip = Point {
                        height: mempool_info.chain_tip.0,
                        hash: BlockHash::from_byte_array(mempool_info.chain_tip.1),
                    };

                    // psuedo-point we will use in rollback buffer for changes made when processing
                    // mempool blocks. one point in the buffer represents any and all mempool blocks
                    let mempool_rollback_buffer_point = Point {
                        height: mempool_info.chain_tip.0 + 1,
                        hash: BlockHash::from_byte_array([0; 32]),
                    };

                    // this contains the storage actions applied to chain tip data to get to the tip
                    // of mempool blocks data.
                    let mut total_mempool_action_merger = ActionMerger::new();

                    let mut block_points = Vec::with_capacity(blocks.len());

                    let mut pre_merged_actions = 0;

                    for (point, outputs) in blocks {
                        // convert the reducer outputs into a list of storage actions
                        // to be executed against the db
                        let reducer_storage_ops: StorageActions = outputs
                            .clone()
                            .into_iter()
                            .map(|x| self.reducer_output_to_storage_ops(x))
                            .flatten()
                            .collect();

                        pre_merged_actions += reducer_storage_ops.len();

                        total_mempool_action_merger.push_actions(reducer_storage_ops);

                        block_points.push(point);
                    }

                    let current_refresh_merged_actions =
                        total_mempool_action_merger.into_batch_actions_with_keys();

                    let merged_actions_count = current_refresh_merged_actions.len();

                    // if we have a cache of the actions taken to process the previous mempool
                    // refresh, we can compare these to the actions we need to take to process the
                    // current mempool refresh (if the chain tip matches). if any of these actions
                    // are the same, then we do not need to perform actions for the corresponding
                    // keys because the data in TiKV is already correct.

                    let mut diff_cache = HashMap::new();

                    if let Some((cache_tip, cache_actions)) = self.mempool_refresh_cache.take() {
                        // if previous refresh cache exists and chain tip matches...
                        if cache_tip == chain_tip {
                            // for each action for the current refresh...
                            for (key, action) in current_refresh_merged_actions.iter() {
                                // check if the previous refresh also had an action for that key...
                                if let Some(prev_action) = cache_actions.get(key) {
                                    // and if so insert the 'diff' of these actions into the cache
                                    diff_cache.insert(
                                        key.clone(),
                                        prev_action.clone().diff(action.clone()),
                                    );
                                }
                            }
                        }
                    }

                    // -- first we will undo the previous mempool refresh blocks (if any)

                    // fetch the required points and inverse storage actions from the memory
                    // rollback buffer
                    let points_and_results = self
                        .rollback_buffer
                        .points_since(&chain_tip)
                        .expect("could not handle rollback");

                    let rb_points_count = points_and_results.len();

                    // create one set of actions to rollback any and all mempool blocks by merging
                    // the inverse actions from each block, starting with most recent

                    let mut inverse_merger = ActionMerger::new();

                    for p in points_and_results {
                        inverse_merger.push_actions(p.result);
                    }

                    let diff_cache_keys: HashSet<_> = diff_cache.keys().into_iter().collect();

                    let mut inverses_to_apply = inverse_merger.into_merged_actions();

                    let inverses_pre_dr = inverses_to_apply.len();

                    // we don't need to undo keys which we are going to update in current refresh
                    inverses_to_apply.retain(|x| !diff_cache_keys.contains(x.key()));

                    let inverses_post_dr = inverses_to_apply.len();

                    if !inverses_to_apply.is_empty() {
                        // apply the inverse actions (and delete them from persistent rollback buffer)
                        // in parallel batches
                        self.apply_inverse_actions_split_commit(
                            vec![(mempool_rollback_buffer_point, inverses_to_apply.clone())],
                            &chain_tip,
                        )
                        .await
                        .or_restart()?;

                        // remove inverse actions from in memory rb buf
                        self.rollback_buffer.remove_actions_for_point(
                            &mempool_rollback_buffer_point,
                            inverses_to_apply,
                        );
                    }

                    // --- now we will process the mempool blocks for the current mempool refresh

                    // these actions will be 'diffs' - they are changing keys which were changed by
                    // the previous refresh. importantly, that means we do not need to and should
                    // not write the inverse operation to the rollback buffer, as the rollback
                    // buffer must already contain the correct inverse operation.
                    let mut apply_diffs = vec![];

                    // these actions are for keys which were not changed by the previous refresh, or
                    // no cache was available. in either case the rollback buffer will not contain
                    // any entry for these keys, so we will write the inverse action to the
                    // rollback buffer for the corresponding keys.
                    let mut apply_actions_and_inverse = vec![];

                    let mut diff_cache_hits = 0;
                    let mut exact_cache_hits = 0;
                    let mut cache_misses = 0;

                    for (key, action) in current_refresh_merged_actions.clone() {
                        if let Some(maybe_diff) = diff_cache.remove(&key) {
                            if let Some(diff_action) = maybe_diff {
                                apply_diffs.push(diff_action);

                                diff_cache_hits += 1;
                            } else {
                                // this key was processed in the last refresh with the exact same
                                // storage action, so we can skip touching this key as the value in
                                // TiKV is already correct

                                exact_cache_hits += 1;
                            }
                        } else {
                            apply_actions_and_inverse.push(action);

                            cache_misses += 1;
                        }
                    }

                    let forward_actions = apply_diffs
                        .into_iter()
                        .map(|x| (x, false)) // dont write inverses for `apply_diffs` actions
                        .chain(apply_actions_and_inverse.into_iter().map(|x| (x, true)))
                        .collect::<Vec<_>>();

                    let (final_ts, inverse_actions) = self
                        .apply_actions_split_commit_mempool(
                            forward_actions,
                            mempool_rollback_buffer_point,
                            chain_tip,
                        )
                        .await
                        .or_restart()?;

                    // after every refresh, we will note the actions we applied to the tip to get
                    // to the current data in tikv (the tip of the mempool refresh)
                    self.mempool_refresh_cache = Some((chain_tip, current_refresh_merged_actions));

                    // now we have successfully processed the mempool refresh we can remove the 2pc
                    // unit
                    // 2pc unlock: remove the processed action from front of actions list
                    self.work_unit_2pc.drain(..1);

                    // insert inverse actions into in-memory rb buf using psuedo-point
                    if self
                        .rollback_buffer
                        .points_since(&mempool_rollback_buffer_point)
                        .is_ok()
                    {
                        self.rollback_buffer.insert_actions_for_point(
                            &mempool_rollback_buffer_point,
                            inverse_actions,
                        );
                    } else {
                        let evicted_block = self
                            .rollback_buffer
                            .add_block(mempool_rollback_buffer_point.clone(), inverse_actions);

                        if let Some(evicted) = evicted_block {
                            self.spawn_garbage_collect_rollback_metadata(evicted);
                        }
                    }

                    // write a timestamp for the final ts entry
                    if let Some(ts) = final_ts {
                        // TODO: move cleanup locks into parallel batches?
                        self.cleanup_tikv_locks(&ts, true).await.or_restart()?;

                        let mempool_tip = block_points.last().unwrap();

                        self.insert_timestamp_entry(
                            mempool_tip,
                            ts,
                            Some((mempool_info.chain_tip, mempool_info.mempool_view_ts)),
                        )
                        .await?;
                    }

                    let tip_hash = BlockHash::from_byte_array(mempool_info.chain_tip.1);

                    let storage_duration_ms = start_time.elapsed().as_millis();
                    let total_duration_ms =
                        fetch_duration_ms + reduce_duration_ms + storage_duration_ms;

                    if total_duration_ms > 1500 {
                        warn!(
                            "processed mempool refresh in {}ms (fetch: {}ms, reduce: {}ms, storage: {}ms): tip {}:{}, mpv {}, {} blocks, {} actions (pre-merge: {}), rb points: {}, inverse: {}/{}, cache hits: {} exact/{} diff/{} miss",
                            total_duration_ms,
                            fetch_duration_ms,
                            reduce_duration_ms,
                            storage_duration_ms,
                            mempool_info.chain_tip.0,
                            tip_hash,
                            mempool_info.mempool_view_ts,
                            block_points.len(),
                            merged_actions_count,
                            pre_merged_actions,
                            rb_points_count,
                            inverses_post_dr,
                            inverses_pre_dr,
                            exact_cache_hits,
                            diff_cache_hits,
                            cache_misses
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), gasket::error::Error> {
        Ok(())
    }
}

async fn batch_get_required_kvs_for_batch(
    txn: &mut Transaction,
    actions: &StorageActions,
    need_inverses: bool,
    safe_mode: bool,
) -> Result<HashMap<Vec<u8>, Vec<u8>>, gasket::error::Error> {
    let batch_get_start = Instant::now();

    let mut required_keys: Vec<Vec<u8>> = if need_inverses {
        // if we need inverses then we require all keys other than SetOnce and SetPermanent
        actions
            .iter()
            .filter(|x| {
                !matches!(
                    x,
                    StorageAction::SetOnce(_, _) | StorageAction::SetPermanent(_, _)
                ) || safe_mode
            })
            .map(|a| a.key().clone())
            .collect()
    } else {
        // if not, we only need the keys for incr/decrs/inserts
        actions
            .iter()
            .filter(|a| a.requires_previous_value())
            .map(|a| a.key().clone())
            .collect()
    };

    // split the required keys into smaller batches to avoid large response size
    // which can cause an error

    let mut acc = HashMap::new();

    required_keys.sort();

    for key_batch in required_keys.chunks(10000) {
        let kvs: HashMap<Vec<u8>, Vec<u8>> = txn
            .batch_get(key_batch.to_vec())
            .await
            .or_restart()?
            .map(|KvPair(k, v)| (k.into(), v))
            .collect();

        acc.extend(kvs)
    }

    debug!(
        "batch fetched required {}/{}/{} kvs in {}ms",
        acc.len(),
        required_keys.len(),
        actions.len(),
        batch_get_start.elapsed().as_millis()
    );

    Ok(acc)
}

/// Given a transaction and a StorageAction, perform the required operations
/// against to database to execute the strorage action.
async fn execute_storage_op_in_txn(
    txn: &mut Transaction,
    op: StorageAction,
    kv_map: &mut HashMap<Vec<u8>, Vec<u8>>,
    key_warnings: bool,
) -> Result<(), gasket::error::Error> {
    match op {
        Set(k, v) | SetOnce(k, v) | SetPermanent(k, v) => txn.put(k, v).await.or_restart(),
        Delete(k) => txn.delete(k).await.or_restart(),
        Insert(k, v) | InsertPermanent(k, v) => match kv_map.remove(&k) {
            Some(_) => Ok(()),
            None => txn.put(k.clone(), v).await.or_restart(),
        },
        Increment(k, d) => match kv_map.remove(&k) {
            Some(prev_value) => {
                let (prev_amount, _) = u128::decode(&prev_value).or_panic()?;

                let new_value = prev_amount + d;

                txn.put(k, new_value.encode()).await.or_restart()
            }
            None => txn.put(k, d.encode()).await.or_restart(),
        },
        Decrement(k, d) => match kv_map.remove(&k) {
            Some(prev_value) => {
                let (prev_amount, _) = u128::decode(&prev_value).or_panic()?;

                match prev_amount.checked_sub(d) {
                    Some(0) => txn.delete(k).await.or_restart(),
                    Some(v) => txn.put(k, v.encode()).await.or_restart(),
                    None => {
                        if key_warnings {
                            warn!(
                                "Trying to decrement integer KV by more than it's current value: [{}] {prev_amount} - {d}, deleting key",
                                hex::encode(k.clone())
                            );
                        }

                        txn.delete(k).await.or_restart()
                    }
                }
            }
            None => {
                if key_warnings {
                    warn!(
                        "Trying to decrement integer KV which does not exist: [{}] - {d}, doing nothing",
                        hex::encode(k)
                    )
                }

                Ok(())
            }
        },
        DecrementNoDelete(k, d) => match kv_map.remove(&k) {
            Some(prev_value) => {
                let (prev_amount, _) = u128::decode(&prev_value).or_panic()?;

                match prev_amount.checked_sub(d) {
                    Some(v) => txn.put(k, v.encode()).await.or_restart(),
                    None => {
                        if key_warnings {
                            warn!(
                                "Trying to decrement integer KV by more than it's current value: [{}] {prev_amount} - {d}, deleting key",
                                hex::encode(k.clone())
                            );
                        }

                        txn.put(k, 0u128.encode()).await.or_restart()
                    }
                }
            }
            None => {
                if key_warnings {
                    warn!(
                        "Trying to decrement integer KV which does not exist: [{}] - {d}, doing nothing",
                        hex::encode(k)
                    )
                }

                Ok(())
            }
        },
        PointAggregate(k, points) => {
            // fetch lastest value or default to 0, and note the inverse action for the latest value
            // kv. we will update this value after each point we write
            let mut prev_value = match kv_map.remove(&k) {
                Some(prev) => u128::decode(&prev).or_panic()?.0,
                None => 0,
            };

            for (point, delta) in points.into_iter().sorted_by_key(|(x, _)| *x) {
                let point_key = [k.clone(), point.encode()].concat();

                let point_value = match delta {
                    IncrOrDecr::Increment(d) => prev_value.checked_add(d).unwrap(),
                    IncrOrDecr::Decrement(d) => match prev_value.checked_sub(d) {
                        Some(res) => res,
                        None => {
                            if key_warnings {
                                warn!(
                                    "Trying to decrement point aggregate KV by more than it's current value: [{}] {prev_value} - {d}, setting key to 0",
                                    hex::encode(k.clone())
                                );
                            }

                            0u128
                        }
                    },
                };

                // write the point aggregate
                txn.put(point_key.clone(), point_value.encode())
                    .await
                    .or_restart()?;

                prev_value = point_value;
            }

            // update latest/main key to new total
            txn.put(k, prev_value.encode()).await.or_restart()?;

            Ok(())
        }
    }
}

/// Same as above but does extra work in order to return the StorageAction
/// which will revert the effects of the action being applied.
async fn execute_storage_op_in_txn_with_inverse(
    txn: &mut Transaction,
    op: StorageAction,
    kv_map: &mut HashMap<Vec<u8>, Vec<u8>>,
    key_warnings: bool,
    safe_mode: bool,
) -> Result<Option<Vec<StorageAction>>, gasket::error::Error> {
    match op {
        Set(k, v) => {
            // TODO key clones
            match kv_map.remove(&k) {
                Some(prev_value) => {
                    if *prev_value == v {
                        // exact key value pair already exists, do nothing
                        Ok(None)
                    } else {
                        txn.put(k.clone(), v).await.or_restart()?;

                        Ok(Some(vec![StorageAction::Set(k, prev_value)]))
                    }
                }
                None => {
                    txn.put(k.clone(), v).await.or_restart()?;

                    Ok(Some(vec![StorageAction::Delete(k)]))
                }
            }
        }
        SetOnce(k, v) => {
            if safe_mode {
                // We should know, by reducer logic, this key cannot already exist. With
                // safe-mode enabled we will enforce this invariant so that we can detect
                // indexing issues.
                if let Some(found_v) = kv_map.remove(&k) {
                    panic!("found existing value when applying SetOnce: {k:?} {v:?} {found_v:?}")
                }
            }

            // We should know, by reducer logic, this key cannot already exist. So the inverse
            // action must be Delete, as opposed to setting the key to some overwritten value.
            txn.put(k.clone(), v).await.or_restart()?;

            Ok(Some(vec![StorageAction::Delete(k)]))
        }
        SetPermanent(k, v) => {
            // SetPermanant is used when `k` should always equal `v`, and we don't need to roll
            // it back because it is always true. For example mapping a script hash to script.
            if safe_mode {
                if let Some(found_v) = kv_map.remove(&k) {
                    assert_eq!(
                        v, found_v,
                        "existing value mismatch when applying SetPermament"
                    );
                }
            }

            txn.put(k.clone(), v).await.or_restart()?;

            // We do not want to/care for removing this key if there is a rollback
            Ok(None)
        }
        Delete(k) => match kv_map.remove(&k) {
            Some(prev_value) => {
                txn.delete(k.clone()).await.or_restart()?;

                Ok(Some(vec![StorageAction::Set(k, prev_value)]))
            }
            None => {
                if key_warnings {
                    warn!("Trying to delete a non-existent key: {}", hex::encode(k));
                }

                Ok(None)
            }
        },
        Insert(k, v) => match kv_map.remove(&k) {
            Some(_) => Ok(None),
            None => {
                txn.put(k.clone(), v).await.or_restart()?;

                Ok(Some(vec![StorageAction::Delete(k)]))
            }
        },
        InsertPermanent(k, v) => match kv_map.remove(&k) {
            // `InsertPermanent` is used when `k` should always equal `v`, meaning it is
            // assigned only once and never deleted. For example, mapping a tx to the timestamp
            // of the first time it was seen.
            // Therefore, the semantic is basically that of `Insert`, except that it does not
            // produce any inverse actions, like `SetPermanent`.
            Some(_) => Ok(None),
            None => {
                txn.put(k.clone(), v).await.or_restart()?;

                // We do not want to remove/care for removing this key if there is a rollback.
                Ok(None)
            }
        },
        Increment(k, d) => match kv_map.remove(&k) {
            Some(prev_value) => {
                let (prev_amount, _) = u128::decode(&prev_value).or_panic()?;

                let new_value = prev_amount + d;

                txn.put(k.clone(), new_value.encode()).await.or_restart()?;

                Ok(Some(vec![StorageAction::Set(k, prev_value)]))
            }
            None => {
                txn.put(k.clone(), d.encode()).await.or_restart()?;

                Ok(Some(vec![StorageAction::Delete(k)]))
            }
        },
        Decrement(k, d) => match kv_map.remove(&k) {
            Some(prev_value) => {
                let (prev_amount, _) = u128::decode(&prev_value).or_panic()?;

                match prev_amount.checked_sub(d) {
                    Some(0) => {
                        txn.delete(k.clone()).await.or_restart()?;
                    }
                    Some(v) => {
                        txn.put(k.clone(), v.encode()).await.or_restart()?;
                    }
                    None => {
                        if key_warnings {
                            warn!(
                                "Trying to decrement integer KV by more than it's current value: [{}] {prev_amount} - {d}, deleting key",
                                hex::encode(k.clone())
                            );
                        }

                        txn.delete(k.clone()).await.or_restart()?;
                    }
                }

                Ok(Some(vec![StorageAction::Set(k, prev_value)]))
            }
            None => {
                if key_warnings {
                    warn!(
                        "Trying to decrement integer KV which does not exist: [{}] - {d}, doing nothing",
                        hex::encode(k)
                    );
                }

                Ok(None)
            }
        },
        DecrementNoDelete(k, d) => match kv_map.remove(&k) {
            Some(prev_value) => {
                let (prev_amount, _) = u128::decode(&prev_value).or_panic()?;

                match prev_amount.checked_sub(d) {
                    Some(v) => {
                        txn.put(k.clone(), v.encode()).await.or_restart()?;
                    }
                    None => {
                        if key_warnings {
                            warn!(
                                "Trying to decrement integer KV by more than it's current value: [{}] {prev_amount} - {d}, setting to 0",
                                hex::encode(k.clone())
                            );
                        }

                        txn.put(k.clone(), 0u128.encode()).await.or_restart()?;
                    }
                }

                Ok(Some(vec![StorageAction::Set(k, prev_value)]))
            }
            None => {
                if key_warnings {
                    warn!(
                        "Trying to decrement integer KV which does not exist: [{}] - {d}, doing nothing",
                        hex::encode(k)
                    );
                }

                Ok(None)
            }
        },
        // if k points to the latest, then the prev value
        PointAggregate(k, points) => {
            let mut inverses = vec![];

            // fetch lastest value or default to 0, and note the inverse action for the latest value
            // kv. we will update this value after each point we write
            let mut prev_value = match kv_map.remove(&k) {
                Some(prev) => {
                    inverses.push(StorageAction::Set(k.clone(), prev.clone()));

                    u128::decode(&prev).or_panic()?.0
                }
                None => {
                    inverses.push(StorageAction::Delete(k.clone()));

                    0
                }
            };

            for (point, delta) in points.into_iter().sorted_by_key(|(x, _)| *x) {
                let point_key = [k.clone(), point.encode()].concat();

                let point_value = match delta {
                    IncrOrDecr::Increment(d) => prev_value.checked_add(d).unwrap(),
                    IncrOrDecr::Decrement(d) => match prev_value.checked_sub(d) {
                        Some(res) => res,
                        None => {
                            if key_warnings {
                                warn!(
                                    "Trying to decrement point aggregate KV by more than it's current value: [{}] {prev_value} - {d}, setting key to 0",
                                    hex::encode(k.clone())
                                );
                            }
                            0u128
                        }
                    },
                };

                // write the point aggregate
                txn.put(point_key.clone(), point_value.encode())
                    .await
                    .or_restart()?;
                inverses.push(StorageAction::Delete(point_key));

                prev_value = point_value;
            }

            // update latest/main key to new total
            txn.put(k, prev_value.encode()).await.or_restart()?;

            // inverse: return latest/main key to previous total, delete all point keys
            Ok(Some(inverses))
        }
    }
}

async fn apply_storage_rb_actions(
    txn: &mut Transaction,
    inverse_ops: &StorageActions,
    point: &Point,
    key_encoder: &Prefix,
) -> Result<(), gasket::error::Error> {
    let mut mutations = vec![];

    for action in inverse_ops.iter() {
        let key = key_encoder.rollback(MetadataKey {
            height: point.height,
            hash: point.hash.to_byte_array(),
            modified_key: action.key().clone(),
        });

        // Value is NIL if the inverse action deleting the key, otherwise is
        // the overwritten value.
        let value = match action {
            Set(_, v) => v.clone(),
            Delete(_) => vec![],
            d => {
                error!("inverse operation was not 'Set' or 'Delete': {d:?}");

                return Err(gasket::error::Error::WorkPanic);
            }
        };

        debug!(
            "write storage rb [{}] -> [{}]",
            hex::encode(&key),
            hex::encode(&value)
        );

        mutations.push(set_mutation(key, value));
    }

    txn.batch_mutate(mutations).await.or_restart()?;

    Ok(())
}
