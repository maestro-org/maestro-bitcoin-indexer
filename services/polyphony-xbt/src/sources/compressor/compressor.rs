use std::collections::HashMap;
use std::time::Duration;

use bitcoin::consensus::Decodable;
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, OutPoint, ScriptHash, Transaction, Txid};
use futures::StreamExt;
use gasket::runtime::{ScheduleResult, WorkSchedule};
use itertools::Itertools;
use ord::InscriptionId;
use ordinals::RuneId;
use tokio::time::Instant;
use tonic::codec::CompressionEncoding;
use tracing::{debug, warn};

use gasket::error::AsWorkError;
use tonic::transport::Channel;
use tonic::{Code, Streaming};
use tracing::info;

use crate::crosscut::Point;
use crate::model::{BRC20Message, BlockContext, EnrichedBlockPayload, MempoolInfo};
use crate::sources::compressor::compressor_api::MempoolBlocksWithCtxRequest;
use crate::sources::utils::{block_ref_to_point, point_to_block_ref};
use crate::{Error, crosscut, model, sources::utils, storage};

use super::Config;
use super::compressor_api::brc20_action::Kind;
use super::compressor_api::stream_updates_with_ctx_response::Action;
use super::compressor_api::sync_service_client::SyncServiceClient;
use super::compressor_api::{
    BlockRef, BlockWithContext, MempoolBlockWithContext, PageBlocksWithCtxRequest,
    StreamUpdatesWithCtxRequest, StreamUpdatesWithCtxResponse,
};

pub type OutputPort = gasket::messaging::tokio::OutputPort<model::EnrichedBlockPayload>;

pub struct SourceWorkUnit {
    actions: Vec<BlockAction>,
    mutable: bool,
}

#[derive(Debug, Clone)]
pub enum BlockAction {
    Apply(BlockWithContext),
    MempoolRefresh(BlockRef, Vec<MempoolBlockWithContext>, Duration),
    Undo(BlockRef),
    Reset(BlockRef),
}

impl std::fmt::Display for BlockAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockAction::Apply(block) => write!(f, "Apply({})", {
                let x = block.r#ref.clone().unwrap();

                Point {
                    height: x.height,
                    hash: BlockHash::from_slice(&x.hash).unwrap(),
                }
            }),
            BlockAction::MempoolRefresh(tip, blocks, fetch) => write!(
                f,
                "MempoolRefresh(tip: {:?}, mpv: {:?}, blocks: {}, ({fetch:?}))",
                tip,
                blocks.iter().map(|b| b.mempool_view_ts).next(),
                blocks.len()
            ),
            BlockAction::Undo(block_ref) => write!(f, "Undo({})", {
                Point {
                    height: block_ref.height,
                    hash: BlockHash::from_slice(&block_ref.hash).unwrap(),
                }
            }),
            BlockAction::Reset(block_ref) => write!(f, "Reset({})", {
                Point {
                    height: block_ref.height,
                    hash: BlockHash::from_slice(&block_ref.hash).unwrap(),
                }
            }),
        }
    }
}

impl Into<BlockAction> for Action {
    fn into(self) -> BlockAction {
        match self {
            Action::Apply(x) => BlockAction::Apply(x),
            Action::Undo(x) => BlockAction::Undo(x),
            Action::Reset(x) => BlockAction::Reset(x),
        }
    }
}

#[derive(Debug, Clone)]
enum ProcessedMempool {
    True(BlockRef, u64), // real chain tip we applied mempool blocks on top of, mempool view ts
    False,
}

enum Mode {
    Init,
    Dumping(Option<BlockRef>),
    Streaming,
}

pub struct Worker {
    config: Config,
    intersect: crosscut::IntersectConfig,
    cursor: storage::Cursor,
    client: Option<SyncServiceClient<Channel>>,
    mode: Mode,
    stream: Option<Streaming<StreamUpdatesWithCtxResponse>>,
    output: OutputPort,
    upstream_height: Option<u64>,
    /// does the chain sent downstream contain mempool blocks, if so built on what tip
    processed_mempool: ProcessedMempool,
    /// current tip of the chain we have received from compressor
    chain_tip_point: Option<BlockRef>,
    /// cache of transactions from previous mempool fetches, to avoid re-sending same transaction bytes
    mempool_tx_cache: HashMap<Txid, Transaction>,
    block_count: gasket::metrics::Counter,
    chain_tip: gasket::metrics::Gauge,
    buffer_size: usize,
    persistent_buf: Option<Vec<crate::rollback::PersistentBufferValue>>,
}

impl Worker {
    pub fn new(
        config: Config,
        intersect: crosscut::IntersectConfig,
        cursor: storage::Cursor,
        output: OutputPort,
        buffer_size: usize,
        persistent_buf: Option<Vec<crate::rollback::PersistentBufferValue>>,
    ) -> Self {
        Self {
            config,
            intersect,
            cursor,
            mode: Mode::Init,
            output,
            client: None,
            stream: None,
            upstream_height: None,
            processed_mempool: ProcessedMempool::False,
            chain_tip_point: None,
            mempool_tx_cache: HashMap::new(),
            block_count: Default::default(),
            chain_tip: Default::default(),
            buffer_size,
            persistent_buf,
        }
    }

    async fn process_action(
        &mut self,
        action: BlockAction,
        mutable: bool,
    ) -> Result<(), gasket::error::Error> {
        match action {
            BlockAction::Apply(block) => {
                // println!(
                //     "processing Apply: {} {}",
                //     block.r#ref.clone().unwrap().height,
                //     hex::encode(block.r#ref.clone().unwrap().hash)
                // );

                self.process_apply(block, mutable).await?
            }
            BlockAction::MempoolRefresh(_, blocks, fetch_duration) => {
                // println!(
                //     "processing MempoolApply: {}",
                //     blocks.len(),
                // );

                self.process_mempool_refresh(blocks, fetch_duration).await?
            }
            BlockAction::Reset(reset) => {
                // println!(
                //     "processing Reset: {} {}",
                //     reset.height,
                //     hex::encode(&reset.hash)
                // );

                let payload =
                    EnrichedBlockPayload::roll_back(block_ref_to_point(reset.clone()), false);

                self.output.send(payload).await.or_panic()?;

                self.chain_tip.set(reset.height as i64);
                self.chain_tip_point = Some(reset)
            }
            BlockAction::Undo(_) => (),
        }

        Ok(())
    }

    async fn process_apply(
        &mut self,
        block: BlockWithContext,
        mutable: bool,
    ) -> Result<(), gasket::error::Error> {
        let block_ref = block
            .clone()
            .r#ref
            .ok_or(Error::source("missing block_ref"))
            .or_panic()?;

        let decoded_block = Block::consensus_decode(&mut &block.raw[..])
            .map_err(crate::Error::encoding)
            .or_panic()?;

        let txs = decoded_block
            .txdata
            .clone()
            .into_iter()
            .map(|x| (x.compute_txid(), x))
            .map(|(txid, tx)| (tx, txid))
            .collect::<Vec<_>>();

        let payload = EnrichedBlockPayload::roll_forward(
            block_ref_to_point(block_ref.clone()),
            decoded_block,
            txs,
            block_ctx_from_apply(block).or_panic()?,
            mutable,
        );

        self.output.send(payload).await.or_panic()?;

        self.chain_tip.set(block_ref.height as i64);
        self.chain_tip_point = Some(block_ref);

        Ok(())
    }

    async fn process_mempool_refresh(
        &mut self,
        blocks: Vec<MempoolBlockWithContext>,
        fetch_duration: Duration,
    ) -> Result<(), gasket::error::Error> {
        let mut blocks_out = vec![];

        let chain_tip = blocks[0].chain_tip.clone().unwrap();

        let mempool_info = MempoolInfo {
            chain_tip: (chain_tip.height, chain_tip.hash.try_into().unwrap()),
            mempool_view_ts: blocks[0].mempool_view_ts,
        };

        // create a new cache to replace the old one (keeping only transactions still in mempool)
        let mut new_cache = HashMap::new();

        for block in blocks {
            let mut txs = vec![];

            // iterate over transaction IDs and raw bytes together
            for (txid_bytes, tx_bytes) in block.txids.iter().zip(block.raw_txs.iter()) {
                let txid = Txid::from_slice(txid_bytes)
                    .map_err(crate::Error::encoding)
                    .or_panic()?;

                let tx = if tx_bytes.is_empty() {
                    // transaction bytes are empty, fetch from cache
                    self.mempool_tx_cache
                        .get(&txid)
                        .cloned()
                        .ok_or_else(|| {
                            crate::Error::message(format!(
                                "transaction {} not found in cache",
                                txid
                            ))
                        })
                        .or_panic()?
                } else {
                    // decode transaction from bytes
                    Transaction::consensus_decode(&mut &tx_bytes[..])
                        .map_err(crate::Error::encoding)
                        .or_panic()?
                };

                // add to new cache
                new_cache.insert(txid, tx.clone());
                txs.push((tx, txid))
            }

            // for mempool blocks, we will use the merkle root for the block hash
            let point = Point {
                height: block.height,
                hash: BlockHash::from_slice(&block.merkle_root).unwrap(),
            };

            blocks_out.push((point, txs, block_ctx_from_mempool_apply(block).or_panic()?))
        }

        // replace the cache with the new one
        self.mempool_tx_cache = new_cache;

        // println!("processed new mempool apply {} with txhashes: {:?}", block_ref.height, txs.iter().map(|x| x.txid().to_string()).collect::<Vec<_>>());

        let payload = EnrichedBlockPayload::mempool_refresh(
            mempool_info,
            blocks_out,
            fetch_duration.as_millis(),
        );

        self.output.send(payload).await.or_panic()?;

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::runtime::Worker for Worker {
    type WorkUnit = SourceWorkUnit;

    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("received_blocks", &self.block_count)
            .with_gauge("chain_tip", &self.chain_tip)
            .build()
    }

    async fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        debug!("bootstrapping, config: {:?}", self.config);

        let mut client = SyncServiceClient::connect(self.config.url.clone())
            .await
            .or_restart()?
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);

        // Take the persistent buffer from the initial startup, or fetch it if this is a restart
        let persistent_buf = if let Some(buf) = self.persistent_buf.take() {
            Some(buf)
        } else {
            self.cursor.fetch_persistent_buffer().await.or_restart()?
        };

        // first try intersect with persistent rollback buffer if one is found
        let intersect = if let Some(point) =
            utils::try_intersect_with_rollback_buf(&mut self.cursor, &mut client, persistent_buf)
                .await
                .or_restart()?
        {
            // send rollback to intersect point, in case it is behind our tip
            let payload = EnrichedBlockPayload::roll_back(point.clone(), true);
            self.output.send(payload).await.or_panic()?;

            Some(point)
        } else {
            // otherwise try intersect with cursor in storage, if one is found,
            // or use config if not
            utils::try_intersect_with_last_point_or_config(
                &self.intersect,
                &mut self.cursor,
                &mut client,
            )
            .await
            .or_restart()?
        };

        self.upstream_height = client
            .page_blocks_with_context(PageBlocksWithCtxRequest {
                cursor: None,
                max_items: 1,
            })
            .await
            .or_restart()?
            .into_inner()
            .chain_tip
            .map(|x| x.height);

        if let Some(i) = intersect {
            let bref = point_to_block_ref(i);
            self.chain_tip.set(i.height as i64);
            self.mode = Mode::Dumping(Some(bref.clone()));
            self.chain_tip_point = Some(bref);
        } else {
            self.mode = Mode::Dumping(None); // TODO
        }

        self.client = Some(client);

        Ok(())
    }

    async fn schedule(&mut self) -> ScheduleResult<Self::WorkUnit> {
        debug!("scheduling");

        // if we are dumping and in mutable zone, try switch to streaming by
        // intersecting with last processed block
        if let Some(upstream_height) = self.upstream_height {
            if let Mode::Dumping(Some(cursor)) = &self.mode {
                if cursor.height + self.buffer_size as u64 >= upstream_height {
                    let stream_req = StreamUpdatesWithCtxRequest {
                        intersects: vec![cursor.clone()],
                    };

                    info!(
                        "within mutable zone ({} v {}), trying to intersect with mutable with {cursor:?}",
                        cursor.height, upstream_height
                    );

                    match self
                        .client
                        .as_mut()
                        .unwrap()
                        .stream_updates_with_context(stream_req)
                        .await
                    {
                        Ok(stream) => {
                            info!("switching to streaming (intersected using {cursor:?})");

                            self.mode = Mode::Streaming;
                            self.stream = Some(stream.into_inner())
                        }
                        Err(err) if err.code() == Code::NotFound => (),
                        e @ Err(_) => {
                            e.or_restart()?;
                        }
                    }
                } else {
                    debug!(
                        "not yet in mutable zone ({} v {})",
                        cursor.height, upstream_height
                    )
                }
            }
        }

        match &self.mode {
            Mode::Init => unreachable!(),
            // if we are streaming, await next action and schedule it
            Mode::Streaming => {
                let use_mempool = self.config.use_mempool.unwrap_or(false);

                /*
                   Each iteration we need to check the compressor stream first, because processing
                   new REAL chain actions takes priority over refreshing the mempool - we want to
                   react to these as soon as possible.

                   If there are none, then we should try fetch any mempool blocks using the tip. If
                   we have currently processed the mempool snapshot with ts MPTS1, and we receive
                   blocks for the same mempool snapshot, we should ignore them to avoid reprocessing
                   them.
                */

                // first, receive some chain actions from compressor, by reading actions from the
                // stream until we hit some number of actions or there are no actions reading

                let stream = self.stream.as_mut().unwrap();

                // buffer of chain actions being read from compressor stream
                let mut chain_actions: Vec<BlockAction> = vec![];

                loop {
                    tokio::select! {
                        Some(item) = stream.next() => {
                            let action = item
                                .or_restart()?
                                .action
                                .ok_or(Error::source("no action"))
                                .or_panic()?;

                            // skip undo actions. reset actions will suffice for us.
                            if matches!(action, Action::Undo(_)) {
                                continue;
                            }

                            let action = action.into();

                            match &action {
                                BlockAction::Apply(_) | BlockAction::Reset(_) => debug!("received {}", action),
                                _ => unreachable!("undo/mempool apply"),
                            };

                            chain_actions.push(action);

                            // limit the number of actions we can pass to next stage, else we may
                            // load all the actions from the mutableKV into memory. maybe that could
                            // be ok if we have a small mutable zone.
                            if chain_actions.len() >= 10 {
                                info!("10 actions buffered");
                                break
                            }
                        },
                        // if we don't receive an action within 50ms, continue
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {
                            break
                        }
                    }
                }

                // if mempool mode not enabled, simply forward on the received chain actions...
                if !use_mempool {
                    if chain_actions.is_empty() {
                        return Ok(WorkSchedule::Idle);
                    } else {
                        debug!("scheduling actions [{}]", chain_actions.iter().join(", "));

                        return Ok(WorkSchedule::Unit(SourceWorkUnit {
                            actions: chain_actions,
                            mutable: true,
                        }));
                    }
                }

                // otherwise we are in mempool mode, and we may need to adjust/supplement the
                // received chain actions with actions related to processing mempool blocks

                let mut adjusted_actions = vec![];
                let mut chain_tip = self.chain_tip_point.clone();

                for action in chain_actions {
                    match action.clone() {
                        BlockAction::Reset(b) => {
                            // if the action is reset then simply forward on the action
                            adjusted_actions.push(action.into());

                            // we have passed on a rollback which means we will have undone any
                            // mempool blocks
                            self.processed_mempool = ProcessedMempool::False;

                            // note the new chain tip
                            chain_tip = Some(b);
                        }
                        BlockAction::Apply(new_block) => {
                            // before applying a new block we need to undo any mempool blocks
                            // in our downstream chain
                            if let ProcessedMempool::True(before_mempool, _) =
                                &self.processed_mempool
                            {
                                adjusted_actions.push(BlockAction::Reset(before_mempool.clone()));

                                self.processed_mempool = ProcessedMempool::False;
                            }

                            // then apply the new block
                            adjusted_actions.push(action.into());

                            // note the new chain tip
                            chain_tip = Some(new_block.r#ref.unwrap());
                        }
                        _ => unreachable!("undo/mempool apply"),
                    }
                }

                // at this point, we have a list of chain actions from compressor we need to
                // process, and if necessary we inserted our own action to undo any mempool
                // blocks that we had previously added.

                // we are in mempool mode so we will should try fetch mempool blocks from compressor
                // which are built upon our current chain tip. ideally we would only do this when
                // we know we are at the tip, but that is a bit tricky.

                let Some(ref true_tip) = chain_tip else {
                    warn!("no chain tip");
                    return Ok(WorkSchedule::Idle);
                };

                let mempool_fetch_start = Instant::now();

                // collect cached transaction IDs to send to compressor
                let cached_txs: Vec<Vec<u8>> = self
                    .mempool_tx_cache
                    .keys()
                    .map(|txid| txid.as_raw_hash().as_byte_array().to_vec())
                    .collect();

                match self
                    .client
                    .as_mut()
                    .unwrap()
                    .mempool_blocks_with_context(MempoolBlocksWithCtxRequest {
                        tip_intersect: Some(true_tip.clone()),
                        cached_txs,
                    })
                    .await
                {
                    Ok(result) => {
                        let result = result.into_inner();

                        // if we found some mempool blocks then note that we are
                        // including mempool blocks in our downstream chain
                        if !result.blocks.is_empty() {
                            let mempool_view_ts = result.blocks[0].clone().mempool_view_ts;

                            let block_action = BlockAction::MempoolRefresh(
                                true_tip.clone(),
                                result.blocks,
                                mempool_fetch_start.elapsed(),
                            );

                            if let ProcessedMempool::True(_, prev_mempool_view_ts) =
                                self.processed_mempool.clone()
                            {
                                // only process the blocks if we have not already processed them
                                // (ignore mempool blocks for mempool view we have already seen)
                                if mempool_view_ts != prev_mempool_view_ts {
                                    // insert rollback action to undo currently processed mempool blocks
                                    adjusted_actions.push(block_action);

                                    self.processed_mempool =
                                        ProcessedMempool::True(true_tip.clone(), mempool_view_ts);
                                }
                            } else {
                                // insert apply actions for the received mempool blocks
                                adjusted_actions.push(block_action);

                                self.processed_mempool =
                                    ProcessedMempool::True(true_tip.clone(), mempool_view_ts);
                            }
                        }
                    }
                    // compressor does not have mempool blocks built upon the chain tip intersect
                    // we provided. probably we are not yet at the chain tip
                    Err(err) if err.code() == Code::NotFound => (),
                    e @ Err(_) => {
                        e.or_restart()?;
                    }
                }

                // `adjusted_actions` is a list of chain actions streamed from compressor, but:
                // - if we had previously applied some mempool blocks to our chain we passed
                //   downstream, then in order to undo these mempool blocks before adding a new
                //   block received from compressor, we inserted a rollback action to undo these
                //   mempool blocks before adding the apply action for the new block.
                // - if we were able to fetch any mempool blocks from the new chain tip then we have
                //   appended MempoolApply actions for these mempool blocks to the list of chain
                //   actions received from compressor

                if adjusted_actions.is_empty() {
                    Ok(WorkSchedule::Idle)
                } else {
                    debug!(
                        "scheduling actions [{}]",
                        adjusted_actions.iter().join(", ")
                    );

                    Ok(WorkSchedule::Unit(SourceWorkUnit {
                        actions: adjusted_actions,
                        mutable: true,
                    }))
                }
            }
            // if we are dumping, fetch the next page and schedule all the
            // blocks as Apply actions
            Mode::Dumping(cursor) => {
                let dump_request = PageBlocksWithCtxRequest {
                    cursor: cursor.clone(),
                    max_items: self.config.max_items_per_page.unwrap_or(20),
                };

                debug!("compressor requesting page: {dump_request:?}");

                let result = self
                    .client
                    .as_mut()
                    .unwrap()
                    .page_blocks_with_context(dump_request)
                    .await
                    .or_restart()?
                    .into_inner();

                if let Some(last_block) = result.blocks.last() {
                    self.mode = Mode::Dumping(last_block.r#ref.clone());
                }

                self.upstream_height = result.chain_tip.map(|x| x.height);

                let actions = result
                    .blocks
                    .into_iter()
                    .map(BlockAction::Apply)
                    .collect::<Vec<_>>();

                if !actions.is_empty() {
                    debug!("compressor scheduling {} actions", actions.len());
                    Ok(WorkSchedule::Unit(SourceWorkUnit {
                        actions,
                        mutable: false,
                    }))
                } else {
                    warn!("page contained no blocks, waiting 5s");
                    tokio::time::sleep(Duration::from_secs(5)).await;

                    Ok(WorkSchedule::Idle)
                }
            }
        }
    }

    async fn execute(&mut self, unit: &Self::WorkUnit) -> Result<(), gasket::error::Error> {
        let mutable = unit.mutable;

        debug!(
            "compressor processing actions (first: {:?})",
            unit.actions.last()
        );

        for action in unit.actions.clone() {
            self.process_action(action, mutable).await.or_panic()?;
        }

        debug!("compressor finished processing actions");

        Ok(())
    }
}

pub fn block_ctx_from_mempool_apply(block: MempoolBlockWithContext) -> Result<BlockContext, Error> {
    let mut ctx = BlockContext::new();

    for txo in block.txo_resolver {
        let txo_ref = txo.r#ref.ok_or(Error::source("missing txoref"))?;

        let ref_tx_hash: [u8; 32] = txo_ref
            .tx_hash
            .try_into()
            .map_err(|_| Error::source("malformed txoref txhash"))?;

        let output_ref = OutPoint::new(Txid::from_byte_array(ref_tx_hash), txo_ref.txo_index);

        ctx.insert_txo(&output_ref, txo.height, txo.raw, txo.ord_ranges)
    }

    if let Some(runes_info) = block.runes {
        for resolved_txo in runes_info.txo_resolver.into_iter() {
            let txo_ref = resolved_txo.r#ref.ok_or(Error::source("missing txoref"))?;

            let ref_tx_hash: [u8; 32] = txo_ref
                .tx_hash
                .try_into()
                .map_err(|_| Error::source("malformed txoref txhash"))?;

            let output_ref = OutPoint::new(Txid::from_byte_array(ref_tx_hash), txo_ref.txo_index);

            let mut runes = resolved_txo
                .runes
                .into_iter()
                .map(|x| {
                    let id = x.id.unwrap();
                    let id = RuneId {
                        block: id.block,
                        tx: id.tx,
                    };

                    let amount = u128::from_be_bytes(x.amount.try_into().unwrap());

                    (id, amount)
                })
                .collect::<Vec<_>>();

            runes.sort_by_key(|x| x.0);

            if !runes.is_empty() {
                ctx.insert_runes(&output_ref, runes)
            }
        }

        ctx.rune_etch_idxs = runes_info.successful_etchs;
        ctx.rune_mint_idxs = runes_info.successful_mints;
    }

    if let Some(inscriptions_info) = block.inscriptions {
        for resolved_txo in inscriptions_info.txo_resolver.into_iter() {
            let txo_ref = resolved_txo.r#ref.ok_or(Error::source("missing txoref"))?;

            let ref_tx_hash: [u8; 32] = txo_ref
                .tx_hash
                .try_into()
                .map_err(|_| Error::source("malformed txoref txhash"))?;

            let output_ref = OutPoint::new(Txid::from_byte_array(ref_tx_hash), txo_ref.txo_index);

            let mut inscriptions = resolved_txo
                .inscriptions
                .into_iter()
                .map(|x| {
                    let id = x.id.unwrap();

                    let id = InscriptionId {
                        txid: Txid::from_byte_array(id.tx_hash.try_into().unwrap()),
                        index: id.index,
                    };

                    (x.offset, id)
                })
                .collect::<Vec<_>>();

            inscriptions.sort_by_key(|x| x.0);

            if !inscriptions.is_empty() {
                ctx.insert_inscriptions(&output_ref, inscriptions)
            }
        }

        ctx.valid_reinscriptions = inscriptions_info
            .valid_reinscriptions
            .into_iter()
            .map(|x| (x.tx_index, x.inscription_index))
            .collect();

        for brc20_action in inscriptions_info.brc20_resolver.into_iter() {
            let id = brc20_action.id.unwrap();

            let id = InscriptionId {
                txid: Txid::from_byte_array(id.tx_hash.try_into().unwrap()),
                index: id.index,
            };

            let action = match brc20_action.action.unwrap().kind.unwrap() {
                Kind::Deploy(x) => BRC20Message::Deploy(x.ticker),
                Kind::Mint(x) => BRC20Message::Mint(
                    x.ticker,
                    u128::from_be_bytes(x.amt.try_into().unwrap()),
                    ScriptHash::from_byte_array(x.script_hash.try_into().unwrap()),
                ),
                Kind::TransferInit(x) => BRC20Message::TransferInit(
                    x.ticker,
                    u128::from_be_bytes(x.amt.try_into().unwrap()),
                    ScriptHash::from_byte_array(x.script_hash.try_into().unwrap()),
                ),
                Kind::Transfer(x) => {
                    let first_output = x.first_output.unwrap();

                    let output = OutPoint {
                        txid: Txid::from_byte_array(first_output.tx_hash.try_into().unwrap()),
                        vout: first_output.txo_index,
                    };

                    BRC20Message::Transfer(
                        x.ticker,
                        u128::from_be_bytes(x.amt.try_into().unwrap()),
                        output,
                        ScriptHash::from_byte_array(x.script_hash.try_into().unwrap()),
                    )
                }
            };

            ctx.insert_brc20(&id, action);
        }

        for inscription_origin in inscriptions_info.new_inscriptions.into_iter() {
            let id = inscription_origin.id.unwrap();

            let id = InscriptionId {
                txid: Txid::from_byte_array(id.tx_hash.try_into().unwrap()),
                index: id.index,
            };

            ctx.insert_new_inscription(&id, inscription_origin.inscription_num);
        }
    }

    Ok(ctx)
}

pub fn block_ctx_from_apply(block: BlockWithContext) -> Result<BlockContext, Error> {
    let mut ctx = BlockContext::new();

    for txo in block.txo_resolver {
        let txo_ref = txo.r#ref.ok_or(Error::source("missing txoref"))?;

        let ref_tx_hash: [u8; 32] = txo_ref
            .tx_hash
            .try_into()
            .map_err(|_| Error::source("malformed txoref txhash"))?;

        let output_ref = OutPoint::new(Txid::from_byte_array(ref_tx_hash), txo_ref.txo_index);

        ctx.insert_txo(&output_ref, txo.height, txo.raw, txo.ord_ranges)
    }

    if let Some(runes_info) = block.runes {
        for resolved_txo in runes_info.txo_resolver.into_iter() {
            let txo_ref = resolved_txo.r#ref.ok_or(Error::source("missing txoref"))?;

            let ref_tx_hash: [u8; 32] = txo_ref
                .tx_hash
                .try_into()
                .map_err(|_| Error::source("malformed txoref txhash"))?;

            let output_ref = OutPoint::new(Txid::from_byte_array(ref_tx_hash), txo_ref.txo_index);

            let mut runes = resolved_txo
                .runes
                .into_iter()
                .map(|x| {
                    let id = x.id.unwrap();
                    let id = RuneId {
                        block: id.block,
                        tx: id.tx,
                    };

                    let amount = u128::from_be_bytes(x.amount.try_into().unwrap());

                    (id, amount)
                })
                .collect::<Vec<_>>();

            runes.sort_by_key(|x| x.0);

            if !runes.is_empty() {
                ctx.insert_runes(&output_ref, runes)
            }
        }

        ctx.rune_etch_idxs = runes_info.successful_etchs;
        ctx.rune_mint_idxs = runes_info.successful_mints;
    }

    if let Some(inscriptions_info) = block.inscriptions {
        for resolved_txo in inscriptions_info.txo_resolver.into_iter() {
            let txo_ref = resolved_txo.r#ref.ok_or(Error::source("missing txoref"))?;

            let ref_tx_hash: [u8; 32] = txo_ref
                .tx_hash
                .try_into()
                .map_err(|_| Error::source("malformed txoref txhash"))?;

            let output_ref = OutPoint::new(Txid::from_byte_array(ref_tx_hash), txo_ref.txo_index);

            let mut inscriptions = resolved_txo
                .inscriptions
                .into_iter()
                .map(|x| {
                    let id = x.id.unwrap();

                    let id = InscriptionId {
                        txid: Txid::from_byte_array(id.tx_hash.try_into().unwrap()),
                        index: id.index,
                    };

                    (x.offset, id)
                })
                .collect::<Vec<_>>();

            inscriptions.sort_by_key(|x| x.0);

            if !inscriptions.is_empty() {
                ctx.insert_inscriptions(&output_ref, inscriptions)
            }
        }

        ctx.valid_reinscriptions = inscriptions_info
            .valid_reinscriptions
            .into_iter()
            .map(|x| (x.tx_index, x.inscription_index))
            .collect();

        for brc20_action in inscriptions_info.brc20_resolver.into_iter() {
            let id = brc20_action.id.unwrap();

            let id = InscriptionId {
                txid: Txid::from_byte_array(id.tx_hash.try_into().unwrap()),
                index: id.index,
            };

            let action = match brc20_action.action.unwrap().kind.unwrap() {
                Kind::Deploy(x) => BRC20Message::Deploy(x.ticker),
                Kind::Mint(x) => BRC20Message::Mint(
                    x.ticker,
                    u128::from_be_bytes(x.amt.try_into().unwrap()),
                    ScriptHash::from_byte_array(x.script_hash.try_into().unwrap()),
                ),
                Kind::TransferInit(x) => BRC20Message::TransferInit(
                    x.ticker,
                    u128::from_be_bytes(x.amt.try_into().unwrap()),
                    ScriptHash::from_byte_array(x.script_hash.try_into().unwrap()),
                ),
                Kind::Transfer(x) => {
                    let first_output = x.first_output.unwrap();

                    let output = OutPoint {
                        txid: Txid::from_byte_array(first_output.tx_hash.try_into().unwrap()),
                        vout: first_output.txo_index,
                    };

                    BRC20Message::Transfer(
                        x.ticker,
                        u128::from_be_bytes(x.amt.try_into().unwrap()),
                        output,
                        ScriptHash::from_byte_array(x.script_hash.try_into().unwrap()),
                    )
                }
            };

            ctx.insert_brc20(&id, action);
        }

        for inscription_origin in inscriptions_info.new_inscriptions.into_iter() {
            let id = inscription_origin.id.unwrap();

            let id = InscriptionId {
                txid: Txid::from_byte_array(id.tx_hash.try_into().unwrap()),
                index: id.index,
            };

            ctx.insert_new_inscription(&id, inscription_origin.inscription_num);
        }
    }

    Ok(ctx)
}
