use std::collections::HashMap;

use bitcoin::{block::Header, Block, BlockHash};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use gasket::framework::*;
use tracing::{debug, error, info, warn};

use crate::{peer::Peer, storage::ChainDB};

use super::{
    model::{Point, PullEvent},
    BitcoinCompatibleNetwork,
};

pub type DownstreamPort = gasket::messaging::tokio::OutputPort<PullEvent>;

pub struct Worker {
    peer_session: Peer,
    _peer_rpc: Client,
    cursor: Vec<Point>,
    tip: Point,
    init: bool,
}

impl Worker {
    async fn send(&mut self, stage: &mut Stage, event: PullEvent) -> Result<(), WorkerError> {
        stage
            .downstream
            .send(event.clone().into())
            .await
            .or_panic()?;

        // stage
        //     .health_downstream
        //     .send(event.into())
        //     .await
        //     .or_panic()?;

        Ok(())
    }

    async fn process_next(
        &mut self,
        stage: &mut Stage,
        next: &PullEvent,
    ) -> Result<(), WorkerError> {
        match next {
            p @ PullEvent::RollForward(height, hash, _, _) => {
                debug!(height, %hash, "pull roll forward");

                self.send(stage, p.clone().into()).await.or_panic()?;

                if height % 4000 == 0 {
                    let tip_height = self.tip.0;
                    let height_pct = (height * 100) / tip_height;

                    info!("sync status: {height_pct}% by height");
                }

                Ok(())
            }
            p @ PullEvent::RollBack(height, hash, _) => {
                info!(height, %hash, "pull roll backwards");

                self.send(stage, p.clone().into()).await.or_panic()?;

                Ok(())
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        info!("connecting to node {}", &stage.node_address);

        let peer_session = Peer::connect(&stage.node_address, stage.network.magic())
            .await
            .or_retry()?;

        info!("connecting to node rpc {}", &stage.node_rpc);

        let peer_rpc = Client::new(&stage.node_rpc, stage.node_rpc_auth.clone()).or_panic()?;

        let tip = peer_rpc
            .get_chain_tips()
            .or_restart()?
            .into_iter()
            .max_by_key(|x| x.height)
            .ok_or_else(|| {
                error!("No chaintip found");
                WorkerError::Restart
            })?;

        let tip = (tip.height, tip.hash);

        info!(
            node = stage.node_address,
            rpc = stage.node_rpc,
            network = ?stage.network,
            upstream_tip = ?tip,
            "connected to upstream node"
        );

        let intersects = stage
            .chain_db
            .intersect_options(&stage.genesis_hash)
            .or_retry()?;

        info!("chain db intersects: {intersects:?}");

        let worker = Self {
            peer_session,
            _peer_rpc: peer_rpc,
            cursor: intersects,
            tip,
            init: false,
        };

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<Vec<PullEvent>>, WorkerError> {
        // Check if peer handler is still alive
        if self.peer_session.handler.is_finished() {
            error!("peer handler has terminated, restarting worker");
            return Err(WorkerError::Restart);
        }

        // TODO: optimise block hash

        let mut units = vec![];

        // if we are just starting up, send a rollback to our intersect (not genesis)
        if !self.init {
            if let Some((i_height, i_hash)) = self.cursor.first() {
                if *i_height != 0 {
                    units.push(PullEvent::RollBack(*i_height, *i_hash, self.tip));
                }
                self.init = true;
            }
        }

        let instant_a = tokio::time::Instant::now();

        // fetch page of headers
        let mut headers = self
            .peer_session
            .get_new_headers(self.cursor.iter().map(|x| x.1).collect())
            .await
            .or_restart()?;

        let hdr_download_duration = instant_a.elapsed().as_millis();

        // fetch configurable amount of blocks at once
        headers.truncate(stage.block_page_size);

        while headers.is_empty() {
            debug!(
                "no headers returned, awaiting new block notification ({:?})",
                self.cursor
            );

            tokio::select! {
                _ = self.peer_session.new_block_notification.notified() => {},
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                    if self.peer_session.handler.is_finished() {
                        error!("peer handler terminated while waiting for new block");
                        return Err(WorkerError::Restart);
                    }
                }
            }

            headers = self
                .peer_session
                .get_new_headers(self.cursor.iter().map(|x| x.1).collect())
                .await
                .or_restart()?;
        }

        let instant_b = tokio::time::Instant::now();

        // fetch blocks for headers we fetched
        let blocks = self
            .peer_session
            .get_blocks(headers.iter().map(|x| x.block_hash()).collect())
            .await
            .or_restart()?;

        let blk_download_duration = instant_b.elapsed().as_millis();

        // zip headers and blocks
        if headers.len() != blocks.len() {
            error!("header len/block len mismatch");
            return Err(WorkerError::Restart);
        }

        let headers = headers.into_iter().zip(blocks).collect::<Vec<_>>();

        // TODO: must include genesis for the intersect height fetch
        let cursor_map: HashMap<_, _> = self
            .cursor
            .iter()
            .map(|(height, hash)| (hash, height))
            .collect();

        let cursor_tip = self.cursor.first().map(|x| x.0);

        let headers = if let Some((first, _)) = headers.first() {
            // find the height of the first returned header using the cursor
            let intersect_height = cursor_map.get(&first.prev_blockhash).ok_or_else(|| {
                warn!(
                    "could not find intersect height for {:?}",
                    first.prev_blockhash
                );
                WorkerError::Restart
            })?;

            // send a rollback to the intersect if it was behind our tip
            if let Some(cursor_tip_height) = cursor_tip {
                if **intersect_height != cursor_tip_height {
                    units.push(PullEvent::RollBack(
                        **intersect_height,
                        first.prev_blockhash,
                        self.tip,
                    ))
                }
            }

            // assign heights to the received headers using the intersect height
            headers
                .into_iter()
                .zip((*intersect_height + 1)..)
                .collect::<Vec<((Header, Block), u64)>>()
        } else {
            debug!(
                "no headers returned from peer with cursors: {:?}",
                self.cursor
            );
            return Ok(WorkSchedule::Idle);
        };

        // try use just processed headers as intersects
        self.cursor = headers
            .iter()
            .rev()
            .take(50)
            .map(|x| (x.1, x.0 .0.block_hash()))
            .collect::<Vec<_>>();

        // else fetch more from db if we dont get many
        if self.cursor.len() < 50 {
            let mut options = stage
                .chain_db
                .intersect_options(&stage.genesis_hash)
                .or_retry()?;

            if let Some(last) = self.cursor.last() {
                // don't have multiple points for same height
                options.retain(|x| x.0 < last.0);
                self.cursor.extend(options)
            } else {
                self.cursor = options
            }

            debug!("got intersect options: {:?}", self.cursor)
        }

        self.cursor.push((0, stage.genesis_hash));

        for ((header, block), height) in headers {
            if header.block_hash() != block.block_hash() {
                error!("header/block hash mismatch");
                return Err(WorkerError::Restart);
            }

            if height > self.tip.0 {
                self.tip = (height, header.block_hash());
            }

            units.push(PullEvent::RollForward(
                height,
                header.block_hash(),
                block,
                self.tip,
            ));
        }

        debug!(
            hdr_download_duration,
            blk_download_duration, "scheduling new page"
        );

        Ok(WorkSchedule::Unit(units))
    }

    async fn execute(
        &mut self,
        unit: &Vec<PullEvent>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        for u in unit {
            self.process_next(stage, u).await.or_panic()?;
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        self.peer_session.handler.abort();

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "peer", unit = "Vec<PullEvent>", worker = "Worker")]
pub struct Stage {
    node_address: String,
    node_rpc: String,
    node_rpc_auth: Auth,
    network: BitcoinCompatibleNetwork,
    genesis_hash: BlockHash,
    chain_db: ChainDB,
    block_page_size: usize,

    pub downstream: DownstreamPort,
    pub health_downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    chain_tip: gasket::metrics::Gauge,
}

impl Stage {
    pub fn new(
        node_address: String,
        node_rpc: String,
        node_rpc_auth: Auth,
        network: BitcoinCompatibleNetwork,
        chain_db: ChainDB,
        block_page_size: usize,
    ) -> Self {
        Self {
            node_address,
            node_rpc,
            node_rpc_auth,
            network,
            genesis_hash: network.genesis_block().block_hash(),
            chain_db,
            block_page_size,
            downstream: Default::default(),
            health_downstream: Default::default(),
            block_count: Default::default(),
            chain_tip: Default::default(),
        }
    }
}
