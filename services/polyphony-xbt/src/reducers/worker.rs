use bitcoin::{Block, BlockHash, hashes::Hash};
use gasket::runtime::{ScheduleResult, WorkSchedule};
use tokio::time::Instant;
use tracing::debug;

use crate::{
    crosscut::Point,
    model::{self, EnrichedBlockPayload, MempoolInfo, StorageActionPayload, TransactionsWithIds},
};

use super::Reducer;

type InputPort = gasket::messaging::tokio::InputPort<model::EnrichedBlockPayload>;
type OutputPort = gasket::messaging::tokio::OutputPort<model::StorageActionPayload>;

pub struct Worker {
    input: InputPort,
    output: OutputPort,
    reducers: Vec<Reducer>,
    ops_count: gasket::metrics::Counter,
    last_block: gasket::metrics::Gauge,
    last_processed: Option<Point>,
}

impl Worker {
    pub fn new(reducers: Vec<Reducer>, input: InputPort, output: OutputPort) -> Self {
        Worker {
            reducers,
            input,
            output,
            ops_count: Default::default(),
            last_block: Default::default(),
            last_processed: None,
        }
    }

    async fn reduce_block(
        &mut self,
        point: Point,
        block: Block,
        txs: TransactionsWithIds,
        ctx: &model::BlockContext,
        mutable: bool,
    ) -> Result<(), gasket::error::Error> {
        debug!("reducing block {:?}", point);

        if let Some(prev) = self.last_processed {
            // check that the "previous block hash" field of the block currently being processed
            // matches the hash of the last block we processed
            if prev.hash != block.header.prev_blockhash {
                panic!(
                    "previous block hash mismatch: {:?} vs {}",
                    prev, block.header.prev_blockhash
                );
            }

            // check that the height of the block currently being processed is 1 greater than the
            // height of the last block we processed
            if prev.height + 1 != point.height {
                panic!(
                    "previous block height unexpected: {:?} vs {:?}",
                    prev, point
                );
            }
        }

        self.last_block.set(point.height as i64);

        let mut outputs = Vec::new();

        let block_time: u64 = block.header.time.into();
        let block = Some(block);

        // Clear reducer states
        for reducer in self.reducers.iter_mut() {
            reducer.reset_state();
        }

        // Instead of passing the output port to the reducers, we pass a vec which
        // we will add all the storage actions to, then we will send these down
        // the outport port later.
        for reducer in self.reducers.iter_mut() {
            reducer.reduce_block(point.height, &txs, &block, block_time, ctx, &mut outputs)?;
            self.ops_count.inc(1);
        }

        outputs.push(super::ReducerOutput::Cursor(
            point.clone(),
            false,
            chrono::Utc::now().timestamp() as u64,
            None,
        ));

        debug!(
            "finished reducing block {:?} resulting in {} outputs",
            point,
            outputs.len()
        );

        self.output
            .send(gasket::messaging::Message::from(
                StorageActionPayload::RollForward(point, outputs, mutable),
            ))
            .await?;

        self.last_processed = Some(point);

        Ok(())
    }

    async fn reduce_mempool_blocks(
        &mut self,
        blocks: &Vec<(Point, TransactionsWithIds, model::BlockContext)>,
        mempool_info: &MempoolInfo,
        fetch_duration_ms: u128,
    ) -> Result<(), gasket::error::Error> {
        let reduce_start = Instant::now();
        let mut block_outputs = vec![];

        // Clear reducer states
        for reducer in self.reducers.iter_mut() {
            reducer.reset_state();
        }

        self.last_processed = Some(Point {
            height: mempool_info.chain_tip.0,
            hash: BlockHash::from_byte_array(mempool_info.chain_tip.1),
        });

        for (point, txs, ctx) in blocks {
            let mut outputs = Vec::new();

            debug!("reducing mempool blocks {:?}", point);

            if let Some(prev) = self.last_processed {
                // check that the height of the block currently being processed is 1 greater than the
                // height of the last block we processed
                if prev.height + 1 != point.height {
                    panic!(
                        "previous block height unexpected: {:?} vs {:?}",
                        prev, point
                    );
                }
            }

            self.last_block.set(point.height as i64);

            // Instead of passing the output port to the reducers, we pass a vec which
            // we will add all the storage actions to, then we will send these down
            // the outport port later.
            for reducer in self.reducers.iter_mut() {
                reducer.reduce_block(
                    point.height,
                    &txs,
                    &None,
                    mempool_info.mempool_view_ts,
                    ctx,
                    &mut outputs,
                )?;
                self.ops_count.inc(1);
            }

            let cursor_mempool_info = (mempool_info.chain_tip, mempool_info.mempool_view_ts);

            outputs.push(super::ReducerOutput::Cursor(
                point.clone(),
                true,
                chrono::Utc::now().timestamp() as u64,
                Some(cursor_mempool_info),
            ));

            debug!(
                "finished reducing mempool block {:?}, new outputs: {}",
                point,
                outputs.len()
            );

            self.last_processed = Some(point.clone());

            block_outputs.push((*point, outputs));
        }

        let reduce_duration_ms = reduce_start.elapsed().as_millis();

        self.output
            .send(gasket::messaging::Message::from(
                StorageActionPayload::MempoolRefresh(
                    mempool_info.clone(),
                    block_outputs,
                    fetch_duration_ms,
                    reduce_duration_ms,
                ),
            ))
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::runtime::Worker for Worker {
    type WorkUnit = EnrichedBlockPayload;

    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("ops_count", &self.ops_count)
            .with_gauge("last_block", &self.last_block)
            .build()
    }

    async fn schedule(&mut self) -> ScheduleResult<Self::WorkUnit> {
        let msg = self.input.recv().await?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &Self::WorkUnit) -> Result<(), gasket::error::Error> {
        match unit {
            model::EnrichedBlockPayload::RollForward(point, block, txs, ctx, mutable) => {
                self.reduce_block(
                    point.clone(),
                    block.clone(),
                    txs.clone(),
                    &ctx.clone(),
                    mutable.clone(),
                )
                .await
            }
            model::EnrichedBlockPayload::RollBack(point, mutable) => {
                self.last_processed = Some(point.clone());

                self.output
                    .send(gasket::messaging::Message::from(
                        StorageActionPayload::RollBack(point.clone(), *mutable),
                    ))
                    .await
            }
            model::EnrichedBlockPayload::MempoolRefresh(
                mempool_info,
                blocks,
                fetch_duration_ms,
            ) => {
                self.reduce_mempool_blocks(blocks, mempool_info, *fetch_duration_ms)
                    .await
            }
        }
    }
}
