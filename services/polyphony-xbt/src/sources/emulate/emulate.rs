use bitcoin::{Block, consensus::Decodable};
use gasket::runtime::{ScheduleResult, WorkSchedule};
use std::collections::VecDeque;
use std::fs;
use tracing::debug;

use crate::{
    crosscut::Point,
    model::{self, BlockContext},
};

type OutputPort = gasket::messaging::tokio::OutputPort<model::EnrichedBlockPayload>;

pub enum Instruction {
    Forwards,
    Backwards(u8),
}

struct EmulatorInstructions(VecDeque<Instruction>);

impl TryFrom<String> for EmulatorInstructions {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut ops = VecDeque::new();
        let mut tokens = value.chars();

        loop {
            let token = match tokens.next() {
                Some(t) => t,
                None => return Ok(EmulatorInstructions(ops)),
            };

            match token {
                'F' => ops.push_back(Instruction::Forwards),
                'B' => {
                    let amount_token = match tokens.next() {
                        Some(t) => t,
                        None => {
                            return Err(
                                "Backwards instruction B must be immediately followed by a u8",
                            );
                        }
                    };

                    let amount: u8 = match amount_token.to_digit(10) {
                        Some(n) => n as u8,
                        None => return Err("Invalid backwards instruction amount"),
                    };

                    if amount == 0 {
                        return Err("Backwards amount cannot be 0");
                    }

                    ops.push_back(Instruction::Backwards(amount))
                }
                _ => {
                    return Err(
                        "Invalid emulator instruction, must be 'F' or 'Bn' where 'n' is a u8",
                    );
                }
            }
        }
    }
}

pub struct Worker {
    instructions: EmulatorInstructions,
    blocks: Vec<(u64, Vec<u8>)>,
    output: OutputPort,
    block_count: gasket::metrics::Counter,
    chain_tip: gasket::metrics::Gauge,
    block_ptr: usize,
}

impl Worker {
    pub fn new(instructions: Option<String>, output: OutputPort) -> Self {
        let paths: Vec<_> = fs::read_dir("../../src/sources/emulate/block_data/")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        let ops = EmulatorInstructions::try_from(instructions.unwrap_or("F".repeat(paths.len())))
            .unwrap();

        Self {
            instructions: ops,
            output,
            block_count: Default::default(),
            chain_tip: Default::default(),
            blocks: Vec::new(),
            block_ptr: 0,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::runtime::Worker for Worker {
    type WorkUnit = Instruction;

    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("received_blocks", &self.block_count)
            .with_gauge("chain_tip", &self.chain_tip)
            .build()
    }

    /// Read all of the test block-bytes files into a vector
    async fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let mut blocks = Vec::new();

        let mut paths: Vec<_> = fs::read_dir("../../src/sources/emulate/block_data/")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        paths.sort_by_key(|dir| dir.path());

        for (idx, path) in paths.iter().enumerate() {
            if path.file_type().unwrap().is_file() {
                let contents = hex::decode(fs::read(path.path()).unwrap()).unwrap();

                blocks.push((idx as u64, contents));
            }
        }

        self.blocks = blocks;

        Ok(())
    }

    async fn schedule(&mut self) -> ScheduleResult<Self::WorkUnit> {
        match self.instructions.0.pop_front() {
            Some(i) => Ok(WorkSchedule::Unit(i)),
            None => Ok(WorkSchedule::Done),
        }
    }

    async fn execute(&mut self, unit: &Self::WorkUnit) -> Result<(), gasket::error::Error> {
        match unit {
            Instruction::Forwards => {
                let (block_height, block_bytes) = self.blocks.get(self.block_ptr).unwrap();

                let block = Block::consensus_decode(&mut &block_bytes[..]).unwrap();

                let txs = block
                    .txdata
                    .clone()
                    .into_iter()
                    .map(|x| (x.compute_txid(), x))
                    .map(|(txid, tx)| (tx, txid))
                    .collect::<Vec<_>>();

                let point = Point {
                    height: *block_height,
                    hash: block.block_hash(),
                };

                self.block_ptr += 1;

                debug!("emulating rollforward to {:?}", point);

                self.output
                    .send(model::EnrichedBlockPayload::roll_forward(
                        point.clone(),
                        block,
                        txs,
                        BlockContext::new(),
                        true,
                    ))
                    .await
            }
            Instruction::Backwards(n) => {
                let (block_height, block_bytes) =
                    self.blocks.get(self.block_ptr - (*n as usize + 1)).unwrap();

                let block = Block::consensus_decode(&mut &block_bytes[..]).unwrap();

                let point = Point {
                    height: *block_height,
                    hash: block.block_hash(),
                };

                debug!("emulating rollbackwards to {:?}", point);

                self.output
                    .send(model::EnrichedBlockPayload::roll_back(point.clone(), true))
                    .await
            }
        }
    }
}
