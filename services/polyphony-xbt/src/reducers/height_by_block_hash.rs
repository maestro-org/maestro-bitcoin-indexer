/*
   Block Height by Block Hash

   Creates reducer outputs to store the block height associated to a block hash.
*/

use super::ReducerOutput;
use crate::model;
use bitcoin::{Block, Transaction, Txid, hashes::Hash};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    // block hash
    pub block_hash: [u8; 32],
    // block height
    pub block_height: u64,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        block_height: u64,
        _txs: &Vec<(Transaction, Txid)>,
        block: &Option<Block>,
        _ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        match block {
            Some(block) => {
                outputs.push(ReducerOutput::HeightByBlockHash(Output {
                    block_hash: block.block_hash().to_byte_array(),
                    block_height,
                }));
                Ok(())
            }
            None => return Ok(()),
        }
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::HeightByBlockHash(reducer)
    }
}
