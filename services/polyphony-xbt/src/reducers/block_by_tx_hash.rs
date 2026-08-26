/*
   Block Height by Tx Hash

   Given a tx hash, creates a reducer output to associate it to the corresponding block height and
   transaction index in that block.
*/

use super::ReducerOutput;
use crate::model;
use bitcoin::{Transaction, Txid, hashes::Hash};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    // tx hash
    pub tx_hash: [u8; 32],
    // block height
    pub height: u64,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        block_height: u64,
        txs: &Vec<(Transaction, Txid)>,
        _ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (_, tx_hash) in txs.iter() {
            outputs.push(ReducerOutput::BlockByTxHash(Output {
                tx_hash: tx_hash.to_byte_array(),
                height: block_height,
            }));
        }
        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::BlockByTxHash(reducer)
    }
}
