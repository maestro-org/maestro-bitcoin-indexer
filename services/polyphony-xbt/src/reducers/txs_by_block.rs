/*
   Transactions by Block

   Creates reducer outputs mapping a block height and the index of a transaction in it to the hash of that transaction.
*/

use super::ReducerOutput;
use bitcoin::{Transaction, Txid, hashes::Hash};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    // Block height
    pub height: u64,

    // Transaction hashes.
    pub tx_hashes: Vec<[u8; 32]>,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let tx_hashes = txs
            .iter()
            .map(|(_, txid)| txid.to_byte_array())
            .collect::<Vec<_>>();

        outputs.push(ReducerOutput::TxsByBlock(Output { height, tx_hashes }));

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::TxsByBlock(reducer)
    }
}
