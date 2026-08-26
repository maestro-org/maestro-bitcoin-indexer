/*
   Tx First Seen Timestamp

   Creates permanent reducer outputs associating each time a tx is first seen with the timestamp of when we took a snapshot of the mempool to create the estimated block.
*/

use super::ReducerOutput;
use bitcoin::{Transaction, Txid, hashes::Hash};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    // Tx hash
    pub tx_hash: [u8; 32],

    // Timestamp.
    pub timestamp: u64,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        txs: &Vec<(Transaction, Txid)>,
        timestamp: u64,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (_, tx_id) in txs.into_iter() {
            outputs.push(ReducerOutput::TxFirstSeenTimestamp(Output {
                tx_hash: tx_id.to_byte_array(),
                timestamp,
            }));
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::TxFirstSeenTimestamp(reducer)
    }
}
