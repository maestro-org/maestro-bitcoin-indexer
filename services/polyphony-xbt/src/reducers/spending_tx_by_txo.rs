/*
   Spending Transaction by Transaction Output

   Creates reducer outputs to signal that a (former) UTxO has been spent by a transaction.
*/

use super::ReducerOutput;
use bitcoin::{Transaction, Txid, hashes::Hash};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    // Hash of the transaction that produced this output.
    pub utxo_tx_hash: [u8; 32],

    // Output index.
    pub utxo_vout: u32,

    // Tx hash.
    pub tx_hash: [u8; 32],
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        _height: u64,
        txs: &Vec<(Transaction, Txid)>,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, tx_hash) in txs.iter() {
            if !tx.is_coinbase() {
                for outpoint in tx.input.iter().map(|x| x.previous_output) {
                    outputs.push(ReducerOutput::SpendingTxByTxo(Output {
                        utxo_tx_hash: outpoint.txid.to_byte_array(),
                        utxo_vout: outpoint.vout,
                        tx_hash: tx_hash.to_byte_array(),
                    }));
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::SpendingTxByTxo(reducer)
    }
}
