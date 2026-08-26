/*
   Total Tx Outputs by Script Hash

   Creates reducer outputs to increase the count of tx outputs controlled by the script, regardless of being spent or unspent.
*/

use bitcoin::{Transaction, Txid, hashes::Hash};
use serde::Deserialize;
use std::collections::HashMap;

use super::ReducerOutput;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    pub script_hash: [u8; 20],

    pub new_outputs: u64,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        txs: &Vec<(Transaction, Txid)>,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let mut outputs_count = HashMap::new();

        for (tx, _) in txs {
            for output in &tx.output {
                outputs_count
                    .entry(output.script_pubkey.script_hash().to_byte_array())
                    .and_modify(|x| *x += 1)
                    .or_insert(1);
            }
        }

        for (script_hash, new_outputs) in outputs_count {
            outputs.push(ReducerOutput::TotalOutputsByScriptHash(Output {
                script_hash,
                new_outputs,
            }));
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::TotalOutputsByScriptHash(reducer)
    }
}
