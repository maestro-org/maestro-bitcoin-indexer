/*
   Total Sats in Outputs by Script Hash

   Creates reducer outputs to increase the count of sats in tx outputs controlled by the script, regardless of being spent or unspent.
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

    pub new_sats: u64,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        txs: &Vec<(Transaction, Txid)>,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let mut output_sats_count = HashMap::new();

        for (tx, _) in txs {
            for output in &tx.output {
                output_sats_count
                    .entry(output.script_pubkey.script_hash().to_byte_array())
                    .and_modify(|x| *x += output.value.to_sat())
                    .or_insert(output.value.to_sat());
            }
        }

        for (script_hash, new_sats) in output_sats_count {
            outputs.push(ReducerOutput::TotalSatInOutputsByScriptHash(Output {
                script_hash,
                new_sats,
            }));
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::TotalSatInOutputsByScriptHash(reducer)
    }
}
