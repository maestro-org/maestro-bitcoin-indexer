/*
   Total Txs by Script Hash

   Creates reducer outputs to increase the count of txs where the script was involved.
*/

use bitcoin::{Transaction, Txid, hashes::Hash};
use serde::Deserialize;
use std::collections::HashSet;

use crate::{crosscut, model, prelude::*};

use super::ReducerOutput;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    pub script_hash: [u8; 20],
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, _) in txs {
            let mut involved_in_tx = HashSet::new();

            if !tx.is_coinbase() {
                for input in tx.input.iter() {
                    let resolved_utxo = ctx
                        .find_utxo(&input.previous_output)
                        .apply_policy(&self.policy)
                        .or_panic()?;

                    let resolved_utxo = match resolved_utxo {
                        Some(u) => u.txo,
                        None => return Ok(()),
                    };

                    involved_in_tx
                        .insert(resolved_utxo.script_pubkey.script_hash().to_byte_array());
                }
            }

            for output in &tx.output {
                involved_in_tx.insert(output.script_pubkey.script_hash().to_byte_array());
            }

            for script_hash in involved_in_tx {
                outputs.push(ReducerOutput::TotalTxsByScriptHash(Output { script_hash }));
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            policy: policy.clone(),
        };

        super::Reducer::TotalTxsByScriptHash(reducer)
    }
}
