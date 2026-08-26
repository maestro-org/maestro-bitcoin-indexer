/*
   Total Utxos by Script Hash

   Creates reducer outputs to signal spending inputs or producing outputs.
*/

use bitcoin::{Transaction, Txid, hashes::Hash};
use serde::Deserialize;

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

    pub is_new: bool,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, _) in txs {
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

                    outputs.push(ReducerOutput::TotalUtxosByScriptHash(Output {
                        script_hash: resolved_utxo.script_pubkey.script_hash().to_byte_array(),
                        is_new: false,
                    }));
                }
            }

            for output in tx.output.iter() {
                outputs.push(ReducerOutput::TotalUtxosByScriptHash(Output {
                    script_hash: output.script_pubkey.script_hash().to_byte_array(),
                    is_new: true,
                }));
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

        super::Reducer::TotalUtxosByScriptHash(reducer)
    }
}
