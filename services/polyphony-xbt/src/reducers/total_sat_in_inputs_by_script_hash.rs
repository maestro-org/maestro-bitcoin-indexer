/*
   Total Sats in Inputs by Script Hash

   Creates reducer outputs to increase the count of sats in *spent* tx outputs controlled by the script.
*/

use bitcoin::{Transaction, Txid, hashes::Hash};
use gasket::error::AsWorkError;
use serde::Deserialize;
use std::collections::HashMap;

use crate::{crosscut, model, prelude::AppliesPolicy};

use super::ReducerOutput;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    pub script_hash: [u8; 20],

    pub new_sats: u64,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let mut input_sats_count = HashMap::new();

        for (tx, _) in txs {
            // coinbase transactions have no inputs
            if !tx.is_coinbase() {
                for input in &tx.input {
                    let outpoint = input.previous_output;

                    let resolved_utxo = ctx
                        .find_utxo(&outpoint)
                        .apply_policy(&self.policy)
                        .or_panic()?;

                    let resolved_utxo = match resolved_utxo {
                        Some(u) => u,
                        None => return Ok(()),
                    };

                    let txo = resolved_utxo.txo;

                    input_sats_count
                        .entry(txo.script_pubkey.script_hash().to_byte_array())
                        .and_modify(|x| *x += txo.value.to_sat())
                        .or_insert(txo.value.to_sat());
                }
            }
        }

        for (script_hash, new_sats) in input_sats_count {
            outputs.push(ReducerOutput::TotalSatInInputsByScriptHash(Output {
                script_hash,
                new_sats,
            }));
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            policy: policy.clone(),
        };

        super::Reducer::TotalSatInInputsByScriptHash(reducer)
    }
}
