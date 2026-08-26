/*
   Total Inscriptions by Script Hash

   Creates reducer outputs to signal spending inscriptions in inputs or receiving inscriptions in outputs.
*/

use bitcoin::{OutPoint, Transaction, Txid, hashes::Hash};
use serde::Deserialize;

use crate::{crosscut, model, prelude::*};

use super::{IncrOrDecr, ReducerOutput};

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    pub script_hash: [u8; 20],

    pub delta: IncrOrDecr<u128>,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, tx_id) in txs {
            if !tx.is_coinbase() {
                for input_outpoint in tx.input.iter().map(|x| x.previous_output) {
                    let resolved_utxo = ctx
                        .find_utxo(&input_outpoint)
                        .apply_policy(&self.policy)
                        .or_panic()?;

                    let resolved_utxo = match resolved_utxo {
                        Some(u) => u.txo,
                        None => return Ok(()),
                    };

                    if let Some(old_inscriptions) = ctx.utxo_inscriptions(&input_outpoint) {
                        outputs.push(ReducerOutput::TotalInscriptionsByScriptHash(Output {
                            script_hash: resolved_utxo.script_pubkey.script_hash().to_byte_array(),
                            delta: IncrOrDecr::Decrement(old_inscriptions.len() as u128),
                        }));
                    }
                }
            }

            for (output_index, output) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*tx_id, output_index as u32);

                if let Some(new_inscriptions) = ctx.utxo_inscriptions(&outpoint) {
                    outputs.push(ReducerOutput::TotalInscriptionsByScriptHash(Output {
                        script_hash: output.script_pubkey.script_hash().to_byte_array(),
                        delta: IncrOrDecr::Increment(new_inscriptions.len() as u128),
                    }));
                }
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

        super::Reducer::TotalInscriptionsByScriptHash(reducer)
    }
}
