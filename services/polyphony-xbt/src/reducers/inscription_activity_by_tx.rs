/*
   Inscriptions Activity by Tx

   Creates reducer outputs to signal inscriptions activity in a transaction.
*/

use super::ReducerOutput;
use crate::{crosscut, model, prelude::*};
use bitcoin::{OutPoint, Transaction, Txid, hashes::Hash};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    // block height
    pub height: u64,
    // index of tx in block
    pub tx_index: u32,
    // transaction hash
    pub tx_hash: [u8; 32],
    pub inscriptions_activity: Vec<(
        // (reveal tx hash, index of inscription in reveal tx)
        ([u8; 32], u32),
        (
            // (from address, tx input index, inscribed sat offset)
            // NOTE: this is defined as optional to account for new inscriptions
            Option<([u8; 20], u32, u64)>,
            // (to address, tx output index, inscribed sat offset)
            ([u8; 20], u32, u64),
        ),
    )>,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx_index, (tx, tx_id)) in txs.iter().enumerate() {
            let mut tx_inscriptions = Vec::new();

            // First, build input info, mapping into it from the inscription ID
            let mut input_info = HashMap::new();
            for (input_idx, outpoint) in tx.input.iter().map(|x| x.previous_output).enumerate() {
                if let Some(inscriptions) = ctx.utxo_inscriptions(&outpoint) {
                    for (offset, inscription_id) in inscriptions.iter() {
                        // resolve script hash for this input
                        let resolved_utxo = ctx
                            .find_utxo(&outpoint)
                            .apply_policy(&self.policy)
                            .or_panic()?;

                        let resolved_utxo = match resolved_utxo {
                            Some(u) => u,
                            None => return Ok(()),
                        };

                        let script_hash = resolved_utxo
                            .txo
                            .script_pubkey
                            .script_hash()
                            .to_byte_array();

                        // insert input info associated to this inscription
                        input_info.insert(
                            *inscription_id,
                            (script_hash, input_idx as u32, *offset as u64),
                        );
                    }
                }
            }

            // Then, for each output, search for input info to match with, and push `ReducerOutput`
            for (output_idx, txout) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*tx_id, output_idx as u32);
                if let Some(inscriptions) = ctx.utxo_inscriptions(&outpoint) {
                    for (offset, inscription_id) in inscriptions.iter() {
                        tx_inscriptions.push((
                            (inscription_id.txid.to_byte_array(), inscription_id.index),
                            (
                                // note: if no input info is found, then this is a new inscription
                                input_info.get(inscription_id).map(|x| x.clone()),
                                (
                                    txout.script_pubkey.script_hash().to_byte_array(),
                                    output_idx as u32,
                                    *offset as u64,
                                ),
                            ),
                        ));
                    }
                }
            }

            if !tx_inscriptions.is_empty() {
                outputs.push(ReducerOutput::InscriptionActivityByTx(Output {
                    height,
                    tx_index: tx_index as u32,
                    tx_hash: tx_id.to_byte_array(),
                    inscriptions_activity: tx_inscriptions,
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

        super::Reducer::InscriptionActivityByTx(reducer)
    }
}
