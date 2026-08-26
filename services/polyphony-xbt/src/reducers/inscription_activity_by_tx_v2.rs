/*
   Inscriptions Activity by Tx

   Creates reducer outputs to signal inscriptions activity in a transaction.
*/

use super::ReducerOutput;
use crate::{crosscut, model, prelude::*};
use bitcoin::{OutPoint, Transaction, Txid, hashes::Hash};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

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
            // NOTE: this is defined as optional to account for inscriptions spent as fee
            Option<([u8; 20], u32, u64)>,
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
        // New inscriptions may be spent as fee, in which case there is neither input nor output
        // info for them. We must therefore check for newly inscribed sats spent as fee in each tx,
        // which is precisely what `new_inscriptions` is used for.
        let mut new_inscriptions = HashMap::new();

        for (inscription_id, _) in ctx.get_all_new_inscriptions() {
            new_inscriptions
                .entry(inscription_id.txid)
                .and_modify(|inscription_indices: &mut Vec<u32>| {
                    inscription_indices.push(inscription_id.index)
                })
                .or_insert(vec![inscription_id.index]);
        }

        for (tx_index, (tx, tx_id)) in txs.into_iter().enumerate() {
            let tx_hash = tx_id.to_byte_array();

            let mut inscriptions_activity = Vec::new();

            // First, build input info, mapping into it from the inscription ID
            let mut input_info_by_inscription = HashMap::new();
            for (input_idx, outpoint) in tx.input.iter().map(|x| x.previous_output).enumerate() {
                if let Some(inscriptions) = ctx.utxo_inscriptions(&outpoint) {
                    for (offset, inscription_id) in inscriptions {
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
                        input_info_by_inscription.insert(
                            inscription_id,
                            (script_hash, input_idx as u32, offset as u64),
                        );
                    }
                }
            }

            // Used to mark inscriptions seen in outputs, so that we can later see if all new
            // inscriptions in this tx (which have no input info) were sent to an output, or
            // whether they were spent as fee (in which case they don't have output info either).
            let mut seen_in_outputs = HashSet::new();

            // For each inscription in an output, push activity regardless of input info. That is,
            // this covers the case where the inscription is spent from an input and sent to an
            // output, as well as the case where a newly inscribed satoshi is spent as fee.
            for (output_idx, txout) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*tx_id, output_idx as u32);
                if let Some(inscriptions) = ctx.utxo_inscriptions(&outpoint) {
                    for (offset, inscription_id) in inscriptions {
                        // Mark inscription as seen in an output.
                        seen_in_outputs.insert((inscription_id.txid, inscription_id.index));

                        // Push inscription taken from input and transferred to output.
                        inscriptions_activity.push((
                            (inscription_id.txid.to_byte_array(), inscription_id.index),
                            (
                                // note: if no input info is found, then this is a new inscription
                                input_info_by_inscription.remove(&inscription_id),
                                Some((
                                    txout.script_pubkey.script_hash().to_byte_array(),
                                    output_idx as u32,
                                    offset as u64,
                                )),
                            ),
                        ));
                    }
                }
            }

            let mut sorted_input_info_by_inscription =
                input_info_by_inscription.into_iter().collect::<Vec<_>>();
            sorted_input_info_by_inscription.sort();

            // Inscriptions from inputs remaining in `input_info_by_inscription` couldn't be found
            // in the outputs, so they must correspond to inscriptions spent as fee.
            for (inscription_id, input_info) in sorted_input_info_by_inscription {
                // Push inscription taken from input and spent as fee.
                inscriptions_activity.push((
                    (inscription_id.txid.to_byte_array(), inscription_id.index),
                    (Some(input_info), None),
                ))
            }

            // Add new inscriptions (no input info) that were not sent to outputs (no output info).
            if let Some(inscription_indices) = new_inscriptions.remove(tx_id) {
                for inscription_index in inscription_indices {
                    if !seen_in_outputs.contains(&(*tx_id, inscription_index)) {
                        inscriptions_activity.push(((tx_hash, inscription_index), (None, None)))
                    }
                }
            }

            // Truncate to a max cap of 10,000 to avoid rejections from TiKV due to too-large raft entries.
            inscriptions_activity.truncate(10000);

            if !inscriptions_activity.is_empty() {
                outputs.push(ReducerOutput::InscriptionActivityByTxV2(Output {
                    height,
                    tx_index: tx_index as u32,
                    tx_hash,
                    inscriptions_activity,
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

        super::Reducer::InscriptionActivityByTxV2(reducer)
    }
}
