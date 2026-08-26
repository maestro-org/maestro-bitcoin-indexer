/*
   Inscription Activity by Script Hash

   Creates reducer outputs to signal that a script has inscription activity in a transaction.
*/

use super::ReducerOutput;
use crate::{crosscut, model, prelude::*};
use bitcoin::{OutPoint, Transaction, Txid, hashes::Hash};
use ord::InscriptionId;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Deserialize)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
    // Allows us to accumulate indexes over multiple mempool blocks
    activity_indexes: HashMap<[u8; 20], u32>,
}

#[derive(Clone, Debug)]
pub struct Output {
    // Script hash.
    pub script_hash: [u8; 20],

    // Block height.
    pub height: u64,

    // Index of tx in the block.
    pub activity_tx_index: u32,

    // Transaction hash.
    pub tx_hash: [u8; 32],

    // List of self-transferred inscriptions:
    // - inscription ID,
    // - (input index, inscribed sat offset),
    // - (output index, inscribed sat offset),
    pub self_transfers: Vec<(([u8; 32], u32), (u32, u64), (u32, u64))>,

    // List of
    // - inscription ID,
    // - (input index, inscribed sat offset in input),
    // - (output index, inscribed sat offset in output, hash of script controlling the output the inscription is received at).
    pub sent: Vec<(([u8; 32], u32), (u32, u64), Option<(u32, u64, [u8; 20])>)>,

    // List of
    // - inscription ID,
    // - (input index, inscribed sat offset in input, hash of script controlling the input that the inscription is sent from),
    //      NOTE: this is defined as optional to account for new inscriptions.
    // - (output index, inscribed sat offset in output).
    pub received: Vec<(([u8; 32], u32), Option<(u32, u64, [u8; 20])>, (u32, u64))>,
}

impl Reducer {
    /// Resets activity indexes to prepare for processing a batch of mempool blocks.
    pub fn reset_state(&mut self) {
        self.activity_indexes.clear();
    }

    pub fn reduce_block(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, tx_id) in txs {
            let tx_hash = tx_id.to_byte_array();

            // All script hashes seen in the tx.
            let mut all_script_hashes: HashSet<[u8; 20]> = HashSet::new();

            // Script hashes with inscription activity.
            let mut inscription_script_hashes: HashSet<[u8; 20]> = HashSet::new();

            // Map from inscription IDs to input data (input index, inscribed sat offset, script hash).
            let mut sent_inscriptions: HashMap<InscriptionId, (u32, u64, [u8; 20])> =
                HashMap::new();

            // Map from script hashes to IDs of sent inscriptions.
            let mut senders: HashMap<[u8; 20], HashSet<InscriptionId>> = HashMap::new();

            if !tx.is_coinbase() {
                for (input_index, outpoint) in
                    tx.input.iter().map(|x| x.previous_output).enumerate()
                {
                    // Resolve script hash for this input.
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

                    if let Some(inscriptions) = ctx.utxo_inscriptions(&outpoint) {
                        for (offset, inscription_id) in inscriptions {
                            // Map inscription ID to input data.
                            sent_inscriptions.insert(
                                inscription_id,
                                (input_index as u32, offset as u64, script_hash),
                            );

                            // Add inscription ID to set of sent inscriptions associated with script hash.
                            senders
                                .entry(script_hash)
                                .and_modify(|x| {
                                    x.insert(inscription_id);
                                })
                                .or_insert({
                                    let mut new_set = HashSet::new();

                                    new_set.insert(inscription_id);

                                    new_set
                                });
                        }

                        inscription_script_hashes.insert(script_hash);
                    }

                    // Mark script hash as seen.
                    all_script_hashes.insert(script_hash);
                }
            }

            // Map from inscription IDs to output data, as
            // (output index, inscribed sat offset, script hash).
            let mut received_inscriptions: HashMap<InscriptionId, (u32, u64, [u8; 20])> =
                HashMap::new();

            // Map from script hashes to IDs of received inscriptions.
            let mut receivers: HashMap<[u8; 20], HashSet<InscriptionId>> = HashMap::new();

            for (output_index, output) in tx.output.iter().enumerate() {
                if output.script_pubkey.is_op_return() {
                    continue;
                }

                let outpoint = OutPoint::new(*tx_id, output_index as u32);

                let script_hash = output.script_pubkey.script_hash().to_byte_array();

                if let Some(inscriptions) = ctx.utxo_inscriptions(&outpoint) {
                    for (offset, inscription_id) in inscriptions {
                        // Map inscription ID to output data.
                        received_inscriptions.insert(
                            inscription_id,
                            (output_index as u32, offset as u64, script_hash),
                        );

                        // Add inscription ID to set of received inscriptions associated with script
                        // hash.
                        receivers
                            .entry(script_hash)
                            .and_modify(|x| {
                                x.insert(inscription_id);
                            })
                            .or_insert({
                                let mut new_set = HashSet::new();

                                new_set.insert(inscription_id);

                                new_set
                            });
                    }

                    inscription_script_hashes.insert(script_hash);
                }

                // Mark script hash as seen.
                all_script_hashes.insert(script_hash);
            }

            // Push reducer output for each seen script hash.
            for script_hash in inscription_script_hashes {
                let mut self_transfers = vec![];
                let mut sent = vec![];
                let mut received = vec![];

                if let Some(inscriptions) = senders.remove(&script_hash) {
                    for inscription in inscriptions {
                        let (input_index, input_sat_offset, _) = *sent_inscriptions
                            .get(&inscription)
                            .expect("missing inscription sent info");

                        if let Some((output_index, output_sat_offset, output_script_hash)) =
                            received_inscriptions.get(&inscription)
                        {
                            if *output_script_hash == script_hash {
                                // This is a self-transfer.
                                self_transfers.push((
                                    (inscription.txid.to_byte_array(), inscription.index),
                                    (input_index, input_sat_offset),
                                    (*output_index, *output_sat_offset),
                                ));

                                // Remove inscription from list of `received_inscriptions` and
                                // from list of received inscriptions associated to `script_hash`,
                                // to avoid including this inscription as received.
                                receivers
                                    .get_mut(&script_hash)
                                    .map(|x| x.remove(&inscription));
                            } else {
                                // This is a sent inscription, with known output location.
                                sent.push((
                                    (inscription.txid.to_byte_array(), inscription.index),
                                    (input_index, input_sat_offset),
                                    Some((*output_index, *output_sat_offset, *output_script_hash)),
                                ));
                            }
                        } else {
                            // This is a sent inscription, whose output location is unknown because
                            // the inscription was paid as fee.
                            sent.push((
                                (inscription.txid.to_byte_array(), inscription.index),
                                (input_index, input_sat_offset),
                                None,
                            ));
                        }
                    }
                }

                if let Some(inscriptions) = receivers.remove(&script_hash) {
                    for inscription in inscriptions {
                        let (output_index, output_sat_offset, _) = received_inscriptions
                            .get(&inscription)
                            .expect("missing inscription received info");

                        received.push((
                            (inscription.txid.to_byte_array(), inscription.index),
                            sent_inscriptions.get(&inscription).map(|x| *x), // Origin of inscription is optional.
                            (*output_index, *output_sat_offset),
                        ));
                    }
                }

                outputs.push(ReducerOutput::InscriptionActivityByScriptHash(Output {
                    script_hash,
                    height,
                    activity_tx_index: *self.activity_indexes.entry(script_hash).or_default(),
                    tx_hash,
                    self_transfers,
                    sent,
                    received,
                }));
            }

            // Increase `activity_tx_index` for each seen script hash.
            for script_hash in all_script_hashes {
                let activity_tx_index = self.activity_indexes.entry(script_hash).or_default();
                *activity_tx_index += 1;
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            policy: policy.clone(),
            activity_indexes: HashMap::new(),
        };

        super::Reducer::InscriptionActivityByScriptHash(reducer)
    }
}
