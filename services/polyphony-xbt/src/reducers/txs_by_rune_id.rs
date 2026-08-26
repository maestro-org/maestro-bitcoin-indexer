/*
   Txs by Rune ID

   Creates reducer outputs to signal rune activity in a tx.
*/

use super::ReducerOutput;
use crate::{crosscut, model, prelude::*};
use bitcoin::{OutPoint, Transaction, Txid, hashes::Hash};
use itertools::Itertools;
use ordinals::{Artifact, Runestone};
use serde::Deserialize;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    // Rune ID.
    pub rune_id: (u64, u32),

    // Block height.
    pub height: u64,

    // Index of tx with rune activity in the block.
    pub activity_tx_index: u32,

    // Transaction hash.
    pub tx_hash: [u8; 32],

    // Etching tx.
    pub etched: bool,

    // Minting tx.
    pub minted: bool,

    // List of addresses whose rune balances remains unchanged but which were involved in runes activity.
    pub self_transfers: Vec<([u8; 20], u128)>,

    // Final rune balance of addresses who sent runes, as list of addresses and respective sent
    // amounts.
    pub senders: Vec<([u8; 20], u128)>,

    // Final rune balance of addresses who received runes, as list of addresses and respective
    // received amounts.
    pub receivers: Vec<([u8; 20], u128)>,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let mut activity_tx_indexes: HashMap<(u64, u32), u32> = HashMap::new();

        for (tx_index, (tx, tx_id)) in txs.iter().enumerate() {
            let tx_hash = tx_id.to_byte_array();

            // List of all runes involved in the tx.
            let mut seen_runes: HashSet<(u64, u32)> = HashSet::new();
            // List of all runes spent in the tx.
            let mut sent: HashMap<(u64, u32), HashMap<[u8; 20], u128>> = HashMap::new();
            // List of all runes transferred to outputs in the tx.
            let mut received: HashMap<(u64, u32), HashMap<[u8; 20], u128>> = HashMap::new();

            if !tx.is_coinbase() {
                for input in tx.input.iter() {
                    let resolved_utxo = ctx
                        .find_utxo(&input.previous_output)
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

                    if let Some(runes) = ctx.utxo_runes(&input.previous_output) {
                        for (rune_id, rune_amount) in runes.into_iter() {
                            // Store data for these spent runes.
                            sent.entry((rune_id.block, rune_id.tx))
                                .and_modify(|sent_runes| {
                                    sent_runes
                                        .entry(script_hash)
                                        .and_modify(|amount| *amount += rune_amount)
                                        .or_insert(rune_amount);
                                })
                                .or_insert(
                                    vec![(script_hash, rune_amount)]
                                        .into_iter()
                                        .collect::<HashMap<[u8; 20], u128>>(),
                                );
                        }
                    }
                }
            }

            for (output_index, output) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*tx_id, output_index as u32);

                let script_hash = output.script_pubkey.script_hash().to_byte_array();

                if let Some(runes) = ctx.utxo_runes(&outpoint) {
                    for (rune_id, rune_amount) in runes.into_iter() {
                        // Store data for this transferred runes.
                        received
                            .entry((rune_id.block, rune_id.tx))
                            .and_modify(|received_runes| {
                                received_runes
                                    .entry(script_hash)
                                    .and_modify(|amount| *amount += rune_amount)
                                    .or_insert(rune_amount);
                            })
                            .or_insert(
                                vec![(script_hash, rune_amount)]
                                    .into_iter()
                                    .collect::<HashMap<[u8; 20], u128>>(),
                            );
                    }
                }
            }

            // Compute self-transfers, and final balance of senders and receivers.
            let mut self_transfers: HashMap<(u64, u32), HashMap<[u8; 20], u128>> = HashMap::new();
            let mut senders: HashMap<(u64, u32), HashMap<[u8; 20], u128>> = HashMap::new();
            let mut receivers: HashMap<(u64, u32), HashMap<[u8; 20], u128>> = HashMap::new();

            // Traverse all sent balances first, then remaining received balances.
            for (rune_id, sent_balances) in sent.into_iter() {
                // For each rune kind, for each script hash, compute final balance.
                match received.remove(&rune_id) {
                    Some(mut received_balances) => {
                        for (script_hash, sent_amount) in sent_balances.into_iter() {
                            match received_balances.remove(&script_hash) {
                                Some(received_amount) => {
                                    // This script hash has both sent and removed associated
                                    // entries. Compare to see whether the final balance is
                                    // positive, zero or negative.
                                    match received_amount.cmp(&sent_amount) {
                                        Ordering::Equal => {
                                            // This is a self-transfer.
                                            self_transfers
                                                .entry(rune_id)
                                                .and_modify(|self_transferred| {
                                                    self_transferred
                                                        .insert(script_hash, sent_amount);
                                                })
                                                .or_insert({
                                                    let mut new_self_transferred = HashMap::new();
                                                    new_self_transferred
                                                        .insert(script_hash, sent_amount);
                                                    new_self_transferred
                                                });

                                            // Mark rune as seen.
                                            seen_runes.insert(rune_id);
                                        }
                                        Ordering::Less => {
                                            // This script hash has seen its runes balance for this
                                            // specific rune decrease.
                                            let final_balance =
                                                sent_amount.saturating_sub(received_amount);

                                            senders
                                                .entry(rune_id)
                                                .and_modify(|sent_runes| {
                                                    sent_runes.insert(script_hash, final_balance);
                                                })
                                                .or_insert({
                                                    let mut new_send_balances = HashMap::new();
                                                    new_send_balances
                                                        .insert(script_hash, final_balance);
                                                    new_send_balances
                                                });

                                            // Mark rune as seen.
                                            seen_runes.insert(rune_id);
                                        }
                                        Ordering::Greater => {
                                            // This script hash has seen its runes balance for this
                                            // specific rune increase.
                                            let final_balance =
                                                received_amount.saturating_sub(sent_amount);

                                            receivers
                                                .entry(rune_id)
                                                .and_modify(|received_runes| {
                                                    received_runes
                                                        .insert(script_hash, final_balance);
                                                })
                                                .or_insert({
                                                    let mut new_received_balances = HashMap::new();
                                                    new_received_balances
                                                        .insert(script_hash, final_balance);
                                                    new_received_balances
                                                });

                                            // Mark rune as seen.
                                            seen_runes.insert(rune_id);
                                        }
                                    }
                                }
                                None => {
                                    // All runes for this specific rune kind and script hash were
                                    // sent, none were received. That is, all runes of this kind for
                                    // this specific rune kind and script hash, were burned.
                                    senders
                                        .entry(rune_id)
                                        .and_modify(|sent_runes| {
                                            sent_runes.insert(script_hash, sent_amount);
                                        })
                                        .or_insert({
                                            let mut new_send_balances = HashMap::new();
                                            new_send_balances.insert(script_hash, sent_amount);
                                            new_send_balances
                                        });

                                    // Mark rune as seen.
                                    seen_runes.insert(rune_id);
                                }
                            }
                        }

                        // All entries remaining in `received_balances` after removing those which
                        // have matching `sent_balances` entries should be added to `receivers`.
                        receivers.insert(rune_id, received_balances);

                        // Mark rune as seen.
                        seen_runes.insert(rune_id);
                    }
                    None => {
                        // All runes of this kind, for all script hashes in the tx, were burned.
                        senders.insert(rune_id, sent_balances);

                        // Mark rune as seen.
                        seen_runes.insert(rune_id);
                    }
                }
            }

            // We know all entries left in `received` have no matching entry in `sent`, so we can go
            // ahead and add all remaining data to `receivers`.
            for (rune_id, received_balances) in received {
                receivers.insert(rune_id, received_balances);

                // Mark rune as seen.
                seen_runes.insert(rune_id);
            }

            // Compute tx's artifact.
            let artifact = Runestone::decipher(tx);

            if let Some(artifact) = &artifact {
                // Make sure minting and etching activity is included.
                if let Some(rune_id) = artifact.mint() {
                    seen_runes.insert((rune_id.block, rune_id.tx));
                }
                if let Artifact::Runestone(runestone) = artifact {
                    if runestone.etching.is_some() {
                        seen_runes.insert((height, tx_index as u32));
                    }
                }
            }

            // Create reducer outputs for each rune involved in the tx.
            for rune_id in seen_runes.into_iter() {
                let activity_tx_index = activity_tx_indexes.entry(rune_id).or_default();

                outputs.push(ReducerOutput::TxsByRuneId(Output {
                    rune_id,
                    height,
                    activity_tx_index: *activity_tx_index,
                    tx_hash,
                    etched: {
                        match &artifact {
                            Some(Artifact::Runestone(runestone)) => {
                                runestone.etching.is_some() && rune_id == (height, tx_index as u32)
                            }
                            Some(Artifact::Cenotaph(cenotaph)) => {
                                cenotaph.etching.is_some() && rune_id == (height, tx_index as u32)
                            }
                            None => false,
                        }
                    },
                    minted: artifact
                        .as_ref()
                        .and_then(|art| art.mint())
                        .map(|minted_rune| rune_id == (minted_rune.block, minted_rune.tx))
                        .unwrap_or(false),
                    self_transfers: self_transfers
                        .remove(&rune_id)
                        .map(|x| x.into_iter().sorted_by_key(|(k, _)| *k).collect::<Vec<_>>())
                        .unwrap_or_default(),
                    senders: senders
                        .remove(&rune_id)
                        .map(|x| x.into_iter().sorted_by_key(|(k, _)| *k).collect::<Vec<_>>())
                        .unwrap_or_default(),
                    receivers: receivers
                        .remove(&rune_id)
                        .map(|x| x.into_iter().sorted_by_key(|(k, _)| *k).collect::<Vec<_>>())
                        .unwrap_or_default(),
                }));

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
        };

        super::Reducer::TxsByRuneId(reducer)
    }
}
