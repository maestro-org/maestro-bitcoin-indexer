/*
   Txs with Rune Activity by Script Hash

   Creates reducer outputs to signal rune activity in a tx related to a script hash.
*/

use super::ReducerOutput;
use crate::{crosscut, model, prelude::*};
use bitcoin::{OutPoint, Transaction, Txid, hashes::Hash};
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
    // Allows us to accumulate indexes over multiple mempool blocks
    activity_indexes: HashMap<[u8; 20], u32>,
}

#[derive(Clone, Debug)]
pub struct Output {
    // Script hash.
    pub script_hash: [u8; 20],

    // Block height.
    pub height: u64,

    // Index of tx with rune activity involving this script hash in the block.
    pub activity_tx_index: u32,

    // Transaction hash.
    pub tx_hash: [u8; 32],

    // Etched runes, as rune ID and premined runes amount.
    pub etched: Option<((u64, u32), Option<u128>)>,

    // Minted runes, if any, as rune ID and minted runes amount.
    pub minted: Option<(u64, u32)>,

    // Rune balances that remained unchanged but were involved in self-transfers.
    pub self_transfers: Vec<((u64, u32), u128)>,

    // Increased runes balance after the tx, as rune ID and amount of received runes.
    pub increased_balances: Vec<((u64, u32), u128)>,

    // Decreased runes balance after the tx, as rune ID and amount of sent runes.
    pub decreased_balances: Vec<((u64, u32), u128)>,
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
        for (tx_index, (tx, tx_id)) in txs.iter().enumerate() {
            let tx_hash = tx_id.to_byte_array();

            // All script hashes seen in the tx.
            let mut all_script_hashes: HashSet<[u8; 20]> = HashSet::new();

            // Script hashes with runes activity.
            let mut rune_script_hashes: HashSet<[u8; 20]> = HashSet::new();

            // List of all script hashes sending runes to this tx.
            let mut sent: HashMap<[u8; 20], HashMap<(u64, u32), u128>> = HashMap::new();

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
                            sent.entry(script_hash)
                                .and_modify(|sent_runes| {
                                    sent_runes
                                        .entry((rune_id.block, rune_id.tx))
                                        .and_modify(|amount| *amount += rune_amount)
                                        .or_insert(rune_amount);
                                })
                                .or_insert(
                                    vec![((rune_id.block, rune_id.tx), rune_amount)]
                                        .into_iter()
                                        .collect::<HashMap<(u64, u32), u128>>(),
                                );

                            // Mark script hash as being involved in rune activity in this tx.
                            rune_script_hashes.insert(script_hash);
                        }
                    }

                    // Mark input script hash as seen.
                    all_script_hashes.insert(script_hash);
                }
            }

            // List of all script hashes receiving runes in this tx.
            let mut received: HashMap<[u8; 20], HashMap<(u64, u32), u128>> = HashMap::new();

            for (output_index, output) in tx.output.iter().enumerate() {
                if output.script_pubkey.is_op_return() {
                    continue;
                }

                let outpoint = OutPoint::new(*tx_id, output_index as u32);

                let script_hash = output.script_pubkey.script_hash().to_byte_array();

                if let Some(runes) = ctx.utxo_runes(&outpoint) {
                    for (rune_id, rune_amount) in runes.into_iter() {
                        // Store data for this transferred runes.
                        received
                            .entry(script_hash)
                            .and_modify(|received_runes| {
                                received_runes
                                    .entry((rune_id.block, rune_id.tx))
                                    .and_modify(|amount| *amount += rune_amount)
                                    .or_insert(rune_amount);
                            })
                            .or_insert(
                                vec![((rune_id.block, rune_id.tx), rune_amount)]
                                    .into_iter()
                                    .collect::<HashMap<(u64, u32), u128>>(),
                            );
                    }

                    // Mark script hash as being involved in rune activity in this tx.
                    rune_script_hashes.insert(script_hash);
                }

                // Mark output script hash as seen.
                all_script_hashes.insert(script_hash);
            }

            // Compute final balance of senders and receivers of runes in this tx.
            let mut self_transfers: HashMap<[u8; 20], Vec<((u64, u32), u128)>> = HashMap::new();
            let mut increase_activity: HashMap<[u8; 20], HashMap<(u64, u32), u128>> =
                HashMap::new();
            let mut decrease_activity: HashMap<[u8; 20], HashMap<(u64, u32), u128>> =
                HashMap::new();

            // Traverse all sent balances first, and compare with received balances, deciding for
            // each rune kind whether the final balance is positive or negative. Then include
            // remaining received balances.
            for (script_hash, sent_runes) in sent.into_iter() {
                // For each script hash, for each rune kind, compute final balance and add it to
                // either one of the activity maps.
                match received.remove(&script_hash) {
                    Some(mut received_runes) => {
                        for (rune_id, sent_amount) in sent_runes.into_iter() {
                            match received_runes.remove(&rune_id) {
                                Some(received_amount) => {
                                    // This script hash has both sent and removed associated
                                    // entries. Compare to see whether the final balance for this
                                    // specific rune and script hash is.
                                    match received_amount.cmp(&sent_amount) {
                                        Ordering::Equal => {
                                            // Balance remains unchanged. Add as self-transfer.
                                            self_transfers
                                                .entry(script_hash)
                                                .and_modify(|self_transferred| {
                                                    self_transferred
                                                        .push((rune_id, received_amount))
                                                })
                                                .or_insert(vec![(rune_id, received_amount)]);
                                        }
                                        Ordering::Less => {
                                            // This script hash has seen its balance for this
                                            // specific rune decrease.
                                            let final_balance =
                                                sent_amount.saturating_sub(received_amount);

                                            decrease_activity
                                                .entry(script_hash)
                                                .and_modify(|sent_runes| {
                                                    sent_runes.insert(rune_id, final_balance);
                                                })
                                                .or_insert({
                                                    let mut new_send_balances = HashMap::new();
                                                    new_send_balances
                                                        .insert(rune_id, final_balance);
                                                    new_send_balances
                                                });
                                        }
                                        Ordering::Greater => {
                                            // This script hash has seen its balance for this
                                            // specific rune increase.
                                            let final_balance =
                                                received_amount.saturating_sub(sent_amount);

                                            increase_activity
                                                .entry(script_hash)
                                                .and_modify(|received_runes| {
                                                    received_runes.insert(rune_id, final_balance);
                                                })
                                                .or_insert({
                                                    let mut new_received_balances = HashMap::new();
                                                    new_received_balances
                                                        .insert(rune_id, final_balance);
                                                    new_received_balances
                                                });
                                        }
                                    }
                                }
                                None => {
                                    // All runes for this specific rune and script hash were sent somewhere else.
                                    decrease_activity
                                        .entry(script_hash)
                                        .and_modify(|sent_runes| {
                                            sent_runes.insert(rune_id, sent_amount);
                                        })
                                        .or_insert({
                                            let mut new_send_balances = HashMap::new();
                                            new_send_balances.insert(rune_id, sent_amount);
                                            new_send_balances
                                        });
                                }
                            }
                        }

                        // The remaining entries in `received_runes` have not matching entries in
                        // `sent_runes`, representing a balance increase.
                        increase_activity.insert(script_hash, received_runes);
                    }
                    None => {
                        // All runes for this specific script hash were sent somewhere else.
                        decrease_activity.insert(script_hash, sent_runes);
                    }
                }
            }

            // We know all entries left in `received` have no matching entries in `sent`, so we can
            // go ahead and add them all to `receivers`.
            for (script_hash, received_runes) in received {
                increase_activity.insert(script_hash, received_runes);
            }

            // Compute tx's artifact.
            let artifact = Runestone::decipher(tx);

            let etched = match &artifact {
                Some(Artifact::Runestone(runestone)) => match runestone.etching {
                    Some(etching_terms) => match etching_terms.premine {
                        Some(premined_amount) => {
                            Some(((height, tx_index as u32), Some(premined_amount)))
                        }
                        None => Some(((height, tx_index as u32), None)),
                    },
                    None => None,
                },
                _ => None,
            };

            let minted = artifact
                .and_then(|x| x.mint())
                .and_then(|rune_id| Some((rune_id.block, rune_id.tx)));

            // Create reducer outputs for each rune involved in the tx.
            for script_hash in rune_script_hashes {
                outputs.push(ReducerOutput::RuneTxsByScriptHash(Output {
                    script_hash,
                    height,
                    activity_tx_index: *self.activity_indexes.entry(script_hash).or_default(),
                    tx_hash,
                    etched,
                    minted,
                    self_transfers: self_transfers.remove(&script_hash).unwrap_or_default(),
                    increased_balances: increase_activity
                        .remove(&script_hash)
                        .map(|x| x.into_iter().collect::<Vec<_>>())
                        .unwrap_or_default(),
                    decreased_balances: decrease_activity
                        .remove(&script_hash)
                        .map(|x| x.into_iter().collect::<Vec<_>>())
                        .unwrap_or_default(),
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

        super::Reducer::RuneTxsByScriptHash(reducer)
    }
}
