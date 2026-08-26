/*
   Satoshi Activity in Transactions by Script Hash

   Creates reducer outputs to signal sat activity in a tx, as increased or decreased sat balance controlled by a script.
*/

use super::ReducerOutput;
use crate::{crosscut, model, prelude::*};
use bitcoin::{Transaction, Txid, hashes::Hash};
use serde::Deserialize;
use std::{cmp::Ordering, collections::HashMap};
use timbre_xbt::reducers::sat_txs_by_script_hash::SatActivityType;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
    // Allows us to accumlate indexes over multiple mempool blocks
    activity_indexes: HashMap<[u8; 20], u32>,
}

#[derive(Clone, Debug)]
pub struct Output {
    // Script hash.
    pub script_hash: [u8; 20],

    // Block height.
    pub height: u64,

    // Index of tx in the block (or mempool blocks).
    pub activity_tx_index: u32,

    // Transaction hash.
    pub tx_hash: [u8; 32],

    // Amount relative to the activity type:
    // - amount == n && activity_type == SatActivityType::Increased: balance increased by n sats.
    // - amount == n && activity_type == SatActivityType::Decreased: balance decreased by n sats.
    // - amount == n && activity_type == SatActivityType::SelfTransferred: balance stayed the same, script self-transferred n sats.
    pub amount: u64,

    pub activity_type: SatActivityType,
}

impl Reducer {
    /// Resets activity indexes to prepare for processing a batch of mempool blocks.
    /// This enables activity indexes to accumulate across all mempool blocks.
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

            // Map of script hashes to total spent satoshis in this tx.
            let mut total_spent: HashMap<[u8; 20], u64> = HashMap::new();

            if !tx.is_coinbase() {
                // For each input, decrease the sat balance controlled by the script.
                for input in &tx.input {
                    let resolved_utxo = ctx
                        .find_utxo(&input.previous_output)
                        .apply_policy(&self.policy)
                        .or_panic()?;

                    let resolved_txo = match resolved_utxo {
                        Some(u) => u.txo,
                        None => return Ok(()),
                    };

                    let script_hash = resolved_txo.script_pubkey.script_hash().to_byte_array();

                    let sats = resolved_txo.value.to_sat();

                    total_spent
                        .entry(script_hash)
                        .and_modify(|balance| *balance += sats)
                        .or_insert(sats);
                }
            }

            // Map of script hashes to total received satoshis in this tx.
            let mut total_received: HashMap<[u8; 20], u64> = HashMap::new();

            // For each output, increase the sat balance controlled by the script.
            for output in &tx.output {
                if output.script_pubkey.is_op_return() {
                    continue;
                }

                let script_hash = output.script_pubkey.script_hash().to_byte_array();

                let sats = output.value.to_sat();

                total_received
                    .entry(script_hash)
                    .and_modify(|balance| *balance += sats)
                    .or_insert(sats);
            }

            // Push an output for each script controlling inputs or outputs in the tx (provided
            // the total computed balance is different from 0).
            // for every address which received some sats...
            for (script_hash, received_balance) in total_received {
                let activity_index = self.activity_indexes.entry(script_hash).or_default();

                if let Some(spent_balance) = total_spent.remove(&script_hash) {
                    // Compare spent and received amounts.
                    let (amount, activity_type) = match received_balance.cmp(&spent_balance) {
                        Ordering::Less => {
                            // Received balance is less than spent balance. Balance decreased.
                            (
                                spent_balance.saturating_sub(received_balance),
                                SatActivityType::Decreased,
                            )
                        }
                        Ordering::Equal => (received_balance, SatActivityType::SelfTransferred),
                        Ordering::Greater => {
                            // Received balance is greater than spent balance. Balance increased.
                            (
                                received_balance.saturating_sub(spent_balance),
                                SatActivityType::Increased,
                            )
                        }
                    };

                    outputs.push(ReducerOutput::SatTxsByScriptHash(Output {
                        script_hash,
                        height,
                        activity_tx_index: *activity_index,
                        tx_hash,
                        amount,
                        activity_type,
                    }));
                } else {
                    // No inputs controlled by the script. Balance increased.
                    outputs.push(ReducerOutput::SatTxsByScriptHash(Output {
                        script_hash,
                        height,
                        activity_tx_index: *activity_index,
                        tx_hash,
                        amount: received_balance,
                        activity_type: SatActivityType::Increased,
                    }));
                }

                *activity_index += 1;
            }

            // Remaining entries in total_spent are related to scripts controlling inputs and no
            // outputs. Balance decreased.
            for (script_hash, spent_balance) in total_spent {
                let activity_index = self.activity_indexes.entry(script_hash).or_default();

                outputs.push(ReducerOutput::SatTxsByScriptHash(Output {
                    script_hash,
                    height,
                    activity_tx_index: *activity_index,
                    tx_hash,
                    amount: spent_balance,
                    activity_type: SatActivityType::Decreased,
                }));

                *activity_index += 1;
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

        super::Reducer::SatTxsByScriptHash(reducer)
    }
}
