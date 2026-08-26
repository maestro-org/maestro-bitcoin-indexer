/*
   Historical Satoshi Balance By Script Hash

   Creates reducer outputs to signal block-wide increases or decreases in satoshi balance for each script hash.
*/

use bitcoin::{OutPoint, Transaction, TxOut, Txid, hashes::Hash};
use serde::Deserialize;
use std::{cmp::Ordering, collections::HashMap};

use crate::{crosscut, model, prelude::*};

use super::{IncrOrDecr, ReducerOutput};

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    // Script hash.
    pub script_hash: [u8; 20],

    // Block height.
    pub height: u64,

    // Amount by which the stored value should be increased or decreased
    pub satoshi_delta: IncrOrDecr<u128>,
}

impl Reducer {
    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutPoint,
        balance_decrease: &mut HashMap<[u8; 20], u128>,
    ) -> Result<(), gasket::error::Error> {
        let resolved_utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let resolved_utxo = match resolved_utxo {
            Some(u) => u.txo,
            None => return Ok(()),
        };

        let script_hash = resolved_utxo.script_pubkey.script_hash().to_byte_array();
        let sats_decrease = resolved_utxo.value.to_sat() as u128;

        balance_decrease
            .entry(script_hash)
            .and_modify(|x| *x += sats_decrease)
            .or_insert(sats_decrease);

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        output: &TxOut,
        balance_increase: &mut HashMap<[u8; 20], u128>,
    ) {
        let script_hash = output.script_pubkey.script_hash().to_byte_array();
        let sats_increase = output.value.to_sat() as u128;

        balance_increase
            .entry(script_hash)
            .and_modify(|x| *x += sats_increase)
            .or_insert(sats_increase);
    }

    pub fn reduce_block(
        &mut self,
        txs: &Vec<(Transaction, Txid)>,
        height: u64,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        // Block-wide balance increases by script hash.
        let mut balance_increase: HashMap<[u8; 20], u128> = HashMap::new();

        // Block-wide balance decreases by script hash.
        let mut balance_decrease: HashMap<[u8; 20], u128> = HashMap::new();

        for (tx, _) in txs {
            // Process inputs and add to balance_decrease.
            for input in tx.input.iter().map(|x| x.previous_output) {
                // Skip coinbase inputs.
                if !tx.is_coinbase() {
                    self.process_consumed_txo(&ctx, &input, &mut balance_decrease)?;
                }
            }

            // Process outputs and add to balance_increase.
            for output in tx.output.iter() {
                self.process_produced_txo(&output, &mut balance_increase);
            }
        }

        // Merge balance changes into a single output.
        // Start by iterating over increases.
        for (script_hash, increase) in balance_increase.into_iter() {
            if let Some(decrease) = balance_decrease.remove(&script_hash) {
                // The script hash has seen its balance both increase and decrease in different
                // parts of the block.
                match increase.cmp(&decrease) {
                    Ordering::Equal => (), // Balance unchanged.
                    Ordering::Greater => {
                        // Balance increased
                        outputs.push(ReducerOutput::HistoricalSatBalanceByScriptHash(Output {
                            script_hash,
                            height,
                            satoshi_delta: IncrOrDecr::Increment(increase.saturating_sub(decrease)),
                        }));
                    }
                    Ordering::Less => {
                        // Balance decreased
                        outputs.push(ReducerOutput::HistoricalSatBalanceByScriptHash(Output {
                            script_hash,
                            height: height.clone(),
                            satoshi_delta: IncrOrDecr::Decrement(decrease.saturating_sub(increase)),
                        }));
                    }
                }
            } else {
                // The script hash has only seen its balance increase in the block.
                outputs.push(ReducerOutput::HistoricalSatBalanceByScriptHash(Output {
                    script_hash,
                    height: height.clone(),
                    satoshi_delta: IncrOrDecr::Increment(increase),
                }));
            }
        }

        // Now iterate over remaining decreases that have no matching increases.
        for (script_hash, decrease) in balance_decrease.into_iter() {
            outputs.push(ReducerOutput::HistoricalSatBalanceByScriptHash(Output {
                script_hash,
                height: height.clone(),
                satoshi_delta: IncrOrDecr::Decrement(decrease),
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

        super::Reducer::HistoricalSatBalanceByScriptHash(reducer)
    }
}
