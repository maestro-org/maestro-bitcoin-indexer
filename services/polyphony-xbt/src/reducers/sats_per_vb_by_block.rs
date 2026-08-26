/*
   Block Sats/vB

   Creates reducer output for every block which details min, median and max sats/vB values of
   transactions within the block.
*/

use bitcoin::{Transaction, Txid};
use gasket::error::AsWorkError;
use serde::Deserialize;

use crate::{crosscut, model, prelude::AppliesPolicy};

use super::ReducerOutput;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    pub height: u64,
    pub min: u64,
    pub median: u64,
    pub max: u64,
}

impl Reducer {
    pub fn reduce_block<'b>(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let mut sats_per_vb_values = Vec::with_capacity(txs.len());

        for (tx, _) in txs {
            if tx.is_coinbase() {
                continue;
            }

            let mut total_input = 0;

            for input in tx.input.iter() {
                let resolved_utxo = ctx
                    .find_utxo(&input.previous_output)
                    .apply_policy(&self.policy)
                    .or_panic()?;

                let resolved_utxo = match resolved_utxo {
                    Some(u) => u,
                    None => return Ok(()),
                };

                total_input += resolved_utxo.txo.value.to_sat();
            }

            let mut total_output = 0;

            for output in tx.output.iter() {
                total_output += output.value.to_sat();
            }

            let tx_fee = total_input.saturating_sub(total_output);

            // ensure round up, avoiding floating point
            let sats_per_vb = (tx_fee + tx.vsize() as u64 - 1)
                .checked_div(tx.vsize() as u64)
                .unwrap_or(0);

            sats_per_vb_values.push(sats_per_vb)
        }

        sats_per_vb_values.sort();

        let output = Output {
            height,
            min: *sats_per_vb_values.first().unwrap_or(&0),
            median: *sats_per_vb_values
                .get(sats_per_vb_values.len() / 2)
                .unwrap_or(&0),
            max: *sats_per_vb_values.last().unwrap_or(&0),
        };

        outputs.push(ReducerOutput::SatsPerVbByBlock(output));

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            policy: policy.clone(),
        };

        super::Reducer::SatsPerVbByBlock(reducer)
    }
}
