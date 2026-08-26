/*
   Satoshi Balance By Script Hash

   Creates reducer outputs to signal increases or decreases in satoshi balance of UTxOs controlled by each script hash.
*/

use bitcoin::{OutPoint, Transaction, TxOut, Txid, hashes::Hash};
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
    // Script hash.
    pub script_hash: [u8; 20],

    // Amount by which the stored value should be increased or decreased
    pub delta: IncrOrDecr<u128>,
}

impl Reducer {
    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutPoint,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let resolved_utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let resolved_utxo = match resolved_utxo {
            Some(u) => u.txo,
            None => return Ok(()),
        };

        outputs.push(ReducerOutput::SatBalanceByScriptHash(Output {
            script_hash: resolved_utxo.script_pubkey.script_hash().to_byte_array(),
            delta: IncrOrDecr::Decrement(resolved_utxo.value.to_sat() as u128),
        }));

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        tx_output: &TxOut,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let script_hash = tx_output.script_pubkey.script_hash();

        outputs.push(ReducerOutput::SatBalanceByScriptHash(Output {
            script_hash: script_hash.to_byte_array(),
            delta: IncrOrDecr::Increment(tx_output.value.to_sat() as u128),
        }));

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, _) in txs {
            if !tx.is_coinbase() {
                // Skip coinbase inputs.
                for txin in tx.input.iter().map(|x| x.previous_output) {
                    self.process_consumed_txo(&ctx, &txin, outputs)?;
                }
            }

            for txo in tx.output.iter() {
                self.process_produced_txo(txo, outputs)?;
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

        super::Reducer::SatBalanceByScriptHash(reducer)
    }
}
