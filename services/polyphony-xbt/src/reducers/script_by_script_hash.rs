/*
   Script by Script Hash

   Creates reducer output for every UTxO which maps a script hash to the
   script pubkey preimage. This allows us to store script hash instead of the
   full in most places instead of duplicating potentially large scripts, then
   we can resolve the actual script if needed.
*/

use bitcoin::{OutPoint, Transaction, TxOut, Txid, hashes::Hash};
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
    pub script_hash: [u8; 20],
    pub script: Vec<u8>,
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
            Some(u) => u,
            None => return Ok(()),
        }
        .txo;

        let script_hash = resolved_utxo.script_pubkey.script_hash();
        let script = resolved_utxo.script_pubkey.to_bytes();

        outputs.push(ReducerOutput::ScriptByScriptHash(Output {
            script_hash: script_hash.to_byte_array(),
            script,
        }));

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        tx_output: &TxOut,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let script_hash = tx_output.script_pubkey.script_hash();
        let script = tx_output.script_pubkey.to_bytes();

        outputs.push(ReducerOutput::ScriptByScriptHash(Output {
            script_hash: script_hash.to_byte_array(),
            script,
        }));

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        _height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, _) in txs {
            if !tx.is_coinbase() {
                for txin in tx.input.iter() {
                    self.process_consumed_txo(ctx, &txin.previous_output, outputs)?;
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

        super::Reducer::ScriptByScriptHash(reducer)
    }
}
