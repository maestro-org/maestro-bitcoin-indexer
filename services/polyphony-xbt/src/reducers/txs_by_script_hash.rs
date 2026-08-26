/*
   Txs By Script Hash

   Creates reducer outputs to signal that a UTxO was controlled by the script buf
   was consumed or produced by a transaction.
*/

use std::collections::HashMap;

use bitcoin::{OutPoint, ScriptHash, Transaction, TxOut, Txid, hashes::Hash};
use serde::Deserialize;

use crate::{crosscut, model, prelude::*};

use super::ReducerOutput;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(PartialEq)]
enum TxInvolvement {
    Input,
    Output,
}

#[derive(Clone, Debug)]
pub struct Output {
    pub script_hash: [u8; 20], // hash of scriptbuf involved in tx
    pub height: u64,
    pub tx_hash: [u8; 32],
    pub address_tx_index: u32,
    pub input: bool,
    pub output: bool,
}

impl Reducer {
    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutPoint,
        seen: &mut HashMap<ScriptHash, Vec<TxInvolvement>>,
    ) -> Result<(), gasket::error::Error> {
        let resolved_utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let resolved_utxo = match resolved_utxo {
            Some(u) => u,
            None => return Ok(()),
        };

        let script_hash = resolved_utxo.txo.script_pubkey.script_hash();

        seen.entry(script_hash)
            .or_default()
            .push(TxInvolvement::Input);

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        tx_output: &TxOut,
        seen: &mut HashMap<ScriptHash, Vec<TxInvolvement>>,
    ) -> Result<(), gasket::error::Error> {
        let script_hash = tx_output.script_pubkey.script_hash();

        seen.entry(script_hash)
            .or_default()
            .push(TxInvolvement::Output);

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let mut address_counters: HashMap<ScriptHash, u32> = HashMap::new();

        for (tx, txid) in txs.iter() {
            let mut seen: HashMap<ScriptHash, Vec<TxInvolvement>> = HashMap::new();

            // skip dummy input of coinbase tx
            if !tx.is_coinbase() {
                for txin in tx.input.iter().map(|x| x.previous_output) {
                    self.process_consumed_txo(&ctx, &txin, &mut seen)?;
                }
            }

            for txo in tx.output.iter() {
                self.process_produced_txo(txo, &mut seen)?;
            }

            for (script_hash, actions) in seen {
                let input = actions.contains(&TxInvolvement::Input);
                let output = actions.contains(&TxInvolvement::Output);

                let address_tx_index = address_counters.entry(script_hash).or_default();

                outputs.push(ReducerOutput::TxsByScriptHash(Output {
                    script_hash: script_hash.to_byte_array(),
                    height,
                    tx_hash: txid.to_byte_array(),
                    address_tx_index: address_tx_index.clone(),
                    input,
                    output,
                }));

                *address_tx_index += 1;
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

        super::Reducer::TxsByScriptHash(reducer)
    }
}
