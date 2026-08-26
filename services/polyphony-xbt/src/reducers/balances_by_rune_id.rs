/*
   Holder Balances by Rune Id

   Creates reducer outputs which signal increases or decreases in balance
   of a specific Rune Id for a script.
*/

use bitcoin::{OutPoint, Transaction, TxOut, Txid, hashes::Hash};
use gasket::error::AsWorkError;
use serde::Deserialize;

use crate::{crosscut, model, prelude::AppliesPolicy};

use super::{IncrOrDecr, ReducerOutput};

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    pub rune_id: (u64, u32),
    pub script_hash: [u8; 20],
    pub action: IncrOrDecr<u128>,
}

impl Reducer {
    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutPoint,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let Some(utxo_runes) = ctx.utxo_runes(input) else {
            return Ok(());
        };

        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let utxo = match utxo {
            Some(u) => u,
            None => return Ok(()),
        };

        for (rune, amount) in utxo_runes {
            outputs.push(ReducerOutput::BalancesByRuneId(Output {
                rune_id: (rune.block, rune.tx),
                script_hash: utxo.txo.script_pubkey.script_hash().to_byte_array(),
                action: IncrOrDecr::Decrement(amount),
            }))
        }

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        ctx: &model::BlockContext,
        outpoint: OutPoint,
        tx_output: &TxOut,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (rune, amount) in ctx.utxo_runes(&outpoint).unwrap_or_default() {
            outputs.push(ReducerOutput::BalancesByRuneId(Output {
                rune_id: (rune.block, rune.tx),
                script_hash: tx_output.script_pubkey.script_hash().to_byte_array(),
                action: IncrOrDecr::Increment(amount),
            }))
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        _height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, txid) in txs {
            if !tx.is_coinbase() {
                for txin in tx.input.iter().map(|x| x.previous_output) {
                    self.process_consumed_txo(&ctx, &txin, outputs)?;
                }
            }

            for (idx, output) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*txid, idx as u32);

                self.process_produced_txo(&ctx, outpoint, output, outputs)?;
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

        super::Reducer::BalancesByRuneId(reducer)
    }
}
