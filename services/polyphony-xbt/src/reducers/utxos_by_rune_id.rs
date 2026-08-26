/*
   UTxOs By Rune ID

   Creates reducer outputs to signal that a UTxO containing at least one of the
   Rune was consumed or produced, along with the amount of the asset in the UTxO
   and the address which controls the UTxO.
*/

use bitcoin::{OutPoint, Transaction, TxOut, Txid, hashes::Hash};
use serde::Deserialize;

use crate::{crosscut, model, prelude::*};

use super::{ReducerOutput, UtxoAction};

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    pub rune_id: (u64, u32),
    pub height: u64,
    pub utxo_hash: [u8; 32],
    pub utxo_index: u32,
    pub action: UtxoAction<([u8; 20], u128, u64)>, // scriptbuf hash, amount of asset, satoshis
}

impl Reducer {
    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutPoint,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        if let Some(runes) = ctx.utxo_runes(input) {
            let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

            let utxo = match utxo {
                Some(u) => u,
                None => return Ok(()),
            };

            for (rune, _) in runes.iter() {
                outputs.push(ReducerOutput::UtxosByRuneId(Output {
                    rune_id: (rune.block, rune.tx),
                    height: utxo.height,
                    utxo_hash: input.txid.to_byte_array(),
                    utxo_index: input.vout,
                    action: UtxoAction::Consumed,
                }))
            }
        }

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        ctx: &model::BlockContext,
        outpoint: OutPoint,
        tx_output: &TxOut,
        outputs: &mut Vec<ReducerOutput>,
        height: u64,
    ) -> Result<(), gasket::error::Error> {
        let script_hash = tx_output.script_pubkey.script_hash();

        if let Some(runes) = ctx.utxo_runes(&outpoint) {
            for (rune, amount) in runes.into_iter() {
                outputs.push(ReducerOutput::UtxosByRuneId(Output {
                    rune_id: (rune.block, rune.tx),
                    height,
                    utxo_hash: outpoint.txid.to_byte_array(),
                    utxo_index: outpoint.vout,
                    action: UtxoAction::Produced((
                        script_hash.to_byte_array(),
                        amount,
                        tx_output.value.to_sat(),
                    )),
                }))
            }
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let block_txids = txs.iter().map(|x| x.1).collect::<Vec<_>>();

        // all txos which are both produced AND consumed in this block
        let chained_txos = txs
            .iter()
            .flat_map(|x| x.0.input.iter())
            .map(|x| x.previous_output)
            .filter(|x| block_txids.contains(&x.txid))
            .collect::<Vec<_>>();

        for (tx, txid) in txs {
            for txin in tx.input.iter().map(|x| x.previous_output) {
                // skip utxos produced earlier in this block and coinbase inputs
                if block_txids.contains(&txin.txid) || tx.is_coinbase() {
                    continue;
                }

                self.process_consumed_txo(&ctx, &txin, outputs)?;
            }

            for (idx, txo) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*txid, idx as u32);

                // skip utxos consumed later in the block
                if chained_txos.contains(&outpoint) {
                    continue;
                }

                self.process_produced_txo(&ctx, outpoint, txo, outputs, height)?;
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

        super::Reducer::UtxosByRuneId(reducer)
    }
}
