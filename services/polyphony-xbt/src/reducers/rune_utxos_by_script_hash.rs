/*
   Rune UTxOs By Script Hash

   Creates reducer outputs to signal that a UTxO contains runes, specifying whether the UTxO was
   consumed or produced, along with the specific rune IDs and amounts when produced.
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
    // Script hash.
    pub script_hash: [u8; 20],

    // Block height.
    pub height: u64,

    // Tx hash of the UTxO containing the runes.
    pub utxo_hash: [u8; 32],

    // Tx output index of the UTxO.
    pub utxo_index: u32,

    // (satoshis, ((etching block, etching tx), runes amount))
    pub action: UtxoAction<(u64, Vec<((u64, u32), u128)>)>,
}

impl Reducer {
    fn process_consumed_txo(
        &mut self,
        input: &OutPoint,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        if ctx.utxo_runes(input).is_some() {
            let resolved_utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

            let resolved_utxo = match resolved_utxo {
                Some(u) => u,
                None => return Ok(()),
            };

            outputs.push(ReducerOutput::RuneUtxosByScriptHash(Output {
                script_hash: resolved_utxo
                    .txo
                    .script_pubkey
                    .script_hash()
                    .to_byte_array(),
                height: resolved_utxo.height,
                utxo_hash: input.txid.to_byte_array(),
                utxo_index: input.vout,
                action: UtxoAction::Consumed,
            }))
        }

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        height: u64,
        tx_hash: &[u8; 32],
        output: &TxOut,
        outpoint: OutPoint,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        if let Some(runes) = ctx.utxo_runes(&outpoint) {
            outputs.push(ReducerOutput::RuneUtxosByScriptHash(Output {
                script_hash: output.script_pubkey.script_hash().to_byte_array(),
                height,
                utxo_hash: tx_hash.clone(),
                utxo_index: outpoint.vout,
                action: UtxoAction::Produced((
                    output.value.to_sat(),
                    runes
                        .into_iter()
                        .map(|(rune_id, amount)| ((rune_id.block, rune_id.tx), amount))
                        .collect(),
                )),
            }));
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
        for (tx, tx_id) in txs {
            if !tx.is_coinbase() {
                for input in tx.input.iter() {
                    self.process_consumed_txo(&input.previous_output, ctx, outputs)?;
                }
            }

            let tx_hash = tx_id.to_byte_array();

            for (output_index, output) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*tx_id, output_index as u32);

                self.process_produced_txo(height, &tx_hash, output, outpoint, ctx, outputs)?;
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

        super::Reducer::RuneUtxosByScriptHash(reducer)
    }
}
