/*
   Inscription UTxOs by Script Hash

   Creates reducer outputs to signal that a UTxO containing inscriptions was consumed or produced,
   along with the hash of the script controlling it and the offset in the UTxO of the inscribed sat.
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
    pub script_hash: [u8; 20],
    // block height
    pub height: u64,
    pub utxo_hash: [u8; 32],
    pub utxo_index: u32,
    // satoshis amount and list of inscriptions held in the UTxO, consisting in:
    //      - offset in the UTxO of the inscribed sat,
    //      - inscription ID (reveal tx hash, index of new inscription in reveal tx)
    pub action: UtxoAction<(u64, Vec<(u64, ([u8; 32], u32))>)>,
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
        };

        let script_hash = resolved_utxo
            .txo
            .script_pubkey
            .script_hash()
            .to_byte_array();

        if ctx.utxo_inscriptions(input).is_some() {
            outputs.push(ReducerOutput::InscriptionUtxosByScriptHash(Output {
                script_hash,
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
        ctx: &model::BlockContext,
        height: u64,
        output: OutPoint,
        tx_output: &TxOut,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let script_hash = tx_output.script_pubkey.script_hash().to_byte_array();

        if let Some(inscriptions) = ctx.utxo_inscriptions(&output) {
            let inscriptions = inscriptions
                .into_iter()
                .map(|(offset, id)| (offset as u64, (id.txid.to_byte_array(), id.index)))
                .collect();

            outputs.push(ReducerOutput::InscriptionUtxosByScriptHash(Output {
                script_hash,
                height,
                utxo_hash: output.txid.to_byte_array(),
                utxo_index: output.vout,
                action: UtxoAction::Produced((tx_output.value.to_sat(), inscriptions)),
            }))
        }

        Ok(())
    }

    pub fn reduce_block(
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

                self.process_consumed_txo(ctx, &txin, outputs)?;
            }

            for (idx, txout) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*txid, idx as u32);

                // skip utxos consumed later in the block
                if chained_txos.contains(&outpoint) {
                    continue;
                }

                self.process_produced_txo(ctx, height, outpoint, txout, outputs)?;
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

        super::Reducer::InscriptionUtxosByScriptHash(reducer)
    }
}
