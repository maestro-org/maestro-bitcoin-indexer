/*
   Transaction Info

   Creates reducer outputs with information about a transaction, including a flag to indicate if the transaction involved metaprotocols.
*/

use super::ReducerOutput;
use crate::{crosscut, model, prelude::*};
use bitcoin::{Block, OutPoint, Transaction, Txid, hashes::Hash};
use ordinals::Runestone;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

pub type Inscription = (u64, ([u8; 32], u32));
pub type Rune = ((u64, u32), u128);

#[derive(Clone, Debug)]
pub struct Output {
    // Tx hash
    pub tx_hash: [u8; 32],
    // Block height
    pub block_height: u64,
    // Block hash. `None` variant used for mempool blocks.
    pub block_hash: Option<[u8; 32]>,
    // Block timestamp.  `None` variant used for mempool blocks.
    pub timestamp: Option<u32>,
    // Volume of satoshis in tx, minus fees
    pub volume: u128,
    pub fees: u64,
    pub sats_per_vb: u64,
    // Whether any of the inputs or outputs of the transaction involves inscriptions.
    pub involves_inscriptions: bool,
    // Whether any of the inputs or outputs of the transaction involves runes.
    pub involves_runes: bool,
    // Whether the transaction involves BRC-20.
    pub involves_brc20: bool,
    // (UTxO tx hash, UTxO vout, address script hash, satoshis)
    pub ins: Vec<([u8; 32], u32, [u8; 20], u64, Vec<Inscription>, Vec<Rune>)>,
    // (output index, address script hash, satoshis)
    pub outs: Vec<([u8; 20], u64, Vec<Inscription>, Vec<Rune>)>,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        block: &Option<Block>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        let (block_hash, timestamp) = match block {
            Some(b) => (Some(b.block_hash().to_byte_array()), Some(b.header.time)),
            None => (None, None),
        };

        for (tx, tx_hash) in txs.iter() {
            let mut involves_inscriptions = false;
            // Check transaction's Runestone for involvement in runes metaprotocol.
            let mut involves_runes = Runestone::decipher(tx)
                .map(|artifact| match artifact {
                    ordinals::Artifact::Runestone(Runestone {
                        edicts,
                        etching,
                        mint,
                        ..
                    }) => !edicts.is_empty() || etching.is_some() || mint.is_some(),
                    _ => false,
                })
                .unwrap_or(false);

            let mut involves_brc20 = false;

            let mut ins: Vec<([u8; 32], u32, [u8; 20], u64, Vec<Inscription>, Vec<Rune>)> = vec![];
            let mut total_input = 0u128;

            // coinbase transactions have no inputs
            if !tx.is_coinbase() {
                for input in tx.input.iter() {
                    let outpoint = input.previous_output;

                    let resolved_utxo = ctx
                        .find_utxo(&outpoint)
                        .apply_policy(&self.policy)
                        .or_panic()?;

                    let resolved_utxo = match resolved_utxo {
                        Some(u) => u,
                        None => return Ok(()),
                    };

                    let sats = resolved_utxo.txo.value.to_sat();
                    total_input += sats as u128;

                    let inscriptions = ctx
                        .utxo_inscriptions(&outpoint)
                        .map(|inscriptions| {
                            involves_inscriptions = true;
                            let mut res = vec![];
                            for (offset, id) in inscriptions.iter() {
                                if let Some(brc20s) = ctx.inscription_brc20s(&id) {
                                    involves_brc20 |= !brc20s.is_empty();
                                }
                                res.push((*offset as u64, (id.txid.to_byte_array(), id.index)));
                            }
                            res
                        })
                        .unwrap_or_default();

                    let runes = ctx
                        .utxo_runes(&outpoint)
                        .map(|runes| {
                            involves_runes = true;

                            runes
                                .into_iter()
                                .map(|(id, amount)| ((id.block, id.tx), amount))
                                .collect()
                        })
                        .unwrap_or_default();

                    ins.push((
                        *input.previous_output.txid.as_ref(),
                        input.previous_output.vout,
                        resolved_utxo
                            .txo
                            .script_pubkey
                            .script_hash()
                            .to_byte_array(),
                        sats,
                        inscriptions,
                        runes,
                    ));
                }
            }

            let mut outs: Vec<([u8; 20], u64, Vec<Inscription>, Vec<Rune>)> = vec![];
            let mut total_output = 0u128;

            for (output_index, output) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*tx_hash, output_index as u32);

                // Update total_output.
                let sats = output.value.to_sat();

                total_output += sats as u128;

                let inscriptions = ctx
                    .utxo_inscriptions(&outpoint)
                    .map(|inscriptions| {
                        involves_inscriptions = true;
                        let mut res = vec![];
                        for (offset, id) in inscriptions.iter() {
                            if let Some(brc20s) = ctx.inscription_brc20s(&id) {
                                involves_brc20 |= !brc20s.is_empty();
                            }
                            res.push((*offset as u64, (id.txid.to_byte_array(), id.index)));
                        }
                        res
                    })
                    .unwrap_or_default();

                let runes = ctx
                    .utxo_runes(&outpoint)
                    .map(|runes| {
                        involves_runes = true;

                        runes
                            .into_iter()
                            .map(|(id, amount)| ((id.block, id.tx), amount))
                            .collect()
                    })
                    .unwrap_or_default();

                outs.push((
                    output.script_pubkey.script_hash().to_byte_array(),
                    sats,
                    inscriptions,
                    runes,
                ));
            }

            let fees = total_input.saturating_sub(total_output) as u64;

            outputs.push(ReducerOutput::TxInfo(Output {
                tx_hash: tx_hash.to_byte_array(),
                block_height: height,
                block_hash,
                timestamp,
                volume: total_output,
                fees,
                sats_per_vb: (fees as u64 + tx.vsize() as u64 - 1)
                    .checked_div(tx.vsize() as u64)
                    .unwrap_or(0),
                involves_inscriptions,
                involves_runes,
                involves_brc20,
                ins,
                outs,
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

        super::Reducer::TxInfo(reducer)
    }
}
