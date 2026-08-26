/*
    Transfer inscriptions by script hash

    Creates reducer outputs to signal that a transfer inscription was consumed or produced. When
    produced, we additionally store:
        - number of BRC20 tokens locked in the transfer inscription,
        - number of sats locked in the transfer inscription,
        - the UTxO ref containing the inscribed sat,
        - the offset of the inscribed sat in this UTxO, and
        - the block height of this UTxO.
*/

use bitcoin::{OutPoint, Transaction, Txid, hashes::Hash};
use ord::{InscriptionId, ParsedEnvelope};
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
    // UTF-8 encoding of the inscription ticker.
    pub ticker: Vec<u8>,
    // Inscription ID: (hash of reveal tx, index of new inscription in reveal tx).
    pub inscription_id: ([u8; 32], u32),
    // (
    //      token amount locked in the transfer inscription,
    //      sat amount locked in the transfer inscription,
    //      UTxO ref,
    //      sat offset in UTxO,
    //      block height,
    // )
    pub action: UtxoAction<(u128, u64, ([u8; 32], u32), u64, u64)>,
}

impl Reducer {
    fn process_consumed_txo(
        &mut self,
        tx: &Transaction,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for input in tx.input.iter().map(|x| x.previous_output) {
            for (_, id) in ctx.utxo_inscriptions(&input).unwrap_or_default() {
                for action in ctx.inscription_brc20s(&id).unwrap_or_default() {
                    if let model::BRC20Message::Transfer(ticker, _, initial_utxo, _) = action {
                        // check whether this is the first time the inscription is spent
                        if input == initial_utxo {
                            let resolved_utxo = ctx
                                .find_utxo(&input)
                                .apply_policy(&self.policy)
                                .or_panic()?;
                            let resolved_utxo = match resolved_utxo {
                                Some(u) => u,
                                None => return Ok(()),
                            };
                            let script_hash = resolved_utxo
                                .txo
                                .script_pubkey
                                .script_hash()
                                .to_byte_array();
                            let inscription_id = (id.txid.to_byte_array(), id.index);

                            outputs.push(ReducerOutput::TransferInscriptionsByScriptHash(Output {
                                script_hash,
                                ticker,
                                inscription_id,
                                action: UtxoAction::Consumed,
                            }))
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        tx: &Transaction,
        tx_id: &Txid,
        height: u64,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        // Extract inscriptions inscribed in this tx.
        let envelopes = ParsedEnvelope::from_transaction(tx);

        for (inscription_index, _) in envelopes.into_iter().enumerate() {
            let id = InscriptionId {
                txid: *tx_id,
                index: inscription_index as u32,
            };

            // Search for BRC-20 messages associated with the inscription.
            for action in ctx.inscription_brc20s(&id).unwrap_or_default() {
                // Try and parse the BRC-20 message as a `TransferInit`.
                if let model::BRC20Message::TransferInit(ticker, amt, receiver) = action {
                    // Try and find the output controlling the inscription found via
                    // `ParsedEnvelope::from_transaction`.
                    for (output_index, output) in tx.output.iter().enumerate() {
                        let outpoint = OutPoint::new(*tx_id, output_index as u32);

                        for (sat_offset, inscription_id) in
                            ctx.utxo_inscriptions(&outpoint).unwrap_or_default()
                        {
                            if id == inscription_id {
                                outputs.push(ReducerOutput::TransferInscriptionsByScriptHash(
                                    Output {
                                        script_hash: receiver.to_byte_array(),
                                        ticker: ticker.clone(),
                                        inscription_id: (id.txid.to_byte_array(), id.index),
                                        action: UtxoAction::Produced((
                                            amt,
                                            output.value.to_sat(),
                                            (tx_id.to_byte_array(), output_index as u32),
                                            sat_offset as u64,
                                            height,
                                        )),
                                    },
                                ));
                            }
                        }
                    }
                }
            }
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
        for (tx, tx_id) in txs.iter() {
            // Process inputs, skipping coinbase tx.
            if !tx.is_coinbase() {
                self.process_consumed_txo(tx, ctx, outputs)?;
            }

            // Process outputs.
            self.process_produced_txo(tx, tx_id, height, ctx, outputs)?;
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            policy: policy.clone(),
        };

        super::Reducer::TransferInscriptionsByScriptHash(reducer)
    }
}
