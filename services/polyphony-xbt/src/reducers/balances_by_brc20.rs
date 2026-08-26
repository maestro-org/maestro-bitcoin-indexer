/*
   Holder Total Balances by BRC20 Ticker

   Creates reducer outputs which signal increases or decreases in total balance
   of a specific BRC20 for a script.
*/

use bitcoin::{OutPoint, Transaction, Txid, hashes::Hash};
use gasket::error::AsWorkError;
use ord::{InscriptionId, ParsedEnvelope};
use serde::Deserialize;

use crate::{
    crosscut,
    model::{self, BRC20Message},
    prelude::AppliesPolicy,
};

use super::{IncrOrDecr, ReducerOutput};

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    policy: crosscut::policies::RuntimePolicy,
}

#[derive(Clone, Debug)]
pub struct Output {
    pub brc_ticker: Vec<u8>,
    pub script_hash: [u8; 20],
    pub total_delta: IncrOrDecr<u128>,
}

impl Reducer {
    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutPoint,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        if let Some(inscriptions) = ctx.utxo_inscriptions(input) {
            let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

            let utxo = match utxo {
                Some(u) => u,
                None => return Ok(()),
            };

            // check if any of the inscriptions in the input was an unused transfer
            for (_, inscription) in inscriptions {
                if let Some(brc20_actions) = ctx.inscription_brc20s(&inscription) {
                    for action in brc20_actions {
                        match action {
                            BRC20Message::Transfer(tick, amt, op, receiver) if op == *input => {
                                // decrease total balance of sender
                                outputs.push(ReducerOutput::BalancesByBrc20(Output {
                                    brc_ticker: tick.clone(),
                                    script_hash: utxo
                                        .txo
                                        .script_pubkey
                                        .script_hash()
                                        .to_byte_array(),
                                    total_delta: IncrOrDecr::Decrement(amt),
                                }));

                                // increase balance of receiver
                                outputs.push(ReducerOutput::BalancesByBrc20(Output {
                                    script_hash: receiver.to_byte_array(),
                                    brc_ticker: tick,
                                    total_delta: IncrOrDecr::Increment(amt),
                                }));
                            }
                            _ => (),
                        }
                    }
                }
            }
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
            // skip dummy input of coinbase tx
            if !tx.is_coinbase() {
                for txin in tx.input.iter().map(|x| x.previous_output) {
                    self.process_consumed_txo(&ctx, &txin, outputs)?;
                }
            }

            let envelopes = ParsedEnvelope::from_transaction(tx);

            for (idx, _) in envelopes.into_iter().enumerate() {
                let inscription_id = InscriptionId {
                    txid: *txid,
                    index: idx as u32,
                };

                if let Some(brc20_actions) = ctx.inscription_brc20s(&inscription_id) {
                    for action in brc20_actions {
                        match action {
                            BRC20Message::Deploy(_) => (),
                            BRC20Message::Mint(ticker, amt, receiver) => {
                                outputs.push(ReducerOutput::BalancesByBrc20(Output {
                                    script_hash: receiver.to_byte_array(),
                                    brc_ticker: ticker,
                                    total_delta: IncrOrDecr::Increment(amt),
                                }));
                            }
                            BRC20Message::TransferInit(..) => (),
                            BRC20Message::Transfer(..) => (),
                        }
                    }
                }
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

        super::Reducer::BalancesByBrc20(reducer)
    }
}
