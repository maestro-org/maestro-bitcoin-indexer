/*
   Mints by Rune Id

   Creates reducer output which signals that a transaction contained a Runestone
   which minted a Rune, so we can increment a total mints by rune ID counter.
*/

use bitcoin::{Transaction, Txid};
use ordinals::Runestone;
use serde::Deserialize;
use tracing::warn;

use crate::model;

use super::ReducerOutput;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    pub rune_id: (u64, u32),
}

impl Reducer {
    pub fn reduce_block<'b>(
        &mut self,
        _height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        // filter for txs with successful mints (meaning OK according to terms), including cenotaphs
        for (_, (tx, txid)) in txs
            .iter()
            .enumerate()
            .filter(|(idx, _)| ctx.rune_mint_idxs.contains(&(*idx as u32)))
        {
            // try decode runestone
            let artifact = Runestone::decipher(tx);

            let minted_id = match artifact {
                Some(a) => {
                    if let Some(id) = a.mint() {
                        Some((id.block, id.tx))
                    } else {
                        warn!("expected mint for {} but didn't find one", txid);
                        None
                    }
                }
                None => {
                    warn!("expected artifact for {} but didn't find one", txid);
                    None
                }
            };

            if let Some(rune_id) = minted_id {
                outputs.push(ReducerOutput::MintsByRuneId(Output { rune_id }))
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::MintsByRuneId(reducer)
    }
}
