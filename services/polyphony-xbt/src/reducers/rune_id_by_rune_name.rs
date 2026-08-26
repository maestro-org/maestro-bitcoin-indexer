/*
   Rune Id by Rune Name
*/

use bitcoin::{Transaction, Txid};
use ordinals::{Artifact, Rune, Runestone};
use serde::Deserialize;
use tracing::{info, warn};

use crate::constants::{GENESIS_RUNE_ID, UNCOMMON_GOODS_RUNE};
use crate::model;

use super::ReducerOutput;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    bootstrapped: bool,
}

#[derive(Clone, Debug)]
pub struct Output {
    pub rune_name: u128,
    pub rune_id: (u64, u32),
    /// If true, use SetPermanent instead of SetOnce (for genesis rune bootstrap)
    pub is_bootstrap: bool,
}

impl Reducer {
    pub fn reduce_block<'b>(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        // Bootstrap genesis rune (UNCOMMON•GOODS) on first call
        if !self.bootstrapped {
            info!("bootstrapping genesis rune name UNCOMMON•GOODS -> (1:0)");

            outputs.push(ReducerOutput::RuneIdByRuneName(Output {
                rune_name: UNCOMMON_GOODS_RUNE,
                rune_id: GENESIS_RUNE_ID,
                is_bootstrap: true,
            }));

            self.bootstrapped = true;
        }

        // filter for txs with successful etchs (includes cenotaphs)
        for (idx, (tx, txid)) in txs
            .iter()
            .enumerate()
            .filter(|(idx, _)| ctx.rune_etch_idxs.contains(&(*idx as u32)))
        {
            // try decode runestone
            let artifact = Runestone::decipher(tx);

            match artifact {
                Some(Artifact::Runestone(r)) => {
                    if let Some(etch) = r.etching {
                        let rune = etch.rune.unwrap_or(Rune::reserved(height, idx as u32));

                        outputs.push(ReducerOutput::RuneIdByRuneName(Output {
                            rune_name: rune.0,
                            rune_id: (height, idx as u32),
                            is_bootstrap: false,
                        }))
                    } else {
                        warn!("expected etching for {} but didn't find one", txid);
                    }
                }
                Some(Artifact::Cenotaph(c)) => {
                    if let Some(rune) = c.etching {
                        outputs.push(ReducerOutput::RuneIdByRuneName(Output {
                            rune_name: rune.0,
                            rune_id: (height, idx as u32),
                            is_bootstrap: false,
                        }))
                    } else {
                        warn!("expected etching for {} but didn't find one", txid);
                    }
                }
                None => {
                    warn!("expected artifact for {} but didn't find one", txid);
                }
            };
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer {
            bootstrapped: false,
        };

        super::Reducer::RuneIdByRuneName(reducer)
    }
}
