/*
   Etching by Rune Id

   Creates reducer output containing details of the etching for each Rune Id
*/

use bitcoin::{Transaction, Txid, hashes::Hash};
use ordinals::{Artifact, Runestone};
use serde::Deserialize;
use tracing::{info, warn};

use crate::constants::{
    GENESIS_RUNE_ID, UNCOMMON_GOODS_END_HEIGHT, UNCOMMON_GOODS_RUNE, UNCOMMON_GOODS_SPACERS,
    UNCOMMON_GOODS_START_HEIGHT, UNCOMMON_GOODS_SYMBOL,
};
use crate::model;

use super::ReducerOutput;

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer {
    bootstrapped: bool,
}

#[derive(Debug, Clone)]
pub struct EtchingInfo {
    pub name: Option<u128>, // TODO: what is the reserved name is etching omits name
    pub spacers: Option<u32>,
    pub symbol: Option<u32>, // char as u32
    pub divisibility: Option<u8>,
    pub premine: Option<u128>,
    pub max_mint_txs: Option<u128>,
    pub amount_per_mint: Option<u128>,
    pub start_height: Option<u64>,
    pub end_height: Option<u64>,
    pub start_offset: Option<u64>,
    pub end_offset: Option<u64>,
    pub turbo: bool,
}

#[derive(Clone, Debug)]
pub struct Output {
    pub rune_id: (u64, u32),
    pub tx_hash: [u8; 32],
    pub etching: EtchingInfo,
    pub cenotaph: bool,
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
            info!("bootstrapping genesis rune UNCOMMON•GOODS (1:0)");

            outputs.push(ReducerOutput::EtchingByRuneId(Output {
                rune_id: GENESIS_RUNE_ID,
                tx_hash: [0u8; 32], // no etching transaction
                etching: EtchingInfo {
                    name: Some(UNCOMMON_GOODS_RUNE),
                    spacers: Some(UNCOMMON_GOODS_SPACERS),
                    symbol: Some(UNCOMMON_GOODS_SYMBOL as u32),
                    divisibility: Some(0),
                    premine: Some(0),
                    max_mint_txs: Some(u128::MAX),
                    amount_per_mint: Some(1),
                    start_height: Some(UNCOMMON_GOODS_START_HEIGHT),
                    end_height: Some(UNCOMMON_GOODS_END_HEIGHT),
                    start_offset: None,
                    end_offset: None,
                    turbo: false,
                },
                cenotaph: false,
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

            let mut cenotaph = false;

            let info = match artifact {
                Some(Artifact::Runestone(r)) => {
                    if let Some(etch) = r.etching {
                        Some(EtchingInfo {
                            name: etch.rune.map(|x| x.0),
                            spacers: etch.spacers,
                            symbol: etch.symbol.map(|x| x.into()),
                            divisibility: etch.divisibility,
                            premine: etch.premine,
                            max_mint_txs: etch.terms.map(|x| x.cap).flatten(),
                            amount_per_mint: etch.terms.map(|x| x.amount).flatten(),
                            start_height: etch.terms.map(|x| x.height.0).flatten(),
                            end_height: etch.terms.map(|x| x.height.1).flatten(),
                            start_offset: etch.terms.map(|x| x.offset.0).flatten(),
                            end_offset: etch.terms.map(|x| x.offset.1).flatten(),
                            turbo: etch.turbo,
                        })
                    } else {
                        warn!("expected etching for {} but didn't find one", txid);
                        None
                    }
                }
                Some(Artifact::Cenotaph(c)) => {
                    if let Some(rune) = c.etching {
                        cenotaph = true;

                        Some(EtchingInfo {
                            name: Some(rune.0), // for cenotaphs ord library does not differentiate between 'etching present with no rune name' and 'no etching present'?
                            spacers: Some(0),
                            symbol: None,
                            divisibility: Some(0),
                            premine: Some(0),
                            max_mint_txs: None,
                            amount_per_mint: None,
                            start_height: None,
                            end_height: None,
                            start_offset: None,
                            end_offset: None,
                            turbo: false,
                        })
                    } else {
                        warn!("expected etching for {} but didn't find one", txid);
                        None
                    }
                }
                None => {
                    warn!("expected artifact for {} but didn't find one", txid);
                    None
                }
            };

            if let Some(info) = info {
                outputs.push(ReducerOutput::EtchingByRuneId(Output {
                    rune_id: (height, idx as u32),
                    tx_hash: txid.to_byte_array(),
                    etching: info,
                    cenotaph,
                    is_bootstrap: false,
                }))
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer {
            bootstrapped: false,
        };

        super::Reducer::EtchingByRuneId(reducer)
    }
}
