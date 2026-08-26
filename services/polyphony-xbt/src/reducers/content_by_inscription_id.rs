/*
   Content by inscription ID

   Creates reducer outputs to store new inscriptions.
*/

use bitcoin::{Transaction, Txid, hashes::Hash};
use serde::Deserialize;

use crate::model;

use super::ReducerOutput;

use ord::{InscriptionId, ParsedEnvelope};

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    // (reveal tx hash, index of inscriptions in reveal tx)
    pub inscription_id: ([u8; 32], u32),
    // block height of the reveal tx
    pub created_at: u64,
    // global inscription number
    pub inscription_num: u64,
    // type of the content body
    pub content_type: Vec<u8>,
    // inscription content body raw data
    pub content_body: Vec<u8>,
}

impl Reducer {
    pub fn reduce_block(
        &mut self,
        block_height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        for (tx, txid) in txs.iter() {
            let envelopes = ParsedEnvelope::from_transaction(tx);

            for (idx, parsed_envelope) in envelopes.into_iter().enumerate() {
                let index = idx as u32;
                let inscription_id = InscriptionId { txid: *txid, index };

                if let Some(inscription_num) = ctx.new_inscriptions(&inscription_id) {
                    outputs.push(ReducerOutput::ContentByInscriptionId(Output {
                        inscription_id: (txid.to_byte_array(), index),
                        created_at: block_height,
                        inscription_num,
                        content_type: parsed_envelope.payload.content_type.unwrap_or_default(),
                        content_body: parsed_envelope.payload.body.unwrap_or_default(),
                    }))
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::ContentByInscriptionId(reducer)
    }
}
