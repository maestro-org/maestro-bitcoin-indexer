/*
   Txs By Inscription

   Creates reducer outputs to signal that an inscription was either inscribed or transferred in a transaction.
*/

use super::ReducerOutput;
use crate::model;
use bitcoin::{OutPoint, Transaction, Txid, hashes::Hash};
use indexmap::IndexMap;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use timbre_xbt::reducers::txs_by_inscription::{PREFIX, PREFIX_LENGTH, SUFFIX};

#[derive(Deserialize, Debug)]
pub struct Config; // no config

pub struct Reducer;

#[derive(Clone, Debug)]
pub struct Output {
    // Bucket ID. Contains only inscriptions whose reveal tx hash prefixes match the bucket ID
    pub bucket_id: PREFIX,
    // block height
    pub height: u64,
    // map from
    // (reveal tx hash suffix, index of inscription in reveal tx)
    // to a list of activities that this transaction was involved in in this block, represented as
    // a transaction index and the activity index within that transaction
    pub activity: IndexMap<(SUFFIX, u32), Vec<(u32, u32)>>,
}

impl Reducer {
    fn activity_map_entry(
        tx_index: u32,
        counter: u32,
        reveal_tx_id: Txid,
        inscription_index: u32,
    ) -> ([u8; PREFIX_LENGTH], (SUFFIX, u32), (u32, u32)) {
        let id = reveal_tx_id.to_byte_array();
        let prefix: [u8; PREFIX_LENGTH] = id[0..PREFIX_LENGTH].try_into().unwrap(); // id is guaranteed to be a 32-byte slice
        let suffix = id[PREFIX_LENGTH..].try_into().unwrap();

        (prefix, (suffix, inscription_index), (tx_index, counter))
    }

    pub fn reduce_block(
        &mut self,
        height: u64,
        txs: &Vec<(Transaction, Txid)>,
        ctx: &model::BlockContext,
        outputs: &mut Vec<ReducerOutput>,
    ) -> Result<(), gasket::error::Error> {
        // New inscriptions may be spent as fee, in which case there is neither input nor output
        // info for them. We must therefore check for newly inscribed sats spent as fee in each tx,
        // which is precisely what `new_inscriptions_by_tx_id` is used for.
        let mut new_inscriptions_by_tx_id = HashMap::new();

        for (inscription_id, _) in ctx.get_all_new_inscriptions() {
            new_inscriptions_by_tx_id
                .entry(inscription_id.txid.to_byte_array())
                .and_modify(|x: &mut Vec<u32>| x.push(inscription_id.index))
                .or_insert(vec![inscription_id.index]);
        }

        let mut buckets: IndexMap<PREFIX, IndexMap<(SUFFIX, u32), Vec<(u32, u32)>>> =
            IndexMap::new();

        for (tx_index, (tx, tx_id)) in txs.iter().enumerate() {
            let mut counter = 0;

            // Collect inscriptions in inputs, so that we can later push inscriptions in the right
            // order (matching the order produced by `InscriptionActivityByTx`), needed for the
            // "Activity by Inscription" endpoint. The order is:
            //      1. inscriptions taken from inputs and sent to outputs,
            //      2. inscriptions inscribed in some other past tx and spent as fee, and
            //      3. newly inscribed inscription spent as fee.
            let mut input_inscriptions = HashSet::new();

            if !tx.is_coinbase() {
                for input in tx.input.iter() {
                    if let Some(inscriptions) = ctx.utxo_inscriptions(&input.previous_output) {
                        for (_, inscription_id) in inscriptions {
                            input_inscriptions.insert(inscription_id);
                        }
                    }
                }
            }

            // Used to mark inscriptions seen in outputs, so that we can later see if all new
            // inscriptions in this tx (which have no input info) were sent to an output, or
            // whether they were spent as fee (in which case they don't have output info either).
            let mut seen_in_outputs = HashSet::new();

            for (output_idx, _) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(*tx_id, output_idx as u32);
                if let Some(inscriptions) = ctx.utxo_inscriptions(&outpoint) {
                    for (_, inscription_id) in inscriptions.iter() {
                        // Remove inscription from list of inscriptions seen in inputs.
                        input_inscriptions.remove(inscription_id);

                        // Mark inscription as seen in an output.
                        seen_in_outputs.insert((inscription_id.txid, inscription_id.index));

                        let (prefix, suffix_and_index, new_activity) = Self::activity_map_entry(
                            tx_index as u32,
                            counter,
                            inscription_id.txid,
                            inscription_id.index,
                        );

                        buckets
                            .entry(prefix)
                            .and_modify(|bucket_activity| {
                                bucket_activity
                                    .entry(suffix_and_index)
                                    .and_modify(|v| v.push(new_activity))
                                    .or_insert(vec![new_activity]);
                            })
                            .or_insert({
                                let mut new_bucket = IndexMap::new();
                                new_bucket.insert(suffix_and_index, vec![new_activity]);
                                new_bucket
                            });

                        counter += 1;
                    }
                }
            }

            let mut sorted_input_inscriptions = input_inscriptions.into_iter().collect::<Vec<_>>();
            sorted_input_inscriptions.sort();

            // All items remaining in `input_inscriptions` correspond to inscriptions taken from input
            // and spent as fee.
            for inscription_id in sorted_input_inscriptions {
                let (prefix, suffix_and_index, new_activity) = Self::activity_map_entry(
                    tx_index as u32,
                    counter,
                    inscription_id.txid,
                    inscription_id.index,
                );

                buckets
                    .entry(prefix)
                    .and_modify(|bucket_activity| {
                        bucket_activity
                            .entry(suffix_and_index)
                            .and_modify(|v| v.push(new_activity))
                            .or_insert(vec![new_activity]);
                    })
                    .or_insert({
                        let mut new_bucket = IndexMap::new();
                        new_bucket.insert(suffix_and_index, vec![new_activity]);
                        new_bucket
                    });

                counter += 1;
            }

            // Add new inscriptions (no input info) that were not sent to outputs (no output info).
            if let Some(inscription_id_indexes) =
                new_inscriptions_by_tx_id.remove(&tx_id.to_byte_array())
            {
                for inscription_id_index in inscription_id_indexes {
                    if seen_in_outputs
                        .get(&(*tx_id, inscription_id_index))
                        .is_none()
                    {
                        let (prefix, suffix_and_index, new_activity) = Self::activity_map_entry(
                            tx_index as u32,
                            counter,
                            tx_id.clone(),
                            inscription_id_index,
                        );

                        buckets
                            .entry(prefix)
                            .and_modify(|bucket_activity| {
                                bucket_activity
                                    .entry(suffix_and_index)
                                    .and_modify(|v| v.push(new_activity))
                                    .or_insert(vec![new_activity]);
                            })
                            .or_insert({
                                let mut new_bucket = IndexMap::new();
                                new_bucket.insert(suffix_and_index, vec![new_activity]);
                                new_bucket
                            });

                        counter += 1;
                    }
                }
            }
        }

        for (bucket_id, activity) in buckets.into_iter() {
            outputs.push(ReducerOutput::TxsByInscription(Output {
                bucket_id,
                height,
                activity,
            }));
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer;

        super::Reducer::TxsByInscription(reducer)
    }
}
