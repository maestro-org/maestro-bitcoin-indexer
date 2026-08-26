use std::collections::{HashMap, HashSet};

use bitcoin::consensus::Encodable;
use bitcoin::{Block, BlockHash, Txid};
use itertools::izip;
use ord::InscriptionId;
use ordinals::Height;
use rocksdb::{OptimisticTransactionDB, Transaction};
use tracing::{trace, warn};

use super::inscriptions::updater::{index_inscriptions, BRC20Message};
use super::inscriptions::Counters;
use super::{ChainDB, Error};
use crate::ordinals::{OrdinalRange, OrdinalRanges};
use crate::storage::kvtable::*;
use crate::storage::runes::updater::index_runes;
use crate::storage::{TxoBody, TxoRef};
use crate::sync::roll::UtxoResolver;
use crate::sync::BitcoinCompatibleNetwork;

// txo ref -> (txo cbor)
pub struct TxoKV;

impl KVTable<DBSerde<TxoRef>, DBSerde<TxoBody>> for TxoKV {
    const CF_NAME: &'static str = "TxoKV";
}

pub struct ResolveAndProcessResult {
    // map of input and output references to output details
    pub resolver: HashMap<TxoRef, TxoBody>,
    // indices of txs in block which had successful rune etches
    pub successful_etches: Vec<u32>,
    // indices of txs in block which had successful rune mines
    pub successful_mints: Vec<u32>,
    // tx ids so we don't need to compute them again later
    pub tx_ids: Vec<Txid>,
    // input references (excluding coinbase)
    pub input_refs: HashSet<TxoRef>,
    // information relating to resolving
    pub stats: UtxoStats,
    // tx index and inscription index for valid reinscriptions
    pub valid_reinscriptions: Vec<(u32, u32)>,
    // map of inscription id to brc20 actions
    pub brc20_resolver: Vec<(InscriptionId, BRC20Message)>,
    // new inscriptions
    pub new_inscriptions: Vec<(InscriptionId, u64)>,
}

#[derive(Debug)]
pub struct UtxoStats {
    pub not_found_in_mem: u64,
    pub found_in_mem: u64,
    // millis
    pub mem_resolve_duration: u128,
    pub db_resolve_duration: u128,
}

impl TxoKV {
    pub fn resolve_and_process(
        memory_resolver: &mut Option<UtxoResolver>,
        db: &ChainDB,
        db_tx: &mut Transaction<OptimisticTransactionDB>,
        height: u64,
        block: &Block,
        block_hash: &BlockHash,
        first_rune_height: u64,
        first_inscription_height: u64,
        jubilee_height: u64,
        network: BitcoinCompatibleNetwork,
        mut inscription_counters: &mut Counters,
    ) -> Result<ResolveAndProcessResult, Error> {
        let txs = &block.txdata;

        // compute tx ids once
        let tx_ids = txs.iter().map(|x| x.compute_txid()).collect::<Vec<_>>();

        // fetch inputs from db

        let mut input_refs = txs
            .iter()
            .skip(1) // skip coinbase
            .map(|x| &x.input)
            .flatten()
            .map(|x| x.previous_output)
            .map(|x| TxoRef(x.txid, x.vout))
            .collect::<HashSet<TxoRef>>();

        let all_input_refs = input_refs.clone();

        // remove input refs for UTxOs produced in block as they wont be in db
        input_refs.retain(|k| !tx_ids.contains(&k.0));

        let mut to_fetch_from_db = Vec::with_capacity(input_refs.len());

        let mut resolver = HashMap::with_capacity(input_refs.len()); // at least

        let mem_resolve_start = tokio::time::Instant::now();

        // populate block specific resolver using memory resolver
        if let Some(mem_resolver) = memory_resolver {
            for input_ref in input_refs {
                // its ok to remove, we will just fallback to DB if needed
                if let Some(txo_body) = mem_resolver.remove(&input_ref) {
                    resolver.insert(input_ref.clone(), txo_body);
                } else {
                    // note which inputs we need to use db in order resolve
                    to_fetch_from_db.push(input_ref);
                }
            }
        } else {
            to_fetch_from_db = input_refs.into_iter().collect();
        }

        let mem_resolve_duration = mem_resolve_start.elapsed().as_millis();

        let not_found_in_mem = to_fetch_from_db.len() as u64;
        let found_in_mem = resolver.len() as u64;

        let db_resolve_start = tokio::time::Instant::now();

        // fetch utxos we didn't have in memory from the DB
        if !to_fetch_from_db.is_empty() {
            let txo_cf = Self::cf(&db.db);

            let txos = db_tx
                .multi_get_cf(
                    to_fetch_from_db
                        .iter()
                        .map(|x| (&txo_cf, Box::<[u8]>::from(DBSerde(x.clone())))),
                )
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(Error::Rocks)?;

            let kvs = to_fetch_from_db.into_iter().zip(txos);

            let mut db_misses = vec![];

            for (txo_ref, txo) in kvs {
                if let Some(b) = txo {
                    let DBSerde(txo_body) = <DBSerde<TxoBody>>::from(Box::from(b.as_slice()));
                    resolver.insert(txo_ref, txo_body);
                } else {
                    db_misses.push(txo_ref)
                }
            }

            if resolver.is_empty() {
                warn!("resolver empty, misses: {db_misses:?}");
            }
        }

        let db_resolve_duration = db_resolve_start.elapsed().as_millis();

        let coinbase_tx = block
            .coinbase()
            .ok_or(Error::Decoding("no coinbase tx".into()))?;

        // runes

        let mut successful_etches = vec![];
        let mut successful_mints = vec![];

        // coinbase tx
        let coinbase_runes = if height >= first_rune_height {
            let runes_result = index_runes(&resolver, db, &db_tx, 0, coinbase_tx, height, network)?;

            if runes_result.successful_etch {
                successful_etches.push(0)
            }

            if runes_result.successful_mint {
                successful_mints.push(0)
            }

            runes_result.output_runes
        } else {
            vec![HashMap::new(); coinbase_tx.output.len()]
        };

        // compute ordinals created by coinbase tx
        let mut coinbase_ords = OrdinalRanges::new();

        coinbase_ords.push(OrdinalRange::new_coinbase_ords(height).map_err(Error::Ordinals)?);

        let mut cb_inscriptions = Vec::new();
        let mut reward = Height(height as u32).subsidy();
        let mut valid_reinscriptions = Vec::new();
        let mut brc20_resolver = Vec::new();
        let mut new_inscriptions = Vec::new();

        // process non-coinbase transactions
        for (idx, (tx, tx_id)) in txs.iter().zip(tx_ids.iter()).enumerate().skip(1) {
            // process runes to get runes in each tx output
            let output_runes = if height >= first_rune_height {
                let runes_result =
                    index_runes(&resolver, db, &db_tx, idx as u32, tx, height, network)?;

                if runes_result.successful_etch {
                    successful_etches.push(idx as u32)
                }

                if runes_result.successful_mint {
                    successful_mints.push(idx as u32)
                }

                runes_result.output_runes
            } else {
                vec![HashMap::new(); tx.output.len()]
            };

            // TODO: other inscriptions
            let (output_inscriptions, _other_inscriptions, new_brc20_transfers) =
                if height >= first_inscription_height {
                    let res = index_inscriptions(
                        &resolver,
                        db,
                        &db_tx,
                        tx,
                        *tx_id,
                        height,
                        jubilee_height,
                        &mut inscription_counters,
                        &mut reward,
                        &mut cb_inscriptions,
                        &mut new_inscriptions,
                    )?;

                    valid_reinscriptions
                        .extend(res.valid_reinscriptions.iter().map(|x| (idx as u32, *x)));

                    brc20_resolver.extend(res.brc20_resolver);

                    (
                        res.output_inscriptions,
                        res.lost_or_unbound_inscriptions,
                        res.new_unused_brc20_transfers,
                    )
                } else {
                    (vec![vec![]; tx.output.len()], vec![], HashMap::new())
                };

            let mut tx_ords = OrdinalRanges::new();

            for input in tx.input.iter().map(|x| x.previous_output) {
                let txo_ref = TxoRef(input.txid, input.vout);

                let txo = resolver
                    .get(&txo_ref)
                    .ok_or_else(|| Error::MissingTxo(block_hash.clone(), input.txid, input.vout))?;

                if let Some(mem_resolver) = memory_resolver {
                    // ok to remove, we can use DB if we need to
                    mem_resolver.remove(&txo_ref);
                }

                tx_ords.extend(txo.ord_ranges.clone());
            }

            for (idx, output, runes, inscriptions) in
                izip!(0.., tx.output.iter(), output_runes, output_inscriptions)
            {
                let txo_ref = TxoRef(*tx_id, idx as u32);

                // let output_sats = output.value;

                // let ord_ranges = tx_ords.take(output_sats).map_err(Error::Ordinals)?;

                let ord_ranges = OrdinalRanges::new();

                let mut body = vec![];
                output
                    .consensus_encode(&mut body)
                    .map_err(|err| Error::BitcoinEncode(err.into()))?;

                let runes = runes.into_iter().map(|(x, y)| (x.into(), y)).collect();

                let inscids = inscriptions.iter().map(|(_, x)| x).collect::<Vec<_>>();

                let mut unused_brc20_transfers = new_brc20_transfers.clone();

                // filter unused brc20 transfers to inscriptions in this output
                unused_brc20_transfers.retain(|k, _| inscids.contains(&k));

                let txo_body = TxoBody {
                    height,
                    raw: body,
                    ord_ranges,
                    runes,
                    inscriptions,
                    unused_brc20_transfers,
                };

                resolver.insert(txo_ref.clone(), txo_body.clone());

                if let Some(mem_resolver) = memory_resolver {
                    mem_resolver.insert(txo_ref, txo_body);
                }
            }

            // extend coinbase ords with remaining ordinals from inputs (tx fee)
            coinbase_ords.extend(tx_ords);
        }

        let (cb_inscriptions, _cb_other_inscriptions, cb_brc_transfers) =
            if height >= first_inscription_height {
                let res = index_inscriptions(
                    &resolver,
                    db,
                    &db_tx,
                    coinbase_tx,
                    tx_ids[0],
                    height,
                    jubilee_height,
                    &mut inscription_counters,
                    &mut reward,
                    &mut cb_inscriptions,
                    &mut new_inscriptions,
                )?;

                valid_reinscriptions.extend(res.valid_reinscriptions.iter().map(|x| (0u32, *x)));

                brc20_resolver.extend(res.brc20_resolver);

                (
                    res.output_inscriptions,
                    res.lost_or_unbound_inscriptions,
                    res.new_unused_brc20_transfers,
                )
            } else {
                (
                    vec![vec![]; coinbase_tx.output.len()],
                    vec![],
                    HashMap::new(),
                )
            };

        for (idx, output, runes, inscriptions) in izip!(
            0..,
            coinbase_tx.output.iter(),
            coinbase_runes,
            cb_inscriptions
        ) {
            let txo_ref = TxoRef(tx_ids[0], idx as u32);

            // let output_sats = output.value;

            // let ord_ranges = coinbase_ords.take(output_sats).map_err(Error::Ordinals)?;

            let ord_ranges = OrdinalRanges::new(); // TODO

            let mut body = vec![];
            output
                .consensus_encode(&mut body)
                .map_err(|err| Error::BitcoinEncode(err.into()))?;

            let runes = runes.into_iter().map(|(x, y)| (x.into(), y)).collect();

            let inscids = inscriptions.iter().map(|(_, x)| x).collect::<Vec<_>>();

            let mut unused_brc20_transfers = cb_brc_transfers.clone();

            // filter unused brc20 transfers to inscriptions in this output
            unused_brc20_transfers.retain(|k, _| inscids.contains(&k));

            let txo_body = TxoBody {
                height,
                raw: body,
                ord_ranges,
                runes,
                inscriptions,
                unused_brc20_transfers,
            };

            resolver.insert(txo_ref.clone(), txo_body.clone());

            if let Some(mem_resolver) = memory_resolver {
                mem_resolver.insert(txo_ref.clone(), txo_body.clone());
            }
        }

        let stats = UtxoStats {
            not_found_in_mem,
            found_in_mem,
            mem_resolve_duration,
            db_resolve_duration,
        };

        if !brc20_resolver.is_empty() {
            trace!("brc20: {brc20_resolver:?}")
        }

        Ok(ResolveAndProcessResult {
            resolver,
            successful_etches,
            successful_mints,
            tx_ids,
            input_refs: all_input_refs,
            stats,
            valid_reinscriptions,
            brc20_resolver,
            new_inscriptions,
        })
    }
}
