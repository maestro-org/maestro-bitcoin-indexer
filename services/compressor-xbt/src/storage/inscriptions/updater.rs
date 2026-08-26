use std::{
    cmp::min,
    collections::{BTreeMap, HashMap, HashSet},
};

use bitcoin::{consensus::Decodable, hashes::Hash, BlockHash, OutPoint, ScriptHash, TxOut, Txid};
use ord::{InscriptionId, ParsedEnvelope};
use ordinals::{Height, SatPoint};
use rocksdb::{OptimisticTransactionDB, Transaction};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, trace, warn};

use crate::storage::{
    BlockHeight, ChainDB, DBBytes, DBSerde, DBUInt128, Error, KVTable, TxoBody, TxoRef,
};

use super::{
    brc20::{self, DeployAction, MintAction, TransferAction},
    BRC20Balances, BRC20Terms, Counters, CursedOrVindicatedByInscriptionId, ScriptAndBRC20Kind,
    SupplyByBRC20, TermsByBRC20Ticker,
};

fn unbound_outpoint() -> OutPoint {
    OutPoint {
        txid: Hash::all_zeros(),
        vout: 0,
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Curse {
    DuplicateField,
    IncompleteField,
    NotAtOffsetZero,
    NotInFirstInput,
    Pointer,
    Pushnum,
    Reinscription,
    Stutter,
    UnrecognizedEvenField,
}

#[derive(Debug, Clone)]
pub struct Flotsam {
    pub inscription_id: InscriptionId,
    pub offset: u64,
    pub origin: Origin,
}

#[derive(Debug, Clone)]
pub enum Origin {
    New {
        cursed: bool,
        fee: u64,
        hidden: bool,
        parents: Vec<InscriptionId>,
        pointer: Option<u64>,
        reinscription: bool,
        unbound: bool,
        vindicated: bool,
    },
    Old {
        old_satpoint: SatPoint,
    },
}

pub struct IndexInscriptionsResult {
    // for each output, vec of inscriptions and their offsets in the output
    pub output_inscriptions: Vec<Vec<(u64, InscriptionId)>>,
    // for each output, vec of inscriptions and their offsets in the output
    pub lost_or_unbound_inscriptions: Vec<(SatPoint, InscriptionId)>,
    // reinscriptions indexes which were allowed
    pub valid_reinscriptions: Vec<u32>,
    pub brc20_resolver: Vec<(InscriptionId, BRC20Message)>,
    pub new_unused_brc20_transfers: HashMap<InscriptionId, (Vec<u8>, u128)>,
}

pub fn index_inscriptions(
    resolver: &HashMap<TxoRef, TxoBody>,
    chain_db: &ChainDB,
    db_tx: &Transaction<OptimisticTransactionDB>,
    tx: &bitcoin::Transaction,
    txid: Txid,
    height: BlockHeight,
    jubilee_height: BlockHeight,
    counters: &mut Counters,
    reward: &mut u64,
    mut cb_inscriptions: &mut Vec<Flotsam>,
    new_inscriptions: &mut Vec<(InscriptionId, u64)>,
) -> Result<IndexInscriptionsResult, Error> {
    let mut outgoing_inscriptions = Vec::new();
    let mut valid_reinscriptions = Vec::new();

    let mut outpoint_to_script = HashMap::with_capacity(tx.output.len());
    let mut brc_20_inscriptions = Vec::new();
    let mut unused_brc20_transfers = HashMap::new();
    let mut new_unused_brc20_transfers = HashMap::new();
    let mut valid_brc20s = Vec::new();

    let mut floating_inscriptions = Vec::new();
    let mut id_counter = 0;
    let mut inscribed_offsets = BTreeMap::new();
    let jubilant = height >= jubilee_height;
    let mut total_input_value = 0;
    let total_output_value = tx
        .output
        .iter()
        .map(|txout| txout.value.to_sat())
        .sum::<u64>();

    let envelopes = ParsedEnvelope::from_transaction(tx);
    let mut envelopes = envelopes.into_iter().peekable();

    // process inputs, adding inscriptions contained in the inputs and new
    // inscriptions to the floating inscriptions
    for (input_index, txin) in tx.input.iter().enumerate() {
        // skip subsidy since no inscriptions possible
        if txin.previous_output.is_null() {
            total_input_value += Height(height as u32).subsidy();
            continue;
        }

        let txo_ref = TxoRef(txin.previous_output.txid, txin.previous_output.vout as u32);

        let txo_body = match resolver.get(&txo_ref) {
            Some(x) => x,
            None => {
                error!(
                    "missing {txo_ref:?} for {:?} with resolver: {:?}",
                    tx.compute_txid(),
                    resolver
                );
                return Err(Error::MissingTxo(
                    BlockHash::from_byte_array([0; 32]),
                    txo_ref.0,
                    txo_ref.1,
                ));
            }
        };

        let txout = TxOut::consensus_decode(&mut &txo_body.raw[..]).unwrap();

        for (offset, inscription_id) in txo_body.inscriptions.clone() {
            let old_satpoint = SatPoint {
                outpoint: txin.previous_output,
                offset,
            };

            let offset = total_input_value + offset;
            floating_inscriptions.push(Flotsam {
                offset,
                inscription_id,
                origin: Origin::Old { old_satpoint },
            });

            inscribed_offsets
                .entry(offset)
                .or_insert((inscription_id, 0))
                .1 += 1;

            if let Some((tick, amt)) = txo_body.unused_brc20_transfers.get(&inscription_id) {
                unused_brc20_transfers.insert(
                    inscription_id,
                    (tick, amt, txout.script_pubkey.script_hash(), old_satpoint),
                );
            }
        }

        let offset = total_input_value;

        total_input_value += txout.value.to_sat();

        // go through all inscriptions for this input
        while let Some(inscription) = envelopes.peek() {
            if inscription.input != u32::try_from(input_index).unwrap() {
                break;
            }

            let inscription_id = InscriptionId {
                txid,
                index: id_counter,
            };

            let curse = if inscription.payload.unrecognized_even_field {
                Some(Curse::UnrecognizedEvenField)
            } else if inscription.payload.duplicate_field {
                Some(Curse::DuplicateField)
            } else if inscription.payload.incomplete_field {
                Some(Curse::IncompleteField)
            } else if inscription.input != 0 {
                Some(Curse::NotInFirstInput)
            } else if inscription.offset != 0 {
                Some(Curse::NotAtOffsetZero)
            } else if inscription.payload.pointer.is_some() {
                Some(Curse::Pointer)
            } else if inscription.pushnum {
                Some(Curse::Pushnum)
            } else if inscription.stutter {
                Some(Curse::Stutter)
            } else if let Some((id, count)) = inscribed_offsets.get(&offset) {
                if *count > 1 {
                    Some(Curse::Reinscription)
                } else {
                    let initial_inscription_was_cursed_or_vindicated =
                        CursedOrVindicatedByInscriptionId::get_by_key(
                            &chain_db.db,
                            db_tx,
                            DBSerde(id.clone()),
                        )?
                        .is_some();

                    // allow reinscription if the initial inscription was cursed or vindicated
                    if initial_inscription_was_cursed_or_vindicated {
                        valid_reinscriptions.push(inscription_id.index);

                        None
                    } else {
                        Some(Curse::Reinscription)
                    }
                }
            } else {
                None
            };

            let offset = inscription
                .payload
                .pointer()
                .filter(|&pointer| pointer < total_output_value)
                .unwrap_or(offset);

            let unbound = txout.value.to_sat() == 0
                || curse == Some(Curse::UnrecognizedEvenField)
                || inscription.payload.unrecognized_even_field;

            if let Some(m) = brc20::unparsed_message(&inscription.payload) {
                if !unbound {
                    brc_20_inscriptions.push((inscription_id, m))
                }
            }

            floating_inscriptions.push(Flotsam {
                inscription_id,
                offset,
                origin: Origin::New {
                    cursed: curse.is_some() && !jubilant,
                    fee: 0,
                    hidden: inscription.payload.hidden(),
                    parents: inscription.payload.parents(),
                    pointer: inscription.payload.pointer(),
                    reinscription: inscribed_offsets.contains_key(&offset),
                    unbound,
                    vindicated: curse.is_some() && jubilant,
                },
            });

            inscribed_offsets
                .entry(offset)
                .or_insert((inscription_id, 0))
                .1 += 1;

            envelopes.next();
            id_counter += 1;
        }
    }

    // remove invalid parents from floating new inscriptions
    let potential_parents = floating_inscriptions
        .iter()
        .map(|flotsam| flotsam.inscription_id)
        .collect::<HashSet<InscriptionId>>();

    for flotsam in &mut floating_inscriptions {
        if let Flotsam {
            origin:
                Origin::New {
                    parents: purported_parents,
                    ..
                },
            ..
        } = flotsam
        {
            let mut seen = HashSet::new();
            purported_parents
                .retain(|parent| seen.insert(*parent) && potential_parents.contains(parent));
        }
    }

    // still have to normalize over inscription size
    for flotsam in &mut floating_inscriptions {
        if let Flotsam {
            origin: Origin::New { ref mut fee, .. },
            ..
        } = flotsam
        {
            *fee = (total_input_value - total_output_value) / u64::from(id_counter);
        }
    }

    let is_coinbase = tx
        .input
        .first()
        .map(|tx_in| tx_in.previous_output.is_null())
        .unwrap_or_default();

    if is_coinbase {
        floating_inscriptions.append(&mut cb_inscriptions);
    }

    floating_inscriptions.sort_by_key(|flotsam| flotsam.offset);
    let mut inscriptions = floating_inscriptions.into_iter().peekable();

    let mut range_to_vout = BTreeMap::new();
    let mut new_locations = Vec::new();
    let mut output_value = 0;
    for (vout, tx_out) in tx.output.iter().enumerate() {
        outpoint_to_script.insert(
            OutPoint {
                txid,
                vout: vout as u32,
            },
            tx_out.script_pubkey.script_hash(),
        );

        let end = output_value + tx_out.value.to_sat();

        while let Some(flotsam) = inscriptions.peek() {
            if flotsam.offset >= end {
                break;
            }

            let new_satpoint = SatPoint {
                outpoint: OutPoint {
                    txid,
                    vout: vout.try_into().unwrap(),
                },
                offset: flotsam.offset - output_value,
            };

            new_locations.push((new_satpoint, inscriptions.next().unwrap()));
        }

        range_to_vout.insert((output_value, end), vout.try_into().unwrap());

        output_value = end;
    }

    for (new_satpoint, mut flotsam) in new_locations.into_iter() {
        let new_satpoint = match flotsam.origin {
            Origin::New {
                pointer: Some(pointer),
                ..
            } if pointer < output_value => {
                match range_to_vout.iter().find_map(|((start, end), vout)| {
                    (pointer >= *start && pointer < *end).then(|| (vout, pointer - start))
                }) {
                    Some((vout, offset)) => {
                        flotsam.offset = pointer;
                        SatPoint {
                            outpoint: OutPoint { txid, vout: *vout },
                            offset,
                        }
                    }
                    _ => new_satpoint,
                }
            }
            _ => new_satpoint,
        };

        update_inscription_location(
            chain_db,
            db_tx,
            flotsam,
            new_satpoint,
            counters,
            &mut outgoing_inscriptions,
            new_inscriptions,
        )?;
    }

    if is_coinbase {
        for flotsam in inscriptions {
            let new_satpoint = SatPoint {
                outpoint: OutPoint::null(),
                offset: counters.lost_sats + flotsam.offset - output_value,
            };

            update_inscription_location(
                chain_db,
                db_tx,
                flotsam,
                new_satpoint,
                counters,
                &mut outgoing_inscriptions,
                new_inscriptions,
            )?;
        }

        counters.lost_sats += *reward - output_value;
    } else {
        // if unused brc20 transfer inscription lost as fee, return brc20 to sender
        for flotsam in inscriptions.clone() {
            if let Some((tick, amt, sender, original_point)) =
                unused_brc20_transfers.get(&flotsam.inscription_id)
            {
                let receiver = sender;

                info!(
                    "found unused brc20transfer as fee {:?} {}",
                    &(tick, amt, sender, original_point),
                    hex::encode(receiver.to_byte_array())
                );

                let balance_key = DBSerde(ScriptAndBRC20Kind {
                    script: receiver.to_byte_array(),
                    brc20_ticker: tick.to_vec(),
                });

                let old_balance =
                    BRC20Balances::get_by_key(&chain_db.db, db_tx, balance_key.clone())?
                        .map(|DBUInt128(x)| x)
                        .unwrap_or_default();

                let new_balance = old_balance + **amt;

                BRC20Balances::stage_upsert(
                    &chain_db.db,
                    balance_key,
                    DBUInt128(new_balance),
                    db_tx,
                )?;

                valid_brc20s.push((
                    flotsam.inscription_id.clone(),
                    BRC20Message::Transfer(
                        tick.to_vec(),
                        **amt,
                        original_point.outpoint.clone(),
                        *receiver,
                    ),
                ))
            }
        }

        cb_inscriptions.extend(inscriptions.map(|flotsam| Flotsam {
            offset: *reward + flotsam.offset - output_value,
            ..flotsam
        }));

        *reward += total_input_value - output_value;
    }

    let mut inscription_id_to_satpoint = HashMap::new();
    let mut inscription_id_to_script = HashMap::new();

    for (satpoint, inscid) in outgoing_inscriptions.iter() {
        inscription_id_to_satpoint.insert(inscid, satpoint);

        // could be outgoing to unbound or miner
        if let Some(script) = outpoint_to_script.get(&satpoint.outpoint) {
            inscription_id_to_script.insert(inscid, script);
        }

        // if the inscription is an unused transfer, increase available balance of receiver
        // unless outpoint is null, in which case increase balance of sender (return)
        if let Some((tick, amt, sender, original_point)) = unused_brc20_transfers.get(inscid) {
            let receiver = if satpoint.outpoint.is_null() {
                warn!("unexpected null outpoint for brc20 transfer {:?}", inscid);
                sender // return to sender as inscription lost as fee
            } else {
                outpoint_to_script.get(&satpoint.outpoint).unwrap() // send to inscription receiver
            };

            let balance_key = DBSerde(ScriptAndBRC20Kind {
                script: receiver.to_byte_array(),
                brc20_ticker: tick.to_vec(),
            });

            let old_balance = BRC20Balances::get_by_key(&chain_db.db, db_tx, balance_key.clone())?
                .map(|DBUInt128(x)| x)
                .unwrap_or_default();

            let new_balance = old_balance + **amt;

            BRC20Balances::stage_upsert(&chain_db.db, balance_key, DBUInt128(new_balance), db_tx)?;

            valid_brc20s.push((
                inscid.clone(),
                BRC20Message::Transfer(
                    tick.to_vec(),
                    **amt,
                    original_point.outpoint.clone(),
                    *receiver,
                ),
            ))
        }
    }

    // for each new brc_20 message
    for (insc, brc20_msg) in brc_20_inscriptions {
        // fetch terms for ticker

        let terms = TermsByBRC20Ticker::get_by_key(
            &chain_db.db,
            db_tx,
            DBBytes(brc20_msg.tick.to_lowercase().as_bytes().to_vec()),
        )?
        .map(|DBSerde(x)| x);

        // deploy:
        //      invalid if terms found, valid if not
        //      effects:
        //          insert terms into database

        if let Some(deploy) = DeployAction::parse(&brc20_msg) {
            if terms.is_none() {
                let new_terms = BRC20Terms {
                    max: deploy.max,
                    mint_amt_limit: deploy.limit,
                    dec: deploy.dec,
                    self_mint: deploy.self_mint,
                    deploy_id: insc,
                };

                TermsByBRC20Ticker::stage_upsert(
                    &chain_db.db,
                    DBBytes(deploy.ticker.clone()),
                    DBSerde(new_terms),
                    db_tx,
                )?;

                valid_brc20s.push((insc, BRC20Message::Deploy(deploy.ticker)));
            }

            // next brc 20 msg
            continue;
        }

        // mint:
        //      invalid if no terms found (OK)
        //      invalid if amt > lim (OK)
        //      invalid if supply = max (OK)
        //      invalid if sent as fee ? (we dont see unbound here)
        //      invalid if self mint and deploy inscription not in tx possible parents (OK)
        //      effects:
        //          increase balance of receiver by min(max-supply, amt) (OK)
        //          increase token supply by min(max-supply, amt) (OK)

        if let Some(terms) = terms {
            if let Some(mut mint) = MintAction::parse(&brc20_msg, &terms, &potential_parents) {
                let supply =
                    SupplyByBRC20::get_by_key(&chain_db.db, db_tx, DBBytes(mint.ticker.clone()))?
                        .map(|DBUInt128(x)| x)
                        .unwrap_or_default();

                if supply == terms.max {
                    continue;
                }

                let receiver = if let Some(x) = inscription_id_to_script.get(&insc) {
                    x
                } else {
                    warn!("mint to non-output");
                    continue;
                };

                let actual_mint_amount = min(mint.amt, terms.max - supply);

                mint.amt = actual_mint_amount;

                // increase total minted supply

                let new_supply = supply + mint.amt;

                SupplyByBRC20::stage_upsert(
                    &chain_db.db,
                    DBBytes(mint.ticker.clone()),
                    DBUInt128(new_supply),
                    db_tx,
                )?;

                // update receiver balance

                let balance_key = DBSerde(ScriptAndBRC20Kind {
                    script: receiver.to_byte_array(),
                    brc20_ticker: mint.ticker.clone(),
                });

                let old_balance =
                    BRC20Balances::get_by_key(&chain_db.db, db_tx, balance_key.clone())?
                        .map(|DBUInt128(x)| x)
                        .unwrap_or_default();

                let new_balance = old_balance + mint.amt;

                BRC20Balances::stage_upsert(
                    &chain_db.db,
                    balance_key,
                    DBUInt128(new_balance),
                    db_tx,
                )?;

                valid_brc20s.push((insc, BRC20Message::Mint(mint.ticker, mint.amt, **receiver)));

                // next brc 20 msg
                continue;
            }

            // transfer:
            //      invalid if no terms found
            //      invalid if available balance of receiver less than transfer amount
            //      effects:
            //          decrease available balance of receiver (it is now locked in the transfer inscription)

            if let Some(transfer) = TransferAction::parse(&brc20_msg, &terms) {
                // decrease receiver balance

                let receiver = if let Some(x) = inscription_id_to_script.get(&insc) {
                    x
                } else {
                    warn!("transfer init to non-output");
                    continue;
                };

                let balance_key = DBSerde(ScriptAndBRC20Kind {
                    script: receiver.to_byte_array(),
                    brc20_ticker: transfer.ticker.clone(),
                });

                let old_balance = if let Some(bal) =
                    BRC20Balances::get_by_key(&chain_db.db, db_tx, balance_key.clone())?
                        .map(|DBUInt128(x)| x)
                {
                    bal
                } else {
                    warn!("no balance found");
                    continue;
                };

                if old_balance < transfer.amt {
                    warn!("trying to transfer more than balance");
                    continue;
                }

                let new_balance = old_balance - transfer.amt;

                BRC20Balances::stage_upsert(
                    &chain_db.db,
                    balance_key,
                    DBUInt128(new_balance),
                    db_tx,
                )?;

                new_unused_brc20_transfers.insert(insc, (transfer.ticker.clone(), transfer.amt));

                valid_brc20s.push((
                    insc,
                    BRC20Message::TransferInit(transfer.ticker, transfer.amt, **receiver),
                ));

                // next brc 20 msg
                continue;
            }
        }
    }

    let (contained, not_contained): (Vec<_>, Vec<_>) = outgoing_inscriptions
        .into_iter()
        .partition(|x| x.0.outpoint.txid == txid);

    let mut output_inscriptions = vec![Vec::new(); tx.output.len()];

    for (satpoint, inscription) in contained {
        let output_index = satpoint.outpoint.vout;

        output_inscriptions
            .get_mut(output_index as usize)
            .unwrap()
            .push((satpoint.offset, inscription));
    }

    if output_inscriptions.iter().any(|x| !x.is_empty()) {
        trace!("{:?}", output_inscriptions);
    }

    Ok(IndexInscriptionsResult {
        output_inscriptions,
        lost_or_unbound_inscriptions: not_contained,
        valid_reinscriptions,
        brc20_resolver: valid_brc20s,
        new_unused_brc20_transfers,
    })
}

fn update_inscription_location(
    chain_db: &ChainDB,
    db_tx: &Transaction<OptimisticTransactionDB>,
    flotsam: Flotsam,
    new_satpoint: SatPoint,
    counters: &mut Counters,
    output_inscriptions: &mut Vec<(SatPoint, InscriptionId)>,
    new_inscriptions: &mut Vec<(InscriptionId, u64)>,
) -> Result<(), Error> {
    let inscription_id = flotsam.inscription_id;

    let unbound = match flotsam.origin {
        Origin::Old { .. } => false,
        Origin::New {
            cursed,
            unbound,
            vindicated,
            ..
        } => {
            // increment global counters
            if cursed {
                debug!(
                    "new cursed inscription: {:?} : {} : {} : {:?}",
                    inscription_id, counters.cursed_count, counters.next_sequence_num, new_satpoint
                );

                counters.cursed_count += 1;
            } else {
                debug!(
                    "new inscription: {:?} : {} : {} : {:?}",
                    inscription_id,
                    counters.blessed_count,
                    counters.next_sequence_num,
                    new_satpoint
                );

                counters.blessed_count += 1;
            };

            // add new inscription to map inscription ID => inscription number
            new_inscriptions.push((inscription_id, counters.next_sequence_num));

            counters.next_sequence_num += 1;

            if vindicated || cursed {
                CursedOrVindicatedByInscriptionId::stage_upsert(
                    &chain_db.db,
                    DBSerde(inscription_id),
                    DBBytes(vec![0]),
                    db_tx,
                )?
            }

            unbound
        }
    };

    let satpoint = if unbound {
        let new_unbound_satpoint = SatPoint {
            outpoint: unbound_outpoint(),
            offset: counters.unbound_count,
        };
        counters.unbound_count += 1;
        new_unbound_satpoint
    } else {
        new_satpoint
    };

    output_inscriptions.push((satpoint, inscription_id));

    Ok(())
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum BRC20Message {
    Deploy(Vec<u8>),
    Mint(Vec<u8>, u128, ScriptHash),
    Transfer(Vec<u8>, u128, OutPoint, ScriptHash),
    TransferInit(Vec<u8>, u128, ScriptHash),
}
