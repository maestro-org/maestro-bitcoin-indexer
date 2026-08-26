use std::collections::HashMap;

use bitcoin::{consensus::Decodable, hashes::Hash, BlockHash, TxOut};
use ordinals::{Artifact, Edict, Etching, Height, Rune, RuneId, Runestone};
use rocksdb::{OptimisticTransactionDB, Transaction};
use tracing::{debug, error};

use crate::{
    storage::{self, BlockHeight, ChainDB, DBSerde, DBUInt128, Error, KVTable, TxoBody, TxoRef},
    sync::BitcoinCompatibleNetwork,
};

use super::{RuneIdByNameKV, RuneMintsByIdKV, RuneTerms, RuneTermsByIdKV};

pub struct IndexRunesResult {
    // hashmap of runes to amounts for each output
    pub output_runes: Vec<HashMap<RuneId, u128>>,
    // etching was present and successful
    pub successful_etch: bool,
    // mint was present and successful
    pub successful_mint: bool,
}

pub fn index_runes(
    resolver: &HashMap<TxoRef, TxoBody>,
    chain_db: &ChainDB,
    db_tx: &Transaction<OptimisticTransactionDB>,
    tx_index: u32,
    tx: &bitcoin::Transaction,
    height: BlockHeight,
    network: BitcoinCompatibleNetwork,
) -> Result<IndexRunesResult, Error> {
    let mut successful_etch = false;
    let mut successful_mint = false;

    let artifact = Runestone::decipher(tx);

    let mut unallocated = unallocated(tx, resolver)?;

    let mut allocated: Vec<HashMap<RuneId, u128>> = vec![HashMap::new(); tx.output.len()];

    if let Some(artifact) = &artifact {
        if let Some(id) = artifact.mint() {
            if let Some(amount) = mint(&chain_db, &db_tx, id, height)? {
                debug!("minted rune: {height}:{tx_index} {:?}:{}", id, amount);

                *unallocated.entry(id).or_default() += amount;
                successful_mint = true;
            }
        }

        let etched = etched(
            &chain_db, &db_tx, &resolver, tx_index, tx, artifact, height, network,
        )?;

        if etched.is_some() {
            successful_etch = true;
        }

        if let Artifact::Runestone(runestone) = artifact {
            if let Some((id, ..)) = etched {
                *unallocated.entry(id).or_default() +=
                    runestone.etching.unwrap().premine.unwrap_or_default();
            }

            for Edict { id, amount, output } in runestone.edicts.iter().copied() {
                let amount = amount;

                // edicts with output values greater than the number of outputs
                // should never be produced by the edict parser
                let output = usize::try_from(output).unwrap();
                assert!(output <= tx.output.len());

                let id = if id == RuneId::default() {
                    let Some((id, ..)) = etched else {
                        continue;
                    };

                    id
                } else {
                    id
                };

                let Some(balance) = unallocated.get_mut(&id) else {
                    continue;
                };

                let mut allocate = |balance: &mut u128, amount: u128, output: usize| {
                    if amount > 0 {
                        *balance -= amount;
                        *allocated[output].entry(id).or_default() += amount;
                    }
                };

                if output == tx.output.len() {
                    // find non-OP_RETURN outputs
                    let destinations = tx
                        .output
                        .iter()
                        .enumerate()
                        .filter_map(|(output, tx_out)| {
                            (!tx_out.script_pubkey.is_op_return()).then_some(output)
                        })
                        .collect::<Vec<usize>>();

                    if amount == 0 {
                        if !destinations.is_empty() {
                            // if amount is zero, divide balance between eligible outputs
                            let amount = *balance / destinations.len() as u128;
                            let remainder =
                                usize::try_from(*balance % destinations.len() as u128).unwrap();

                            for (i, output) in destinations.iter().enumerate() {
                                allocate(
                                    balance,
                                    if i < remainder { amount + 1 } else { amount },
                                    *output,
                                );
                            }
                        }
                    } else {
                        // if amount is non-zero, distribute amount to eligible outputs
                        for output in destinations {
                            allocate(balance, amount.min(*balance), output);
                        }
                    }
                } else {
                    // Get the allocatable amount
                    let amount = if amount == 0 {
                        *balance
                    } else {
                        amount.min(*balance)
                    };

                    allocate(balance, amount, output);
                }
            }
        }

        if let Some((id, rune)) = etched {
            debug!("etched rune: {height}:{tx_index} {:?}", etched);
            create_rune_entry(&chain_db, db_tx, artifact, id, rune, height)?;
        }
    }

    if let Some(Artifact::Cenotaph(_)) = artifact {
        for (_id, _balance) in unallocated {
            // if cenotaph, all unallocated runes burned
        }
    } else {
        let pointer = artifact
            .map(|artifact| match artifact {
                Artifact::Runestone(runestone) => runestone.pointer,
                Artifact::Cenotaph(_) => unreachable!(),
            })
            .unwrap_or_default();

        // assign all un-allocated runes to the default output, or the first non
        // OP_RETURN output if there is no default, or if the default output is
        // too large
        if let Some(vout) = pointer
            .map(|pointer| pointer as usize)
            .inspect(|&pointer| assert!(pointer < allocated.len()))
            .or_else(|| {
                tx.output
                    .iter()
                    .enumerate()
                    .find(|(_vout, tx_out)| !tx_out.script_pubkey.is_op_return())
                    .map(|(vout, _tx_out)| vout)
            })
        {
            for (id, balance) in unallocated {
                if balance > 0 {
                    *allocated[vout].entry(id).or_default() += balance;
                }
            }
        } else {
            for (_id, _balance) in unallocated {
                // if balance > 0 {
                //   *burned.entry(id).or_default() += balance;
                // }
            }
        }
    }

    Ok(IndexRunesResult {
        output_runes: allocated,
        successful_etch,
        successful_mint,
    })
}

fn create_rune_entry(
    chain_db: &ChainDB,
    db_tx: &Transaction<OptimisticTransactionDB>,
    artifact: &Artifact,
    id: RuneId,
    rune: Rune,
    height: BlockHeight,
) -> Result<(), Error> {
    let kv_rune_id: storage::RuneId = id.into();

    // insert into (name -> ID) table
    RuneIdByNameKV::stage_upsert(
        &chain_db.db,
        DBUInt128(rune.0),
        DBSerde(kv_rune_id.clone()),
        db_tx,
    )?;

    // TODO: rune number?

    let terms = match artifact {
        Artifact::Cenotaph(_) => RuneTerms {
            name: rune.0,
            amount: None,
            cap: None,
            start_height: None,
            end_height: None,
        },
        Artifact::Runestone(Runestone { etching, .. }) => {
            let Etching { terms, .. } = etching.unwrap();

            if let Some(terms) = terms {
                let amount = terms.amount;
                let cap = terms.cap;

                let relative_start = terms.offset.0.map(|offset| height.saturating_add(offset));

                let absolute_start = terms.height.0;

                let start = relative_start
                    .zip(absolute_start)
                    .map(|(relative, absolute)| relative.max(absolute))
                    .or(relative_start)
                    .or(absolute_start);

                let relative_end = terms.offset.1.map(|offset| height.saturating_add(offset));

                let absolute_end = terms.height.1;

                let end = relative_end
                    .zip(absolute_end)
                    .map(|(relative, absolute)| relative.min(absolute))
                    .or(relative_end)
                    .or(absolute_end);

                RuneTerms {
                    name: rune.0,
                    amount,
                    cap,
                    start_height: start,
                    end_height: end,
                }
            } else {
                RuneTerms {
                    name: rune.0,
                    amount: None,
                    cap: None,
                    start_height: None,
                    end_height: None,
                }
            }
        }
    };

    RuneTermsByIdKV::stage_upsert(&chain_db.db, kv_rune_id, DBSerde(terms), db_tx)?;

    Ok(())
}

fn etched(
    chain_db: &ChainDB,
    db_tx: &Transaction<OptimisticTransactionDB>,
    resolver: &HashMap<TxoRef, TxoBody>,
    tx_index: u32,
    tx: &bitcoin::Transaction,
    artifact: &Artifact,
    height: BlockHeight,
    network: BitcoinCompatibleNetwork,
) -> Result<Option<(RuneId, Rune)>, Error> {
    let rune = match artifact {
        Artifact::Runestone(runestone) => match runestone.etching {
            Some(etching) => etching.rune,
            None => return Ok(None),
        },
        Artifact::Cenotaph(cenotaph) => match cenotaph.etching {
            Some(rune) => Some(rune),
            None => return Ok(None),
        },
    };

    let minimum = Rune::minimum_at_height(network.into(), Height(height as u32));

    let rune = if let Some(rune) = rune {
        if rune < minimum
            || rune.is_reserved()
            || RuneIdByNameKV::get_by_key(&chain_db.db, &db_tx, rune.0.into())?.is_some()
            || !tx_commits_to_rune(resolver, tx, rune, height)?
        {
            return Ok(None);
        }
        rune
    } else {
        Rune::reserved(height, tx_index)
    };

    Ok(Some((
        RuneId {
            block: height,
            tx: tx_index,
        },
        rune,
    )))
}

fn mint(
    chain_db: &ChainDB,
    db_tx: &Transaction<OptimisticTransactionDB>,
    id: RuneId,
    height: BlockHeight,
) -> Result<Option<u128>, Error> {
    let id: storage::RuneId = id.into();

    let Some(DBSerde(terms)) = RuneTermsByIdKV::get_by_key(&chain_db.db, &db_tx, id.clone())?
    else {
        return Ok(None);
    };

    if let Some(start) = terms.start_height {
        if height < start {
            return Ok(None);
        }
    }

    if let Some(end) = terms.end_height {
        if height >= end {
            return Ok(None);
        }
    }

    let cap = terms.cap.unwrap_or_default();

    let current_mints = RuneMintsByIdKV::get_by_key(&chain_db.db, db_tx, id.clone())?
        .map(|DBUInt128(x)| x)
        .unwrap_or_default();

    if current_mints >= cap {
        return Ok(None);
    }

    let new_mints = current_mints + 1;

    RuneMintsByIdKV::stage_upsert(&chain_db.db, id, DBUInt128(new_mints), db_tx)?;

    Ok(Some(terms.amount.unwrap_or_default()))
}

fn tx_commits_to_rune(
    resolver: &HashMap<TxoRef, TxoBody>,
    tx: &bitcoin::Transaction,
    rune: Rune,
    height: BlockHeight,
) -> Result<bool, Error> {
    let commitment = rune.commitment();

    for input in &tx.input {
        // extracting a tapscript does not indicate that the input being spent
        // was actually a taproot output. this is checked below, when we load the
        // output's entry from the database
        let Some(tapscript) = input.witness.tapscript() else {
            continue;
        };

        for instruction in tapscript.instructions() {
            // ignore errors, since the extracted script may not be valid
            let Ok(instruction) = instruction else {
                break;
            };

            let Some(pushbytes) = instruction.push_bytes() else {
                continue;
            };

            if pushbytes.as_bytes() != commitment {
                continue;
            }

            let txo_ref = TxoRef(input.previous_output.txid, input.previous_output.vout);

            let tx_info = resolver
                .get(&txo_ref)
                .expect("missing txo resolver in rune commit"); // TODO

            let output = TxOut::consensus_decode_from_finite_reader(&mut &tx_info.raw[..]).unwrap();

            // check taproot
            if !output.script_pubkey.as_script().is_p2tr() {
                continue;
            }

            let commit_tx_height = tx_info.height;

            let confirmations = height
                .checked_sub(commit_tx_height.try_into().unwrap())
                .unwrap()
                + 1;

            if confirmations >= Runestone::COMMIT_CONFIRMATIONS.into() {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

fn unallocated(
    tx: &bitcoin::Transaction,
    resolver: &HashMap<TxoRef, TxoBody>,
) -> Result<HashMap<RuneId, u128>, Error> {
    // map of rune ID to un-allocated balance of that rune
    let mut unallocated: HashMap<RuneId, u128> = HashMap::new();

    // increment unallocated runes with the runes in tx inputs
    for input in tx.input.iter() {
        // skip coinbase input
        if !input.previous_output.is_null() {
            let outpoint = input.previous_output;

            let txo_ref = TxoRef(outpoint.txid, outpoint.vout);

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

            for (id, balance) in txo_body.runes.iter() {
                let id = id.clone().into();

                *unallocated.entry(id).or_default() += balance;
            }
        }
    }

    Ok(unallocated)
}
