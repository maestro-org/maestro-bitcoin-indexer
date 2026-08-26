use std::{
    cmp::min,
    collections::{BTreeMap, HashMap, HashSet},
};

use bitcoin::{consensus::Decodable, hashes::Hash, BlockHash, OutPoint, TxOut, Txid};
use ord::{unbound_outpoint, InscriptionId, ParsedEnvelope};
use ordinals::{Artifact, Edict, Etching, Height, Rune, Runestone, SatPoint};
use tracing::{debug, error, warn};

use crate::{
    storage::{
        self,
        inscriptions::{
            brc20::{self, DeployAction, MintAction, TransferAction},
            updater::{BRC20Message, Curse, Flotsam, IndexInscriptionsResult, Origin},
            BRC20Balances, BRC20Terms, Counters, CursedOrVindicatedByInscriptionId,
            InscriptionCountersKV, ScriptAndBRC20Kind, SupplyByBRC20, TermsByBRC20Ticker,
            INSCRIPTION_COUNTERS_KEY,
        },
        kvtable::{DBBytes, DBSerde, DBUInt128, KVTable},
        runes::{RuneIdByNameKV, RuneMintsByIdKV, RuneTerms, RuneTermsByIdKV},
        txos::TxoKV,
        ChainDB, Error, RuneId, TxoBody, TxoRef,
    },
    sync::BitcoinCompatibleNetwork,
};

use super::{model::Snapshot, roll::UtxoResolver};

/// Cache for storage lookups to avoid repeated queries when chain tip hasn't changed
/// This caches all the data we need to fetch from storage during mempool processing
#[derive(Debug, Clone)]
pub struct StorageCache {
    /// The chain tip (block hash) when this data was resolved
    pub chain_tip: BlockHash,
    /// Cached UTxOs resolved from storage
    pub resolved_utxos: HashMap<TxoRef, TxoBody>,
    /// Cached BRC20 terms by ticker
    pub brc20_terms: HashMap<Vec<u8>, BRC20Terms>,
    /// Cached BRC20 supply by ticker
    pub brc20_supply: HashMap<Vec<u8>, u128>,
    /// Cached BRC20 balances by (ticker, script_hash)
    pub brc20_balances: HashMap<(Vec<u8>, [u8; 20]), u128>,
    /// Cached rune IDs by name
    pub rune_ids: HashMap<u128, RuneId>,
    /// Cached rune terms by ID
    pub rune_terms: HashMap<RuneId, RuneTerms>,
    /// Cached rune mints by ID
    pub rune_mints: HashMap<RuneId, u128>,
}

impl StorageCache {
    pub fn new(chain_tip: BlockHash) -> Self {
        Self {
            chain_tip,
            resolved_utxos: HashMap::new(),
            brc20_terms: HashMap::new(),
            brc20_supply: HashMap::new(),
            brc20_balances: HashMap::new(),
            rune_ids: HashMap::new(),
            rune_terms: HashMap::new(),
            rune_mints: HashMap::new(),
        }
    }

    pub fn is_valid_for_tip(&self, current_tip: BlockHash) -> bool {
        self.chain_tip == current_tip
    }

    pub fn len(&self) -> usize {
        self.resolved_utxos.len()
            + self.brc20_terms.len()
            + self.brc20_supply.len()
            + self.brc20_balances.len()
            + self.rune_ids.len()
            + self.rune_terms.len()
            + self.rune_mints.len()
    }
}

pub struct MempoolProcessor {
    pub chain_db: ChainDB,
    resolver: UtxoResolver,
    /// The current chain tip (block hash) for this processing iteration
    chain_tip: BlockHash,
    /// Cache passed from the previous iteration
    input_cache: Option<StorageCache>,
    /// New cache being built during this processing iteration (always exists)
    output_cache: StorageCache,
    brc20_terms: HashMap<Vec<u8>, BRC20Terms>,
    brc20_supply: HashMap<Vec<u8>, u128>,
    brc20_balances: HashMap<(Vec<u8>, [u8; 20]), u128>,
    rune_ids: HashMap<u128, RuneId>,
    rune_terms: HashMap<RuneId, RuneTerms>,
    rune_mints: HashMap<RuneId, u128>,
    counters: Option<Counters>,
    cursed_or_vindicated: HashSet<InscriptionId>,
    pub network: BitcoinCompatibleNetwork,
}

impl MempoolProcessor {
    pub fn new(
        chain_db: ChainDB,
        network: BitcoinCompatibleNetwork,
        chain_tip: BlockHash,
        input_cache: Option<StorageCache>,
    ) -> Self {
        // Check if input cache is valid for the current chain tip
        let cache_valid = if let Some(cache) = &input_cache {
            cache.is_valid_for_tip(chain_tip)
        } else {
            false
        };

        if !cache_valid && input_cache.is_some() {
            debug!(
                "Input cache invalid for current chain tip {}, discarding",
                chain_tip
            );
        }

        Self {
            chain_db,
            resolver: UtxoResolver::new(),
            chain_tip,
            input_cache: if cache_valid { input_cache } else { None },
            output_cache: StorageCache::new(chain_tip),
            brc20_terms: HashMap::new(),
            brc20_supply: HashMap::new(),
            brc20_balances: HashMap::new(),
            rune_ids: HashMap::new(),
            rune_terms: HashMap::new(),
            rune_mints: HashMap::new(),
            counters: None,
            cursed_or_vindicated: HashSet::new(),
            network,
        }
    }

    /// Get the output cache built during this processing iteration.
    /// This should be called at the end of processing to get the cache for the next iteration.
    pub fn take_output_cache(self) -> StorageCache {
        self.output_cache
    }

    // utxo resolver

    pub fn insert_utxo(&mut self, txo_ref: TxoRef, body: TxoBody) {
        self.resolver.insert(txo_ref, body);
    }

    pub fn resolve_utxo(
        &mut self,
        txo_ref: &TxoRef,
        snapshot: &Snapshot,
    ) -> Result<Option<TxoBody>, storage::Error> {
        // Check resolver first (txos created in this mempool snapshot)
        if let Some(u) = self.resolver.get(txo_ref).cloned() {
            return Ok(Some(u));
        }

        // Check input cache (data from previous iteration)
        if let Some(cache) = &self.input_cache {
            if let Some(txo_body) = cache.resolved_utxos.get(txo_ref).cloned() {
                // Add to output cache since it came from storage
                self.output_cache
                    .resolved_utxos
                    .insert(txo_ref.clone(), txo_body.clone());
                return Ok(Some(txo_body));
            }
        }

        // Fetch from storage
        let result =
            TxoKV::get_by_key_snapshot(&self.chain_db.db, snapshot, DBSerde(txo_ref.clone()))?
                .map(|DBSerde(txo)| txo);

        // Add to output cache if found
        if let Some(txo) = &result {
            self.output_cache
                .resolved_utxos
                .insert(txo_ref.clone(), txo.clone());
        }

        Ok(result)
    }

    pub fn resolve_utxos(
        &mut self,
        txo_refs: Vec<TxoRef>,
        snapshot: &Snapshot,
    ) -> Result<HashMap<TxoRef, TxoBody>, Error> {
        let mut to_fetch_from_db = Vec::with_capacity(txo_refs.len());
        let mut fetched = HashMap::with_capacity(txo_refs.len());
        let mut cache_hits = 0;
        let mut resolver_hits = 0;

        for txo_ref in txo_refs {
            // check resolver (txos created in this mempool snapshot)
            if let Some(txo_body) = self.resolver.get(&txo_ref) {
                fetched.insert(txo_ref, txo_body.clone());
                resolver_hits += 1;
            }
            // those we don't find in resolver we should find in storage, but if we have a valid
            // cache from previous iteration we can check that for those txos before trying to
            // fetch them from storage
            else if let Some(cache) = &self.input_cache {
                if let Some(txo_body) = cache.resolved_utxos.get(&txo_ref) {
                    fetched.insert(txo_ref.clone(), txo_body.clone());
                    cache_hits += 1;

                    // Add cache hits to output cache since they came from storage originally
                    self.output_cache
                        .resolved_utxos
                        .insert(txo_ref, txo_body.clone());
                } else {
                    to_fetch_from_db.push(txo_ref);
                }
            } else {
                to_fetch_from_db.push(txo_ref);
            }
        }

        let total_requested = resolver_hits + cache_hits + to_fetch_from_db.len();
        if total_requested > 0 {
            debug!(
                "UTXO resolution: {} total requested, {} resolver hits, {} cache hits, {} DB fetches (cache_valid: {})",
                total_requested,
                resolver_hits,
                cache_hits,
                to_fetch_from_db.len(),
                self.input_cache.is_some()
            );
        }

        // fetch remaining txos from storage
        if !to_fetch_from_db.is_empty() {
            let txo_cf = TxoKV::cf(&self.chain_db.db);

            let txos = snapshot
                .multi_get_cf(
                    to_fetch_from_db
                        .iter()
                        .map(|x| (&txo_cf, Box::<[u8]>::from(DBSerde(x.clone())))),
                )
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(Error::Rocks)?;

            let kvs = to_fetch_from_db.into_iter().zip(txos);

            for (txo_ref, txo) in kvs {
                if let Some(b) = txo {
                    let DBSerde(txo_body) = <DBSerde<TxoBody>>::from(Box::from(b.as_slice()));
                    fetched.insert(txo_ref.clone(), txo_body.clone());

                    // Add to output cache
                    self.output_cache.resolved_utxos.insert(txo_ref, txo_body);
                } else {
                    return Err(Error::MissingTxo(self.chain_tip, txo_ref.0, txo_ref.1));
                }
            }
        }

        Ok(fetched)
    }

    // brc20 terms

    pub fn set_brc20_terms(&mut self, ticker: Vec<u8>, terms: BRC20Terms) {
        self.brc20_terms.insert(ticker, terms);
    }

    pub fn get_brc20_terms(
        &mut self,
        ticker: Vec<u8>,
        snapshot: &Snapshot,
    ) -> Result<Option<BRC20Terms>, storage::Error> {
        // Check in-memory state first (writes from this iteration)
        if let Some(terms) = self.brc20_terms.get(&ticker).cloned() {
            return Ok(Some(terms));
        }

        // Check input cache (data from previous iteration) - already validated in constructor
        if let Some(cache) = &self.input_cache {
            if let Some(terms) = cache.brc20_terms.get(&ticker).cloned() {
                // Add to output cache since it came from storage
                self.output_cache.brc20_terms.insert(ticker, terms.clone());
                return Ok(Some(terms));
            }
        }

        // Fetch from storage
        let result = TermsByBRC20Ticker::get_by_key_snapshot(
            &self.chain_db.db,
            snapshot,
            DBBytes(ticker.clone()),
        )?
        .map(|DBSerde(terms)| terms);

        // Add to output cache if found
        if let Some(terms) = &result {
            self.output_cache.brc20_terms.insert(ticker, terms.clone());
        }

        Ok(result)
    }

    // brc20 supply

    pub fn set_brc20_supply(&mut self, ticker: Vec<u8>, amt: u128) {
        self.brc20_supply.insert(ticker, amt);
    }

    pub fn get_brc20_supply(
        &mut self,
        ticker: Vec<u8>,
        snapshot: &Snapshot,
    ) -> Result<Option<u128>, storage::Error> {
        // Check in-memory state first (writes from this iteration)
        if let Some(supply) = self.brc20_supply.get(&ticker).cloned() {
            return Ok(Some(supply));
        }

        // Check input cache (data from previous iteration)
        if let Some(cache) = &self.input_cache {
            if let Some(supply) = cache.brc20_supply.get(&ticker).cloned() {
                // Add to output cache since it came from storage
                self.output_cache.brc20_supply.insert(ticker, supply);
                return Ok(Some(supply));
            }
        }

        // Fetch from storage
        let result = SupplyByBRC20::get_by_key_snapshot(
            &self.chain_db.db,
            snapshot,
            DBBytes(ticker.clone()),
        )?
        .map(|DBUInt128(supply)| supply);

        // Add to output cache if found
        if let Some(supply) = result {
            self.output_cache.brc20_supply.insert(ticker, supply);
            return Ok(Some(supply));
        }

        Ok(None)
    }

    // brc20 balance

    pub fn set_brc20_balance(&mut self, ticker: Vec<u8>, sh: [u8; 20], amt: u128) {
        self.brc20_balances.insert((ticker, sh), amt);
    }

    pub fn get_brc20_balance(
        &mut self,
        ticker: Vec<u8>,
        sh: [u8; 20],
        snapshot: &Snapshot,
    ) -> Result<Option<u128>, storage::Error> {
        // Check in-memory state first (writes from this iteration)
        if let Some(bal) = self.brc20_balances.get(&(ticker.clone(), sh)).cloned() {
            return Ok(Some(bal));
        }

        // Check input cache (data from previous iteration)
        if let Some(cache) = &self.input_cache {
            if let Some(bal) = cache.brc20_balances.get(&(ticker.clone(), sh)).cloned() {
                // Add to output cache since it came from storage
                self.output_cache.brc20_balances.insert((ticker, sh), bal);
                return Ok(Some(bal));
            }
        }

        // Fetch from storage
        let result = BRC20Balances::get_by_key_snapshot(
            &self.chain_db.db,
            snapshot,
            DBSerde(ScriptAndBRC20Kind {
                brc20_ticker: ticker.clone(),
                script: sh,
            }),
        )?
        .map(|DBUInt128(bal)| bal);

        // Add to output cache if found
        if let Some(bal) = result {
            self.output_cache.brc20_balances.insert((ticker, sh), bal);
            return Ok(Some(bal));
        }

        Ok(None)
    }

    // rune terms

    pub fn set_rune_terms(&mut self, id: RuneId, terms: RuneTerms) {
        self.rune_terms.insert(id, terms);
    }

    pub fn get_rune_terms(
        &mut self,
        id: RuneId,
        snapshot: &Snapshot,
    ) -> Result<Option<RuneTerms>, storage::Error> {
        // Check in-memory state first (writes from this iteration)
        if let Some(terms) = self.rune_terms.get(&id).cloned() {
            return Ok(Some(terms));
        }

        // Check input cache (data from previous iteration)
        if let Some(cache) = &self.input_cache {
            if let Some(terms) = cache.rune_terms.get(&id).cloned() {
                // Add to output cache since it came from storage
                self.output_cache.rune_terms.insert(id, terms.clone());
                return Ok(Some(terms));
            }
        }

        // Fetch from storage
        let result = RuneTermsByIdKV::get_by_key_snapshot(&self.chain_db.db, snapshot, id.clone())?
            .map(|DBSerde(terms)| terms);

        // Add to output cache if found
        if let Some(terms) = &result {
            self.output_cache.rune_terms.insert(id, terms.clone());
        }

        Ok(result)
    }

    // rune id by name (?)

    pub fn set_rune_id(&mut self, name: u128, id: RuneId) {
        self.rune_ids.insert(name, id);
    }

    pub fn get_rune_id(
        &mut self,
        name: u128,
        snapshot: &Snapshot,
    ) -> Result<Option<RuneId>, storage::Error> {
        // Check in-memory state first (writes from this iteration)
        if let Some(id) = self.rune_ids.get(&name).cloned() {
            return Ok(Some(id));
        }

        // Check input cache (data from previous iteration)
        if let Some(cache) = &self.input_cache {
            if let Some(id) = cache.rune_ids.get(&name).cloned() {
                // Add to output cache since it came from storage
                self.output_cache.rune_ids.insert(name, id.clone());
                return Ok(Some(id));
            }
        }

        // Fetch from storage
        let result =
            RuneIdByNameKV::get_by_key_snapshot(&self.chain_db.db, snapshot, DBUInt128(name))?
                .map(|DBSerde(id)| id);

        // Add to output cache if found
        if let Some(id) = &result {
            self.output_cache.rune_ids.insert(name, id.clone());
        }

        Ok(result)
    }

    // rune mints

    pub fn set_rune_mints(&mut self, id: RuneId, mints: u128) {
        self.rune_mints.insert(id, mints);
    }

    pub fn get_rune_mints(
        &mut self,
        id: RuneId,
        snapshot: &Snapshot,
    ) -> Result<Option<u128>, storage::Error> {
        // Check in-memory state first (writes from this iteration)
        if let Some(mints) = self.rune_mints.get(&id).cloned() {
            return Ok(Some(mints));
        }

        // Check input cache (data from previous iteration)
        if let Some(cache) = &self.input_cache {
            if let Some(mints) = cache.rune_mints.get(&id).cloned() {
                // Add to output cache since it came from storage
                self.output_cache.rune_mints.insert(id, mints);
                return Ok(Some(mints));
            }
        }

        // Fetch from storage
        let result = RuneMintsByIdKV::get_by_key_snapshot(&self.chain_db.db, snapshot, id.clone())?
            .map(|DBUInt128(mints)| mints);

        // Add to output cache if found
        if let Some(mints) = result {
            self.output_cache.rune_mints.insert(id, mints);
            return Ok(Some(mints));
        }

        Ok(None)
    }

    //

    pub fn init_inscription_counters(&mut self, snapshot: &Snapshot) -> Result<(), storage::Error> {
        assert!(self.counters.is_none());

        let counters = InscriptionCountersKV::get_by_key_snapshot(
            &self.chain_db.db,
            snapshot,
            DBBytes(INSCRIPTION_COUNTERS_KEY.to_vec()),
        )?
        .map(|DBSerde(x)| x)
        .unwrap_or_default();

        self.counters = Some(counters);

        Ok(())
    }

    pub fn set_cursed_or_vindicated(&mut self, id: InscriptionId) {
        self.cursed_or_vindicated.insert(id);
    }

    pub fn is_cursed_or_vindicated(
        &mut self,
        id: InscriptionId,
        snapshot: &Snapshot,
    ) -> Result<bool, storage::Error> {
        if self.cursed_or_vindicated.contains(&id) {
            Ok(true)
        } else {
            Ok(CursedOrVindicatedByInscriptionId::get_by_key_snapshot(
                &self.chain_db.db,
                snapshot,
                DBSerde(id.clone()),
            )?
            .is_some())
        }
    }

    //

    pub fn index_runes(
        &mut self,
        tx_index: u32,
        tx: &bitcoin::Transaction,
        height: u64,
        snapshot: &Snapshot,
    ) -> Result<IndexRunesResult, Error> {
        let mut successful_etch = false;
        let mut successful_mint = false;

        let artifact = Runestone::decipher(tx);

        let mut unallocated = self.unallocated(tx, snapshot)?;

        let mut allocated: Vec<HashMap<ordinals::RuneId, u128>> =
            vec![HashMap::new(); tx.output.len()];

        if let Some(artifact) = &artifact {
            if let Some(id) = artifact.mint() {
                if let Some(amount) = self.mint(id, height, snapshot)? {
                    debug!("minted rune: {}:{tx_index} {:?}:{}", height, id, amount);

                    *unallocated.entry(id.into()).or_default() += amount;
                    successful_mint = true;
                }
            }

            let etched = self.etched(tx_index, tx, artifact, height, snapshot)?;

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

                    let id = if id == ordinals::RuneId::default() {
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
                debug!("etched rune: {}:{tx_index} {:?}", height, etched);
                self.create_rune_entry(artifact, id, rune, height)?;
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

    pub fn unallocated(
        &mut self,
        tx: &bitcoin::Transaction,
        snapshot: &Snapshot,
    ) -> Result<HashMap<ordinals::RuneId, u128>, Error> {
        // map of rune ID to un-allocated balance of that rune
        let mut unallocated: HashMap<ordinals::RuneId, u128> = HashMap::new();

        // increment unallocated runes with the runes in tx inputs
        for input in tx.input.iter() {
            let outpoint = input.previous_output;

            let txo_ref = TxoRef(outpoint.txid, outpoint.vout);

            let txo_body = match self.resolve_utxo(&txo_ref, snapshot)? {
                Some(x) => x,
                None => {
                    tracing::error!("(mp) missing {txo_ref:?} for {:?}", tx.compute_txid(),);
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

        Ok(unallocated)
    }

    pub fn mint(
        &mut self,
        id: ordinals::RuneId,
        height: u64,
        snapshot: &Snapshot,
    ) -> Result<Option<u128>, Error> {
        let id: storage::RuneId = id.into();

        let Some(terms) = self.get_rune_terms(id.clone(), snapshot)? else {
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

        let current_mints = self
            .get_rune_mints(id.clone(), snapshot)?
            .unwrap_or_default();

        if current_mints >= cap {
            return Ok(None);
        }

        let new_mints = current_mints + 1;

        self.set_rune_mints(id, new_mints);

        Ok(Some(terms.amount.unwrap_or_default()))
    }

    pub fn create_rune_entry(
        &mut self,
        artifact: &Artifact,
        id: ordinals::RuneId,
        rune: Rune,
        height: u64,
    ) -> Result<(), Error> {
        let kv_rune_id: storage::RuneId = id.into();

        self.set_rune_id(rune.0, kv_rune_id.clone());

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

        self.set_rune_terms(kv_rune_id, terms);

        Ok(())
    }

    pub fn etched(
        &mut self,
        tx_index: u32,
        tx: &bitcoin::Transaction,
        artifact: &Artifact,
        height: u64,
        snapshot: &Snapshot,
    ) -> Result<Option<(ordinals::RuneId, Rune)>, Error> {
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

        let minimum = Rune::minimum_at_height(self.network.into(), Height(height as u32));

        let rune = if let Some(rune) = rune {
            if rune < minimum
                || rune.is_reserved()
                || self.get_rune_id(rune.0.into(), snapshot)?.is_some()
                || !self.tx_commits_to_rune(tx, rune, height, snapshot)?
            {
                return Ok(None);
            }
            rune
        } else {
            Rune::reserved(height, tx_index)
        };

        Ok(Some((
            ordinals::RuneId {
                block: height,
                tx: tx_index,
            },
            rune,
        )))
    }

    pub fn tx_commits_to_rune(
        &mut self,
        tx: &bitcoin::Transaction,
        rune: Rune,
        height: u64,
        snapshot: &Snapshot,
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

                let tx_info = self
                    .resolve_utxo(&txo_ref, snapshot)?
                    .expect("missing txo resolver in rune commit"); // TODO

                let output =
                    TxOut::consensus_decode_from_finite_reader(&mut &tx_info.raw[..]).unwrap();

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

    pub fn index_inscriptions(
        &mut self,
        resolver: &HashMap<TxoRef, TxoBody>,
        snapshot: &Snapshot,
        tx: &bitcoin::Transaction,
        txid: Txid,
        height: u64,
        jubilee_height: u64,
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
                            self.is_cursed_or_vindicated(id.clone(), &snapshot)?;

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

            self.update_inscription_location(
                flotsam,
                new_satpoint,
                &mut outgoing_inscriptions,
                new_inscriptions,
            )?;
        }

        if is_coinbase {
            for flotsam in inscriptions {
                let new_satpoint = SatPoint {
                    outpoint: OutPoint::null(),
                    offset: self.counters.as_mut().unwrap().lost_sats + flotsam.offset
                        - output_value,
                };

                self.update_inscription_location(
                    flotsam,
                    new_satpoint,
                    &mut outgoing_inscriptions,
                    new_inscriptions,
                )?;
            }

            self.counters.as_mut().unwrap().lost_sats += *reward - output_value;
        } else {
            // if unused brc20 transfer inscription lost as fee, return brc20 to sender
            for flotsam in inscriptions.clone() {
                if let Some((tick, amt, sender, original_point)) =
                    unused_brc20_transfers.get(&flotsam.inscription_id)
                {
                    let receiver = sender;

                    debug!(
                        "found unused brc20transfer as fee {:?} {}",
                        &(tick, amt, sender, original_point),
                        hex::encode(receiver.to_byte_array())
                    );

                    let old_balance = self
                        .get_brc20_balance(tick.to_vec(), receiver.to_byte_array(), &snapshot)?
                        .unwrap_or_default();

                    let new_balance = old_balance + **amt;

                    self.set_brc20_balance(tick.to_vec(), receiver.to_byte_array(), new_balance);

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

                let old_balance = self
                    .get_brc20_balance(tick.to_vec(), receiver.to_byte_array(), &snapshot)?
                    .unwrap_or_default();

                let new_balance = old_balance + **amt;

                self.set_brc20_balance(tick.to_vec(), receiver.to_byte_array(), new_balance);

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
            let terms =
                self.get_brc20_terms(brc20_msg.tick.to_lowercase().as_bytes().to_vec(), &snapshot)?;

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

                    self.set_brc20_terms(deploy.ticker.clone(), new_terms);

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
                    let supply = self
                        .get_brc20_supply(mint.ticker.clone(), snapshot)?
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

                    self.set_brc20_supply(mint.ticker.clone(), new_supply);

                    // update receiver balance

                    let old_balance = self
                        .get_brc20_balance(
                            mint.ticker.clone(),
                            receiver.to_byte_array(),
                            &snapshot,
                        )?
                        .unwrap_or_default();

                    let new_balance = old_balance + mint.amt;

                    self.set_brc20_balance(
                        mint.ticker.clone(),
                        receiver.to_byte_array(),
                        new_balance,
                    );

                    valid_brc20s
                        .push((insc, BRC20Message::Mint(mint.ticker, mint.amt, **receiver)));

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

                    let old_balance = if let Some(bal) = self.get_brc20_balance(
                        transfer.ticker.clone(),
                        receiver.to_byte_array(),
                        snapshot,
                    )? {
                        bal
                    } else {
                        debug!("no balance found");
                        continue;
                    };

                    if old_balance < transfer.amt {
                        debug!("trying to transfer more than balance");
                        continue;
                    }

                    let new_balance = old_balance - transfer.amt;

                    self.set_brc20_balance(
                        transfer.ticker.clone(),
                        receiver.to_byte_array(),
                        new_balance,
                    );

                    new_unused_brc20_transfers
                        .insert(insc, (transfer.ticker.clone(), transfer.amt));

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
            debug!("{:?}", output_inscriptions);
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
        &mut self,
        flotsam: Flotsam,
        new_satpoint: SatPoint,
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
                        inscription_id,
                        self.counters.as_ref().unwrap().cursed_count,
                        self.counters.as_ref().unwrap().next_sequence_num,
                        new_satpoint
                    );

                    self.counters.as_mut().unwrap().cursed_count += 1;
                } else {
                    debug!(
                        "new inscription: {:?} : {} : {} : {:?}",
                        inscription_id,
                        self.counters.as_ref().unwrap().blessed_count,
                        self.counters.as_ref().unwrap().next_sequence_num,
                        new_satpoint
                    );

                    self.counters.as_mut().unwrap().blessed_count += 1;
                };

                // add new inscription to map inscription ID => inscription number
                new_inscriptions.push((
                    inscription_id,
                    self.counters.as_ref().unwrap().next_sequence_num,
                ));

                self.counters.as_mut().unwrap().next_sequence_num += 1;

                if vindicated || cursed {
                    self.set_cursed_or_vindicated(inscription_id);
                }

                unbound
            }
        };

        let satpoint = if unbound {
            let new_unbound_satpoint = SatPoint {
                outpoint: unbound_outpoint(),
                offset: self.counters.as_ref().unwrap().unbound_count,
            };
            self.counters.as_mut().unwrap().unbound_count += 1;
            new_unbound_satpoint
        } else {
            new_satpoint
        };

        output_inscriptions.push((satpoint, inscription_id));

        Ok(())
    }
}

pub struct IndexRunesResult {
    // hashmap of runes to amounts for each output
    pub output_runes: Vec<HashMap<ordinals::RuneId, u128>>,
    // etching was present and successful
    pub successful_etch: bool,
    // mint was present and successful
    pub successful_mint: bool,
}
