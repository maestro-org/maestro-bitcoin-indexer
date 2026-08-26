use std::ops::Range;

use tikv_client::{Key, KvPair};
use timbre_xbt::{
    reducers::{
        content_by_inscription_id::{Key as InscriptionInfoKey, Value as InscriptionInfoValue},
        inscription_utxos_by_script_hash, rune_utxos_by_script_hash,
        utxos_by_script_hash::{Key as UtxosByScriptHashKey, Value as UtxosByScriptHashValue},
    },
    Decode, Namespace, Reducer,
};
use tracing::warn;

use crate::{
    error::{Error, MapiResult},
    types::OrderParam,
};

use super::{adapter::TiKVAdapter, key_resolver::ReducerType, KV_SCAN_BATCH_SIZE};

pub struct Scanner {
    pub range: Range<Vec<u8>>,
    pub order: OrderParam,
    pub count: Option<usize>,
}

impl Scanner {
    pub fn new(range: Range<Vec<u8>>) -> Self {
        Self {
            range,
            order: OrderParam::Asc,
            count: None,
        }
    }

    pub fn order(mut self, order: OrderParam) -> Scanner {
        self.order = order;

        self
    }

    pub fn count(mut self, count: usize) -> Scanner {
        self.count = Some(count);

        self
    }

    pub async fn execute<K, V>(
        self,
        tikv: &mut TiKVAdapter,
        reducer_type: ReducerType,
    ) -> MapiResult<Vec<(K, V)>>
    where
        K: Decode + Send + Sync,
        V: Decode + Send + Sync,
    {
        self.execute_with_filter::<K, V, fn(&KvPair) -> bool>(tikv, reducer_type, None)
            .await
    }

    pub async fn execute_keys_only<K>(
        self,
        tikv_adapter: &mut TiKVAdapter,
        reducer_type: ReducerType,
    ) -> MapiResult<Vec<K>>
    where
        K: Decode + Send + Sync,
    {
        let snapshot = tikv_adapter.get_snapshot(reducer_type)?;

        let mut scan_size = self.count.unwrap_or(1000) as u32;

        let mut range_remaining = self.range;
        let mut ks = Vec::new();

        'scan_loop: loop {
            let fetched = match self.order {
                OrderParam::Asc => snapshot
                    .scan_keys(range_remaining.clone(), scan_size)
                    .await
                    .map(|iter| iter.collect::<Vec<Key>>()),
                OrderParam::Desc => snapshot
                    .scan_keys_reverse(range_remaining.clone(), scan_size)
                    .await
                    .map(|iter| iter.collect::<Vec<Key>>()),
            };

            match fetched {
                Ok(ks_vec) => {
                    let num_kvs_fetched = ks_vec.len();

                    // use the last key fetched to update the remaining range, or
                    // break if no keys were fetched
                    range_remaining = if let Some(k) = ks_vec.last() {
                        let mut last_key = Into::<Vec<u8>>::into(k.clone());

                        // modify the range according to the order
                        match self.order {
                            OrderParam::Asc => {
                                last_key.push(0);
                                last_key..range_remaining.end
                            }
                            OrderParam::Desc => range_remaining.start..last_key,
                        }
                    } else {
                        break;
                    };

                    for key in ks_vec.into_iter() {
                        let k = Into::<Vec<u8>>::into(key);

                        // name space || data || reducer || break
                        let k = k
                            .get(Namespace::size() + 3..)
                            .ok_or_else(|| Error::MalformedData(k.clone(), None))?;

                        let key = K::decode(k)
                            .map_err(|e| Error::MalformedData(k.to_vec(), Some(e)))?
                            .0;

                        ks.push(key);

                        // if max_count was provided and we have enough kvs to satisfy
                        // that count then stop scanning
                        if let Some(max) = self.count {
                            if ks.len() >= max {
                                break 'scan_loop;
                            }
                        }
                    }

                    // if the number of total fetched (unfiltered) kvs is less than
                    // the scan size then we have exhausted the scan range, so stop
                    if (num_kvs_fetched as u32) < scan_size {
                        break;
                    }
                }
                Err(tikv_client::Error::Grpc(e))
                    if e.to_string().contains("Received message larger than max") =>
                {
                    warn!("scan size {scan_size} too large when fetching entire range {range_remaining:?}");
                    scan_size /= 2
                }
                Err(e) => return Err(Error::TiKV(e)),
            }
        }

        if let Some(max) = self.count {
            ks.truncate(max)
        }

        Ok(ks)
    }

    pub async fn execute_with_filter<K, V, F>(
        self,
        tikv_adapter: &mut TiKVAdapter,
        reducer_type: ReducerType,
        filter_fn: Option<F>,
    ) -> MapiResult<Vec<(K, V)>>
    where
        K: Decode + Send + Sync,
        V: Decode + Send + Sync,
        F: Fn(&KvPair) -> bool,
    {
        let snapshot = tikv_adapter.get_snapshot(reducer_type)?;

        // if there is no filter we can just scan exactly the number of keys we want
        let mut scan_size = if filter_fn.is_none() {
            self.count.map(|x| x as u32).unwrap_or(KV_SCAN_BATCH_SIZE)
        } else {
            KV_SCAN_BATCH_SIZE
        };

        let mut range_remaining = self.range;
        let mut kvs = Vec::new();

        'scan_loop: loop {
            let fetched = match self.order {
                OrderParam::Asc => snapshot
                    .scan(range_remaining.clone(), scan_size)
                    .await
                    .map(|iter| iter.collect::<Vec<KvPair>>()),
                OrderParam::Desc => snapshot
                    .scan_reverse(range_remaining.clone(), scan_size)
                    .await
                    .map(|iter| iter.collect::<Vec<KvPair>>()),
            };

            match fetched {
                Ok(mut kvs_vec) => {
                    let num_kvs_fetched = kvs_vec.len();

                    // use the last key fetched to update the remaining range, or
                    // break if no keys were fetched
                    range_remaining = if let Some(kv) = kvs_vec.last() {
                        let mut last_key = Into::<Vec<u8>>::into(kv.clone().into_key());

                        // modify the range according to the order
                        match self.order {
                            OrderParam::Asc => {
                                last_key.push(0);
                                last_key..range_remaining.end
                            }
                            OrderParam::Desc => range_remaining.start..last_key,
                        }
                    } else {
                        break;
                    };

                    if let Some(f) = &filter_fn {
                        kvs_vec.retain(|kv| f(kv))
                    }

                    for KvPair(k, v) in kvs_vec {
                        let k = Into::<Vec<u8>>::into(k);

                        // name space || data || reducer || break
                        let k = k
                            .get(Namespace::size() + 3..)
                            .ok_or_else(|| Error::MalformedData(k.clone(), None))?;

                        let key = K::decode(k)
                            .map_err(|e| Error::MalformedData(k.to_vec(), Some(e)))?
                            .0;

                        let value = V::decode(&v)
                            .map_err(|e| Error::MalformedData(v.clone(), Some(e)))?
                            .0;

                        kvs.push((key, value));

                        // if max_count was provided and we have enough kvs to satisfy
                        // that count then stop scanning
                        if let Some(max) = self.count {
                            if kvs.len() >= max {
                                break 'scan_loop;
                            }
                        }
                    }

                    // if the number of total fetched (unfiltered) kvs is less than
                    // the scan size then we have exhausted the scan range, so stop
                    if (num_kvs_fetched as u32) < scan_size {
                        break;
                    }
                }
                Err(tikv_client::Error::Grpc(e))
                    if e.to_string().contains("Received message larger than max") =>
                {
                    warn!("scan size {scan_size} too large when fetching entire range {range_remaining:?}");
                    scan_size /= 2
                }
                Err(e) => return Err(Error::TiKV(e)),
            }
        }

        if let Some(max) = self.count {
            kvs.truncate(max)
        }

        Ok(kvs)
    }

    // Similar to execute, except that a `map_fn` function must be provided, which should take a
    // KvPair, decode it, and compute as many (K, V) pairs as possible from it.
    pub async fn execute_with_map<K, V, A, B>(
        self,
        tikv_adapter: &mut TiKVAdapter,
        reducer_type: ReducerType,
        map_fn: impl Fn(K, V) -> MapiResult<Vec<(A, B)>>,
    ) -> MapiResult<Vec<(A, B)>>
    where
        K: Clone + Decode + Send + Sync,
        V: Clone + Decode + Send + Sync,
    {
        let snapshot = tikv_adapter.get_snapshot(reducer_type)?;

        // TODO: config on scanner
        let mut scan_size = KV_SCAN_BATCH_SIZE;

        let mut range_remaining = self.range;
        let mut kvs = Vec::new();

        'scan_loop: loop {
            let fetched = match self.order {
                OrderParam::Asc => snapshot
                    .scan(range_remaining.clone(), scan_size)
                    .await
                    .map(|iter| iter.collect::<Vec<KvPair>>()),
                OrderParam::Desc => snapshot
                    .scan_reverse(range_remaining.clone(), scan_size)
                    .await
                    .map(|iter| iter.collect::<Vec<KvPair>>()),
            };

            match fetched {
                Ok(kvs_vec) => {
                    let num_kvs_fetched = kvs_vec.len();

                    // use the last key fetched to update the remaining range, or
                    // break if no keys were fetched
                    range_remaining = if let Some(kv) = kvs_vec.last() {
                        let mut last_key = Into::<Vec<u8>>::into(kv.clone().into_key());

                        // modify the range according to the order
                        match self.order {
                            OrderParam::Asc => {
                                last_key.push(0);
                                last_key..range_remaining.end
                            }
                            OrderParam::Desc => range_remaining.start..last_key,
                        }
                    } else {
                        break;
                    };
                    for KvPair(k, v) in kvs_vec {
                        let k = Into::<Vec<u8>>::into(k);

                        // name space || data || reducer || break
                        let k = k
                            .get(Namespace::size() + 3..)
                            .ok_or_else(|| Error::MalformedData(k.clone(), None))?;

                        let key = K::decode(k)
                            .map_err(|e| Error::MalformedData(k.to_vec(), Some(e)))?
                            .0;

                        let value = V::decode(&v)
                            .map_err(|e| Error::MalformedData(v.clone(), Some(e)))?
                            .0;

                        // extend result with as many (K, V) pairs as possible
                        kvs.extend(map_fn(key, value)?);

                        // if max_count was provided and we have enough kvs to satisfy
                        // that count then stop scanning
                        if let Some(max) = self.count {
                            if kvs.len() >= max {
                                break 'scan_loop;
                            }
                        }
                    }

                    // if the number of total fetched (unfiltered) kvs is less than
                    // the scan size then we have exhausted the scan range, so stop
                    if (num_kvs_fetched as u32) < scan_size {
                        break;
                    }
                }
                Err(tikv_client::Error::Grpc(e))
                    if e.to_string().contains("Received message larger than max") =>
                {
                    warn!("scan size {scan_size} too large when fetching entire range {range_remaining:?}");
                    scan_size /= 2
                }
                Err(e) => return Err(Error::TiKV(e)),
            }
        }

        if let Some(max) = self.count {
            kvs.truncate(max)
        }

        Ok(kvs)
    }

    // Similar to execute_with_filter, except that metaprotocol UTxOs are excluded
    pub async fn get_non_metaprotocol_utxos<F>(
        self,
        tikv_adapter: &mut TiKVAdapter,
        filter_fn: Option<F>,
        ignore_used_brc20: bool,
    ) -> MapiResult<Vec<(UtxosByScriptHashKey, UtxosByScriptHashValue)>>
    where
        F: Fn(&KvPair) -> bool,
    {
        let (utxos_reducer, runes_reducer, inscriptions_reducer) = (
            ReducerType::UtxosByScriptHash,
            ReducerType::RuneUtxosByScriptHash,
            ReducerType::InscriptionUtxosByScriptHash,
        );

        let mut scan_size = KV_SCAN_BATCH_SIZE;

        let mut range_remaining = self.range;
        let mut kvs = Vec::new();

        'scan_loop: loop {
            let (mut utxos_snapshot, encoder) =
                tikv_adapter.take_snapshot_and_encoder(utxos_reducer)?;

            // TODO: scan rune and inscription UTxO KVs instead of point get

            let fetched = match self.order {
                OrderParam::Asc => utxos_snapshot
                    .scan(range_remaining.clone(), scan_size)
                    .await
                    .map(|iter| iter.collect::<Vec<KvPair>>()),
                OrderParam::Desc => utxos_snapshot
                    .scan_reverse(range_remaining.clone(), scan_size)
                    .await
                    .map(|iter| iter.collect::<Vec<KvPair>>()),
            };

            tikv_adapter.insert_snapshot_and_encoder(utxos_reducer, utxos_snapshot, encoder);

            match fetched {
                Ok(mut kvs_vec) => {
                    let num_kvs_fetched = kvs_vec.len();

                    // use the last key fetched to update the remaining range, or
                    // break if no keys were fetched
                    range_remaining = if let Some(kv) = kvs_vec.last() {
                        let mut last_key = Into::<Vec<u8>>::into(kv.clone().into_key());

                        // modify the range according to the order
                        match self.order {
                            OrderParam::Asc => {
                                last_key.push(0);
                                last_key..range_remaining.end
                            }
                            OrderParam::Desc => range_remaining.start..last_key,
                        }
                    } else {
                        break;
                    };

                    if let Some(f) = &filter_fn {
                        kvs_vec.retain(|kv| f(kv))
                    }

                    for KvPair(k, v) in kvs_vec {
                        let k = Into::<Vec<u8>>::into(k);

                        // name space || data || reducer || break
                        let k = k
                            .get(Namespace::size() + 3..)
                            .ok_or_else(|| Error::MalformedData(k.clone(), None))?;

                        let key = UtxosByScriptHashKey::decode(k)
                            .map_err(|e| Error::MalformedData(k.to_vec(), Some(e)))?
                            .0;

                        let value = UtxosByScriptHashValue::decode(&v)
                            .map_err(|e| Error::MalformedData(v.clone(), Some(e)))?
                            .0;

                        // Evaluate whether this is a metaprotocol UTxO
                        // First, check whether it's related to the Runes metaprotocol
                        let runes: Option<rune_utxos_by_script_hash::Value> = tikv_adapter
                            .get_reducer_key_maybe::<_, rune_utxos_by_script_hash::Value>(
                                (runes_reducer, Reducer::RuneUtxosByScriptHash),
                                &key, // same key structure
                            )
                            .await?;

                        // Then, check whether it's related to the inscriptions metaprotocol
                        let inscriptions: Option<inscription_utxos_by_script_hash::Value> = tikv_adapter
                            .get_reducer_key_maybe::<_, inscription_utxos_by_script_hash::Value>(
                                (inscriptions_reducer, Reducer::InscriptionUtxosByScriptHash),
                                &key // same key structure
                            )
                            .await?;

                        let inscriptions = inscriptions.map(|x| x.inscriptions).unwrap_or_default();
                        let runes = runes.map(|x| x.runes).unwrap_or_default();

                        let inscriptions = if ignore_used_brc20 {
                            let mut filtered_inscriptions = vec![];

                            for (offset, inscription_id) in inscriptions {
                                // Get inscription info to check content
                                let info = tikv_adapter
                                    .get_reducer_key::<InscriptionInfoKey, InscriptionInfoValue>(
                                        (
                                            ReducerType::ContentByInscriptionId,
                                            Reducer::ContentByInscriptionId,
                                        ),
                                        &InscriptionInfoKey { inscription_id },
                                    )
                                    .await?;

                                let Ok(content) = String::from_utf8(info.content_body)
                                    .map(|x| x.to_ascii_lowercase())
                                else {
                                    filtered_inscriptions.push((offset, inscription_id));
                                    continue;
                                };

                                if !content.contains("brc-20") {
                                    filtered_inscriptions.push((offset, inscription_id));
                                    continue;
                                }

                                // dont ignore unspent brc20 transfer inscriptions
                                if content.contains("transfer") && key.utxo_hash == inscription_id.0
                                {
                                    filtered_inscriptions.push((offset, inscription_id));
                                    continue;
                                }

                                // ignore brc20 inscriptions if they are deploy, mint or spent transfer
                            }

                            filtered_inscriptions
                        } else {
                            inscriptions
                        };

                        if runes.is_empty() && inscriptions.is_empty() {
                            kvs.push((key, value));

                            // if max_count was provided and we have enough kvs to satisfy
                            // that count then stop scanning
                            if let Some(max) = self.count {
                                if kvs.len() >= max {
                                    break 'scan_loop;
                                }
                            }
                        }
                    }

                    // if the number of total fetched (unfiltered) kvs is less than
                    // the scan size then we have exhausted the scan range, so stop
                    if (num_kvs_fetched as u32) < scan_size {
                        break 'scan_loop;
                    }
                }
                Err(tikv_client::Error::Grpc(e))
                    if e.to_string().contains("Received message larger than max") =>
                {
                    warn!("scan size {scan_size} too large when fetching entire range {range_remaining:?}");
                    scan_size /= 2
                }
                Err(e) => return Err(Error::TiKV(e)),
            }
        }

        if let Some(max) = self.count {
            kvs.truncate(max)
        }

        Ok(kvs)
    }
}
