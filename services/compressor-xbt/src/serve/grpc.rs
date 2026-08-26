use std::{collections::HashMap, pin::Pin};

use bitcoin::{hashes::Hash, BlockHash};
use brc20_action::Kind;
use futures_core::Stream;
use futures_util::StreamExt;
use tonic::{Request, Response, Status};
use tracing::{debug, error, warn};

use crate::mempool::SharedMempoolCache;
use crate::storage::{
    chain::BlockByHeightKV,
    inscriptions::updater::BRC20Message,
    kvtable::{DBInt, DBSerde, KVTable},
    mempool::{MempoolBlockValue, MempoolInfoValue},
    mutable::{Log, MutableKV},
    resolver::ResolverByHeightKV,
    ChainDB, TxoBody, TxoRef,
};

pub mod compressor_api {
    tonic::include_proto!("compressorxbt.sync.v1"); // The string specified here must match the proto package name
}

use compressor_api::*;

use self::stream_updates_with_ctx_response::Action;

pub struct SyncServerImpl {
    chain_db: ChainDB,
    shared_cache: Option<SharedMempoolCache>,
}

impl SyncServerImpl {
    pub fn new(chain_db: ChainDB, shared_cache: Option<SharedMempoolCache>) -> Self {
        Self {
            chain_db,
            shared_cache,
        }
    }
}

#[async_trait::async_trait]
impl sync_service_server::SyncService for SyncServerImpl {
    type StreamUpdatesWithContextStream =
        Pin<Box<dyn Stream<Item = Result<StreamUpdatesWithCtxResponse, Status>> + Send + 'static>>;

    async fn page_blocks_with_context(
        &self,
        request: Request<PageBlocksWithCtxRequest>,
    ) -> Result<Response<PageBlocksWithCtxResponse>, Status> {
        let request = request.into_inner();

        if request.max_items < 1 {
            return Err(Status::invalid_argument("max items must be greater than 0"));
        }

        let max_items = request.max_items as usize;

        debug!(
            "received page blocks with context request, {max_items} items from cursor: {:?}",
            request.cursor.as_ref().map(|x| hex::encode(&x.hash))
        );

        let db_tx = self.chain_db.db.snapshot();

        // --- start chain iterator and check intersect on chain

        let intersect_hash = request.cursor.clone().map(|x| x.hash);
        let intersect_height = request
            .cursor
            .as_ref()
            .map(|x| x.height)
            .unwrap_or_default();

        let instant_a = tokio::time::Instant::now();

        let mut block_by_height_iter = BlockByHeightKV::iter_entries_from_snapshot(
            &self.chain_db.db,
            &db_tx,
            DBInt(intersect_height),
        );

        // if we have an intersect hash, get the intersect entry from block by height
        if let Some(hash) = intersect_hash.clone() {
            let (DBInt(found_height), DBSerde((found_hash, _))) = block_by_height_iter
                .next()
                .ok_or(Status::not_found("intersect not found (no entry)"))?
                .map_err(Status::internal)?;

            if found_height != intersect_height {
                return Err(Status::not_found("intersect not found (height mismatch)"));
            };

            if found_hash.to_byte_array().to_vec() != hash {
                return Err(Status::not_found("intersect not found (hash mismatch)"));
            }
        }

        // --- take page of blocks

        let mut page_blocks = block_by_height_iter
            .take(max_items + 1)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Status::internal)?;

        let next_cursor = if page_blocks.len() == max_items + 1 {
            page_blocks.remove(max_items);
            page_blocks
                .last()
                .map(|(DBInt(height), DBSerde((hash, _)))| BlockRef {
                    height: *height,
                    hash: hash.to_byte_array().to_vec(),
                })
        } else {
            None
        };

        let blockfetch_duration = instant_a.elapsed();

        // --- fetch resolvers

        let instant_b = tokio::time::Instant::now();

        let mut resolver_by_height_iter = ResolverByHeightKV::iter_entries_from_snapshot(
            &self.chain_db.db,
            &db_tx,
            DBInt(intersect_height),
        );

        if intersect_hash.is_some() {
            resolver_by_height_iter.next(); // skip intersect if needed
        }

        let page_resolvers = resolver_by_height_iter
            .take(max_items)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Status::internal)?;

        let resolving_duration = instant_b.elapsed();

        // ---

        if page_blocks.len() != page_resolvers.len() {
            error!(
                "blocks/resolvers len mismatch {:?} vs {:?}",
                page_blocks
                    .iter()
                    .map(|x| x.0 .0.clone())
                    .collect::<Vec<_>>(),
                page_resolvers
                    .iter()
                    .map(|x| x.0 .0.clone())
                    .collect::<Vec<_>>()
            );
            return Err(Status::internal("blocks/resolvers len mismatch"));
        }

        let mut page = vec![];

        for (block, block_resolver) in page_blocks.into_iter().zip(page_resolvers) {
            let (DBInt(height), DBSerde((block_hash, bytes))) = block;
            let (
                _,
                DBSerde((
                    resolver_hash,
                    resolver,
                    output_runes_resolver,
                    etchs,
                    mints,
                    output_inscriptions_resolver,
                    valid_reinscriptions,
                    _,
                    brc20_actions,
                    new_inscriptions,
                )),
            ) = block_resolver;

            if block_hash != resolver_hash {
                error!(
                    "blocks/resolvers hash mismatch ({}, {}, {})",
                    height, block_hash, resolver_hash
                );

                return Err(Status::internal("blocks/resolvers hash mismatch"));
            }

            let runes_info = {
                let mut runes_resolver: Vec<RunesResolvedTxo> = resolver
                    .clone()
                    .into_iter()
                    .map(
                        |(TxoRef(ref_hash, ref_index), TxoBody { runes, .. })| RunesResolvedTxo {
                            r#ref: Some(compressor_api::TxoRef {
                                tx_hash: ref_hash.as_byte_array().to_vec(),
                                txo_index: ref_index as u32,
                            }),
                            runes: runes
                                .into_iter()
                                .map(|(r, amount)| compressor_api::Rune {
                                    id: Some(compressor_api::RuneId {
                                        block: r.block,
                                        tx: r.tx as u32,
                                    }),
                                    amount: u128::to_be_bytes(amount).to_vec(),
                                })
                                .collect(),
                        },
                    )
                    .collect();

                let output_runes: Vec<RunesResolvedTxo> = output_runes_resolver
                    .into_iter()
                    .map(|(TxoRef(ref_hash, ref_index), runes)| RunesResolvedTxo {
                        r#ref: Some(compressor_api::TxoRef {
                            tx_hash: ref_hash.as_byte_array().to_vec(),
                            txo_index: ref_index as u32,
                        }),
                        runes: runes
                            .into_iter()
                            .map(|(r, amount)| compressor_api::Rune {
                                id: Some(compressor_api::RuneId {
                                    block: r.block,
                                    tx: r.tx as u32,
                                }),
                                amount: u128::to_be_bytes(amount).to_vec(),
                            })
                            .collect(),
                    })
                    .collect();

                runes_resolver.extend(output_runes);

                Some(RunesInfo {
                    txo_resolver: runes_resolver,
                    successful_etchs: etchs,
                    successful_mints: mints,
                })
            };

            let inscriptions_info = {
                let mut inscriptions_resolver: HashMap<TxoRef, Vec<InscriptionAtOffset>> =
                    HashMap::new();

                for (txoref, body) in resolver.clone() {
                    for (offset, inscription) in body.inscriptions {
                        let x = InscriptionAtOffset {
                            id: Some(InscriptionId {
                                tx_hash: inscription.txid.to_byte_array().to_vec(),
                                index: inscription.index,
                            }),
                            offset: offset as u32,
                        };

                        inscriptions_resolver
                            .entry(txoref.clone())
                            .or_default()
                            .push(x);
                    }
                }

                let mut inscriptions_resolver: Vec<InscriptionResolvedTxo> = inscriptions_resolver
                    .into_iter()
                    .map(
                        |(TxoRef(ref_hash, ref_index), inscriptions)| InscriptionResolvedTxo {
                            r#ref: Some(compressor_api::TxoRef {
                                tx_hash: ref_hash.as_byte_array().to_vec(),
                                txo_index: ref_index as u32,
                            }),
                            inscriptions,
                        },
                    )
                    .collect();

                // outputs

                let mut inscriptions_resolver_outs: HashMap<TxoRef, Vec<InscriptionAtOffset>> =
                    HashMap::new();

                for (txoref, ins) in output_inscriptions_resolver {
                    for (offset, inscription) in ins {
                        let x = InscriptionAtOffset {
                            id: Some(InscriptionId {
                                tx_hash: inscription.txid.to_byte_array().to_vec(),
                                index: inscription.index,
                            }),
                            offset: offset as u32,
                        };

                        inscriptions_resolver_outs
                            .entry(txoref.clone())
                            .or_default()
                            .push(x);
                    }
                }

                let output_inscs: Vec<InscriptionResolvedTxo> = inscriptions_resolver_outs
                    .into_iter()
                    .map(
                        |(TxoRef(ref_hash, ref_index), inscriptions)| InscriptionResolvedTxo {
                            r#ref: Some(compressor_api::TxoRef {
                                tx_hash: ref_hash.as_byte_array().to_vec(),
                                txo_index: ref_index as u32,
                            }),
                            inscriptions,
                        },
                    )
                    .collect();

                inscriptions_resolver.extend(output_inscs);

                let valid_reinscriptions = valid_reinscriptions
                    .into_iter()
                    .map(|(x, y)| ValidReinscription {
                        tx_index: x,
                        inscription_index: y,
                    })
                    .collect();

                let brc20_resolver = brc20_actions
                    .into_iter()
                    .map(|(id, action)| {
                        let x = match action {
                            BRC20Message::Deploy(ticker) => Kind::Deploy(Brc20Deploy { ticker }),
                            BRC20Message::Mint(ticker, amt, sh) => Kind::Mint(Brc20Mint {
                                ticker,
                                amt: amt.to_be_bytes().to_vec(),
                                script_hash: sh.as_byte_array().to_vec(),
                            }),
                            BRC20Message::Transfer(ticker, amt, op, sh) => {
                                let txo_ref = compressor_api::TxoRef {
                                    tx_hash: op.txid.as_byte_array().to_vec(),
                                    txo_index: op.vout,
                                };

                                Kind::Transfer(Brc20Transfer {
                                    ticker,
                                    amt: amt.to_be_bytes().to_vec(),
                                    first_output: Some(txo_ref),
                                    script_hash: sh.as_byte_array().to_vec(),
                                })
                            }
                            BRC20Message::TransferInit(ticker, amt, sh) => {
                                Kind::TransferInit(Brc20TransferInit {
                                    ticker,
                                    amt: amt.to_be_bytes().to_vec(),
                                    script_hash: sh.as_byte_array().to_vec(),
                                })
                            }
                        };

                        Brc20Resolver {
                            id: Some(InscriptionId {
                                tx_hash: id.txid.as_byte_array().to_vec(),
                                index: id.index,
                            }),
                            action: Some(Brc20Action { kind: Some(x) }),
                        }
                    })
                    .collect();

                let new_inscriptions = new_inscriptions
                    .into_iter()
                    .map(|(inscription_id, inscription_num)| {
                        let id: InscriptionId = InscriptionId {
                            tx_hash: inscription_id.txid.as_byte_array().to_vec(),
                            index: inscription_id.index,
                        };
                        InscriptionOrigin {
                            id: Some(id),
                            inscription_num,
                        }
                    })
                    .collect();

                Some(InscriptionsInfo {
                    txo_resolver: inscriptions_resolver,
                    valid_reinscriptions,
                    brc20_resolver,
                    new_inscriptions,
                })
            };

            let txo_resolver = resolver
                .into_iter()
                .map(
                    |(
                        TxoRef(ref_hash, ref_index),
                        TxoBody {
                            height: txo_height,
                            raw,
                            ord_ranges: ords,
                            ..
                        },
                    )| ResolvedTxo {
                        r#ref: Some(compressor_api::TxoRef {
                            tx_hash: ref_hash.as_byte_array().to_vec(),
                            txo_index: ref_index as u32,
                        }),
                        height: txo_height,
                        raw,
                        ord_ranges: ords
                            .to_vec()
                            .into_iter()
                            .map(|r| compressor_api::OrdinalRange {
                                lower: r.lower(),
                                upper: r.upper(),
                            })
                            .collect(),
                    },
                )
                .collect();

            page.push(BlockWithContext {
                r#ref: Some(BlockRef {
                    height,
                    hash: block_hash.as_byte_array().to_vec(),
                }),
                raw: bytes,
                txo_resolver,
                runes: runes_info,
                inscriptions: inscriptions_info,
            });
        }

        // ---

        let chain_tip = match BlockByHeightKV::iter_entries_snapshot(
            &self.chain_db.db,
            &db_tx,
            rocksdb::IteratorMode::End,
        )
        .next()
        {
            Some(entry) => {
                let (DBInt(tip_height), DBSerde((tip_hash, _))) =
                    entry.map_err(Status::internal)?;

                Some(BlockRef {
                    height: tip_height,
                    hash: tip_hash.to_byte_array().to_vec(),
                })
            }
            None => None,
        };

        // ---

        let response = PageBlocksWithCtxResponse {
            blocks: page,
            next_cursor,
            chain_tip,
        };

        debug!(
            "finished processing page with context req (fetching: {:?}ms, resolving: {:?}ms)",
            blockfetch_duration.as_millis(),
            resolving_duration.as_millis()
        );

        Ok(Response::new(response))
    }

    async fn stream_updates_with_context(
        &self,
        request: Request<StreamUpdatesWithCtxRequest>,
    ) -> std::result::Result<Response<Self::StreamUpdatesWithContextStream>, Status> {
        let db_tx = self.chain_db.db.snapshot();

        let request = request.into_inner();

        for intersect in request.intersects {
            let BlockRef {
                height: i_height,
                hash: i_hash,
            } = intersect;

            let i_hash: [u8; 32] = i_hash
                .try_into()
                .map_err(|_| Status::invalid_argument("invalid intersect block hash"))?;

            let maybe_wal_seq = MutableKV::find_wal_seq(
                &self.chain_db.db,
                &db_tx,
                i_height,
                BlockHash::from_byte_array(i_hash),
            )
            .map_err(Status::internal)?;

            if let Some(wal_seq) = maybe_wal_seq {
                let stream = MutableKV::stream_mutable(&self.chain_db, wal_seq).map(|x| match x {
                    Ok(log) => Ok(log_to_response_with_ctx(log)),
                    Err(_) => Err(Status::internal("streamupdateswithctx returned error")),
                });

                return Ok(Response::new(Box::pin(stream)));
            }
        }

        return Err(Status::not_found(
            "no intersect found with mutable part of chain",
        ));
    }

    async fn mempool_blocks_with_context(
        &self,
        request: Request<MempoolBlocksWithCtxRequest>,
    ) -> Result<Response<MempoolBlocksWithCtxResponse>, Status> {
        let request = request.into_inner();

        debug!(
            "received mempool blocks with context request, with tip intersect cursor: {:?}, {} cached txs",
            request.tip_intersect.as_ref().map(|x| hex::encode(&x.hash)),
            request.cached_txs.len()
        );

        // Build a set of cached tx hashes for fast lookup
        let cached_tx_set: std::collections::HashSet<Vec<u8>> =
            request.cached_txs.into_iter().collect();

        // If we have a shared cache, try to use it first
        if let Some(ref shared_cache) = self.shared_cache {
            let cache_guard = shared_cache.read().await;
            if let Some(ref cache) = *cache_guard {
                // Check intersect if provided
                if let Some(tip_intersect) = &request.tip_intersect {
                    let intersect_height = tip_intersect.height;
                    let intersect_hash = tip_intersect.hash.clone();

                    if cache.mempool_info.chain_tip.0 == intersect_height
                        && cache.mempool_info.chain_tip.1.to_vec() == intersect_hash
                    {
                        debug!(
                            "serving mempool blocks from cache, chain_tip: ({}, {}), mempool_view_ts: {}",
                            cache.mempool_info.chain_tip.0,
                            hex::encode(cache.mempool_info.chain_tip.1),
                            cache.mempool_info.mempool_view_ts
                        );

                        // Filter out cached transactions
                        let mut response = cache.response.clone();
                        filter_cached_txs(&mut response, &cached_tx_set);

                        return Ok(Response::new(response));
                    } else {
                        warn!(
                            "cache chain tip mismatch: expected ({}, {}), got ({}, {})",
                            intersect_height,
                            hex::encode(&intersect_hash),
                            cache.mempool_info.chain_tip.0,
                            hex::encode(cache.mempool_info.chain_tip.1)
                        );
                        return Err(Status::not_found(
                            "mempool data not available - cache chain tip",
                        ));
                    }
                } else {
                    // No intersect check, return cached data if available
                    debug!(
                        "serving mempool blocks from cache (no intersect check), chain_tip: ({}, {}), mempool_view_ts: {}",
                        cache.mempool_info.chain_tip.0,
                        hex::encode(cache.mempool_info.chain_tip.1),
                        cache.mempool_info.mempool_view_ts
                    );

                    // Filter out cached transactions
                    let mut response = cache.response.clone();
                    filter_cached_txs(&mut response, &cached_tx_set);

                    return Ok(Response::new(response));
                }
            }
        }

        // No cache available - return error
        warn!("mempool blocks requested but no shared cache available");
        Err(Status::not_found(
            "mempool data not available - cache not initialized",
        ))
    }
}

/// Filter out cached transactions from mempool blocks response
/// For transactions that are in the cached set, replace their bytes with empty bytes
/// Uses txids field from the response to avoid decoding transactions
fn filter_cached_txs(
    response: &mut MempoolBlocksWithCtxResponse,
    cached_tx_set: &std::collections::HashSet<Vec<u8>>,
) {
    for block in response.blocks.iter_mut() {
        assert_eq!(block.raw_txs.len(), block.txids.len());

        for (tx_idx, tx_bytes) in block.raw_txs.iter_mut().enumerate() {
            if !tx_bytes.is_empty() {
                let txid = &block.txids[tx_idx];

                // If this transaction is in the cached set, replace with empty bytes
                if cached_tx_set.contains(txid) {
                    *tx_bytes = Vec::new();
                }
            }
        }
    }
}

fn log_to_response_with_ctx(log: Log) -> StreamUpdatesWithCtxResponse {
    let action = match log {
        Log::Apply(
            height,
            hash,
            body,
            resolver,
            output_runes_resolver,
            etchs,
            mints,
            output_inscriptions_resolver,
            valid_reinscriptions,
            _,
            brc20_actions,
            new_inscriptions,
        ) => {
            let runes_info = {
                let mut runes_resolver: Vec<RunesResolvedTxo> = resolver
                    .clone()
                    .into_iter()
                    .map(
                        |(TxoRef(ref_hash, ref_index), TxoBody { runes, .. })| RunesResolvedTxo {
                            r#ref: Some(compressor_api::TxoRef {
                                tx_hash: ref_hash.as_byte_array().to_vec(),
                                txo_index: ref_index as u32,
                            }),
                            runes: runes
                                .into_iter()
                                .map(|(r, amount)| compressor_api::Rune {
                                    id: Some(compressor_api::RuneId {
                                        block: r.block,
                                        tx: r.tx as u32,
                                    }),
                                    amount: u128::to_be_bytes(amount).to_vec(),
                                })
                                .collect(),
                        },
                    )
                    .collect();

                let output_runes: Vec<RunesResolvedTxo> = output_runes_resolver
                    .into_iter()
                    .map(|(TxoRef(ref_hash, ref_index), runes)| RunesResolvedTxo {
                        r#ref: Some(compressor_api::TxoRef {
                            tx_hash: ref_hash.as_byte_array().to_vec(),
                            txo_index: ref_index as u32,
                        }),
                        runes: runes
                            .into_iter()
                            .map(|(r, amount)| compressor_api::Rune {
                                id: Some(compressor_api::RuneId {
                                    block: r.block,
                                    tx: r.tx as u32,
                                }),
                                amount: u128::to_be_bytes(amount).to_vec(),
                            })
                            .collect(),
                    })
                    .collect();

                runes_resolver.extend(output_runes);

                Some(RunesInfo {
                    txo_resolver: runes_resolver,
                    successful_etchs: etchs,
                    successful_mints: mints,
                })
            };

            let inscriptions_info = {
                let mut inscriptions_resolver: HashMap<TxoRef, Vec<InscriptionAtOffset>> =
                    HashMap::new();

                for (txoref, body) in resolver.clone() {
                    for (offset, inscription) in body.inscriptions {
                        let x = InscriptionAtOffset {
                            id: Some(InscriptionId {
                                tx_hash: inscription.txid.to_byte_array().to_vec(),
                                index: inscription.index,
                            }),
                            offset: offset as u32,
                        };

                        inscriptions_resolver
                            .entry(txoref.clone())
                            .or_default()
                            .push(x);
                    }
                }

                let mut inscriptions_resolver: Vec<InscriptionResolvedTxo> = inscriptions_resolver
                    .into_iter()
                    .map(
                        |(TxoRef(ref_hash, ref_index), inscriptions)| InscriptionResolvedTxo {
                            r#ref: Some(compressor_api::TxoRef {
                                tx_hash: ref_hash.as_byte_array().to_vec(),
                                txo_index: ref_index as u32,
                            }),
                            inscriptions,
                        },
                    )
                    .collect();

                // outputs

                let mut inscriptions_resolver_outs: HashMap<TxoRef, Vec<InscriptionAtOffset>> =
                    HashMap::new();

                for (txoref, ins) in output_inscriptions_resolver {
                    for (offset, inscription) in ins {
                        let x = InscriptionAtOffset {
                            id: Some(InscriptionId {
                                tx_hash: inscription.txid.to_byte_array().to_vec(),
                                index: inscription.index,
                            }),
                            offset: offset as u32,
                        };

                        inscriptions_resolver_outs
                            .entry(txoref.clone())
                            .or_default()
                            .push(x);
                    }
                }

                let output_inscs: Vec<InscriptionResolvedTxo> = inscriptions_resolver_outs
                    .into_iter()
                    .map(
                        |(TxoRef(ref_hash, ref_index), inscriptions)| InscriptionResolvedTxo {
                            r#ref: Some(compressor_api::TxoRef {
                                tx_hash: ref_hash.as_byte_array().to_vec(),
                                txo_index: ref_index as u32,
                            }),
                            inscriptions,
                        },
                    )
                    .collect();

                inscriptions_resolver.extend(output_inscs);

                let valid_reinscriptions = valid_reinscriptions
                    .into_iter()
                    .map(|(x, y)| ValidReinscription {
                        tx_index: x,
                        inscription_index: y,
                    })
                    .collect();

                let brc20_resolver = brc20_actions
                    .into_iter()
                    .map(|(id, action)| {
                        let x = match action {
                            BRC20Message::Deploy(ticker) => Kind::Deploy(Brc20Deploy { ticker }),
                            BRC20Message::Mint(ticker, amt, sh) => Kind::Mint(Brc20Mint {
                                ticker,
                                amt: amt.to_be_bytes().to_vec(),
                                script_hash: sh.as_byte_array().to_vec(),
                            }),
                            BRC20Message::Transfer(ticker, amt, op, sh) => {
                                let txo_ref = compressor_api::TxoRef {
                                    tx_hash: op.txid.as_byte_array().to_vec(),
                                    txo_index: op.vout,
                                };

                                Kind::Transfer(Brc20Transfer {
                                    ticker,
                                    amt: amt.to_be_bytes().to_vec(),
                                    first_output: Some(txo_ref),
                                    script_hash: sh.as_byte_array().to_vec(),
                                })
                            }
                            BRC20Message::TransferInit(ticker, amt, sh) => {
                                Kind::TransferInit(Brc20TransferInit {
                                    ticker,
                                    amt: amt.to_be_bytes().to_vec(),
                                    script_hash: sh.as_byte_array().to_vec(),
                                })
                            }
                        };

                        Brc20Resolver {
                            id: Some(InscriptionId {
                                tx_hash: id.txid.as_byte_array().to_vec(),
                                index: id.index,
                            }),
                            action: Some(Brc20Action { kind: Some(x) }),
                        }
                    })
                    .collect();

                let new_inscriptions = new_inscriptions
                    .into_iter()
                    .map(|(inscription_id, inscription_num)| {
                        let id: InscriptionId = InscriptionId {
                            tx_hash: inscription_id.txid.as_byte_array().to_vec(),
                            index: inscription_id.index,
                        };
                        InscriptionOrigin {
                            id: Some(id),
                            inscription_num,
                        }
                    })
                    .collect();

                Some(InscriptionsInfo {
                    txo_resolver: inscriptions_resolver,
                    valid_reinscriptions,
                    brc20_resolver,
                    new_inscriptions,
                })
            };

            let txo_resolver = resolver
                .into_iter()
                .map(
                    |(
                        TxoRef(ref_hash, ref_index),
                        TxoBody {
                            height: txo_height,
                            raw,
                            ord_ranges: ords,
                            ..
                        },
                    )| ResolvedTxo {
                        r#ref: Some(compressor_api::TxoRef {
                            tx_hash: ref_hash.to_byte_array().to_vec(),
                            txo_index: ref_index as u32,
                        }),
                        height: txo_height,
                        raw,
                        ord_ranges: ords
                            .to_vec()
                            .into_iter()
                            .map(|r| compressor_api::OrdinalRange {
                                lower: r.lower(),
                                upper: r.upper(),
                            })
                            .collect(),
                    },
                )
                .collect();

            let block_with_ctx = BlockWithContext {
                r#ref: Some(BlockRef {
                    height,
                    hash: hash.to_byte_array().to_vec(),
                }),
                raw: body,
                txo_resolver,
                runes: runes_info,
                inscriptions: inscriptions_info,
            };

            Action::Apply(block_with_ctx)
        }
        Log::Undo(height, hash, _) => Action::Undo(BlockRef {
            height,
            hash: hash.to_byte_array().to_vec(),
        }),
        Log::Mark(height, hash, _) => Action::Reset(BlockRef {
            height,
            hash: hash.to_byte_array().to_vec(),
        }),
    };

    StreamUpdatesWithCtxResponse {
        action: Some(action),
    }
}

/// Convert a single MempoolBlockValue to MempoolBlockWithContext protobuf format
pub fn convert_mempool_block_to_proto(
    height: u64,
    mpbv: MempoolBlockValue,
    mempool_info: &MempoolInfoValue,
) -> MempoolBlockWithContext {
    let runes_info = {
        let mut runes_resolver: Vec<RunesResolvedTxo> = mpbv
            .resolver
            .clone()
            .into_iter()
            .map(
                |(TxoRef(ref_hash, ref_index), TxoBody { runes, .. })| RunesResolvedTxo {
                    r#ref: Some(compressor_api::TxoRef {
                        tx_hash: ref_hash.as_byte_array().to_vec(),
                        txo_index: ref_index as u32,
                    }),
                    runes: runes
                        .into_iter()
                        .map(|(r, amount)| compressor_api::Rune {
                            id: Some(compressor_api::RuneId {
                                block: r.block,
                                tx: r.tx as u32,
                            }),
                            amount: u128::to_be_bytes(amount).to_vec(),
                        })
                        .collect(),
                },
            )
            .collect();

        let output_runes: Vec<RunesResolvedTxo> = mpbv
            .output_runes_resolver
            .into_iter()
            .map(|(TxoRef(ref_hash, ref_index), runes)| RunesResolvedTxo {
                r#ref: Some(compressor_api::TxoRef {
                    tx_hash: ref_hash.as_byte_array().to_vec(),
                    txo_index: ref_index as u32,
                }),
                runes: runes
                    .into_iter()
                    .map(|(r, amount)| compressor_api::Rune {
                        id: Some(compressor_api::RuneId {
                            block: r.block,
                            tx: r.tx as u32,
                        }),
                        amount: u128::to_be_bytes(amount).to_vec(),
                    })
                    .collect(),
            })
            .collect();

        runes_resolver.extend(output_runes);

        Some(RunesInfo {
            txo_resolver: runes_resolver,
            successful_etchs: mpbv.successful_etches,
            successful_mints: mpbv.successful_mints,
        })
    };

    let inscriptions_info = {
        let mut inscriptions_resolver: HashMap<TxoRef, Vec<InscriptionAtOffset>> = HashMap::new();

        for (txoref, body) in mpbv.resolver.clone() {
            for (offset, inscription) in body.inscriptions {
                let x = InscriptionAtOffset {
                    id: Some(InscriptionId {
                        tx_hash: inscription.txid.to_byte_array().to_vec(),
                        index: inscription.index,
                    }),
                    offset: offset as u32,
                };

                inscriptions_resolver
                    .entry(txoref.clone())
                    .or_default()
                    .push(x);
            }
        }

        let mut inscriptions_resolver: Vec<InscriptionResolvedTxo> = inscriptions_resolver
            .into_iter()
            .map(
                |(TxoRef(ref_hash, ref_index), inscriptions)| InscriptionResolvedTxo {
                    r#ref: Some(compressor_api::TxoRef {
                        tx_hash: ref_hash.as_byte_array().to_vec(),
                        txo_index: ref_index as u32,
                    }),
                    inscriptions,
                },
            )
            .collect();

        // outputs

        let mut inscriptions_resolver_outs: HashMap<TxoRef, Vec<InscriptionAtOffset>> =
            HashMap::new();

        for (txoref, ins) in mpbv.output_inscriptions_resolver {
            for (offset, inscription) in ins {
                let x = InscriptionAtOffset {
                    id: Some(InscriptionId {
                        tx_hash: inscription.txid.to_byte_array().to_vec(),
                        index: inscription.index,
                    }),
                    offset: offset as u32,
                };

                inscriptions_resolver_outs
                    .entry(txoref.clone())
                    .or_default()
                    .push(x);
            }
        }

        let output_inscs: Vec<InscriptionResolvedTxo> = inscriptions_resolver_outs
            .into_iter()
            .map(
                |(TxoRef(ref_hash, ref_index), inscriptions)| InscriptionResolvedTxo {
                    r#ref: Some(compressor_api::TxoRef {
                        tx_hash: ref_hash.as_byte_array().to_vec(),
                        txo_index: ref_index as u32,
                    }),
                    inscriptions,
                },
            )
            .collect();

        inscriptions_resolver.extend(output_inscs);

        let valid_reinscriptions = mpbv
            .valid_reinscriptions
            .into_iter()
            .map(|(x, y)| ValidReinscription {
                tx_index: x,
                inscription_index: y,
            })
            .collect();

        let brc20_resolver = mpbv
            .brc20_resolver
            .into_iter()
            .map(|(id, action)| {
                let x = match action {
                    BRC20Message::Deploy(ticker) => Kind::Deploy(Brc20Deploy { ticker }),
                    BRC20Message::Mint(ticker, amt, sh) => Kind::Mint(Brc20Mint {
                        ticker,
                        amt: amt.to_be_bytes().to_vec(),
                        script_hash: sh.as_byte_array().to_vec(),
                    }),
                    BRC20Message::Transfer(ticker, amt, op, sh) => {
                        let txo_ref = compressor_api::TxoRef {
                            tx_hash: op.txid.as_byte_array().to_vec(),
                            txo_index: op.vout,
                        };

                        Kind::Transfer(Brc20Transfer {
                            ticker,
                            amt: amt.to_be_bytes().to_vec(),
                            first_output: Some(txo_ref),
                            script_hash: sh.as_byte_array().to_vec(),
                        })
                    }
                    BRC20Message::TransferInit(ticker, amt, sh) => {
                        Kind::TransferInit(Brc20TransferInit {
                            ticker,
                            amt: amt.to_be_bytes().to_vec(),
                            script_hash: sh.as_byte_array().to_vec(),
                        })
                    }
                };

                Brc20Resolver {
                    id: Some(InscriptionId {
                        tx_hash: id.txid.as_byte_array().to_vec(),
                        index: id.index,
                    }),
                    action: Some(Brc20Action { kind: Some(x) }),
                }
            })
            .collect();

        let new_inscriptions = mpbv
            .new_inscriptions
            .into_iter()
            .map(|(inscription_id, inscription_num)| {
                let id: InscriptionId = InscriptionId {
                    tx_hash: inscription_id.txid.as_byte_array().to_vec(),
                    index: inscription_id.index,
                };
                InscriptionOrigin {
                    id: Some(id),
                    inscription_num,
                }
            })
            .collect();

        Some(InscriptionsInfo {
            txo_resolver: inscriptions_resolver,
            valid_reinscriptions,
            brc20_resolver,
            new_inscriptions,
        })
    };

    let txo_resolver = mpbv
        .resolver
        .into_iter()
        .map(
            |(
                TxoRef(ref_hash, ref_index),
                TxoBody {
                    height: txo_height,
                    raw,
                    ord_ranges: ords,
                    ..
                },
            )| ResolvedTxo {
                r#ref: Some(compressor_api::TxoRef {
                    tx_hash: ref_hash.as_byte_array().to_vec(),
                    txo_index: ref_index as u32,
                }),
                height: txo_height,
                raw,
                ord_ranges: ords
                    .to_vec()
                    .into_iter()
                    .map(|r| compressor_api::OrdinalRange {
                        lower: r.lower(),
                        upper: r.upper(),
                    })
                    .collect(),
            },
        )
        .collect();

    let txids: Vec<Vec<u8>> = mpbv
        .txs
        .iter()
        .map(|(txid, _)| txid.as_byte_array().to_vec())
        .collect();
    let raw_txs: Vec<Vec<u8>> = mpbv.txs.into_iter().map(|(_, b)| b).collect();

    MempoolBlockWithContext {
        r#ref: Some(BlockRef {
            height,
            hash: mempool_info.chain_tip.1.to_vec(),
        }),
        raw_txs,
        txo_resolver,
        runes: runes_info,
        inscriptions: inscriptions_info,
        chain_tip: Some(BlockRef {
            height: mempool_info.chain_tip.0,
            hash: mempool_info.chain_tip.1.to_vec(),
        }),
        height,
        merkle_root: mpbv.merkle_root.to_vec(),
        mempool_view_ts: mempool_info.mempool_view_ts,
        txids,
    }
}
