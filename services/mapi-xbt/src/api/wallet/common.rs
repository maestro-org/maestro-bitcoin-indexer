use axum::Extension;
use std::collections::{HashMap, HashSet};
use timbre_xbt::{
    reducers::{
        block_info::{Key as BlockInfoKey, Value as BlockInfoValue},
        rune_txs_by_script_hash::{
            Key as RuneTxsByScriptHashKey, Value as RuneTxsByScriptHashValue,
        },
        sat_txs_by_script_hash::{Key as SatTxsByScriptHashKey, Value as SatTxsByScriptHashValue},
        Height,
    },
    Reducer,
};

use crate::{
    error::{Error, MapiResult},
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    util::{fetch_rune_prices, fetch_sat_prices},
};

// Zips each KV with the USD-BTC exchange rate at its block. When no external price service is
// configured (`arranger` is `None`), each KV is zipped with `None` instead.
pub async fn zip_with_exchange_rates(
    kvs: Vec<(SatTxsByScriptHashKey, SatTxsByScriptHashValue)>,
    chain_tip_height: &u64,
    arranger: Option<&str>,
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<
    impl Iterator<
        Item = (
            (SatTxsByScriptHashKey, SatTxsByScriptHashValue),
            Option<f64>,
        ),
    >,
> {
    let Some(arranger) = arranger else {
        // No price service configured: zip with `None` prices.
        let no_prices = vec![None; kvs.len()];
        return Ok(kvs.into_iter().zip(no_prices.into_iter()));
    };

    // Will only be used if there is a mempool block.
    let mut chain_tip_timestamp = None;

    let mut timestamps = vec![];

    for (key, _) in kvs.iter() {
        if key.height > *chain_tip_height {
            // Use exchange rate at chain tip for all mempool blocks. Re-use the value fetched
            // previously if this is not the first mempool block we see.
            if let Some(ctt) = chain_tip_timestamp {
                timestamps.push(ctt)
            } else {
                let ctt = tikv
                    .get_reducer_key_maybe::<_, BlockInfoValue>(
                        (ReducerType::BlockInfo, Reducer::BlockInfo),
                        &BlockInfoKey {
                            height: *chain_tip_height,
                        },
                    )
                    .await?
                    .ok_or(Error::Internal(
                        "Unable to fetch chain tip block timestamp".into(),
                    ))?
                    .timestamp
                    .ok_or(Error::Internal(
                        "Unable to extract chain tip block timestamp".into(),
                    ))?;

                chain_tip_timestamp = Some(ctt);

                timestamps.push(ctt);
            }
        } else {
            // Fetch timestamp from `BlockInfo`.
            let block_timestamp = tikv
                .get_reducer_key_maybe::<_, BlockInfoValue>(
                    (ReducerType::BlockInfo, Reducer::BlockInfo),
                    &BlockInfoKey { height: key.height },
                )
                .await?
                .ok_or(Error::Internal("Unable to fetch block timestamp".into()))?
                .timestamp
                .ok_or(Error::Internal(
                    "Unable to extract timestamp from confirmed block".into(),
                ))?;

            timestamps.push(block_timestamp)
        }
    }

    // Fetch timestamps from external source.
    let usd_prices = fetch_sat_prices(timestamps.to_vec(), arranger)
        .await?
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();

    // Returned KVs and USD prices zipped together, ready to be iterated over.
    Ok(kvs.into_iter().zip(usd_prices.into_iter()))
}

// Builds a map from block height to rune ID to USD price. Returns `None` when no external price
// service is configured (`arranger` is `None`).
pub async fn build_rune_prices_map(
    kvs: &Vec<(RuneTxsByScriptHashKey, RuneTxsByScriptHashValue)>,
    chain_tip_height: Height,
    arranger: Option<&str>,
    tikv: &mut Extension<TiKVAdapter>,
) -> MapiResult<Option<HashMap<Height, HashMap<String, f64>>>> {
    let Some(arranger) = arranger else {
        return Ok(None);
    };

    // Extract
    let mut height_and_runes: HashSet<(Height, String)> = HashSet::new();

    for (key, value) in kvs.iter() {
        if let Some((etching_block, etching_tx)) = value.minted {
            height_and_runes.insert((key.height, format!("{}:{}", etching_block, etching_tx)));
        }

        for ((etching_block, etching_tx), _) in value.self_transfers.iter() {
            height_and_runes.insert((key.height, format!("{}:{}", etching_block, etching_tx)));
        }

        for ((etching_block, etching_tx), _) in value.increased_balances.iter() {
            height_and_runes.insert((key.height, format!("{}:{}", etching_block, etching_tx)));
        }

        for ((etching_block, etching_tx), _) in value.decreased_balances.iter() {
            height_and_runes.insert((key.height, format!("{}:{}", etching_block, etching_tx)));
        }
    }

    // Ensure the by-height order is preserved for zipping.
    let mut height_and_runes = height_and_runes.into_iter().collect::<Vec<_>>();
    height_and_runes.sort_by_key(|(height, _)| *height);

    let heights = height_and_runes
        .iter()
        .map(|(height, _)| *height)
        .collect::<Vec<_>>();

    let runes = height_and_runes
        .iter()
        .map(|(_, rune)| rune.clone())
        .collect::<Vec<_>>();

    // Get timestamp for each block.
    let mut timestamps = vec![];

    // Will only be used if there is a mempool block.
    let mut chain_tip_timestamp = None;

    for height in heights {
        if height > chain_tip_height {
            if let Some(ctt) = chain_tip_timestamp {
                timestamps.push(ctt);
            } else {
                let ctt = tikv
                    .get_reducer_key_maybe::<_, BlockInfoValue>(
                        (ReducerType::BlockInfo, Reducer::BlockInfo),
                        &BlockInfoKey {
                            height: chain_tip_height,
                        },
                    )
                    .await?
                    .ok_or(Error::Internal(
                        "Unable to fetch chain tip block timestamp".into(),
                    ))?
                    .timestamp
                    .ok_or(Error::Internal(
                        "Unable to extract chain tip block timestamp".into(),
                    ))?;

                chain_tip_timestamp = Some(ctt);

                timestamps.push(ctt);
            }
        } else {
            // Fetch timestamp from `BlockInfo`.
            let block_timestamp = tikv
                .get_reducer_key_maybe::<_, BlockInfoValue>(
                    (ReducerType::BlockInfo, Reducer::BlockInfo),
                    &BlockInfoKey { height },
                )
                .await?
                .ok_or(Error::Internal("Unable to fetch block timestamp".into()))?
                .timestamp
                .ok_or(Error::Internal(
                    "Unable to extract timestamp from confirmed block".into(),
                ))?;

            timestamps.push(block_timestamp)
        }
    }

    // Fetch prices from Arranger by submitting a list of pairs of timestamp and rune.
    let usd_prices = fetch_rune_prices(
        timestamps.into_iter().zip(runes).collect::<Vec<_>>(),
        arranger,
    )
    .await?;

    let mut res = HashMap::new();

    for ((height, rune), price) in height_and_runes.into_iter().zip(usd_prices.into_iter()) {
        res.entry(height)
            .and_modify(|rune_and_prices: &mut HashMap<String, f64>| {
                rune_and_prices.insert(rune.clone(), price);
            })
            .or_insert(HashMap::from([(rune, price)]));
    }

    Ok(Some(res))
}
