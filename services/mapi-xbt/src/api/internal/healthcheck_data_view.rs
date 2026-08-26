use axum::{response::IntoResponse, response::Response, Extension};

use bitcoin::{hashes::Hash, BlockHash};
use reqwest::StatusCode;

use crate::{
    api::{addresses, blocks, inscriptions, mempool, runes, transactions},
    error::Error,
    options::Mode,
    tikv::adapter::TiKVAdapter,
};
use std::time::Instant;

pub async fn data_view_healthcheck(
    mut tikv: Extension<TiKVAdapter>,
    _mode: Extension<Mode>,
) -> Result<Response, Error> {
    let mut out_map = serde_json::Map::new();

    let mut errors = vec![];
    let mut warnings = vec![];

    let mut best_height = 0;
    let mut all_heights = vec![];

    // ---

    let endpoint_reducers = vec![
        (
            addresses::satoshi_balance_by_address::REQUIRED_REDUCERS,
            "satoshi_balance_by_address",
        ),
        (
            addresses::txs_by_address::REQUIRED_REDUCERS,
            "txs_by_address",
        ),
        (
            addresses::utxos_by_address::REQUIRED_REDUCERS,
            "utxos_by_address",
        ),
        (blocks::block_info::REQUIRED_REDUCERS, "block_info"),
        (blocks::txs_by_block::REQUIRED_REDUCERS, "txs_by_block"),
        (
            inscriptions::activity_by_inscription::REQUIRED_REDUCERS,
            "activity_by_inscription",
        ),
        (
            inscriptions::brc20_by_address::REQUIRED_REDUCERS,
            "brc20_by_address",
        ),
        (
            inscriptions::brc20_holders_by_ticker::REQUIRED_REDUCERS,
            "brc20_holders_by_ticker",
        ),
        (inscriptions::brc20_info::REQUIRED_REDUCERS, "brc20_info"),
        (
            inscriptions::collection_metadata_by_collection_symbol::REQUIRED_REDUCERS,
            "collection_metadata_by_collection_symbol",
        ),
        (
            inscriptions::collection_metadata_by_inscription::REQUIRED_REDUCERS,
            "collection_metadata_by_inscription",
        ),
        (
            inscriptions::content_by_inscription_id::REQUIRED_REDUCERS,
            "content_by_inscription_id",
        ),
        (
            inscriptions::inscription_activity_by_block::REQUIRED_REDUCERS,
            "inscription_activity_by_block",
        ),
        (
            inscriptions::inscription_activity_by_tx::REQUIRED_REDUCERS,
            "inscription_activity_by_tx",
        ),
        (
            inscriptions::inscription_info::REQUIRED_REDUCERS,
            "inscription_info",
        ),
        (
            inscriptions::inscriptions_by_address::REQUIRED_REDUCERS,
            "inscriptions_by_address",
        ),
        (
            inscriptions::inscriptions_by_collection_symbol::REQUIRED_REDUCERS,
            "inscriptions_by_collection_symbol",
        ),
        (inscriptions::list_brc20s::REQUIRED_REDUCERS, "list_brc20s"),
        (
            inscriptions::token_metadata_by_inscription::REQUIRED_REDUCERS,
            "token_metadata_by_inscription",
        ),
        (runes::holders_by_rune::REQUIRED_REDUCERS, "holders_by_rune"),
        (runes::info_by_rune::REQUIRED_REDUCERS, "info_by_rune"),
        (runes::list_runes::REQUIRED_REDUCERS, "list_runes"),
        (
            runes::rune_utxos_by_address::REQUIRED_REDUCERS,
            "rune_utxos_by_address",
        ),
        (
            runes::runes_by_address::REQUIRED_REDUCERS,
            "runes_by_address",
        ),
        (runes::utxos_by_rune::REQUIRED_REDUCERS, "utxos_by_rune"),
        (
            transactions::tx_info_with_metaprotocols::REQUIRED_REDUCERS,
            "tx_info_with_metaprotocols",
        ),
        (transactions::tx_info::REQUIRED_REDUCERS, "tx_info"),
        (
            transactions::tx_output_info::REQUIRED_REDUCERS,
            "tx_output_info",
        ),
    ];

    for (endpoint_reducers, endpoint) in endpoint_reducers.into_iter() {
        let mut endpoint_map = serde_json::Map::new();

        let start_time = Instant::now();

        let res = tikv.init_tip(endpoint_reducers).await;

        let duration = start_time.elapsed();

        match res {
            Ok(_) => {
                endpoint_map.insert(
                    "initialisation_time".into(),
                    format!("{}ms", duration.as_millis()).into(),
                );

                let (tip_height, tip_hash) = tikv.snapshot_point.unwrap().chain_tip;
                let tip_hash = BlockHash::from_byte_array(tip_hash);

                if tip_height > best_height {
                    best_height = tip_height
                }

                all_heights.push((endpoint, tip_height));

                endpoint_map.insert(
                    "unified_view_tip_height".into(),
                    tip_height.to_string().into(),
                );
                endpoint_map.insert("unified_view_tip_hash".into(), tip_hash.to_string().into());
            }
            Err(e) => {
                let error = format!("tikv adapter initialisation error: {e}");

                errors.push(format!("{endpoint}: {error}"));
                endpoint_map.insert("error".into(), error.into());
            }
        };

        out_map.insert(endpoint.to_string(), endpoint_map.into());
    }

    // ---

    let mempool_endpoint_reducers = vec![
        (
            mempool::fee_rates::REQUIRED_REDUCERS,
            "satoshi_balance_by_address",
        ),
        (
            mempool::holders_by_rune::REQUIRED_REDUCERS,
            "holders_by_rune",
        ),
        (
            mempool::runes_by_address::REQUIRED_REDUCERS,
            "runes_by_address",
        ),
        (
            mempool::satoshi_balance_by_address::REQUIRED_REDUCERS,
            "satoshi_balance_by_address",
        ),
        (mempool::tx_output_info::REQUIRED_REDUCERS, "tx_output_info"),
        (
            mempool::utxos_by_address::REQUIRED_REDUCERS,
            "utxos_by_address",
        ),
    ];

    for (endpoint_reducers, endpoint) in mempool_endpoint_reducers.into_iter() {
        let mut endpoint_map = serde_json::Map::new();

        let start_time = Instant::now();

        let res = tikv.init_mempool(endpoint_reducers, None).await;

        let duration = start_time.elapsed();

        match res {
            Ok(_) => {
                endpoint_map.insert(
                    "initialisation_time".into(),
                    format!("{}ms", duration.as_millis()).into(),
                );

                let snapshot_point = tikv.snapshot_point.unwrap();
                let (tip_height, tip_hash) = snapshot_point.chain_tip;
                let tip_hash = BlockHash::from_byte_array(tip_hash);

                if tip_height > best_height {
                    best_height = tip_height
                }

                all_heights.push((endpoint, tip_height));

                endpoint_map.insert(
                    "unified_view_tip_height".into(),
                    tip_height.to_string().into(),
                );
                endpoint_map.insert("unified_view_tip_hash".into(), tip_hash.to_string().into());

                if let Some(mpv) = snapshot_point.mempool {
                    let view_ts = mpv.mempool_view_ts;

                    let current_ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    let snapshot_age = current_ts.saturating_sub(view_ts);

                    if snapshot_age > 60 {
                        errors.push(format!("{endpoint}: snapshot age {snapshot_age}s old"))
                    } else if snapshot_age > 30 {
                        warnings.push(format!("{endpoint}: snapshot age {snapshot_age}s old"))
                    }

                    endpoint_map.insert(
                        "mempool_blocks".into(),
                        mpv.mempool_blocks.to_string().into(),
                    );
                    endpoint_map.insert("mempool_view_ts".into(), view_ts.to_string().into());
                } else {
                    warnings.push(format!("{endpoint}: no mempool view"));

                    endpoint_map.insert("mempool_blocks".into(), serde_json::Value::Null);
                    endpoint_map.insert("mempool_view_ts".into(), serde_json::Value::Null);
                }
            }
            Err(e) => {
                let error = format!("tikv adapter initialisation error: {e}");

                errors.push(format!("{endpoint}: {error}"));
                endpoint_map.insert("error".into(), error.into());
            }
        };

        out_map.insert(endpoint.to_string(), endpoint_map.into());
    }

    // ---

    all_heights.retain(|(_, h)| *h < best_height.saturating_sub(1));

    if !all_heights.is_empty() {
        let join = all_heights
            .into_iter()
            .map(|(endpoint, height)| format!("{endpoint}: {height}"))
            .collect::<Vec<_>>()
            .join(", ");

        warnings.push(format!(
            "endpoint views more than 1 block from best seen tip: [{join}]"
        ))
    }

    // ---

    let status = if errors.is_empty() {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    // ---

    out_map.insert("errors".into(), errors.into());
    out_map.insert("warnings".into(), warnings.into());

    // Serialize the response as pretty-printed JSON
    let pretty_json = serde_json::to_string_pretty(&out_map)?;

    // ---

    Ok((status, pretty_json).into_response())
}
