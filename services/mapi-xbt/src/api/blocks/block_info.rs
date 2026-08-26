use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use base64::{engine::general_purpose, Engine};
use bitcoin::{hashes::Hash, BlockHash};
use reqwest::StatusCode;
use serde::Deserialize;
use std::str::FromStr;
use timbre_xbt::{
    ingestors::miner::miner_metadata_key_range,
    reducers::{
        block_info::{Key as BlockInfoKey, Value as BlockInfoValue},
        height_by_block_hash::{Key as HeightByBlockHashKey, Value as HeightByBlockHashValue},
        height_by_timestamp::{Key as HeightByTimestampKey, Value as HeightByTimestampValue},
        reducer_key_range,
    },
    MinerIngestor, Reducer, ShortByteString,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        blocks::BlockInfo, BlockParam, Metaprotocol, MiningPool, OrderParam, TimestampedResponse,
    },
    util::{get_miner_tag_maybe, parse_block_param, timestamp_to_string},
};

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub from_timestamp: Option<bool>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::BlockInfo,
    ReducerType::HeightByBlockHash,
    ReducerType::HeightByTimestamp,
];

#[utoipa::path(
    tag = "Blocks",
    get,
    path = "/blocks/{height_or_hash}",
    params(
        ("height_or_hash" = String, Path, description = "Block height or block hash", example="000000000000000000004fe9dc835b41f2da749287c4d1fca9055d83b2e06fa4"),

        // Whether numeric path param should be taken as timestamp instead of block height.
        ("from_timestamp" = Option<bool>, Query, description = "Whether numeric path param should be taken as timestamp instead of block height. Default: false."),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedBlock,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap()),
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "BLOCK_INFO", level = "info", skip(tikv))]
/// Block Info
///
/// Fetches full details of a block using its hash. Returns information such as height, timestamp, transaction count, miner, and size. Can be used to explore block metadata or confirm inclusion of transactions.
pub async fn block_info(
    Path(height_or_hash): Path<String>,
    Query(query_params): Query<QueryParams>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    tikv.with_miner_metadata().await?;

    let miner_metadata_encoder = tikv.get_encoder(ReducerType::MinerMetadata)?;

    // --- parse user params

    let height = match parse_block_param(
        &height_or_hash,
        query_params.from_timestamp.unwrap_or(false),
    )? {
        BlockParam::Hash(block_hash) => {
            tikv.get_reducer_key_maybe::<HeightByBlockHashKey, HeightByBlockHashValue>(
                (ReducerType::HeightByBlockHash, Reducer::HeightByBlockHash),
                &HeightByBlockHashKey { block_hash },
            )
            .await?
            .ok_or_else(|| Error::NotFound)?
            .block_height
        }
        BlockParam::Height(height) => height.into(),
        BlockParam::Timestamp(timestamp) => {
            // Fetch highest block whose timestamp is not greater than specified timestamp.
            let height_by_timestamp_encoder = tikv.get_encoder(ReducerType::HeightByTimestamp)?;

            let (range_lower, range_upper) = reducer_key_range(
                height_by_timestamp_encoder.namespace(),
                &Reducer::HeightByTimestamp,
                &None::<u64>,
                Some(0u64),
                Some(timestamp + 1),
            );

            let highest_block = Scanner::new(range_lower..range_upper)
                .count(1)
                .order(OrderParam::Desc)
                .execute::<HeightByTimestampKey, HeightByTimestampValue>(
                    &mut tikv,
                    ReducerType::HeightByTimestamp,
                )
                .await?;

            match highest_block.get(0) {
                Some((_, value)) => value.height,
                None => {
                    // Unable to resolve timestamp query param.
                    return Err(Error::Internal("Unable to resolve timestamp param.".into()));
                }
            }
        }
    };

    // --- fetch block info

    let block_info = tikv
        .get_reducer_key_maybe::<BlockInfoKey, BlockInfoValue>(
            (ReducerType::BlockInfo, Reducer::BlockInfo),
            &BlockInfoKey { height },
        )
        .await?
        .ok_or_else(|| Error::NotFound)?;

    // --- fetch miners metadata tags, try to find any of them in the coinbase tag of the block
    // --- and use that to fetch the associated value.
    let (range_lower, range_upper) = miner_metadata_key_range(&miner_metadata_encoder.namespace());

    let keys = Scanner::new(range_lower..range_upper)
        .execute_keys_only::<ShortByteString>(&mut tikv, ReducerType::MinerMetadata)
        .await?;

    let keys = keys.into_iter().map(|tag| tag.into()).collect::<Vec<_>>();

    let mut miner_name: Option<String> = None;

    if let Some(miner_tag) = get_miner_tag_maybe(&block_info.coinbase_script_sig, keys) {
        // fetch miner metadata
        let maybe_miner_metadata = tikv
            .get_miner_metadata_key_maybe(&MinerIngestor, &ShortByteString(miner_tag))
            .await?;

        if let Some(json_bytes) = maybe_miner_metadata {
            // extract miner name - unwrapping should never fail because we know at this point that key is in store
            let miner_metadata: MiningPool = serde_json::from_slice(&json_bytes).unwrap();

            miner_name = Some(miner_metadata.name);
        }
    }

    let unix_timestamp = block_info
        .timestamp
        .ok_or(Error::Internal("no block timestamp".into()))?;

    let block_hash = block_info
        .block_hash
        .ok_or(Error::Internal("no block hash".into()))?;

    let mut metaprotocols = vec![];

    if block_info.involves_inscriptions {
        metaprotocols.push(Metaprotocol::Inscriptions);
    }

    if block_info.involves_runes {
        metaprotocols.push(Metaprotocol::Runes);
    }

    if block_info.involves_brc20 {
        metaprotocols.push(Metaprotocol::Brc20);
    }

    let out = TimestampedResponse {
        data: BlockInfo {
            height,
            hash: BlockHash::from_byte_array(block_hash).to_string(),
            size: block_info.block_size,
            weight_units: block_info.block_weight_units,
            timestamp: timestamp_to_string(unix_timestamp as u64),
            unix_timestamp,
            total_fees: block_info.total_fees.to_string(),
            total_volume: block_info.total_volume.to_string(),
            total_txs: block_info.total_txs,
            metaprotocols,
            miner_name,
            coinbase_tag: general_purpose::STANDARD.encode(&(*block_info.coinbase_script_sig)),
        },
        last_updated: tikv.get_snapshot_point()?,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "height": 878027,
        "hash": "000000000000000000004fe9dc835b41f2da749287c4d1fca9055d83b2e06fa4",
        "size": 1865672,
        "weight_units": 3993329,
        "unix_timestamp": 1736129377,
        "timestamp": "2025-01-06 02:09:37",
        "total_fees": "2110512",
        "total_volume": "240371600038",
        "total_txs": 1849,
        "metaprotocols": [
            "inscriptions",
            "runes",
            "brc20"
        ],
        "miner_name": "ViaBTC",
        "coinbase_tag": "A8tlDQgvVmlhQlRDLyz6vm1tPXaDOuJgXZs6zuF9J7o+V55Fu/UWyF9S+hZG5d/z+c8QAAAAAAAAABDThC4BY8HxNM7fc9SQsQ8AAAAAAA=="
    },
    "last_updated": {
        "block_hash": "00000000000000000002467d678b845978ef175b424cff2985860bb156280c53",
        "block_height": 878064
    }
}"##;
