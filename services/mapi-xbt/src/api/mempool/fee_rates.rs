use crate::{
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::{BlockSatsPerVb, EstimatedBlock, MempoolLastUpdated},
    util::timestamp_to_string,
};
use axum::{response::IntoResponse, Extension, Json};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{reducers::sats_per_vb_by_block, Reducer};

use crate::{error::Error, types::MempoolTimestampedResponse};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[ReducerType::SatsPerVbByBlock];

#[utoipa::path(
    tag = "General",
    get,
    path = "/mempool/fee_rates",
    params(),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = MempoolTimestampedFeeRates,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "FEE_RATES", level = "info", skip(tikv))]
/// Mempool Block Fee Rates
///
/// Statistics regarding fee rates of transactions within estimated mempool blocks.
pub async fn fee_rates(mut tikv: Extension<TiKVAdapter>) -> Result<impl IntoResponse, Error> {
    tikv.init_mempool(REQUIRED_REDUCERS, None).await?;

    let snapshot_chain_tip = tikv.get_snapshot_point()?;

    let snapshot_mempool_view = tikv.get_snapshot_mempool_info()?;

    let found_mempool_blocks = snapshot_mempool_view.map(|x| x.mempool_blocks).unwrap_or(0);

    // ---

    let mut estimated_blocks = vec![];

    for i in 0..found_mempool_blocks as u64 {
        let estimated_block_height = snapshot_chain_tip.block_height + (i + 1);

        let sats_per_vb_vals = tikv
            .get_reducer_key::<_, sats_per_vb_by_block::Value>(
                (ReducerType::SatsPerVbByBlock, Reducer::SatsPerVbByBlock),
                &sats_per_vb_by_block::Key {
                    height: estimated_block_height,
                },
            )
            .await?;

        estimated_blocks.push(EstimatedBlock {
            block_height: estimated_block_height,
            sats_per_vb: BlockSatsPerVb {
                min: sats_per_vb_vals.min,
                median: sats_per_vb_vals.median,
                max: sats_per_vb_vals.max,
            },
        })
    }

    let out = MempoolTimestampedResponse {
        data: estimated_blocks.clone(),
        indexer_info: MempoolLastUpdated {
            chain_tip: snapshot_chain_tip,
            mempool_timestamp: snapshot_mempool_view
                .map(|x| timestamp_to_string(x.mempool_view_ts)),
            estimated_blocks,
        },
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "block_height": 874585,
        "sats_per_vb": {
            "min": 1,
            "median": 8,
            "max": 504
        }
    }],
    "indexer_info": {
        "chain_tip": {
            "block_hash": "000000000000000000012c80ffd2f0bd17f1f92a0bb4c098236d7108f727bfe5",
            "block_height": 874584
        },
        "mempool_timestamp": "2025-01-06 16:43:32",
        "estimated_blocks": [{
            "block_height": 874585,
            "sats_per_vb": {
                "min": 1,
                "median": 8,
                "max": 504
            }
        }]
    }
}"##;
