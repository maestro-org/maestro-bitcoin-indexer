use crate::{
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::InvolvedTransaction,
};
use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Script, Txid};
use reqwest::StatusCode;
use serde::Deserialize;
use std::str::FromStr;
use tikv_client::KvPair;
use timbre_xbt::{
    reducers::txs_by_script_hash::{
        Cursor as TxsByScriptHashCursor, Key as TxsByScriptHashKey, Value as TxsByScriptHashValue,
    },
    Decode, Encode, Namespace, Reducer,
};

use crate::{
    error::Error,
    types::{CountParam, HeightPaginationParams, OrderParam, PaginatedResponse},
    util::ParsedHeightPaginationParams,
};

#[derive(Debug, Deserialize)]
pub struct Params {
    pub confirmations: Option<u64>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::TxsByScriptHash,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/txs",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1phyrmjs2jm5c98tldke2ykp0h66lsx3wy0ey8ug2fjj5mxsn8ftqsa24un8"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),

        ("confirmations" = Option<u64>, Query, description = "Only return transactions with at least a certain amount of confirmations"),

        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted (by height at which transaction was included in a block)"),
        ("from" = inline(Option<u64>), Query, description = "Return only transactions included on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only transactions included on or before a specific height"),

        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedInvolvedTransaction,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "TXS_BY_ADDRESS", level = "info", skip(tikv))]
/// Transactions by Address
///
/// List of all transactions which consumed or produced a UTxO controlled by the specified address or script pubkey.
pub async fn txs_by_address(
    page_params: Query<HeightPaginationParams>,
    Path(addr_or_pk): Path<String>,
    params: Query<Params>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let last_updated = tikv.get_snapshot_point()?;

    // --- initialise `next_cursor`

    let mut next_cursor = None;

    // -- parse and try decode user params

    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, script_bytes)) => script_bytes,
        Err(Error::NotFound) => {
            // User param is a Bitcoin address, but the corresponding script pub key could not be
            // found in store.
            let out = PaginatedResponse {
                data: vec![],
                last_updated,
                next_cursor,
            };

            return Ok((StatusCode::OK, Json(out)));
        }
        Err(e) => return Err(e),
    };
    let script = Script::from_bytes(&script_bytes);
    let script_hash = script.script_hash();

    // TODO: cleaner
    let page_params = ParsedHeightPaginationParams::parse::<_, TxsByScriptHashCursor>(
        page_params.0,
        &tikv.get_encoder(ReducerType::TxsByScriptHash)?,
        &Reducer::TxsByScriptHash,
        Some(script_hash.to_byte_array()),
    )?;

    // if confirmations filter param provided, filter transactions with at least that many
    // confirmations (chain tip/most recent block has 1 confirmation)
    let filter_fn = match params.confirmations.clone() {
        Some(x) => Some(move |kv: &KvPair| {
            let key_bytes = &Into::<Vec<u8>>::into(kv.0.clone())[Namespace::size() + 3..];
            let (key, _) = TxsByScriptHashKey::decode(key_bytes).unwrap();

            (last_updated.block_height + 1) - key.height >= x.clone()
        }),
        None => None,
    };

    // --- scan keys for this page (max count plus one to see if there is another page)

    let kvs = Scanner::new(page_params.key_range())
        .count(page_params.count() + 1)
        .order(page_params.order())
        .execute_with_filter::<TxsByScriptHashKey, TxsByScriptHashValue, _>(
            &mut tikv,
            ReducerType::TxsByScriptHash,
            filter_fn,
        )
        .await?;

    // --- process fetched kvs

    let mut kvs = kvs.into_iter().enumerate();

    let mut txs: Vec<InvolvedTransaction> = Vec::new();

    // TODO: cleaner
    while let Some((i, (key, value))) = kvs.next() {
        // if this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            // TODO: timbre Cursor::from(key)
            next_cursor = Some(
                TxsByScriptHashCursor {
                    height: key.height,
                    address_tx_index: key.address_tx_index,
                    tx_hash: key.tx_hash,
                }
                .encode_base64(),
            );
        };

        txs.push(InvolvedTransaction {
            tx_hash: Txid::from_byte_array(key.tx_hash).to_string(),
            height: key.height,
            input: value.input,
            output: value.output,
        })
    }

    let out = PaginatedResponse {
        data: txs,
        last_updated,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "tx_hash": "1cd3a819876660e98d3d5d9e4d36ddbd1ae6f96e58de0d3977f0ef2ce6e4194a",
        "height": 5277680,
        "input": true,
        "output": true
    }, {
        "tx_hash": "ad7b8037fc7551fd9e644ddd39bc0501bc6aac865284fd79dde8b732af45acd9",
        "height": 5277682,
        "input": true,
        "output": true
    }],
    "last_updated": {
        "block_hash": "8ac9689a7901531013c3cd621eae8b8e75b1994f477d616a4faa2a10afd9be58",
        "block_height": 5277710
    },
    "next_cursor": "AAAAAABQh_JgAAlg2axFrzK36N15_YRShqxqvAEFvDndTWSe_VF1_DeAe60"
}"##;
