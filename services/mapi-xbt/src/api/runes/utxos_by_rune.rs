use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Txid};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        etching_by_rune_id,
        utxos_by_rune_id::{
            Cursor as UtxosByRuneIdCursor, Key as UtxosByRuneIdKey, Value as UtxosByRuneIdValue,
        },
    },
    Encode, Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{runes::RuneUtxo, CountParam, HeightPaginationParams, OrderParam, PaginatedResponse},
    util::{decimal, ParsedHeightPaginationParams, RuneIdentifier},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::UtxosByRuneId,
    ReducerType::EtchingByRuneId,
    ReducerType::RuneIdByRuneName,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
];

#[utoipa::path(
    tag = "Runes",
    get,
    path = "/assets/runes/{rune}/utxos",
    params(
        ("rune" = String, Path, description = "Rune, specified either by the Rune ID (etching block number and transaction index) or name (spaced or un-spaced)", example="2519999:31"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),

        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted (by height at which UTxO was produced)"),
        ("from" = inline(Option<u64>), Query, description = "Return only UTxOs created on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only UTxOs created on or before a specific height"),

        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedRuneUtxo,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "UTXOS_BY_RUNE", level = "info", skip(tikv, mode))]
/// UTxOs by Runes
///
/// Returns all UTXOs containing the specified Rune. Useful for raw state tracking and detailed token flow visualization.
pub async fn utxos_by_rune(
    page_params: Query<HeightPaginationParams>,
    Path(rune_id): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let last_updated = tikv.get_snapshot_point()?;

    let utxos_encoder = tikv.get_encoder(ReducerType::UtxosByRuneId)?;

    // -- parse and try decode user params

    let rune_id = match RuneIdentifier::parse(rune_id)? {
        RuneIdentifier::Id(id) => id,
        RuneIdentifier::Name(n) => tikv.resolve_rune_name(n).await?.unwrap_or_default(), // return empty vec instead of 404
    };

    // TODO: cleaner
    let page_params = ParsedHeightPaginationParams::parse::<_, UtxosByRuneIdCursor>(
        page_params.0,
        &utxos_encoder,
        &Reducer::UtxosByRuneId,
        Some(rune_id),
    )?;

    // --- initialise `next_cursor`

    let mut next_cursor = None;

    // --- scan keys for this page (max count plus one to see if there is another page)

    let kvs = Scanner::new(page_params.key_range())
        .count(page_params.count() + 1)
        .order(page_params.order())
        .execute::<UtxosByRuneIdKey, UtxosByRuneIdValue>(&mut tikv, ReducerType::UtxosByRuneId)
        .await?;

    // --- get divisibility (or return empty vec if rune not found)

    let dec = if let Some(etch) = tikv
        .get_reducer_key_maybe::<_, etching_by_rune_id::Value>(
            (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
            &etching_by_rune_id::Key { rune_id },
        )
        .await?
    {
        etch.divisibility.unwrap_or(0) as usize
    } else {
        return Ok((
            StatusCode::OK,
            Json(PaginatedResponse {
                data: vec![],
                last_updated,
                next_cursor: None,
            }),
        ));
    };

    // --- process fetched kvs

    let mut kvs = kvs.into_iter().enumerate();

    let mut utxos: Vec<RuneUtxo> = Vec::new();

    // TODO: cleaner
    while let Some((i, (key, value))) = kvs.next() {
        // if this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            // TODO: timbre Cursor::from(key)
            next_cursor = Some(
                UtxosByRuneIdCursor {
                    height: key.height,
                    utxo_hash: key.utxo_hash,
                    utxo_index: key.utxo_index,
                }
                .encode_base64(),
            );
        };

        let (address, script) = tikv.resolve_script_hash(mode.0, value.script_hash).await?;

        let confirmations = (last_updated.block_height + 1).saturating_sub(key.height);

        utxos.push(RuneUtxo {
            txid: Txid::from_byte_array(key.utxo_hash).to_string(),
            vout: key.utxo_index,
            address: address.map(|x| x.to_string()),
            script_pubkey: hex::encode(script),
            satoshis: value.satoshis.to_string(),
            confirmations,
            height: key.height,
            rune_amount: decimal(value.rune_quantity, dec),
        })
    }

    let out = PaginatedResponse {
        data: utxos,
        last_updated,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "txid": "fa2fa7ea017e9e8eaf701b26bb57c0ed3f550428b59df17bd789ad98f6bf5a2b",
        "vout": 0,
        "address": "bc1ql8k89mfzqwjaqnq0y9uxummllp6v92fkykkv78",
        "script_pubkey": "0014f9ec72ed2203a5d04c0f21786e6f7ff874c2a936",
        "satoshis": "546",
        "confirmations": 50,
        "height": 840001,
        "rune_amount": "1.00"
    }, {
        "txid": "452378b6ef2bd45dbd1bada84b9468b57fcccbfef67777dfb99f5e8f3a7cfd80",
        "vout": 0,
        "address": "bc1qdkzx0dnzuyzjlu7qk86mc7rgkpwwms6zg5y5gd",
        "script_pubkey": "00146d8467b662e1052ff3c0b1f5bc7868b05cedc342",
        "satoshis": "546",
        "confirmations": 50,
        "height": 840001,
        "rune_amount": "1.00"
    }],
    "last_updated": {
        "block_hash": "00000000000000000001332b3017e2b72bdd063145bbf808b3c1722a0fd60859",
        "block_height": 840051
    },
    "next_cursor": "AAAAAAAM0UFggP18Oo9en7nfd3f2_svMf7VolEuorRu9XdQr77Z4I0VgAAAAAA"
}"##;
