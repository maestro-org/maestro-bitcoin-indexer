use crate::tikv::{adapter::TiKVAdapter, key_resolver::ReducerType};
use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Script, Txid};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        inscription_utxos_by_script_hash::{
            Cursor as InscriptionUtxosByScriptHashCursor, Key as InscriptionUtxosByScriptHashKey,
            Value as InscriptionUtxosByScriptHashValue,
        },
        reducer_key_range,
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    tikv::Scanner,
    types::{
        inscriptions::InscriptionByAddress, CountParam, CursorPaginationParams, PaginatedResponse,
    },
    util::MAX_PAGE_COUNT,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::InscriptionUtxosByScriptHash,
    // parsing address parameter
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/inscriptions",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1phyrmjs2jm5c98tldke2ykp0h66lsx3wy0ey8ug2fjj5mxsn8ftqsa24un8"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedInscriptionByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "INSCRIPTIONS_BY_ADDRESS", level = "info", skip(tikv))]
/// Inscriptions by Address
///
/// Retrieves all inscriptions currently controlled by a specific address. Useful for wallet UIs and inscription portfolio views.
pub async fn inscriptions_by_address(
    page_params: Query<CursorPaginationParams>,
    Path(addr_or_pk): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let last_updated = tikv.get_snapshot_point()?;

    let utxos_encoder = tikv.get_encoder(ReducerType::InscriptionUtxosByScriptHash)?;

    // --- initialise `next_cursor`
    let mut next_cursor = None;

    // --- parse and try to decode address
    let script_bytes = match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
        Ok((_, bytes)) => bytes,
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

    // --- parse count and cursor params
    let count = match page_params.count {
        Some(CountParam(count)) => {
            if count > MAX_PAGE_COUNT || count == 0 {
                return Err(Error::MalformedRequest("Invalid page size".into()));
            }
            count
        }
        None => MAX_PAGE_COUNT,
    };

    let cursor = if let Some(cursor) = &page_params.cursor {
        match InscriptionUtxosByScriptHashCursor::decode_base64(&cursor) {
            Ok((cursor, _)) => (cursor.tx_id, cursor.index),
            Err(_) => {
                return Err(Error::MalformedRequest(
                    "Error while decoding cursor".into(),
                ))
            }
        }
    } else {
        ([0u8; 32], 0u32)
    };

    // --- fetch keys
    let (utxos_range_lower, utxos_range_upper) = reducer_key_range(
        &utxos_encoder.namespace(),
        &Reducer::InscriptionUtxosByScriptHash,
        &Some(script_hash.to_byte_array()),
        None::<InscriptionUtxosByScriptHashKey>,
        None::<InscriptionUtxosByScriptHashKey>,
    );

    let inscription_utxo_kvs = Scanner::new(utxos_range_lower..utxos_range_upper)
        .execute::<InscriptionUtxosByScriptHashKey, InscriptionUtxosByScriptHashValue>(
            &mut tikv,
            ReducerType::InscriptionUtxosByScriptHash,
        )
        .await?;

    // --- process: filter and sort inscriptions, truncate response length
    let mut inscriptions: Vec<((u64, String, u32), (String, (u64, ([u8; 32], u32))))> = vec![];

    for (utxo_key, utxo_value) in inscription_utxo_kvs.into_iter() {
        for (offset, inscription_id) in <Vec<_>>::from(utxo_value.inscriptions).into_iter() {
            if cursor < inscription_id {
                inscriptions.push((
                    (
                        utxo_key.height,
                        Txid::from_byte_array(utxo_key.utxo_hash).to_string(),
                        utxo_key.utxo_index,
                    ),
                    (utxo_value.satoshis.to_string(), (offset, inscription_id)),
                ));
            }
        }
    }

    inscriptions.sort_by_key(|(_, (_, (_, inscription_id)))| *inscription_id);

    // --- if response is truncated, update `next_cursor`
    if count < inscriptions.len() {
        let (_, (_, (_, (tx_id, index)))) = inscriptions[count - 1];
        next_cursor = Some(InscriptionUtxosByScriptHashCursor { tx_id, index }.encode_base64());
        inscriptions.truncate(count);
    }

    // --- build response data
    let mut res = Vec::new();

    for ((height, utxo_hash, utxo_index), (satoshis, (offset, inscription_id))) in
        inscriptions.into_iter()
    {
        let inscription_id = format!(
            "{}i{}",
            Txid::from_byte_array(inscription_id.0),
            inscription_id.1,
        );

        let utxo_confirmations = (last_updated.block_height + 1).saturating_sub(height);

        res.push(InscriptionByAddress {
            inscription_id,
            satoshis,
            utxo_sat_offset: offset,
            utxo_txid: utxo_hash,
            utxo_vout: utxo_index,
            utxo_block_height: height,
            utxo_confirmations,
        });
    }

    let out = PaginatedResponse {
        data: res,
        last_updated,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "inscription_id": "f02da3d6bebab13d5d604be1ed73d9a9c677dadf6ca71bc5fff7d99cdead11b0i0",
        "satoshis": 546,
        "utxo_sat_offset": 0,
        "utxo_txid": "e2283e7c915ef074806136e0002cbc69f5fdd2e9f70f14b0eab48cdcbe867cc1",
        "utxo_vout": 0,
        "utxo_block_height": 843010,
        "utxo_confirmations": 23701
    }, {
        "inscription_id": "7d0a2dd897222913d58fc957b0429526117a0a61c964642fe93b077f328ccec1i0",
        "satoshis": 546,
        "utxo_sat_offset": 0,
        "utxo_txid": "3c7c0f5c6a0d3f0ab5c8bcef0adf3be56f5aeed8b2dd1504b7a950fc4fee1f46",
        "utxo_vout": 1,
        "utxo_block_height": 850976,
        "utxo_confirmations": 15735
    }, {
        "inscription_id": "360550a31c9510ed5052c4351619bf68d5ae3f218bf2e9c1092090dbcf86acb3i0",
        "satoshis": 546,
        "utxo_sat_offset": 0,
        "utxo_txid": "3c7c0f5c6a0d3f0ab5c8bcef0adf3be56f5aeed8b2dd1504b7a950fc4fee1f46",
        "utxo_vout": 1,
        "utxo_block_height": 850976,
        "utxo_confirmations": 15735
    }],
    "last_updated": {
        "block_hash": "00000000000000000000ec10254178fe52253f40c1fad252e892d9aa22ee8fa7",
        "block_height": 866710
    },
    "next_cursor": null
}"##;
