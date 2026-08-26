use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Script, Txid};
use reqwest::StatusCode;
use serde::Deserialize;
use std::str::FromStr;
use timbre_xbt::{
    reducers::{
        brc20_terms_by_ticker,
        transfer_inscriptions_by_script_hash::{
            Cursor as TransferInscriptionsByScriptHashCursor,
            Key as TransferInscriptionsByScriptHashKey,
            Value as TransferInscriptionsByScriptHashValue,
        },
    },
    Encode, Reducer, ShortByteString,
};

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        inscriptions::TransferInscriptionByAddress, CountParam, CursorPaginationParams, OrderParam,
        PaginatedResponse, PaginatedTransferInscriptionByAddress,
    },
    util::{decimal, ParsedPaginationParams},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::TransferInscriptionsByScriptHash,
    ReducerType::Brc20TermsByTicker,
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[derive(Debug, Deserialize)]
pub struct Ticker {
    pub ticker: Option<String>,
}

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/brc20/transfer_inscriptions",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1p98p4wj9y5ppa4rkal59vrnp56tf8v6hggymud6awkf9rhdyvvc2s9jp0m3"),
        ("ticker" = inline(Option<String>), Query, description = "BRC20 ticker string", example="oxbt"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedTransferInscriptionByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "TRANSFER_INSCRIPTIONS_BY_ADDRESS", level = "info", skip(tikv))]
/// BRC20 Transfer Inscriptions by Address
///
/// Returns all unspent BRC20 transfer inscriptions residing at the address. This endpoint is critical for applications facilitating token transfers, as it identifies transfer-eligible inscriptions.
pub async fn brc20_transfer_inscriptions_by_address(
    Path(addr_or_pk): Path<String>,
    Query(ticker_param): Query<Ticker>,
    Query(page_params): Query<CursorPaginationParams>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    // --- fetch cursor key  for last updated
    let last_updated = tikv.get_snapshot_point()?;

    // --- initialise `next_cursor`
    let mut next_cursor = None;

    // Parse and try decode user params.
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

    let transfer_inscriptions_encoder =
        tikv.get_encoder(ReducerType::TransferInscriptionsByScriptHash)?;

    // // --- start db snapshot at most recent timestamp
    // let mut snapshot = polyphony.begin_snapshot_latest().await?;

    let page_params = if let Some(ticker) = ticker_param.ticker.clone() {
        // If a ticker filter was given, then the cursor is only an inscription ID.
        ParsedPaginationParams::parse_no_height::<_, ([u8; 32], u32)>(
            page_params,
            &transfer_inscriptions_encoder,
            &Reducer::TransferInscriptionsByScriptHash,
            Some((
                script_hash.to_byte_array(),
                ShortByteString(ticker.to_lowercase().into()),
            )),
        )?
    } else {
        // If no ticker filter was given, then the cursor is both a ticker and an inscription ID.
        ParsedPaginationParams::parse_no_height::<_, TransferInscriptionsByScriptHashCursor>(
            page_params,
            &transfer_inscriptions_encoder,
            &Reducer::TransferInscriptionsByScriptHash,
            Some(script_hash.to_byte_array()),
        )?
    };

    // Scan keys.
    let kvs = Scanner::new(page_params.key_range())
        .count(page_params.count() + 1)
        .order(page_params.order())
        .execute::<TransferInscriptionsByScriptHashKey, TransferInscriptionsByScriptHashValue>(
            &mut tikv,
            ReducerType::TransferInscriptionsByScriptHash,
        )
        .await?;

    // Process fetched KVs.
    let mut res = Vec::new();

    let mut kvs = kvs.into_iter().enumerate();

    while let Some((i, (key, value))) = kvs.next() {
        // if this is the last result of the page, check if there is a subsequent
        // result (and therefore we need to return a cursor for next page)
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            next_cursor = if ticker_param.ticker.is_some() {
                Some(key.inscription_id.encode_base64())
            } else {
                Some(
                    TransferInscriptionsByScriptHashCursor {
                        ticker: key.ticker.clone(),
                        inscription_id: key.inscription_id,
                    }
                    .encode_base64(),
                )
            }
        }

        let dec = tikv
            .get_reducer_key::<_, brc20_terms_by_ticker::Value>(
                (ReducerType::Brc20TermsByTicker, Reducer::Brc20TermsByTicker),
                &brc20_terms_by_ticker::Key {
                    ticker: key.ticker.clone(),
                },
            )
            .await?
            .dec as usize;

        res.push(TransferInscriptionByAddress {
            ticker: String::from_utf8_lossy(&key.ticker.0).to_string(),
            inscription_id: format!(
                "{}i{}",
                Txid::from_byte_array(key.inscription_id.0),
                key.inscription_id.1,
            ),
            token_amount: decimal(value.token_amount, dec),
            satoshis: value.sat_amount.to_string(),
            utxo_txid: Txid::from_byte_array(value.utxo_hash).to_string(),
            utxo_vout: value.utxo_index,
            utxo_sat_offset: value.offset,
            utxo_block_height: value.block_height,
            utxo_confirmations: (last_updated.block_height + 1).saturating_sub(value.block_height),
        });
    }

    let out: PaginatedTransferInscriptionByAddress = PaginatedResponse {
        data: res,
        last_updated,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "ticker": "oxbt",
        "inscription_id": "1da6ca5f0c07634d3e233197997a21091f10907c942b2a8e77a4d77381ca96b8i0",
        "token_amount": "10000000.000000000000",
        "satoshis": "546",
        "utxo_txid": "1da6ca5f0c07634d3e233197997a21091f10907c942b2a8e77a4d77381ca96b8",
        "utxo_vout": 0,
        "utxo_sat_offset": 0,
        "utxo_block_height": 851440,
        "utxo_confirmations": 481
    }],
    "last_updated": {
        "block_hash": "00000000000000000000023f7c4b362352c7948fb6ed7775bcd558ef1c7966c0",
        "block_height": 851921
    },
    "next_cursor": null
}"##;
