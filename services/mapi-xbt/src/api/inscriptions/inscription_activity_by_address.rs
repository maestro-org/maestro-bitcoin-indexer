use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Address, Script, Txid};
use reqwest::StatusCode;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use tikv_client::KvPair;
use timbre_xbt::{
    reducers::inscription_activity_by_script_hash::{
        Cursor as InscriptionActivityByScriptHashCursor, Key as InscriptionActivityByScriptHashKey,
        Value as InscriptionActivityByScriptHashValue,
    },
    Decode, Encode, Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        inscriptions::{InscriptionActivityByAddress, InscriptionActivityKindByAddress},
        CommonPaginatedResponse, CountParam, HeightPaginationParams, OrderParam,
    },
    util::{
        build_inscription_activity, check_op_return_script, parse_inscription_id,
        ParsedHeightPaginationParams,
    },
};

#[derive(Debug, Deserialize)]
pub struct FilterParams {
    pub activity_kind: Option<InscriptionActivityKindByAddress>,
    pub exclude_self_transfers: Option<bool>,
    pub inscription_id: Option<String>,
}

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::InscriptionActivityByScriptHash,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
    ReducerType::ScriptHashByAddressPayloadHash,
];

#[utoipa::path(
    tag = "Addresses",
    get,
    path = "/addresses/{address}/inscriptions/activity",
    params(
        ("address" = String, Path, description = "Bitcoin address or hex encoded script pubkey", example="bc1p27j3fa2mr3d50m3uaavr0ntyzr0v2a27n48lc9gxpkzd4xye6dgs2tzx6p"),

        // Pagination params applicable regardless of the sorting order and property.
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted. Supported values: asc, desc"),
        ("count" = inline(Option<CountParam>), Query, description = "The max number of results per page"),
        ("from" = inline(Option<u64>), Query, description = "Return only transactions created on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only transactions created on or before a specific height"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),

        // Filter by presence of specific inscription.
        ("inscription_id" = Option<String>, Query, description = "Return only transactions containing a specific inscription, specified by an inscription ID. In presence of activity_kind, it relates to this specific inscription. In presence of exclude_self_transfers, it is this specific inscription that should be sent or received but not self-transferred.", example="6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0"),

        // Filter by activity kind.
        ("activity_kind" = Option<InscriptionActivityKindByAddress>, Query, description = "Filter txs by presence of specific activity kind. Supported values: send, receive, self_transfer. In presence of inscription filter, the activity kind relates to that specific inscription. In presence of exclude_self_transfers, this activity kind cannot be self_transfer."),

        // Filter self-transfers out.
        ("exclude_self_transfers" = Option<bool>, Query, description = "Exclude txs only containing inscriptions self-transfers. In presence of activity_kind, it cannot be self_transfer. In presence of inscription filter, that specific inscription should be sent or received, not self-transferred."),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedInscriptionActivityByAddress,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(
    name = "INSCRIPTION_ACTIVITY_BY_ADDRESS",
    level = "info",
    skip(tikv, mode)
)]
/// Inscription Activity by Address
///
/// Returns all inscription-related transactions involving a specific address. Can be filtered by activity type (send, receive, self-transfer), narrowed to a specific inscription, and sorted chronologically. Useful for building dashboards, tracking user behavior, or filtering unwanted spam activity.
pub async fn inscription_activity_by_address(
    Path(addr_or_pk): Path<String>,
    page_params: Query<HeightPaginationParams>,
    filter_params: Query<FilterParams>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let last_updated = tikv.get_snapshot_point()?;

    // Initialize `next_cursor`.
    let mut next_cursor: Option<String> = None;

    let inscription_activity_encoder =
        tikv.get_encoder(ReducerType::InscriptionActivityByScriptHash)?;

    let (query_address, query_script_bytes) =
        match tikv.parse_address_or_script_bytes(&addr_or_pk).await {
            Ok(x) => x,
            Err(Error::NotFound) => {
                // User param is a Bitcoin address, but the corresponding script pub key could not be
                // found in store.
                let out = CommonPaginatedResponse {
                    data: vec![],
                    last_updated,
                    next_cursor,
                };

                return Ok((StatusCode::OK, Json(out)));
            }
            Err(e) => return Err(e),
        };
    let script = Script::from_bytes(&query_script_bytes);
    let script_hash = script.script_hash().to_byte_array();

    // Check if the script is an OP_RETURN script pubkey and reject if so.
    check_op_return_script(&script)?;

    // Parse pagination params.
    let page_params =
        ParsedHeightPaginationParams::parse::<_, InscriptionActivityByScriptHashCursor>(
            page_params.0,
            &inscription_activity_encoder,
            &Reducer::InscriptionActivityByScriptHash,
            Some(script_hash),
        )?;

    // Parse `inscription_id` filter param.
    let inscription = match &filter_params.inscription_id {
        Some(inscription_id) => Some(parse_inscription_id(&inscription_id)?),
        None => None,
    };

    // Parse `exclude_self_transfers` filter param.
    let excl_self_transfers: bool = filter_params.exclude_self_transfers.unwrap_or(false);

    // Rule out the ill-formed combination of `excl_self_transfers` on and `activity_kind` set to self-transfers.
    if excl_self_transfers
        && filter_params.activity_kind == Some(InscriptionActivityKindByAddress::SelfTransfer)
    {
        return Err(Error::MalformedRequest(
            "Ill-formed combination of excl_self_transfers and activity_kind.".into(),
        ));
    }

    let kvs = if let Some(activity_kind) = &filter_params.activity_kind {
        // There's an activity kind filter. There may also be an inscription filter and the exclude
        // self-transfers flag may be on. Still, we don't need to check `excl_self_transfers`
        // because we have already ruled out the ill-formed case.
        let filter_fn = Some(|kv: &KvPair| {
            let value = InscriptionActivityByScriptHashValue::decode(&kv.1)
                .unwrap()
                .0;

            if let Some(id) = inscription {
                // If both an inscription filter and an activity kind filter is provided, they are
                // combined.
                match activity_kind {
                    InscriptionActivityKindByAddress::SelfTransfer => {
                        value.self_transfers.iter().any(|x| x.inscription_id == id)
                    }
                    InscriptionActivityKindByAddress::Send => {
                        value.sent.iter().any(|x| x.inscription_id == id)
                    }
                    InscriptionActivityKindByAddress::Receive => {
                        value.received.iter().any(|x| x.inscription_id == id)
                    }
                }
            } else {
                // If an activity kind filter is provided but there's no inscription filter, we
                // check for non-emptiness of each kind of activity independently of the
                // particular inscription it involves.
                match activity_kind {
                    InscriptionActivityKindByAddress::SelfTransfer => {
                        !value.self_transfers.is_empty()
                    }
                    InscriptionActivityKindByAddress::Send => !value.sent.is_empty(),
                    InscriptionActivityKindByAddress::Receive => !value.received.is_empty(),
                }
            }
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<InscriptionActivityByScriptHashKey, InscriptionActivityByScriptHashValue, _>(
                &mut tikv,
                ReducerType::InscriptionActivityByScriptHash,
                filter_fn,
            )
            .await?
    } else if excl_self_transfers {
        // There's no activity kind filter. There may still be an inscription filter.
        let filter_fn = Some(|kv: &KvPair| {
            let value = InscriptionActivityByScriptHashValue::decode(&kv.1)
                .unwrap()
                .0;

            // Exclude activity that only involves self-transfers.
            if let Some(id) = inscription {
                // Inscription must be in either sent or received.
                value.sent.iter().any(|x| x.inscription_id == id)
                    || value.received.iter().any(|x| x.inscription_id == id)
            } else {
                // If no inscription filter is provided, we exclude txs whose activity consists only of self-transfers.
                value.self_transfers.is_empty()
                    || !value.sent.is_empty()
                    || !value.received.is_empty()
            }
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<InscriptionActivityByScriptHashKey, InscriptionActivityByScriptHashValue, _>(
                &mut tikv,
                ReducerType::InscriptionActivityByScriptHash,
                filter_fn,
            )
            .await?
    } else if let Some(id) = inscription {
        // Inscription must be part of the activity of the address, either in self-transfers, sent
        // or received.
        let filter_fn = Some(|kv: &KvPair| {
            let value = InscriptionActivityByScriptHashValue::decode(&kv.1)
                .unwrap()
                .0;

            value.self_transfers.iter().any(|x| x.inscription_id == id)
                || value.sent.iter().any(|x| x.inscription_id == id)
                || value.received.iter().any(|x| x.inscription_id == id)
        });

        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute_with_filter::<InscriptionActivityByScriptHashKey, InscriptionActivityByScriptHashValue, _>(
                &mut tikv,
                ReducerType::InscriptionActivityByScriptHash,
                filter_fn,
            )
            .await?
    } else {
        // No filter was provided.
        Scanner::new(page_params.key_range())
            .count(page_params.count() + 1)
            .order(page_params.order())
            .execute::<InscriptionActivityByScriptHashKey, InscriptionActivityByScriptHashValue>(
                &mut tikv,
                ReducerType::InscriptionActivityByScriptHash,
            )
            .await?
    };

    let mut kvs = kvs.into_iter().enumerate();

    let mut resolved_script_hashes: HashMap<[u8; 20], (Option<Address>, Vec<u8>)> = HashMap::new();

    let mut activity: Vec<InscriptionActivityByAddress> = vec![];

    // Process fetched KVs.
    while let Some((i, (key, value))) = kvs.next() {
        // If this is the last result of the page, check if there is a subsequent result (and
        // therefore we need to return a cursor for next page).
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            next_cursor = Some(
                InscriptionActivityByScriptHashCursor {
                    height: key.height,
                    activity_tx_index: key.activity_tx_index,
                    tx_hash: key.tx_hash,
                }
                .encode_base64(),
            );
        }

        // Build inscription activity.
        let inscription_activity = build_inscription_activity(
            value,
            &query_address,
            &query_script_bytes,
            key.tx_hash,
            key.height,
            &mut resolved_script_hashes,
            &mut tikv,
            &mode,
        )
        .await?;

        activity.push(InscriptionActivityByAddress {
            height: key.height,
            confirmations: (last_updated.block_height + 1).saturating_sub(key.height),
            tx_hash: Txid::from_byte_array(key.tx_hash).to_string(),
            inscription_activity,
        });
    }

    let out = CommonPaginatedResponse {
        data: activity,
        last_updated,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

pub static EXAMPLE_RESPONSE: &str = r##"{
    "data": [{
        "height": 836717,
        "confirmations": 60911,
        "tx_hash": "a977a36055ba3d26203c82e6ee585539cadd12c3d17b236ffb67c051320b8991",
        "self_transferred": [],
        "sent": [],
        "received": [{
          "inscription_id": "696936486c96124784d46dec66dd9e806280a182d1addf9763ff4bb92d0bc918i64",
          "from": {
            "address": "bc1p4at29alvmmtunc5ffcpm9n5e4rvz63hlrjcrgd2zh7d7jy24xgns2g7rw7",
            "script_pubkey": "5120af56a2f7ecded7c9e2894e03b2ce99a8d82d46ff1cb0343542bf9be911553227",
            "input_index": 0,
            "sat_offset": 0
          },
          "to": {
            "address": "bc1p27j3fa2mr3d50m3uaavr0ntyzr0v2a27n48lc9gxpkzd4xye6dgs2tzx6p",
            "script_pubkey": "512057a514f55b1c5b47ee3cef5837cd6410dec5755e9d4ffc15060d84da9899d351",
            "output_vout": 0,
            "sat_offset": 0,
            "output_txid": "a977a36055ba3d26203c82e6ee585539cadd12c3d17b236ffb67c051320b8991"
          }
        }]
    }, {
        "height": 836718,
        "confirmations": 60910,
        "tx_hash": "7647d53756d6f03b5191baaca26d84dbe2715406912eb2543c9d19c892a29c73",
        "self_transferred": [],
        "sent": [{
            "inscription_id": "696936486c96124784d46dec66dd9e806280a182d1addf9763ff4bb92d0bc918i64",
            "from": {
                "address": "bc1pjuewj3dd4kpjen4zaqxd354jnwdjunewu7ncuaqqgsr0xa4cczzseg9066",
                "script_pubkey": "51209732e945adad832ccea2e80cd8d2b29b9b2e4f2ee7a78e74004406f376b8c085",
                "input_index": 2,
                "sat_offset": 0
            },
            "to": {
                "address": "bc1pjuewj3dd4kpjen4zaqxd354jnwdjunewu7ncuaqqgsr0xa4cczzseg9066",
                "script_pubkey": "51209732e945adad832ccea2e80cd8d2b29b9b2e4f2ee7a78e74004406f376b8c085",
                "output_vout": 1,
                "sat_offset": 0,
                "output_txid": "7647d53756d6f03b5191baaca26d84dbe2715406912eb2543c9d19c892a29c73"
            }
        }],
        "received": []
    }],
    "last_updated": {
        "block_hash": "0000000000000000000119bd8dffd7d8285a69744011aa98f0d9091b0555ca46",
        "block_height": 897627
    },
    "next_cursor": null
}"##;
