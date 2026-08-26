use std::collections::HashMap;
use std::str::FromStr;

use axum::{
    extract::{Path, Query},
    response::IntoResponse,
    Extension, Json,
};
use bitcoin::{hashes::Hash, Address, Txid};
use reqwest::StatusCode;
use std::cmp::Ordering;
use timbre_xbt::{
    reducers::{
        etching_by_rune_id::{Key as EtchingByRuneIdKey, Value as EtchingByRuneIdValue},
        txs_by_rune_id::{
            Cursor as TxsByRuneIdCursor, Key as TxsByRuneIdKey, Value as TxsByRuneIdValue,
        },
    },
    Encode, Reducer,
};

use crate::{
    error::Error,
    options::Mode,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType, Scanner},
    types::{
        runes::{AddressAndRuneAmount, TxByRune},
        CountParam, HeightPaginationParams, OrderParam, PaginatedTxsByRune,
    },
    util::{decimal, ParsedHeightPaginationParams, RuneIdentifier},
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[
    ReducerType::RuneIdByRuneName,
    ReducerType::EtchingByRuneId,
    ReducerType::TxsByRuneId,
    // resolving script hashes
    ReducerType::ScriptByScriptHash,
];

#[utoipa::path(
    tag = "Runes",
    get,
    path = "/assets/runes/{rune}/activity",
    params(
        ("rune" = String, Path, description = "Rune, specified either by the Rune ID (etching block number and transaction index) or name (spaced or un-spaced)", example="840110:2698"),

        ("count" = inline(Option<CountParam>), Query, description = "The max number of transactions per page"),
        ("order" = inline(Option<OrderParam>), Query, description = "The order in which the results are sorted (by block height and tx index in the block)"),
        ("cursor" = inline(Option<String>), Query, description = "Pagination cursor string, use the cursor included in a page of results to fetch the next page"),
        ("from" = inline(Option<u64>), Query, description = "Return only transactions created on or after a specific height"),
        ("to" = inline(Option<u64>), Query, description = "Return only transactions created on or before a specific height"),
    ),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = PaginatedTxsByRune,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "ACTIVITY_BY_RUNE", level = "info", skip(tikv, mode))]
/// Activity by Rune
///
/// Returns all transactions where the Rune was used or transferred, beginning with the etching (origin) transaction. Useful for auditing or building live token feeds.
pub async fn activity_by_rune(
    page_params: Query<HeightPaginationParams>,
    Path(rune_id): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
    mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    let last_updated = tikv.get_snapshot_point()?;

    // Initialise `next_cursor`.
    let mut next_cursor: Option<String> = None;

    // Parse rune ID.
    let rune_id = match RuneIdentifier::parse(rune_id)? {
        RuneIdentifier::Id(id) => id,
        RuneIdentifier::Name(n) => tikv.resolve_rune_name(n).await?.unwrap_or_default(), // return empty vec instead of 404
    };

    let page_params = ParsedHeightPaginationParams::parse::<(u64, u32), TxsByRuneIdCursor>(
        page_params.0,
        &tikv.get_encoder(ReducerType::TxsByRuneId)?,
        &Reducer::TxsByRuneId,
        Some(rune_id),
    )?;

    // Fetch etching terms, extract divisibility and amount of runes per mint.
    let etching_terms = tikv
        .get_reducer_key_maybe::<_, EtchingByRuneIdValue>(
            (ReducerType::EtchingByRuneId, Reducer::EtchingByRuneId),
            &EtchingByRuneIdKey { rune_id },
        )
        .await?
        .ok_or(Error::NotFound)?;

    let dec = etching_terms.divisibility.unwrap_or(0) as usize;

    let mint_amount = etching_terms.amount_per_mint;

    // Resolve addresses in inputs and outputs.
    let mut resolved_script_hashes: HashMap<[u8; 20], (Option<Address>, Vec<u8>)> = HashMap::new();

    let mut txs_by_rune: Vec<TxByRune> = vec![];

    let kvs = Scanner::new(page_params.key_range())
        .count(page_params.count() + 1)
        .order(page_params.order())
        .execute::<TxsByRuneIdKey, TxsByRuneIdValue>(&mut tikv, ReducerType::TxsByRuneId)
        .await?;

    let mut kvs = kvs.into_iter().enumerate();

    // Process fetched kvs.
    while let Some((i, (key, value))) = kvs.next() {
        if i == (page_params.count() - 1) && kvs.next().is_some() {
            next_cursor = Some(
                TxsByRuneIdCursor {
                    height: key.height,
                    activity_tx_index: key.activity_tx_index,
                    tx_hash: key.tx_hash,
                }
                .encode_base64(),
            );
        }

        let tx_hash = Txid::from_byte_array(key.tx_hash);

        // Compute minted amount.
        let minted = if value.minted {
            mint_amount.and_then(|mint_amount| Some(decimal(mint_amount, dec)))
        } else {
            None
        };

        // Compute total amount of runes coming from the inputs or minted in this tx.
        let mut total_spent = 0u128;
        for (_, amount) in value.senders.iter() {
            total_spent = total_spent.saturating_add(*amount);
        }

        if value.minted {
            // Add minted amount.
            if let Some(minted_amount) = mint_amount {
                total_spent = total_spent.saturating_add(minted_amount);
            }
        }

        if value.etched {
            // Add premined amount.
            if let Some(premined_amount) = etching_terms.premine {
                total_spent = total_spent.saturating_add(premined_amount);
            }
        }

        // Compute total amount of runes sent to spendable outputs (i.e. unburned runes).
        let mut total_produced = 0u128;
        for (_, amount) in value.receivers.iter() {
            total_produced = total_produced.saturating_add(*amount);
        }

        // Compute burned amount by computing runes balance in the tx.
        let burned = match total_spent.cmp(&total_produced) {
            Ordering::Equal => None, // All runes were sent to the outputs.
            Ordering::Less => {
                // This case should be impossible, and it could be due to a bug in the `TxsByRuneId`
                // or the `EtchingByRuneId` reducers.
                return Err(Error::Internal(format!(
                    "Invalid runes balance in tx {:?}",
                    tx_hash.clone()
                )));
            }
            Ordering::Greater => Some(decimal(total_spent.saturating_sub(total_produced), dec)),
        };

        let mut self_transfers = vec![];
        for (script_hash, amount) in value.self_transfers {
            let (address, script_bytes) = match resolved_script_hashes.get(&script_hash) {
                Some(res) => res.clone(),
                None => {
                    let (address, script_bytes) =
                        tikv.resolve_script_hash(mode.0, script_hash).await?;

                    resolved_script_hashes
                        .insert(script_hash, (address.clone(), script_bytes.clone()));

                    (address, script_bytes)
                }
            };

            self_transfers.push(AddressAndRuneAmount {
                address: address.map(|x| x.to_string()),
                script_pubkey: hex::encode(script_bytes),
                amount: decimal(amount, dec),
            });
        }

        // Process transfers from inputs.
        let mut senders = vec![];
        for (script_hash, amount) in value.senders {
            let (address, script_bytes) = match resolved_script_hashes.get(&script_hash) {
                Some(res) => res.clone(),
                None => {
                    let (address, script_bytes) =
                        tikv.resolve_script_hash(mode.0, script_hash).await?;

                    resolved_script_hashes
                        .insert(script_hash, (address.clone(), script_bytes.clone()));

                    (address, script_bytes)
                }
            };

            senders.push(AddressAndRuneAmount {
                address: address.map(|x| x.to_string()),
                script_pubkey: hex::encode(script_bytes),
                amount: decimal(amount, dec),
            });
        }

        // Process transfers to outputs.
        let mut receivers = vec![];
        for (script_hash, amount) in value.receivers {
            let (address, script_bytes) = match resolved_script_hashes.get(&script_hash) {
                Some(res) => res.clone(),
                None => {
                    let (address, script_bytes) =
                        tikv.resolve_script_hash(mode.0, script_hash).await?;

                    resolved_script_hashes
                        .insert(script_hash, (address.clone(), script_bytes.clone()));

                    (address, script_bytes)
                }
            };

            receivers.push(AddressAndRuneAmount {
                address: address.map(|x| x.to_string()),
                script_pubkey: hex::encode(script_bytes),
                amount: decimal(amount, dec),
            });
        }

        txs_by_rune.push(TxByRune {
            height: key.height,
            confirmations: (last_updated.block_height + 1).saturating_sub(key.height),
            tx_hash: tx_hash.to_string(),
            etching_tx: value.etched,
            minted,
            burned,
            self_transfers,
            senders,
            receivers,
        });
    }

    let out = PaginatedTxsByRune {
        data: txs_by_rune,
        last_updated,
        next_cursor,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
  "data": [
    {
      "height": 840110,
      "confirmations": 361,
      "tx_hash": "5a072b4619077bd2f718dc58784eeb22211aa867a82a523e282df11b0b234a14",
      "etching_tx": true,
      "minted": null,
      "burned": null,
      "self_transfers": [],
      "senders": [],
      "receivers": [
        {
          "address": "bc1plr56q0q4pyj8u7khys2ju8xnscp2y304u83gr06aq8ryt2uf82hq0hzm8z",
          "script_pubkey": "5120f8e9a03c1509247e7ad724152e1cd38602a245f5e1e281bf5d01c645ab893aae",
          "amount": "777777777777.77"
        }
      ]
    },
    {
      "height": 840112,
      "confirmations": 359,
      "tx_hash": "0477cd84732d4fd0aad0dc5fad14f09b0ad985c4184c0d0de9202637cc67ce9b",
      "etching_tx": false,
      "minted": "888888888888.88",
      "burned": null,
      "self_transfers": [],
      "senders": [],
      "receivers": [
        {
          "address": "bc1pc9mhstm0sggvtz0afla0f6mm3jevm3dwq2m5c5sckj4w2y3gsgxs456833",
          "script_pubkey": "5120c177782f6f8210c589fd4ffaf4eb7b8cb2cdc5ae02b74c5218b4aae51228820d",
          "amount": "888888888888.88"
        }
      ]
    },
    {
      "height": 840112,
      "confirmations": 359,
      "tx_hash": "f4ce17a5859a720aa77647d19c24c06e556bfffbb5409e8a270eec0e363a7cab",
      "etching_tx": false,
      "minted": null,
      "burned": null,
      "self_transfers": [],
      "senders": [
        {
          "address": "bc1plr56q0q4pyj8u7khys2ju8xnscp2y304u83gr06aq8ryt2uf82hq0hzm8z",
          "script_pubkey": "5120f8e9a03c1509247e7ad724152e1cd38602a245f5e1e281bf5d01c645ab893aae",
          "amount": "100000.00"
        }
      ],
      "receivers": [
        {
          "address": "bc1pc9mhstm0sggvtz0afla0f6mm3jevm3dwq2m5c5sckj4w2y3gsgxs456833",
          "script_pubkey": "5120c177782f6f8210c589fd4ffaf4eb7b8cb2cdc5ae02b74c5218b4aae51228820d",
          "amount": "100000.00"
        }
      ]
    },
    {
      "height": 840114,
      "confirmations": 357,
      "tx_hash": "e366c55626dbde705361df497c2257853a08068edf3e5ab85daad35b1f70ee2c",
      "etching_tx": false,
      "minted": null,
      "burned": null,
      "self_transfers": [],
      "senders": [
        {
          "address": "bc1plr56q0q4pyj8u7khys2ju8xnscp2y304u83gr06aq8ryt2uf82hq0hzm8z",
          "script_pubkey": "5120f8e9a03c1509247e7ad724152e1cd38602a245f5e1e281bf5d01c645ab893aae",
          "amount": "200000.00"
        }
      ],
      "receivers": [
        {
          "address": "bc1p99uy665vtcnxu9e2hsz4d9lx2tvr59lfaqcsg5na3czqq2hgqw7s4l0937",
          "script_pubkey": "512029784d6a8c5e266e172abc055697e652d83a17e9e83104527d8e04002ae803bd",
          "amount": "200000.00"
        }
      ]
    },
    {
      "height": 840114,
      "confirmations": 357,
      "tx_hash": "358ce9038a14becdce89231709db29b2b23c744e10aef2995b62094830481828",
      "etching_tx": false,
      "minted": null,
      "burned": null,
      "self_transfers": [],
      "senders": [
        {
          "address": "bc1pc9mhstm0sggvtz0afla0f6mm3jevm3dwq2m5c5sckj4w2y3gsgxs456833",
          "script_pubkey": "5120c177782f6f8210c589fd4ffaf4eb7b8cb2cdc5ae02b74c5218b4aae51228820d",
          "amount": "300000.00"
        }
      ],
      "receivers": [
        {
          "address": "bc1qr4cymlscpespfghnkuxqhwmey674fas8uzvrxp",
          "script_pubkey": "00141d704dfe180e6014a2f3b70c0bbb7926bd54f607",
          "amount": "300000.00"
        }
      ]
    }
  ],
  "last_updated": {
    "block_hash": "00000000000000000001901beb6d0ded42e731327a95e9c81a3d29336cc79402",
    "block_height": 840470
  },
  "next_cursor": "AAAAAAAM0bJgAQFgKBhIMEgJYluZ8q4QTnQ8srIp2wkXI4nOzb4UigPpjDU"
}
"##;
