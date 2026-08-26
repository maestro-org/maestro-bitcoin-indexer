use axum::{extract::Path, response::IntoResponse, Extension, Json};
use reqwest::StatusCode;
use std::str::FromStr;
use timbre_xbt::CollectionIngestor;

use crate::{
    error::Error,
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
    types::{
        collections::{InscriptionId, TokenMetadata},
        TimestampedResponse,
    },
    util::parse_inscription_id,
};

pub static REQUIRED_REDUCERS: &[ReducerType] = &[ReducerType::ContentByInscriptionId];

#[utoipa::path(
    tag = "Inscriptions",
    get,
    path = "/assets/inscriptions/{inscription_id}/metadata",
    params(("inscription_id" = String, Path, description = "Inscription ID", example="0001a5f7af47c79fa8acfb4fb4d4588d561a86b9f45dd1cf9120663dc74c0a08i0")),
    responses(
        (
            status = 200,
            description = "Requested data",
            body = TimestampedTokenMetadataByInscription,
            example = json!(serde_json::Value::from_str(EXAMPLE_RESPONSE).unwrap())
        ),
        (status = 400, description = "Malformed query parameters"),
        (status = 404, description = "Requested entity not found on-chain"),
        (status = 500, description = "Internal server error"),
    )
)]
#[tracing::instrument(name = "TOKEN_METADATA_BY_INSCRIPTION", level = "info", skip(tikv))]
/// Token Metadata by Inscription
///
/// Metadata specific to inscription.
pub async fn token_metadata_by_inscription(
    Path(inscription_id): Path<String>,
    mut tikv: Extension<TiKVAdapter>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(REQUIRED_REDUCERS).await?;

    tikv.with_collection_metadata().await?;

    // ---

    let (reveal_tx_hash, inscription_index) = parse_inscription_id(&inscription_id)?;
    let inscription_id = InscriptionId {
        reveal_tx_hash,
        inscription_index,
    };

    let maybe_collection_metadata = tikv
        .get_collection_key_maybe::<InscriptionId>(
            &CollectionIngestor::TokenMetadataByInscription,
            &inscription_id,
        )
        .await?;

    let json_bytes = match maybe_collection_metadata {
        Some(collection_metadata) => collection_metadata,
        None => {
            // --- collection doesn't exist
            return Err(Error::NotFound);
        }
    };

    let data: TokenMetadata = serde_json::from_slice(&json_bytes)?;

    let out = TimestampedResponse {
        data,
        last_updated: tikv.get_snapshot_point()?,
    };

    Ok((StatusCode::OK, Json(out)))
}

static EXAMPLE_RESPONSE: &str = r##"{
    "data": {
        "id": "0001a5f7af47c79fa8acfb4fb4d4588d561a86b9f45dd1cf9120663dc74c0a08i0",
        "contentURI": "https://ordinals.example.com/content/0001a5f7af47c79fa8acfb4fb4d4588d561a86b9f45dd1cf9120663dc74c0a08i0",
        "contentType": "image/gif",
        "contentPreviewURI": "https://ordinals.example.com/preview/0001a5f7af47c79fa8acfb4fb4d4588d561a86b9f45dd1cf9120663dc74c0a08i0",
        "genesisTransaction": "0001a5f7af47c79fa8acfb4fb4d4588d561a86b9f45dd1cf9120663dc74c0a08",
        "genesisTransactionBlockTime": "Wed, 15 Feb 2023 04:54:08 GMT",
        "genesisTransactionBlockHash": "00000000000000000006548ee473a9237f601bae4e968d04cf273089306a6ebf",
        "genesisTransactionBlockHeight": 776602,
        "inscriptionNumber": 100198,
        "chain": "btc",
        "meta": {
            "name": "Action Alien #22",
            "attributes": null
        },
        "location": "f75f3de0977213530dfea6ba4869f5229e778a549ba8f47247c6503e663adda4:1:0",
        "locationBlockHeight": 791716,
        "locationBlockTime": "Sun, 28 May 2023 02:33:26 GMT",
        "locationBlockHash": "000000000000000000016206f6ad6b8989ae590f0b5cb5727ba786dc4f7b15ac",
        "output": "f75f3de0977213530dfea6ba4869f5229e778a549ba8f47247c6503e663adda4:1",
        "outputValue": 10000,
        "owner": "bc1pw57nwvp0g4uz053v7z404zyqn4xykjxsxffh2tga095w5kmdtelsjh656y",
        "listed": true,
        "listedAt": "Wed, 15 Nov 2023 13:52:21 GMT",
        "listedPrice": 4400000,
        "listedMakerFeeBp": 50,
        "listedSellerReceiveAddress": "bc1pw57nwvp0g4uz053v7z404zyqn4xykjxsxffh2tga095w5kmdtelsjh656y",
        "listedForMint": false,
        "collectionSymbol": "aaclub",
        "collection": {
            "symbol": "aaclub",
            "name": "99999 Action Alien Club",
            "imageURI": "https://bafkreihillpn43rubd2wffzot5ccungccpen6btcofn2kylutexkpkaymq.ipfs.nftstorage.link/",
            "chain": "btc",
            "inscriptionIcon": "46b576bc669c03227f273103654c46fa9b516374ed6a2b8d03cc786e95fd50f3i0",
            "description": "Ordinal #99999 has 50 alien friends. Would you like to join his Action Alien Club?",
            "supply": 50,
            "twitterLink": "http://www.twitter.com/oxfordyazuka",
            "discordLink": "",
            "websiteLink": "",
            "createdAt": "Fri, 26 May 2023 04:23:17 GMT",
            "overrideContentType": "",
            "disableRichThumbnailGeneration": false,
            "labels": [],
            "creatorTipsAddress": "",
            "enableCollectionOffer": true
        },
        "itemType": "Inscription",
        "sat": 1916312270479035,
        "satName": "aguozseyvuc",
        "satRarity": "common",
        "satBlockHeight": 756099,
        "satBlockTime": "Wed, 28 Sep 2022 17:20:31 GMT",
        "satributes": [
            "Common"
        ],
        "displayName": "Action Alien #22",
        "lastSalePrice": 200000,
        "updatedAt": "Tue, 11 Jun 2024 20:09:56 GMT",
        "sacAddress": ""
    },
    "last_updated": {
        "block_hash": "00000000000000000001998e2059bcbb25f76fd0ef39db8ddfc5c31c5ea95f1f",
        "block_height": 876644
    }
}"##;
