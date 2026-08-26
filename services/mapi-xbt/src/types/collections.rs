use serde::{Deserialize, Deserializer, Serialize};
use utoipa::ToSchema;

use timbre_xbt::Encode;

/// Deserializes an explicit `null` into `T::default()` instead of failing.
///
/// Record producers that leave a list unset serialize it as `null` rather than `[]` -- a nil
/// slice in Go does exactly this. `#[serde(default)]` alone does not cover that: it applies
/// only to an absent key, so an explicit null still fails with "invalid type: null, expected
/// a sequence". Pair this with `default` to tolerate both.
fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Hash)]
pub struct CollectionMetadata {
    pub symbol: String,
    pub name: String,
    #[serde(rename = "imageURI")]
    pub image_uri: String,
    pub chain: String,
    #[serde(rename = "inscriptionIcon")]
    pub inscription_icon: String,
    pub description: String,
    pub supply: serde_json::Value, // Can handle string or int
    #[serde(rename = "twitterLink")]
    pub twitter_link: String,
    #[serde(rename = "discordLink")]
    pub discord_link: String,
    #[serde(rename = "websiteLink")]
    pub website_link: String,
    #[serde(rename = "min_inscription_number")]
    pub min_inscription_number: String,
    #[serde(rename = "max_inscription_number")]
    pub max_inscription_number: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(default, deserialize_with = "null_as_default")]
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Hash)]
pub struct CollectionStats {
    #[serde(rename = "totalVolume")]
    pub total_volume: String,
    pub owners: String,
    pub supply: String,
    #[serde(rename = "floorPrice")]
    pub floor_price: String,
    #[serde(rename = "totalListed")]
    pub total_listed: String,
    #[serde(rename = "pendingTransactions")]
    pub pending_transactions: String,
    #[serde(rename = "inscriptionNumberMin")]
    pub inscription_number_min: String,
    #[serde(rename = "inscriptionNumberMax")]
    pub inscription_number_max: String,
    pub symbol: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Hash)]
pub struct TokenMetaAttribute {
    pub value: String,
    pub trait_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Hash)]
pub struct TokenMeta {
    pub name: String,
    pub attributes: Option<Vec<TokenMetaAttribute>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Hash)]
pub struct TokenCollection {
    pub symbol: String,
    pub name: String,
    #[serde(rename = "imageURI")]
    pub image_uri: String,
    pub chain: String,
    #[serde(rename = "inscriptionIcon")]
    pub inscription_icon: String,
    pub description: String,
    pub supply: i64,
    #[serde(rename = "twitterLink")]
    pub twitter_link: String,
    #[serde(rename = "discordLink")]
    pub discord_link: String,
    #[serde(rename = "websiteLink")]
    pub website_link: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "overrideContentType")]
    pub override_content_type: String,
    #[serde(rename = "disableRichThumbnailGeneration")]
    pub disable_rich_thumbnail_generation: bool,
    #[serde(default, deserialize_with = "null_as_default")]
    pub labels: Vec<String>,
    #[serde(rename = "creatorTipsAddress")]
    pub creator_tips_address: String,
    #[serde(rename = "enableCollectionOffer")]
    pub enable_collection_offer: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Hash)]
pub struct TokenMetadata {
    #[serde(rename = "id")]
    pub inscription_id: String,
    #[serde(rename = "contentURI")]
    pub content_uri: String,
    #[serde(rename = "contentType")]
    pub content_type: String,
    #[serde(rename = "contentPreviewURI")]
    pub content_preview_uri: String,
    #[serde(rename = "genesisTransaction")]
    pub genesis_transaction: String,
    #[serde(rename = "genesisTransactionBlockTime")]
    pub genesis_transaction_block_time: String,
    #[serde(rename = "genesisTransactionBlockHash")]
    pub genesis_transaction_block_hash: String,
    #[serde(rename = "genesisTransactionBlockHeight")]
    pub genesis_transaction_block_height: i64,
    #[serde(rename = "inscriptionNumber")]
    pub inscription_number: i64,
    pub chain: String,
    pub meta: TokenMeta,
    pub location: String,
    #[serde(rename = "locationBlockHeight")]
    pub location_block_height: i64,
    #[serde(rename = "locationBlockTime")]
    pub location_block_time: String,
    #[serde(rename = "locationBlockHash")]
    pub location_block_hash: String,
    pub output: String,
    #[serde(rename = "outputValue")]
    pub output_value: i64,
    pub owner: String,
    pub listed: bool,
    #[serde(rename = "listedAt")]
    pub listed_at: String,
    #[serde(rename = "listedPrice")]
    pub listed_price: i64,
    #[serde(rename = "listedMakerFeeBp")]
    pub listed_maker_fee_bp: i32,
    #[serde(rename = "listedSellerReceiveAddress")]
    pub listed_seller_receive_address: String,
    #[serde(rename = "listedForMint")]
    pub listed_for_mint: bool,
    #[serde(rename = "collectionSymbol")]
    pub collection_symbol: String,
    pub collection: TokenCollection,
    #[serde(rename = "itemType")]
    pub item_type: String,
    pub sat: i64,
    #[serde(rename = "satName")]
    pub sat_name: String,
    #[serde(rename = "satRarity")]
    pub sat_rarity: String,
    #[serde(rename = "satBlockHeight")]
    pub sat_block_height: i64,
    #[serde(rename = "satBlockTime")]
    pub sat_block_time: String,
    #[serde(default, deserialize_with = "null_as_default")]
    pub satributes: Vec<String>,
    #[serde(rename = "displayName")]
    pub display_name: String,
    #[serde(rename = "lastSalePrice")]
    pub last_sale_price: i64,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "sacAddress")]
    pub sac_address: String,
}

// Liquidium one-off
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Hash)]
pub struct OmgColorGroup {
    #[serde(rename = "inscription_id")]
    pub inscription_id: String,
    #[serde(rename = "color")]
    pub color: String,
    #[serde(rename = "floor_price")]
    pub floor_price: i64,
}

pub struct InscriptionId {
    pub reveal_tx_hash: [u8; 32],
    pub inscription_index: u32,
}

impl Encode for InscriptionId {
    fn encode(&self) -> Vec<u8> {
        let mut res = Vec::new();
        let mut reveal_tx_hash = self.reveal_tx_hash;
        reveal_tx_hash.reverse();
        res.extend_from_slice(&reveal_tx_hash);
        res.extend_from_slice(&self.inscription_index.to_be_bytes());
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Satflow-sourced 'S' and 'C' records were written with `"labels": null` -- a nil
    /// slice on the producer side -- which failed to deserialize into a bare `Vec<String>`
    /// and panicked the request handler, dropping the connection with no status code.
    #[test]
    fn collection_deserializes_with_null_labels() {
        let record = r#"{
            "symbol": "nodemonkes",
            "name": "Node Monkes",
            "imageURI": "https://example.invalid/image.png",
            "chain": "bitcoin",
            "inscriptionIcon": "",
            "description": "",
            "supply": 10000,
            "twitterLink": "",
            "discordLink": "",
            "websiteLink": "",
            "min_inscription_number": "83522",
            "max_inscription_number": "111319",
            "createdAt": "",
            "labels": null
        }"#;

        let parsed: CollectionMetadata =
            serde_json::from_str(record).expect("null labels must deserialize");

        assert_eq!(parsed.symbol, "nodemonkes");
        assert!(parsed.labels.is_empty());
    }

    /// An absent `labels` key must behave the same as an explicit null.
    #[test]
    fn collection_deserializes_with_missing_labels() {
        let record = r#"{
            "symbol": "omb",
            "name": "Ordinal Maxi Biz",
            "imageURI": "",
            "chain": "bitcoin",
            "inscriptionIcon": "",
            "description": "",
            "supply": 9001,
            "twitterLink": "",
            "discordLink": "",
            "websiteLink": "",
            "min_inscription_number": "0",
            "max_inscription_number": "1",
            "createdAt": ""
        }"#;

        let parsed: CollectionMetadata =
            serde_json::from_str(record).expect("absent labels must deserialize");

        assert_eq!(parsed.symbol, "omb");
        assert!(parsed.labels.is_empty());
    }
}
