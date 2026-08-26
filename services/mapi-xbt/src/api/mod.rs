use crate::{
    error::Error,
    options::{arranger::Arranger, Mode},
    tikv::{adapter::TiKVAdapter, key_resolver::ReducerType},
};
use axum::{http::StatusCode, response::IntoResponse, routing::get, Extension, Router};
use blocks::block_info;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

mod addresses;
mod blocks;
mod inscriptions;
mod internal;
mod mempool;
mod runes;
mod transactions;
mod wallet;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Bitcoin - Blockchain Indexer API",
        version = "v0.2.0",
        description = "This API provides core indexer endpoints with support for Bitcoin metaprotocols by delivering real-time, rollback-protected access to Bitcoin's UTXO data, enabling developers to build responsive and reliable blockchain applications without managing complex infrastructure.\n\n#### Key Features:\n- **Real-Time Data with Rollback Protection:** Ensures data accuracy by handling chain reorganizations gracefully, providing live data without sacrificing integrity.\n- **Comprehensive UTXO Indexing:** Specialized pipelines extract, match, and process on-chain information, including handling rollbacks, to provide accurate and up-to-date data.\n\n#### Key Benefits for Developers:\nBy abstracting the complexities of blockchain data retrieval and processing, the Bitcoin Indexer API empowers developers to focus on building innovative applications with confidence in fast and reliable access to historical chain data.",
        license(
            name = "Apache 2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
        )
    ),
    servers(
        (url = "http://localhost:3000", description = "Local instance")
    ),
    paths(
        addresses::address_statistics::address_statistics,
        addresses::satoshi_activity_by_address::satoshi_activity_by_address,
        addresses::satoshi_balance_by_address::satoshi_balance_by_address,
        addresses::historical_satoshi_balance_by_address::historical_satoshi_balance_by_address,
        addresses::utxos_by_address::utxos_by_address,
        addresses::txs_by_address::txs_by_address,
        blocks::block_info::block_info,
        blocks::txs_by_block::txs_by_block,
        inscriptions::brc20_by_address::brc20_by_address,
        inscriptions::brc20_holders_by_ticker::brc20_holders_by_ticker,
        inscriptions::brc20_info::brc20_info,
        inscriptions::content_by_inscription_id::content_by_inscription_id,
        inscriptions::inscription_activity_by_address::inscription_activity_by_address,
        inscriptions::inscription_activity_by_block::inscription_activity_by_block,
        inscriptions::inscription_activity_by_tx::inscription_activity_by_tx,
        inscriptions::inscription_info::inscription_info,
        inscriptions::inscriptions_by_address::inscriptions_by_address,
        inscriptions::list_brc20s::list_brc20s,
        inscriptions::activity_by_inscription::activity_by_inscription,
        inscriptions::inscriptions_by_collection_symbol::inscriptions_by_collection_symbol,
        inscriptions::collection_metadata_by_collection_symbol::collection_metadata_by_collection_symbol,
        inscriptions::collection_stats_by_collection_symbol::collection_stats_by_collection_symbol,
        inscriptions::collection_metadata_by_inscription::collection_metadata_by_inscription,
        inscriptions::brc20_transfer_inscriptions_by_address::brc20_transfer_inscriptions_by_address,
        inscriptions::token_metadata_by_inscription::token_metadata_by_inscription,
        runes::activity_by_rune::activity_by_rune,
        runes::rune_activity_by_address::rune_activity_by_address,
        runes::runes_by_address::runes_by_address,
        runes::rune_utxos_by_address::rune_utxos_by_address,
        runes::rune_utxos_by_address_v2::rune_utxos_by_address_v2,
        runes::info_by_rune::info_by_rune,
        runes::utxos_by_rune::utxos_by_rune,
        runes::holders_by_rune::holders_by_rune,
        runes::list_runes::list_runes,
        transactions::tx_info::tx_info,
        transactions::tx_info_with_metaprotocols::tx_info_with_metaprotocols,
        transactions::tx_output_info::tx_output_info,
    ),
    components(schemas(
        crate::types::blocks::BlockInfo,
        crate::types::Metaprotocol,
        crate::types::TimestampedBlock,
        crate::types::blocks::TxByBlock,
        crate::types::blocks::TxInByBlock,
        crate::types::blocks::TxOutByBlock,
        crate::types::PaginatedTxsByBlock,
        crate::types::Utxo,
        crate::types::PaginatedUtxo,
        crate::types::EstimatedBlock,
        crate::types::BlockSatsPerVb,
        crate::types::inscriptions::Brc20Holder,
        crate::types::inscriptions::Brc20Ticker,
        crate::types::PaginatedBrc20Ticker,
        crate::types::PaginatedBrc20Holder,
        crate::types::runes::RuneUtxo,
        crate::types::runes::DeprecatedRuneUtxoByAddress,
        crate::types::PaginatedDeprecatedRuneUtxoByAddress,
        crate::types::runes::RuneUtxoByAddress,
        crate::types::PaginatedRuneUtxoByAddress,
        crate::types::RuneAndAmount,
        crate::types::PaginatedRuneUtxo,
        crate::types::runes::RuneInfo,
        crate::types::runes::RuneInfoBrief,
        crate::types::runes::Terms,
        crate::types::runes::RuneIdAndName,
        crate::types::PaginatedRuneIdAndName,
        crate::types::runes::RuneHolder,
        crate::types::PaginatedRuneHolder,
        crate::types::InscriptionAndOffset,
        crate::types::TimestampedRuneInfo,
        crate::types::TimestampedRuneQuantities,
        crate::types::TimestampedBrc20Quantities,
        crate::types::inscriptions::Brc20Terms,
        crate::types::inscriptions::Brc20Info,
        crate::types::TimestampedBrc20Info,
        crate::types::inscriptions::InscriptionByAddress,
        crate::types::PaginatedInscriptionByAddress,
        crate::types::inscriptions::InscriptionInfo,
        crate::types::inscriptions::InscriptionLocation,
        crate::types::inscriptions::ContentBody,
        crate::types::TimestampedInscriptionInfo,
        crate::types::PaginatedContentBody,
        crate::types::ChainTip,
        crate::types::PaginatedInvolvedTransaction,
        crate::types::PaginatedInscriptionActivityByBlock,
        crate::types::PaginatedInscriptionActivityByTx,
        crate::types::PaginatedInscriptionsByCollectionSymbol,
        crate::types::InvolvedTransaction,
        crate::types::inscriptions::InscriptionActivity,
        crate::types::inscriptions::InscriptionActivityKindByAddress,
        crate::types::inscriptions::InscriptionActivityByAddress,
        crate::types::PaginatedInscriptionActivityByAddress,
        crate::types::inscriptions::InscriptionActivityByBlock,
        crate::types::inscriptions::InscriptionActivityByTx,
        crate::types::inscriptions::ToInscriptionLocation,
        crate::types::inscriptions::TransferInscriptionByAddress,
        crate::types::inscriptions::FromInscriptionLocation,
        crate::types::inscriptions::TxByInscription,
        crate::types::inscriptions::InscriptionTxKind,
        crate::types::transactions::TxInfo,
        crate::types::transactions::TxIn,
        crate::types::transactions::TxOut,
        crate::types::transactions::TxInfoMetaprotocols,
        crate::types::transactions::TxInMetaprotocols,
        crate::types::transactions::TxOutMetaprotocols,
        crate::types::TimestampedTxOutMetaprotocols,
        crate::types::collections::CollectionMetadata,
        crate::types::collections::CollectionStats,
        crate::types::TimestampedCollectionMetadataBySymbol,
        crate::types::TimestampedCollectionStatsBySymbol,
        crate::types::TimestampedCollectionMetadataByInscription,
        crate::types::TimestampedTokenMetadataByInscription,
        crate::types::collections::TokenMetaAttribute,
        crate::types::collections::TokenMeta,
        crate::types::collections::TokenCollection,
        crate::types::collections::TokenMetadata,
        crate::types::TimestampedSatoshis,
        crate::types::OrderParam,
        crate::types::OrderBy,
        crate::types::runes::TxByRune,
        crate::types::runes::AddressAndRuneAmount,
        crate::types::ActivityByAddress,
        crate::types::ActivityKindByAddress,
        crate::types::runes::RuneActivity,
        crate::types::runes::RuneActivityByAddress,
        crate::types::runes::RuneActivityKindByAddress,
        crate::types::runes::EtchAndPremine,
        crate::types::SatActivity,
        crate::types::address::HistoricalSatBalanceByAddress,
        crate::types::PaginatedHistoricalSatBalanceByAddress,
        crate::types::address::AddressStatistics,
        crate::types::TimestampedAddressStatistics,
        crate::types::BlockSatsPerVb,
        crate::types::MempoolLastUpdated,
        crate::types::PaginatedRuneActivityByAddress,
    )),
)]
pub struct APIDoc;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Bitcoin - Mempool Monitoring API",
        version = "v0.1.0",
        description = "The Bitcoin Mempool Monitoring API offers core indexer endpoints with mempool awareness, providing real-time visibility into unconfirmed transactions, enabling developers to build responsive, fee-optimized, and mempool-aware applications without managing their own node infrastructure.\n\n#### Key Features:\n- **Real-Time Transaction Monitoring:** Track unconfirmed transactions instantly as they enter the mempool, providing immediate insights for enhanced user experience.\n- **Optimal Fee Estimation:** Analyze current mempool conditions to help users set appropriate transaction fees, ensuring timely confirmations and cost efficiency.\n- **Network Health Analysis:** Monitor mempool size and state to detect network congestion and anomalies, aiding in informed decision-making regarding transaction timing.\n- **Custom Transaction Selection for Miners:** Utilize mempool data to prioritize transactions with higher fees, maximizing profits during block construction.\n\n#### Key Benefits for Developers:\nDevelopers can enhance their applications and improve user experience through real-time blockchain insights and optimized transaction processing.",
        license(
            name = "Apache 2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
        )
    ),
    servers(
        (url = "http://localhost:3000", description = "Local instance")
    ),
    paths(
        mempool::fee_rates::fee_rates,
        mempool::holders_by_rune::mempool_holders_by_rune,
        mempool::rune_utxos_by_address::mempool_rune_utxos_by_address,
        mempool::runes_by_address::mempool_runes_by_address,
        mempool::satoshi_balance_by_address::mempool_satoshi_balance_by_address,
        mempool::tx_info_with_metaprotocols::tx_info_with_metaprotocols,
        mempool::tx_output_info::mempool_tx_output_info,
        mempool::utxos_by_address::mempool_utxos_by_address,
    ),
    components(schemas(
        crate::types::runes::RuneHolder,
        crate::types::runes::RuneUtxoByAddress,
        crate::types::transactions::MempoolTxInfoMetaprotocols,
        crate::types::transactions::TxOutMetaprotocols,
        crate::types::transactions::TxInMetaprotocols,
        crate::types::BlockSatsPerVb,
        crate::types::ChainTip,
        crate::types::EstimatedBlock,
        crate::types::InscriptionAndOffset,
        crate::types::MempoolUtxo,
        crate::types::MempoolPaginatedUtxo,
        crate::types::MempoolPaginatedRuneHolder,
        crate::types::MempoolPaginatedRuneUtxoByAddress,
        crate::types::MempoolTimestampedRuneQuantities,
        crate::types::MempoolTimestampedTxInfoMetaprotocols,
        crate::types::MempoolTimestampedTxOutMetaprotocols,
        crate::types::MempoolTimestampedSatoshis,
        crate::types::MempoolLastUpdated,
        crate::types::Metaprotocol,
        crate::types::OrderParam,
        crate::types::OrderBy,
        crate::types::RuneAndAmount,
    )),
)]
pub struct APIDocMempool;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Bitcoin - Wallet API",
        version = "v0.2.0",
        description = "The Bitcoin Wallet API delivers detailed transaction activity data at the address level, spanning native Bitcoin (satoshis) and metaprotocol layers like inscriptions and runes. This API enables deep visibility into balance changes, token movements, and asset-specific behaviors. Useful for powering explorers, wallets, or dashboards with granular insight into address-level history and asset interactions.\n\n#### Key Features:\n- **Satoshi Activity Tracking:** Track and analyze satoshi-level balance changes—including increases, decreases, and self-transfers—with timestamped precision. Historical balances are itemized by either block height or timestamp, enabling accurate auditing, time-based analysis, and USD-denominated valuation over time.\n- **Inscription Insight:** Monitor Ordinals transactions, filtered by inscription ID, activity type (send/receive), or self-transfer logic to reduce noise from spam or internal moves.\n- **Rune Transaction Logging:** Track rune minting, transfers, etchings, and balance changes for a given address, including support for filtering by specific rune.\n- **Unified Metaprotocol View:** Fetch combined activity across satoshis, inscriptions, and runes in a single request to power holistic user or address histories.\n- **Mempool Awareness:** Provides insight into the latest activity from the mempool by _default_. The system monitors for block reorganizations and automatically rolls back unconfirmed or invalidated trades, ensuring the data reflects the confirmed state of the chain.\n\n#### Key Benefits for Developers: \nDevelopers gain the ability to surface address-level insights without having to manually parse raw blockchain data. The Wallet API simplifies historical activity analysis, enables protocol-specific filtering, and lets developers build UX-enhancing features like transaction history views, asset trackers, and real-time alerts for wallet activity without managing indexing infrastructure.",
        license(
            name = "Apache 2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
        )
    ),
    servers(
        (url = "http://localhost:3000", description = "Local instance")
    ),
    paths(
        wallet::address_statistics::wallet_address_statistics,
        wallet::inscription_activity_by_address::wallet_inscription_activity_by_address,
        wallet::historical_satoshi_balance_by_address_wrapper::wallet_historical_satoshi_balance_by_address,
        wallet::metaprotocol_activity_by_address::wallet_metaprotocol_activity_by_address,
        wallet::rune_activity_by_address::wallet_rune_activity_by_address,
        wallet::satoshi_activity_by_address::wallet_satoshi_activity_by_address,
    ),
    components(schemas(
        crate::types::address::HistoricalSatBalanceByAddress,
        crate::types::address::PendingAddressStatistics,
        crate::types::address::WalletActivityByAddress,
        crate::types::address::WalletActivityByAddressWithMetaprotocols,
        crate::types::address::WalletAddressStatistics,
        crate::types::inscriptions::FromInscriptionLocation,
        crate::types::inscriptions::InscriptionActivity,
        crate::types::inscriptions::InscriptionActivityByAddress,
        crate::types::inscriptions::InscriptionActivityByTx,
        crate::types::inscriptions::InscriptionActivityKindByAddress,
        crate::types::inscriptions::ToInscriptionLocation,
        crate::types::inscriptions::WalletInscriptionActivityByAddress,
        crate::types::runes::EtchAndPremine,
        crate::types::runes::WalletRuneAndAmount,
        crate::types::runes::RuneActivity,
        crate::types::runes::WalletRuneActivity,
        crate::types::runes::WalletRuneActivityByAddress,
        crate::types::runes::RuneActivityKindByAddress,
        crate::types::ActivityKindByAddress,
        crate::types::BlockSatsPerVb,
        crate::types::ChainTip,
        crate::types::EstimatedBlock,
        crate::types::InscriptionAndOffset,
        crate::types::MempoolLastUpdated,
        crate::types::MempoolWalletPaginatedActivityByAddress,
        crate::types::MempoolWalletPaginatedActivityByAddressWithMetaprotocols,
        crate::types::MempoolWalletPaginatedInscriptionActivityByAddress,
        crate::types::MempoolWalletPaginatedRuneActivityByAddress,
        crate::types::MempoolWalletTimestampedAddressStatistics,
        crate::types::PaginatedHistoricalSatBalanceByAddress,
        crate::types::RuneAndAmount,
        crate::types::WalletSatActivity,
    )),
)]
pub struct APIDocWallet;

#[utoipa::path(
    get,
    path = "/healthcheck",
    responses(
        (status = 200, description= "Service is working"),
        (status = 500, description= "Internal Server Error")
    )
)]
async fn healthcheck(
    mut tikv: Extension<TiKVAdapter>,
    _mode: Extension<Mode>,
) -> Result<impl IntoResponse, Error> {
    tikv.init_tip(block_info::REQUIRED_REDUCERS).await?;

    let (mut snapshot, encoder) = tikv.take_snapshot_and_encoder(ReducerType::BlockInfo)?;

    snapshot
        .get(encoder.cursor())
        .await?
        .ok_or_else(|| Error::Internal("no cursor".into()))?;

    Ok((StatusCode::ACCEPTED, "OK"))
}

pub async fn router(
    polyphony: TiKVAdapter,
    mode: Mode,
    arranger: Arranger,
) -> Result<Router, Error> {
    let router = Router::new()
        .route("/healthcheck", get(healthcheck))
        .nest("/_internal", internal::router())
        .nest("/blocks", blocks::router())
        .nest("/addresses", addresses::router())
        .nest("/transactions", transactions::router())
        .nest("/mempool", mempool::router())
        .merge(runes::router())
        .merge(inscriptions::router())
        .merge(wallet::router())
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", APIDoc::openapi()))
        .layer(Extension(polyphony))
        .layer(Extension(mode))
        .layer(Extension(arranger));

    Ok(router)
}
