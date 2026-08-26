use std::{fs, time::Duration};

use bb8::Pool;
use bb8_redis_cluster::RedisConnectionManager;
use bb8_tikv::TiKVTransactionalConnectionManager;
use options::Mode;
use tikv::adapter::{IngestorInstances, TiKVAdapter};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::fmt;
use utoipa::OpenApi;

use crate::{
    api::{APIDoc, APIDocMempool, APIDocWallet},
    error::Error,
    options::Options,
};

mod api;
mod error;
mod options;
pub mod tikv;
mod timer;
pub mod types;
pub mod util;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let format = fmt::format()
        .with_level(true)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_ansi(false) // Remove colors and styling
        .without_time(); // Remove timestamps

    fmt().event_format(format).init();

    let options = Options::parse();

    if options.mode == Mode::GenerateOpenApi {
        info!("Generating Open API specification...");
        fs::write(
            "docs/indexer/swagger.json",
            APIDoc::openapi().to_pretty_json()?,
        )?;
        info!("[OK] Done. See docs/indexer/swagger.json for the result.");
        return Ok(()); // Exit the program after writing the docs
    }

    if options.mode == Mode::GenerateOpenApiMempool {
        info!("Generating Open API Mempool specification...");
        fs::write(
            "docs/mempool/swagger.json",
            APIDocMempool::openapi().to_pretty_json()?,
        )?;
        info!("[OK] Done. See docs/mempool/swagger.json for the result.");
        return Ok(()); // Exit the program after writing the docs
    }

    if options.mode == Mode::GenerateOpenApiWallet {
        info!("Generating Open API Wallet specification...");
        fs::write(
            "docs/wallet/swagger.json",
            APIDocWallet::openapi().to_pretty_json()?,
        )?;
        info!("[OK] Done. See docs/wallet/swagger.json for the result.");
        return Ok(()); // Exit the program after writing the docs
    }

    let tikv_pool = {
        let manager =
            TiKVTransactionalConnectionManager::new(vec![options.tikv_address.clone()], None)
                .expect("Failed to create TiKV manager");

        Pool::builder()
            .max_size(options.max_tikv_pool_size)
            .min_idle(options.min_tikv_pool_size.unwrap_or_default())
            .build(manager)
            .await
            .expect("Failed to create TiKV connection pool")
    };

    // tikv timestamps redis pool
    let redis_pool = {
        let db_url = options.redis.clone();

        let manager =
            RedisConnectionManager::new(vec![db_url]).expect("Failed to create Redis manager");

        Pool::builder()
            .max_size(options.max_redis_pool_size)
            .build(manager)
            .await
            .expect("Failed to create Redis connection pool")
    };

    // instances redis pool
    let instances_redis_pool = {
        let mut db_url = options.redis.clone();
        db_url.push('/');
        db_url.push('0'); // instances db is default db

        let manager =
            RedisConnectionManager::new(vec![db_url]).expect("Failed to create Redis manager");

        Pool::builder()
            .max_size(options.max_redis_pool_size)
            .build(manager)
            .await
            .expect("Failed to create Redis connection pool")
    };

    let ingestor_instances = IngestorInstances {
        collections: (
            options.collections_dataplane_id,
            options.collections_instance_id,
        ),
        miners: (
            options.miners_metadata_dataplane_id,
            options.miners_metadata_instance_id,
        ),
    };

    let tikv_adapter = TiKVAdapter::new(
        tikv_pool,
        redis_pool,
        options.mode,
        instances_redis_pool,
        ingestor_instances,
    );

    // spawn task for periodically logging TiKV pool statistics

    let t = tikv_adapter.clone();

    tokio::task::spawn({
        async move {
            loop {
                info!("tikv pool info: {:?}", t.get_tikv_pool_state().await);
                tokio::time::sleep(Duration::from_secs(
                    options.info_log_interval.clone().into(),
                ))
                .await;
            }
        }
    });

    let app = api::router(tikv_adapter, options.mode, options.arranger).await?;

    info!(address = %options.listen_address, "Starting HTTP Server");
    let listener = TcpListener::bind(options.listen_address).await?;

    axum::serve(listener, app).await?;

    Ok(())
}
