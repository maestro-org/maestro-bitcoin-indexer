use serde::Deserialize;
use tonic::transport::Server;
use tracing::info;

use crate::{
    mempool::SharedMempoolCache,
    prelude::Error,
    serve::grpc::{compressor_api::sync_service_server::SyncServiceServer, SyncServerImpl},
    storage::ChainDB,
};

pub mod grpc;

#[derive(Deserialize, Debug)]
pub struct Config {
    listen_address: String,
}

pub async fn serve(
    config: &Config,
    chain_db: ChainDB,
    shared_cache: Option<SharedMempoolCache>,
) -> Result<(), Error> {
    let addr = config.listen_address.parse().unwrap();
    let service = SyncServerImpl::new(chain_db, shared_cache);
    let service = SyncServiceServer::new(service)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

    let mut server = Server::builder().accept_http1(true);

    info!("serving via gRPC on address: {}", config.listen_address);

    server
        .add_service(tonic_web::enable(service))
        .serve(addr)
        .await
        .map_err(Error::server)?;

    Ok(())
}
