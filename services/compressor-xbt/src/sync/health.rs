use std::{ops::Deref, sync::RwLock};

use actix_web::{
    dev::ServerHandle,
    middleware,
    web::{self, Data, Json},
    App, HttpRequest, HttpServer,
};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use gasket::framework::*;
use serde::Serialize;
use tokio::{task::JoinHandle, try_join};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::mempool::{cache::MempoolSource, SharedMempoolCache};

use super::model::HealthEvent;

pub type RollUpstreamPort = gasket::messaging::tokio::InputPort<HealthEvent>;

#[derive(Debug)]
pub enum HealthUnit {
    Roll(HealthEvent),
}

#[derive(Stage)]
#[stage(name = "health", unit = "HealthUnit", worker = "Worker")]
pub struct Stage {
    endpoint: String,
    node_rpc: String,
    node_rpc_auth: Auth,
    mempool_cache: Option<SharedMempoolCache>,
    pub roll_upstream: RollUpstreamPort,
}

impl Stage {
    pub fn new(
        endpoint: String,
        node_rpc: String,
        node_rpc_auth: Auth,
        mempool_cache: Option<SharedMempoolCache>,
    ) -> Self {
        Self {
            endpoint,
            node_rpc,
            node_rpc_auth,
            mempool_cache,
            roll_upstream: Default::default(),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct Tip {
    height: u64,
    hash: String,
}

#[derive(Default, Serialize, Clone)]
pub struct StageHealth {
    roll_stage: Option<Tip>,
}

/// Information about the current mempool snapshot
#[derive(Debug, Serialize, Clone)]
pub struct MempoolInfo {
    /// Source of the mempool data
    source: MempoolSource,
    /// Unix timestamp of when this mempool snapshot was taken
    snapshot_timestamp: u64,
    /// Chain tip height when this mempool was processed
    chain_tip_height: u64,
    /// Chain tip hash when this mempool was processed
    chain_tip_hash: String,
    /// Number of mempool blocks in the snapshot
    block_count: usize,
    /// Total number of transactions across all mempool blocks
    tx_count: usize,
    /// How old the snapshot is in seconds
    age_seconds: u64,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct HealthResponse {
    upstream: Option<Tip>,
    roll_stage: Option<Tip>,
    mempool: Option<MempoolInfo>,
}

#[derive(Clone)]
pub struct RpcConfig {
    url: String,
    auth: Auth,
}

pub struct Worker {
    health: web::Data<RwLock<StageHealth>>,
    handle: JoinHandle<()>,
    server_handle: ServerHandle,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let health_lock = web::Data::new(RwLock::new(Default::default()));
        let rpc_config = web::Data::new(RpcConfig {
            url: stage.node_rpc.clone(),
            auth: stage.node_rpc_auth.clone(),
        });
        let mempool_cache = web::Data::new(stage.mempool_cache.clone());

        let health_lock_clone = health_lock.clone();
        let rpc_config_clone = rpc_config.clone();
        let mempool_cache_clone = mempool_cache.clone();

        let server = HttpServer::new(move || {
            App::new()
                .app_data(health_lock.clone())
                .app_data(rpc_config_clone.clone())
                .app_data(mempool_cache_clone.clone())
                .wrap(middleware::Logger::default())
                .service(web::resource("/health").to(health))
        })
        .bind(stage.endpoint.clone())
        .or_retry()?
        .run();

        let server_handle = server.handle();

        let handle = tokio::spawn(async {
            info!("health server spawned");
            let res = try_join!(server);
            warn!(?res, "health handle ended")
        });

        let worker = Worker {
            health: health_lock_clone,
            handle,
            server_handle,
        };

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<HealthUnit>, WorkerError> {
        if self.handle.is_finished() {
            warn!("health server finished, restarting");
            return Err(WorkerError::Restart);
        }

        let msg = stage.roll_upstream.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(HealthUnit::Roll(msg.payload)))
    }

    async fn execute(&mut self, unit: &HealthUnit, _stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            HealthUnit::Roll(x) => {
                let mut health_lock = self.health.write().or_restart()?;

                let (height, hash) = match x {
                    HealthEvent::RollForward(hi, ha) => (*hi, ha),
                    HealthEvent::RollBack(hi, ha) => (*hi, ha),
                };

                if height % 20 == 0 {
                    debug!("received health roll message: {x:?}");
                }

                health_lock.roll_stage = Some(Tip {
                    height,
                    hash: hex::encode(AsRef::<[u8; 32]>::as_ref(hash)),
                });
            }
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        self.server_handle.stop(true).await;
        self.handle.abort();

        Ok(())
    }
}

/// endpoint handler
async fn health(
    health: Data<RwLock<StageHealth>>,
    rpc_config: Data<RpcConfig>,
    mempool_cache: Data<Option<SharedMempoolCache>>,
    _: HttpRequest,
) -> Json<HealthResponse> {
    let request_id = Uuid::new_v4();

    trace!(
        request_id = request_id.to_string(),
        "received health request"
    );

    let health = match health.read() {
        Ok(h) => Some(h.deref().clone()),
        Err(e) => {
            error!(?e, "error getting health lock");
            None
        }
    };

    // Create a fresh RPC client per request to avoid stale connection issues
    let upstream = match Client::new(&rpc_config.url, rpc_config.auth.clone()) {
        Ok(rpc) => match rpc.get_chain_tips() {
            Ok(t) => t.into_iter().max_by_key(|x| x.height).map(|x| Tip {
                height: x.height,
                hash: hex::encode(AsRef::<[u8; 32]>::as_ref(&x.hash)),
            }),
            Err(e) => {
                error!(?e, "error getting upstream tips");
                None
            }
        },
        Err(e) => {
            error!(?e, "error creating RPC client");
            None
        }
    };

    // Get mempool info if cache is available
    let mempool = if let Some(cache) = mempool_cache.as_ref() {
        let cache_guard = cache.read().await;
        cache_guard.as_ref().map(|c| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            MempoolInfo {
                source: c.source,
                snapshot_timestamp: c.mempool_info.mempool_view_ts,
                chain_tip_height: c.mempool_info.chain_tip.0,
                chain_tip_hash: hex::encode(c.mempool_info.chain_tip.1),
                block_count: c.block_count,
                tx_count: c.tx_count,
                age_seconds: now.saturating_sub(c.mempool_info.mempool_view_ts),
            }
        })
    } else {
        None
    };

    let out = HealthResponse {
        upstream,
        roll_stage: health.map(|x| x.roll_stage).flatten(),
        mempool,
    };

    trace!(
        request_id = request_id.to_string(),
        ?out,
        "returning health response"
    );

    Json(out)
}
