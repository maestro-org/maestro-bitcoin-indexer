pub mod tikv;

use gasket::messaging::tokio::InputPort;
use serde::Deserialize;
use tikv::SplitCommitInfo;
use tracing::info;

use crate::{
    bootstrap,
    crosscut::{self, Point},
    model::{self, StorageAction},
    rollback::PersistentBufferValue,
};

mod action_merger;
mod redis_entry;
mod utils;

pub type StorageActions = Vec<StorageAction>;

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Config {
    TiKV(tikv::Config),
}

impl Config {
    pub fn plugin(
        self,
        policy: &crosscut::policies::RuntimePolicy,
        reducer_names: Vec<String>,
    ) -> Bootstrapper {
        match self {
            Config::TiKV(c) => Bootstrapper::TiKV(c.bootstrapper(policy, reducer_names)),
        }
    }
}

pub enum Bootstrapper {
    TiKV(tikv::Bootstrapper),
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort<model::StorageActionPayload> {
        match self {
            Bootstrapper::TiKV(x) => x.borrow_input_port(),
        }
    }

    pub fn build_cursor(&mut self, buffer_size: usize) -> Cursor {
        info!("building cursor");
        match self {
            Bootstrapper::TiKV(x) => Cursor::TiKV(x.build_cursor(buffer_size)),
        }
    }

    pub fn spawn_stages(
        self,
        pipeline: &mut bootstrap::Pipeline,
        intersect: Option<Point>,
        buf: Option<Vec<PersistentBufferValue>>,
        buffer_size: usize,
        timeout: u64,
        safe_mode: bool,
    ) {
        match self {
            Bootstrapper::TiKV(x) => {
                x.spawn_stages(pipeline, intersect, buf, buffer_size, timeout, safe_mode)
            }
        }
    }
}

#[derive(Clone)]
pub enum Cursor {
    TiKV(tikv::Cursor),
}

impl Cursor {
    pub async fn last_point(&mut self) -> Result<Option<Point>, crate::Error> {
        match self {
            Cursor::TiKV(x) => x.last_point().await,
        }
    }

    pub async fn fetch_persistent_buffer(
        &mut self,
    ) -> Result<Option<Vec<PersistentBufferValue>>, crate::Error> {
        info!("fetching persistent buffer");
        match self {
            Cursor::TiKV(x) => x.fetch_persistent_buffer().await,
        }
    }

    pub async fn split_commit_lock(&mut self) -> Result<Option<SplitCommitInfo>, crate::Error> {
        info!("checking for split commit lock");
        match self {
            Cursor::TiKV(x) => x.split_commit_lock().await,
        }
    }
}
