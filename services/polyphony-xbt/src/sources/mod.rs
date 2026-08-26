use gasket::messaging::tokio::OutputPort;
use serde::Deserialize;

use crate::{bootstrap, crosscut, model, storage};

pub mod compressor;
pub mod emulate;

pub mod utils;

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Config {
    Compressor(compressor::Config),
    Emulate(emulate::Config),
}

impl Config {
    pub fn bootstrapper(self, intersect: &crosscut::IntersectConfig) -> Bootstrapper {
        match self {
            Config::Compressor(c) => Bootstrapper::Compressor(c.bootstrapper(intersect)),
            Config::Emulate(c) => Bootstrapper::Emulate(c.bootstrapper()),
        }
    }
}

pub enum Bootstrapper {
    Compressor(compressor::Bootstrapper),
    Emulate(emulate::Bootstrapper),
}

impl Bootstrapper {
    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<model::EnrichedBlockPayload> {
        match self {
            Bootstrapper::Compressor(p) => p.borrow_output_port(),
            Bootstrapper::Emulate(p) => p.borrow_output_port(),
        }
    }

    pub fn spawn_stages(
        self,
        pipeline: &mut bootstrap::Pipeline,
        cursor: storage::Cursor,
        timeout: u64,
        buffer_size: usize,
        persistent_buf: Option<Vec<crate::rollback::PersistentBufferValue>>,
    ) {
        match self {
            Bootstrapper::Compressor(p) => {
                p.spawn_stages(pipeline, cursor, timeout, buffer_size, persistent_buf)
            }
            Bootstrapper::Emulate(p) => p.spawn_stages(pipeline),
        }
    }
}
