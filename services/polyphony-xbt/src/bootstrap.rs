use crate::{reducers, sources, storage};

use gasket::{messaging::tokio::connect_ports, runtime::Tether};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct GeneralConfig {
    stage_timeout_secs: Option<u64>,
    stage_message_queue: Option<usize>,
    buffer_size: Option<usize>,
    pub safe_mode: Option<bool>,
}

pub struct Pipeline {
    pub tethers: Vec<Tether>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            tethers: Vec::new(),
        }
    }

    pub fn register_stage(&mut self, tether: Tether) {
        self.tethers.push(tether);
    }
}

pub async fn build(
    mut source: sources::Bootstrapper,
    mut reducer: reducers::Bootstrapper,
    mut storage: storage::Bootstrapper,
    config: GeneralConfig,
) -> Result<Pipeline, crate::Error> {
    let buffer_size = config.buffer_size.unwrap_or(32);
    let mut cursor = storage.build_cursor(buffer_size);
    let persistent_buf = cursor.fetch_persistent_buffer().await?;

    let last_point = cursor.last_point().await?;

    let mut pipeline = Pipeline::new();

    let message_queue = config.stage_message_queue.unwrap_or(20);
    let stage_timeout = config.stage_timeout_secs.unwrap_or(60 * 30);
    let safe_mode = config.safe_mode.unwrap_or(false);

    connect_ports(
        source.borrow_output_port(),
        reducer.borrow_input_port(),
        message_queue,
    );

    connect_ports(
        reducer.borrow_output_port(),
        storage.borrow_input_port(),
        message_queue,
    );

    source.spawn_stages(
        &mut pipeline,
        cursor,
        stage_timeout,
        buffer_size,
        persistent_buf.clone(),
    );
    reducer.spawn_stages(&mut pipeline, stage_timeout);
    storage.spawn_stages(
        &mut pipeline,
        last_point,
        persistent_buf,
        buffer_size,
        stage_timeout,
        safe_mode,
    );

    Ok(pipeline)
}
