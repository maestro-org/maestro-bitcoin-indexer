mod emulate;

use serde::Deserialize;
use std::time::Duration;

use crate::{bootstrap, model};

use gasket::messaging::tokio::OutputPort;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub instructions: Option<String>,
}

impl Config {
    pub fn bootstrapper(self) -> Bootstrapper {
        Bootstrapper {
            config: self,
            output: Default::default(),
        }
    }
}

pub struct Bootstrapper {
    config: Config,
    output: OutputPort<model::EnrichedBlockPayload>,
}

impl Bootstrapper {
    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<model::EnrichedBlockPayload> {
        &mut self.output
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline) {
        pipeline.register_stage(gasket::runtime::spawn_stage(
            self::emulate::Worker::new(self.config.instructions.clone(), self.output),
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(1200)),
                bootstrap_retry: gasket::retries::Policy {
                    max_retries: 20,
                    backoff_factor: 2,
                    backoff_unit: Duration::from_secs(1),
                    max_backoff: Duration::from_secs(60),
                },
                ..Default::default()
            },
            Some("emulate"),
        ));
    }
}
