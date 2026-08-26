use clap;
use gasket::runtime::StagePhase;
use polyphony_xbt::{
    bootstrap::{self, GeneralConfig},
    crosscut, reducers, sources, storage,
};
use serde::Deserialize;
use std::time::Duration;
use tracing::info;

use crate::console;

#[derive(Deserialize, Debug)]
struct ConfigRoot {
    general: GeneralConfig,
    source: sources::Config,
    reducers: Vec<reducers::Config>,
    storage: storage::Config,
    intersect: crosscut::IntersectConfig,
    policy: Option<crosscut::policies::RuntimePolicy>,
}

impl ConfigRoot {
    pub fn new(explicit_file: &Option<std::path::PathBuf>) -> Result<Self, config::ConfigError> {
        let mut s = config::Config::builder();

        // our base config will always be in /etc/polyphony
        s = s.add_source(config::File::with_name("/etc/polyphony-xbt/daemon.toml").required(false));

        // but we can override it by having a file in the working dir
        s = s.add_source(config::File::with_name("polyphony-xbt.toml").required(false));

        // if an explicit file was passed, then we load it as mandatory
        if let Some(explicit) = explicit_file.as_ref().and_then(|x| x.to_str()) {
            s = s.add_source(config::File::with_name(explicit).required(true));
        }

        // finally, we use env vars to make some last-step overrides
        s = s.add_source(
            config::Environment::with_prefix("POLYPHONY")
                .separator("__")
                .try_parsing(true),
        );

        s.build()?.try_deserialize()
    }
}

fn should_stop(pipeline: &bootstrap::Pipeline) -> bool {
    pipeline
        .tethers
        .iter()
        .any(|tether| match tether.check_state() {
            gasket::runtime::TetherState::Alive(p) => {
                if matches!(p, StagePhase::Ended) {
                    info!("{} stage has ended, should stop", tether.name());
                    true
                } else {
                    false
                }
            }
            s => {
                info!("{} stage not alive: {:?}", tether.name(), s);
                true
            }
        })
}

async fn shutdown(pipeline: bootstrap::Pipeline) {
    for tether in pipeline.tethers {
        let state = tether.check_state();
        tracing::warn!("dismissing stage: {} with state {:?}", tether.name(), state);
        _ = tether.dismiss_stage();
    }
}

pub async fn run(args: &Args) -> Result<(), polyphony_xbt::Error> {
    console::initialize(&args.console);

    let config = ConfigRoot::new(&args.config)
        .map_err(|err| polyphony_xbt::Error::ConfigError(format!("{:?}", err)))?;

    info!("Initialising with config: {:?}", config);

    let mut policy: crosscut::policies::RuntimePolicy = config.policy.unwrap_or_default().into();

    policy.rollback_errors = None;

    if config.general.safe_mode.unwrap_or(false) {
        policy.missing_data = None; // panic if missing utxo
    } else {
        policy.missing_data = Some(crosscut::policies::ErrorAction::Warn);
    }

    let source = config.source.bootstrapper(&config.intersect);

    // Reducer names are handed to the storage stage so it can advertise this
    // instance (and its indexed tip) in the Redis instance registry per reducer.
    let reducer_names: Vec<String> = config
        .reducers
        .iter()
        .map(|r| r.kebab_name().to_string())
        .collect();

    let reducer = reducers::Bootstrapper::new(config.reducers, &policy);

    let storage = config.storage.plugin(&policy, reducer_names);

    let pipeline = bootstrap::build(source, reducer, storage, config.general).await?;

    info!("Polyphony is running...");

    while !should_stop(&pipeline) {
        console::refresh(&args.console, &pipeline);
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }

    tracing::info!("Polyphony is stopping...");

    shutdown(pipeline).await;

    Ok(())
}

#[derive(clap::Args)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(long, value_parser)]
    //#[clap(description = "config file to load by the daemon")]
    config: Option<std::path::PathBuf>,

    #[clap(long, value_parser)]
    //#[clap(description = "type of progress to display")],
    console: Option<console::Mode>,
}
