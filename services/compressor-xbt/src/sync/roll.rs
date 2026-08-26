use std::collections::BTreeMap;

use gasket::framework::*;
use tracing::{debug, info};

use crate::storage::{self, TxoBody, TxoRef};

use super::model::{HealthEvent, PullEvent};

pub type UpstreamPort = gasket::messaging::tokio::InputPort<PullEvent>;
pub type DownstreamPort = gasket::messaging::tokio::OutputPort<HealthEvent>;

#[derive(Stage)]
#[stage(name = "roll", unit = "PullEvent", worker = "Worker")]
pub struct Stage {
    chain_db: storage::ChainDB,

    pub upstream: UpstreamPort,
    pub health_downstream: DownstreamPort,

    pub mutable: bool,
    pub utxos_in_memory: bool,
    pub memory_resolver: Option<UtxoResolver>,
}

pub struct UtxoResolver(BTreeMap<TxoRef, TxoBody>, u64);

impl UtxoResolver {
    pub fn new() -> Self {
        UtxoResolver(BTreeMap::new(), 0)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, txo_ref: &TxoRef) -> Option<&TxoBody> {
        self.0.get(txo_ref)
    }

    pub fn remove(&mut self, txo_ref: &TxoRef) -> Option<TxoBody> {
        if let Some(x) = self.0.remove(txo_ref) {
            self.1 -= x.raw.len() as u64;
            Some(x)
        } else {
            None
        }
    }

    pub fn insert(&mut self, txo_ref: TxoRef, txo_body: TxoBody) -> Option<TxoBody> {
        self.1 += txo_body.raw.len() as u64;
        self.0.insert(txo_ref, txo_body)
    }

    pub fn contains_key(&self, txo_ref: &TxoRef) -> bool {
        self.0.contains_key(txo_ref)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Stage {
    pub fn new(chain_db: storage::ChainDB, utxos_in_memory: Option<bool>) -> Self {
        let utxos_in_memory = utxos_in_memory.unwrap_or(false);
        let memory_resolver = if utxos_in_memory {
            Some(UtxoResolver::new())
        } else {
            None
        };

        Self {
            chain_db,
            upstream: Default::default(),
            health_downstream: Default::default(),
            mutable: false,
            utxos_in_memory,
            memory_resolver,
        }
    }
}

pub struct Worker {}

impl Worker {}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        if let Some((height, hash)) = stage.chain_db.cursor().or_panic()? {
            stage
                .health_downstream
                .clone()
                .send(HealthEvent::RollForward(height, hash).into())
                .await
                .or_panic()?;
        }

        Ok(Self {})
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<PullEvent>, WorkerError> {
        let msg = stage.upstream.recv().await.or_panic()?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &PullEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            PullEvent::RollForward(height, hash, block, tip) => {
                if !stage.mutable {
                    if let Some(confirmations) = stage.chain_db.config.immutable_after_confs {
                        if *height + confirmations > tip.0 {
                            info!(
                                "switching to mutable mode ({} + {} > {:?})",
                                height, confirmations, tip.0
                            );
                            stage.mutable = true;
                        }
                    } else {
                        stage.mutable = true;
                    }
                }

                stage
                    .chain_db
                    .apply_block_with_context(
                        *height,
                        block.clone(),
                        stage.mutable,
                        &mut stage.memory_resolver,
                    )
                    .or_retry()?;

                debug!(height, %hash, ?tip, "chaindb rollforwarded to point");
            }
            PullEvent::RollBack(rb_height, rb_hash, tip) => {
                if !stage.mutable {
                    if let Some(confirmations) = stage.chain_db.config.immutable_after_confs {
                        if *rb_height + confirmations > tip.0 {
                            info!(
                                "switching to mutable mode ({} + {} > {:?})",
                                rb_height, confirmations, tip.0
                            );
                            stage.mutable = true;
                        }
                    }
                }

                stage
                    .chain_db
                    .rollback(*rb_height, *rb_hash, stage.mutable)
                    .or_retry()?;

                info!(rb_height, %rb_hash, ?tip, "chaindb rollbacked to point");
            }
        }

        let health_event = match unit {
            PullEvent::RollForward(hi, ha, _, _) => HealthEvent::RollForward(*hi, *ha),
            PullEvent::RollBack(hi, ha, _) => HealthEvent::RollBack(*hi, *ha),
        };

        stage
            .health_downstream
            .send(health_event.into())
            .await
            .or_panic()?;

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}
