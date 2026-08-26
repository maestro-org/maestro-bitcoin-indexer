use std::collections::HashMap;
use std::time::Duration;

use futures_core::Stream;
use rocksdb::{OptimisticTransactionDB, SnapshotWithThreadMode, Transaction};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tracing::{debug, error};

use crate::storage::kvtable::*;
use crate::storage::{BlockBody, BlockHash, BlockHeight};

use super::resolver::{
    BRC20Resolver, InscriptionCounters, InscriptionIndices, InscriptionNumbers,
    InscriptonsResolverValue,
};
use super::{ChainDB, Error, RuneId, TxoBody, TxoRef};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Log {
    // _ _ _ txo resolver, output runes resolver, etches, mints
    Apply(
        BlockHeight,
        BlockHash,
        BlockBody,
        HashMap<TxoRef, TxoBody>,
        HashMap<TxoRef, Vec<(RuneId, u128)>>,
        Vec<u32>,
        Vec<u32>,
        InscriptonsResolverValue,
        InscriptionIndices, // indices of inscriptions with valid reinscriptions
        InscriptionCounters,
        BRC20Resolver,
        InscriptionNumbers,
    ),
    Undo(BlockHeight, BlockHash, BlockBody),
    Mark(BlockHeight, BlockHash, BlockBody),
}

impl Log {
    pub fn height(&self) -> BlockHeight {
        match self {
            Log::Apply(s, ..) => *s,
            Log::Undo(s, ..) => *s,
            Log::Mark(s, ..) => *s,
        }
    }

    pub fn hash(&self) -> &BlockHash {
        match self {
            Log::Apply(_, h, ..) => h,
            Log::Undo(_, h, ..) => h,
            Log::Mark(_, h, ..) => h,
        }
    }

    pub fn body(&self) -> &BlockBody {
        match self {
            Log::Apply(_, _, b, ..) => b,
            Log::Undo(_, _, b) => b,
            Log::Mark(_, _, b) => b,
        }
    }

    pub fn is_apply(&self) -> bool {
        matches!(self, Log::Apply(..))
    }

    pub fn is_mark(&self) -> bool {
        matches!(self, Log::Mark(..))
    }

    pub fn is_undo(&self) -> bool {
        matches!(self, Log::Undo(..))
    }
}

// sequence number => WAL action
pub struct MutableKV;

impl KVTable<DBInt, DBSerde<Log>> for MutableKV {
    const CF_NAME: &'static str = "MutableKV";
}

impl MutableKV {
    pub fn prune(
        db: &OptimisticTransactionDB,
        db_tx: &Transaction<OptimisticTransactionDB>,
        before_height: BlockHeight,
    ) -> Result<(), Error> {
        let mut pruned = 0;

        for entry in Self::iter_entries_start(db, db_tx) {
            let (key, val) = entry?;

            // stop deleting entries once we reach the specified height
            if val.height() >= before_height {
                break;
            }

            Self::stage_delete(db, key, db_tx)?;
            pruned += 1;
        }

        debug!("pruned {pruned} mutableKV entries");

        Ok(())
    }

    pub fn find_wal_seq(
        db: &OptimisticTransactionDB,
        tx: &SnapshotWithThreadMode<OptimisticTransactionDB>,
        height: BlockHeight,
        hash: BlockHash,
    ) -> Result<Option<u64>, Error> {
        let found = Self::scan_until_or(
            &db,
            tx,
            rocksdb::IteratorMode::Start,
            |v| (v.is_apply() || v.is_mark()) && (v.height() == height) && (v.hash() == &hash),
            |_| false,
        )?;

        Ok(found.map(|DBInt(n)| n))
    }

    pub fn stream_mutable(
        chain_db: &ChainDB,
        init_wal_seq: u64,
    ) -> impl Stream<Item = Result<Log, ()>> {
        let db = chain_db.db.clone();
        let notifier = chain_db.notifier.clone();

        async_stream::try_stream! {
            let mut last_seq = init_wal_seq;

            loop {
                let mut found_new_entry = false;

                // Eagerly fetch new entries from the database
                let mut iter = MutableKV::iter_entries_from_no_tx(&db, DBInt(last_seq));
                iter.next(); // Skip intersect

                for entry in iter {
                    let (DBInt(wal_seq), DBSerde(log)) = entry.map_err(|e| {
                        error!("rocks error in mutableKV async stream: {}", e);
                        ()
                    })?;

                    if wal_seq != (last_seq + 1) {
                        error!("unexpected next wal seq in mutableKV async stream: {} -> ({}: {:?})", last_seq, wal_seq, log);
                        Err(())?
                    }

                    yield log;
                    last_seq = wal_seq;
                    found_new_entry = true;
                }

                // If no new entries, wait for a notification or poll with a timeout. The timeout
                // ensures that if there is some unreliable or missed notification, we do not wait
                // for another block to be processed before querying the database. But we also
                // don't want to continously query the database to avoid subjecting it to a high
                // load, so we do so on an interval.
                if !found_new_entry {
                    let _ = timeout(Duration::from_secs(30), notifier.notified()).await;
                }
            }
        }
    }
}
