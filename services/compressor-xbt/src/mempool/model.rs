use bitcoin::BlockHash;
use rocksdb::OptimisticTransactionDB;

pub type Timestamp = u64;
pub type ChainTipHash = BlockHash;

pub type Snapshot<'a> = rocksdb::SnapshotWithThreadMode<'a, OptimisticTransactionDB>;
