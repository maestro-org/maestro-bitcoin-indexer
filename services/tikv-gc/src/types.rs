use tikv_client::Timestamp as TiKVTimestamp;

#[derive(Clone, Copy, Debug)]
/// Invariants relating to what data we must not garbage collect
pub enum Invariant {
    /// Ensure we don't garbage collect data for at least one non-mempool block for each instance
    EarliestChaintip,
    /// Ensure we don't garbage collect data sooner than configurable duration, as that data may be
    /// required for current database transactions/snapshots
    SafeZone,
    /// Ensure we have a common block over all instances within each network so we are able to get a
    /// consistent view across all instances at some block
    NetworkIntersection,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp {
    physical: i64,
    logical: i64,
}

impl From<TiKVTimestamp> for Timestamp {
    fn from(value: TiKVTimestamp) -> Self {
        Timestamp {
            physical: value.physical,
            logical: value.logical,
        }
    }
}

impl Into<TiKVTimestamp> for Timestamp {
    fn into(self) -> TiKVTimestamp {
        TiKVTimestamp {
            physical: self.physical,
            logical: self.logical,
            suffix_bits: 0,
        }
    }
}
