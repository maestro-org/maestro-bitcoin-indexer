use std::{collections::HashMap, time::Duration};

use tikv_client::Timestamp as TiKVTimestamp;
use tracing::{info, warn};

use crate::{
    entry::{RedisEntry, RedisKey},
    types::Timestamp,
};

static TWO_HOURS_IN_SECONDS: u64 = 60 * 60 * 2;

pub fn get_earliest_chaintip_ts(
    entry_map: &HashMap<RedisKey, Vec<RedisEntry>>,
    current_ts: i64,
) -> TiKVTimestamp {
    // get chaintip entries
    let mut instance_chaintips = get_instance_chaintips(&entry_map)
        .into_iter()
        .collect::<Vec<_>>();

    // select the earliest instance chaintip safepoint, as this will be safe for all instances
    instance_chaintips.sort_by_key(|(_, b)| Into::<Timestamp>::into(b.commit_ts.clone()));

    let (key, earliest) = instance_chaintips.into_iter().next().unwrap();

    info!(
        "earliest instance chaintip safepoint corresponds to instance [DP {}, INSTANCE {}] for block ({}, {}) with timestamp {:?}",
        key.dataplane,
        key.instance,
        earliest.height,
        hex::encode(earliest.block_hash),
        earliest.commit_ts
    );

    let since_earliest = Duration::from_millis(
        (current_ts - earliest.commit_ts.physical)
            .try_into()
            .unwrap(),
    )
    .as_secs();

    if since_earliest > TWO_HOURS_IN_SECONDS {
        warn!("earliest instance chaintip safepoint is {since_earliest} seconds in the past, is the instance still processing blocks?");
    } else {
        info!("earliest safepoint is {since_earliest} seconds in the past");
    }

    earliest.commit_ts.clone()
}

fn get_instance_chaintips(
    entry_map: &HashMap<RedisKey, Vec<RedisEntry>>,
) -> HashMap<RedisKey, RedisEntry> {
    let mut instance_chaintips = HashMap::with_capacity(entry_map.len());

    // store the chaintip entry for each instance
    for (key, mut block_options) in entry_map.clone() {
        let dp = key.dataplane;
        let instance = key.instance;

        // ignore mempool blocks
        block_options.retain(|b| !b.was_mempool);

        // sort by (height, tikv timestamp), so if we have multiple entries for the same height we
        // will take the most recent
        block_options.sort_by_key(|b| (b.height, Into::<Timestamp>::into(b.commit_ts.clone())));

        // take most recent entry for a non-mempool block
        let Some(instance_chaintip) = block_options.pop() else {
            panic!("[DP {dp}, INSTANCE {instance}]: no non-mempool entry found");
        };

        info!(
            "[DP {dp}, INSTANCE {instance}]: instance safepoint: ({}, {}, {:?})",
            instance_chaintip.height,
            hex::encode(instance_chaintip.block_hash),
            instance_chaintip.commit_ts
        );

        instance_chaintips.insert(key, instance_chaintip);
    }

    instance_chaintips
}

#[cfg(test)]
mod tests {
    use std::i64;

    use tikv_client::TimestampExt;

    use super::*;

    fn redis_entry(height: u64, hash_byte: u8, was_mempool: bool, ts_ver: u64) -> RedisEntry {
        RedisEntry {
            height,
            block_hash: [hash_byte; 32],
            was_mempool,
            commit_ts: TiKVTimestamp::from_version(ts_ver),
            network: "test".into(),
            chain_tip_hash: [0; 32],
            chain_tip_height: 0,
            mempool_view_ts: 0,
        }
    }

    /// Test that we take the highest non-mempool entry as the tip
    #[test]
    fn test_mempool_ignored() {
        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        let instance0_val = vec![
            redis_entry(0, 0, false, 0),
            redis_entry(1, 1, false, 1),
            redis_entry(2, 2, false, 2), // <-- tip
            redis_entry(3, 3, true, 3),
        ];

        // Arrange
        let entry_map = vec![(instance0_key, instance0_val)]
            .into_iter()
            .collect::<HashMap<_, _>>();

        // Act
        let ts = get_earliest_chaintip_ts(&entry_map, i64::MAX);

        // Assert
        assert_eq!(ts, TiKVTimestamp::from_version(2));
    }

    /// We may have multiple entries with the same height but different timestamps, in which case we
    /// should use the most recent timestamp
    #[test]
    fn test_duplicate_height() {
        // instance 0 chain tip is 2 with ts 2
        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        let instance0_val = vec![
            redis_entry(0, 0, false, 0),
            redis_entry(1, 1, false, 1),
            redis_entry(2, 2, false, 2),
            redis_entry(2, 2, false, 4), // <-- tip
            redis_entry(2, 2, false, 3),
            redis_entry(3, 3, true, 4),
        ];

        // Arrange
        let entry_map = vec![(instance0_key, instance0_val)]
            .into_iter()
            .collect::<HashMap<_, _>>();

        // Act
        let ts = get_earliest_chaintip_ts(&entry_map, i64::MAX);

        // Assert
        assert_eq!(ts, TiKVTimestamp::from_version(4));
    }

    /// Test the behaviour when we have more than one instance, we should select the lowest
    /// timestamp for any chain tip entry across all instances.
    #[test]
    fn test_multiple_instances() {
        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        let instance0_val = vec![
            redis_entry(0, 0, false, 00),
            redis_entry(1, 1, false, 11),
            redis_entry(2, 2, false, 12), // <-- tip
            redis_entry(3, 3, true, 13),
        ];

        let instance1_key = RedisKey {
            dataplane: 1,
            instance: 1,
        };

        let instance1_val = vec![
            redis_entry(0, 0, false, 0),
            redis_entry(1, 1, false, 1),
            redis_entry(2, 2, false, 2),
            redis_entry(3, 3, false, 3), // <-- tip
            redis_entry(4, 4, true, 4),
        ];

        // Arrange
        let entry_map = vec![
            (instance0_key, instance0_val),
            (instance1_key, instance1_val),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        // Act
        let ts = get_earliest_chaintip_ts(&entry_map, i64::MAX);

        // Assert
        assert_eq!(ts, TiKVTimestamp::from_version(3));
    }

    /// Test the behaviour when we have more than one instance, we should select the lowest
    /// timestamp for any chain tip entry across all instances.
    #[test]
    fn test_multiple_instances_2() {
        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        let instance0_val = vec![
            redis_entry(0, 0, false, 00),
            redis_entry(1, 1, false, 11),
            redis_entry(2, 2, false, 12), // <-- tip
            redis_entry(3, 3, true, 13),
        ];

        let instance1_key = RedisKey {
            dataplane: 1,
            instance: 1,
        };

        let instance1_val = vec![
            redis_entry(0, 0, false, 0),
            redis_entry(1, 1, false, 14), // <-- tip
        ];

        // Arrange
        let entry_map = vec![
            (instance0_key, instance0_val),
            (instance1_key, instance1_val),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        // Act
        let ts = get_earliest_chaintip_ts(&entry_map, i64::MAX);

        // Assert
        assert_eq!(ts, TiKVTimestamp::from_version(12));
    }
}
