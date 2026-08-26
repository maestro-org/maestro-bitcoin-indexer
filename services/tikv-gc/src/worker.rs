use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    args::Options,
    entry::{timestamp_key, RedisEntry, RedisKey},
    invariants::{
        earliest_chaintip::get_earliest_chaintip_ts,
        network_intersection::get_latest_network_intersection_ts,
    },
    types::{Invariant, Timestamp},
};

use redis::{cluster::ClusterConnection, Commands};
use tikv_client::{Timestamp as TiKVTimestamp, TimestampExt};
use tracing::{debug, info, warn};

pub fn get_new_safepoint(options: Options) -> TiKVTimestamp {
    // connect to redis
    let mut redis = redis::cluster::ClusterClient::new(vec![options.redis_address])
        .expect("could not connect to redis 1")
        .get_connection()
        .expect("could not connect to redis 2");

    // ---

    let entry_map = fetch_entry_map(&mut redis);

    debug!("fetched entry map: {entry_map:?}");

    // ---

    // get current timestamp
    let current_ts: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
        .try_into()
        .expect("ts i64");

    // store safepoints for each invariant we desire and then we will select the earliest, in order
    // to satisfy each invariant
    let mut safepoint_candidates = vec![];

    /*
       1. Do not set the safepoint later than the earliest timestamp corresponding to the chaintip
       entry of any instance, so that we can always query data at an instances chaintip (the current
       data in TiKV may relate to a mempool block, so we must make sure we don't garbage collect
       at least one non-mempool block).
    */

    let earliest_chaintip_ts = get_earliest_chaintip_ts(&entry_map, current_ts);

    safepoint_candidates.push((Invariant::EarliestChaintip, earliest_chaintip_ts));

    /*
       2. Do not set the safepoint later than some configurable window of time from the current time.
       Basically, don't garbage collect anything from the last, for example, 10 minutes. This is
       because we might have database transactions or snapshots which is using this data.
    */

    // create timestamp SAFE_ZONE_INTERVAL_MILLIS in the past
    let safe_zone_ts = TiKVTimestamp {
        physical: current_ts.saturating_sub(options.safe_zone_millis),
        logical: 0,
        suffix_bits: 0,
    };

    info!("safezone interval timestamp is {safe_zone_ts:?}");

    safepoint_candidates.push((Invariant::SafeZone, safe_zone_ts));

    /*
        3. Ensure that we have at least one common non-mempool block across all instances within a
        network, so that we can have at least one consistent view of data across all the instances.
    */

    let network_intersection_ts = get_latest_network_intersection_ts(&entry_map);

    info!("network intersection timestamp is {network_intersection_ts:?}");

    safepoint_candidates.push((Invariant::NetworkIntersection, network_intersection_ts));

    /*
        Now we have timestamps we must not set the garbage collection safepoint to later than for
        each invariant which must hold. By choosing the earliest of these timestamps to be the
        garbage collection safepoint we will ensure all the invariants hold.
    */

    info!("invariant safepoints: {safepoint_candidates:?}");

    safepoint_candidates.sort_by_key(|c| Into::<Timestamp>::into(c.1.clone()));

    let new_safepoint = safepoint_candidates.into_iter().next().unwrap();

    info!("proceeding with new safepoint: {new_safepoint:?}");

    // Write the safepoint to Redis so readers can verify they're not reading GC'd data.
    // Readers should assert their read timestamp >= this safepoint.
    let safepoint_version = new_safepoint.1.version();
    let _: () = redis
        .set("tikv-gc-safepoint", safepoint_version.to_string())
        .expect("failed to write safepoint to redis");

    info!("wrote safepoint version {safepoint_version} to redis key 'tikv-gc-safepoint'");

    let mut total_deleted = 0;

    // first delete all entries from redis with timestamps less than the new safepoint
    for key in entry_map.keys() {
        let key = timestamp_key(key.dataplane, key.instance);

        let deleted: u32 = redis
            .zrembyscore(key, "-inf", new_safepoint.1.physical - 1)
            .expect("redis 3");

        total_deleted += deleted;
    }

    info!("deleted {total_deleted} entries from redis with timestamps less than new safepoint");

    debug!("new redis entry map: {:?}", fetch_entry_map(&mut redis));

    new_safepoint.1
}

fn fetch_entry_map(redis: &mut ClusterConnection) -> HashMap<RedisKey, Vec<RedisEntry>> {
    let mut entry_map = HashMap::new();

    // get all keys, one for each dataplane + instance pair
    let keys: Vec<String> = redis
        .zrangebyscore("tikv-timestamps-keys", "-inf", "+inf")
        .unwrap();

    for key in keys {
        let mut key_parts = key.split(':');

        let _label = key_parts.next().unwrap();
        let dataplane: u8 = key_parts.next().unwrap().parse().unwrap();
        let instance: u16 = key_parts.next().unwrap().parse().unwrap();

        // get the timestamp values for the instance
        let block_options: Vec<RedisEntry> = redis
            .zrangebyscore(key.clone(), "-inf", "+inf")
            .expect("redis error");

        if block_options.is_empty() {
            warn!("[DP {dataplane}, INSTANCE {instance}]: no entries found");
            continue;
        }

        let key = RedisKey {
            dataplane,
            instance,
        };

        entry_map.insert(key, block_options);
    }

    entry_map
}

#[cfg(test)]
mod tests {
    use tikv_client::TimestampExt;
    use tracing_subscriber::fmt;

    use super::*;

    #[allow(dead_code)]
    fn logging() {
        let format = fmt::format()
            .with_level(true)
            .with_target(false)
            .with_thread_ids(false)
            .with_thread_names(false);

        fmt().event_format(format).init();
    }

    fn redis_entry(
        height: u64,
        hash_byte: u8,
        was_mempool: bool,
        ts_ver: u64,
        network: String,
    ) -> RedisEntry {
        RedisEntry {
            height,
            block_hash: [hash_byte; 32],
            was_mempool,
            commit_ts: TiKVTimestamp::from_version(ts_ver),
            network,
            chain_tip_hash: [0; 32],
            chain_tip_height: 0,
            mempool_view_ts: 0,
        }
    }

    #[ignore = "requires redis"]
    #[test]
    fn test_final() {
        // logging();

        let redis_address = "redis://localhost:6379/0";

        let mut redis = redis::Client::open(redis_address)
            .expect("could not connect to redis 1")
            .get_connection()
            .expect("could not connect to redis 2");

        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        let instance0_entries = vec![
            redis_entry(0, 0, false, 0, "testnet".into()),
            redis_entry(1, 1, false, 1, "testnet".into()),
            redis_entry(2, 2, false, 2, "testnet".into()), // <-- tip
            redis_entry(3, 3, true, 3, "testnet".into()),
        ];

        for entry in instance0_entries {
            let key = timestamp_key(instance0_key.dataplane, instance0_key.instance);

            let _: u32 = redis.zadd(key, entry.clone(), entry.height).unwrap();
        }

        let instance1_key = RedisKey {
            dataplane: 1,
            instance: 1,
        };

        let instance1_entries = vec![
            redis_entry(0, 0, false, 0, "mainnet".into()),
            redis_entry(11, 11, false, 1, "mainnet".into()), // <-- tip
            redis_entry(22, 22, true, 2, "mainnet".into()),
        ];

        for entry in instance1_entries {
            let key = timestamp_key(instance1_key.dataplane, instance1_key.instance);

            let _: u32 = redis.zadd(key, entry.clone(), entry.height).unwrap();
        }

        // Act
        let ts = get_new_safepoint(Options {
            tikv_address: "".into(),
            redis_address: redis_address.into(),
            safe_zone_millis: 10,
            cleanup_locks: false,
        });

        // Assert
        assert_eq!(ts, TiKVTimestamp::from_version(1));

        let key = timestamp_key(instance0_key.dataplane, instance0_key.instance);

        let new_instance0_block_options: Vec<RedisEntry> = redis
            .zrangebyscore(key, "-inf", "+inf")
            .expect("redis error");

        assert_eq!(
            new_instance0_block_options,
            vec![
                redis_entry(1, 1, false, 1, "testnet".into()),
                redis_entry(2, 2, false, 2, "testnet".into()),
                redis_entry(3, 3, true, 3, "testnet".into()),
            ]
        );

        let key = timestamp_key(instance1_key.dataplane, instance1_key.instance);

        let new_instance1_block_options: Vec<RedisEntry> = redis
            .zrangebyscore(key, "-inf", "+inf")
            .expect("redis error");

        assert_eq!(
            new_instance1_block_options,
            vec![
                redis_entry(11, 11, false, 1, "mainnet".into()),
                redis_entry(22, 22, true, 2, "mainnet".into()),
            ]
        );
    }
}
