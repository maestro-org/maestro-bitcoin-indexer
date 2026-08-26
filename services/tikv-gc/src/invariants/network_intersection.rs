use std::collections::HashMap;

use tikv_client::Timestamp as TiKVTimestamp;
use tracing::info;

use crate::{
    entry::{RedisEntry, RedisKey},
    types::Timestamp,
};

pub fn get_latest_network_intersection_ts(
    entry_map: &HashMap<RedisKey, Vec<RedisEntry>>,
) -> TiKVTimestamp {
    // create a map each network to it's instances
    let mut network_map: HashMap<String, Vec<RedisKey>> = HashMap::new();

    for (key, entries) in entry_map.iter() {
        if let Some(entry) = entries.into_iter().next() {
            let network = entry.network.clone();

            network_map
                .entry(network)
                .and_modify(|v| v.push(key.clone()))
                .or_insert(vec![key.clone()]);
        }
    }

    // store the safepoint for each network which ensures we have a common block
    // across all instances within a network
    let mut network_intersection_timestamps = HashMap::new();

    // for each network...
    for (network, instances) in network_map {
        if network.to_lowercase().contains("dogecoin") {
            // skip dogecoin networks until we implement unified view
            continue;
        }

        let arbitrary_instance = instances[0];

        let mut candidates = entry_map[&arbitrary_instance]
            .iter()
            .filter(|x| !x.was_mempool)
            .collect::<Vec<_>>();

        // order candidates so that first candidate has largest height and latest tikv timestamp for a given height
        candidates.sort_by_key(|b| (b.height, Into::<Timestamp>::into(b.commit_ts.clone())));
        candidates.reverse();

        // Find the best (highest) intersection and the intersection at exactly height - 1.
        let mut best_intersection: Option<(u64, [u8; 32])> = None;
        let mut previous_intersection: Option<(u64, [u8; 32])> = None;

        'candidate_loop: for candidate in candidates {
            // Skip if we already have an intersection at this height
            if let Some((best_h, _)) = best_intersection {
                if candidate.height == best_h {
                    continue;
                }
                // Candidates are sorted by height descending, so if we're past height-1,
                // there's no intersection at height-1 and we can stop.
                if best_h == 0 || candidate.height != best_h - 1 {
                    break;
                }
            }

            for instance in instances.iter() {
                let instance_options = entry_map[instance].clone();

                // try next candidate if any instance does not have entry corresponding to candidate block
                if instance_options
                    .iter()
                    .filter(|x| !x.was_mempool)
                    .find(|b| (b.height, b.block_hash) == (candidate.height, candidate.block_hash))
                    .is_none()
                {
                    continue 'candidate_loop;
                }
            }

            // all instances have this block - it's an intersection
            let block = (candidate.height, candidate.block_hash);

            if best_intersection.is_none() {
                best_intersection = Some(block);
            } else {
                // We already have best, this is exactly height - 1
                previous_intersection = Some(block);
                break; // We have both, stop searching
            }
        }

        let best_intersection = best_intersection.unwrap_or_else(|| {
            panic!("no network instances intersection found for '{network}', is there a syncing instance?");
        });

        info!(
            "best intersection block for '{network}' is ({}, {})",
            best_intersection.0,
            hex::encode(best_intersection.1)
        );

        // Use the previous (second-best) intersection as the safepoint to ensure we maintain
        // both the current tip AND the previous block. This supports 1-block fallback in MAPI.
        let safepoint_block = if let Some(prev) = previous_intersection {
            info!(
                "'{network}': using previous intersection at height {} as safepoint",
                prev.0
            );
            prev
        } else {
            // Only one height available, use best intersection
            info!(
                "'{network}': no previous height available, using best intersection at height {}",
                best_intersection.0
            );
            best_intersection
        };

        // For each instance, find the entry with the latest commit_ts for the safepoint block
        let mut safepoint_timestamps = vec![];

        for instance in &instances {
            let mut instance_entries: Vec<_> = entry_map[instance]
                .iter()
                .filter(|e| !e.was_mempool)
                .filter(|e| (e.height, e.block_hash) == safepoint_block)
                .collect();

            // select the entry with the latest commit_ts (the most recently refreshed one)
            instance_entries.sort_by_key(|e| Into::<Timestamp>::into(e.commit_ts.clone()));
            instance_entries.reverse();

            let entry = instance_entries
                .into_iter()
                .next()
                .expect("expected at least one entry for safepoint block");

            safepoint_timestamps.push(entry.commit_ts.clone());
        }

        // select the earliest timestamp across all instances (so all instances have an entry >= safepoint)
        safepoint_timestamps.sort_by_key(|ts| Into::<Timestamp>::into(ts.clone()));
        let network_safepoint_ts = safepoint_timestamps.into_iter().next().unwrap();

        network_intersection_timestamps.insert(network, network_safepoint_ts);
    }

    info!("network intersection timestamps: {network_intersection_timestamps:?}");

    let mut network_intersection_timestamps = network_intersection_timestamps
        .into_values()
        .collect::<Vec<_>>();

    network_intersection_timestamps.sort_by_key(|b| Into::<Timestamp>::into(b.clone()));

    let earliest_network_intersection_ts =
        network_intersection_timestamps.into_iter().next().unwrap();

    earliest_network_intersection_ts
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

    /// Test that safepoint uses the previous (second-best) intersection, not the tip
    #[test]
    fn test_uses_previous_intersection() {
        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        let instance0_val = vec![
            redis_entry(0, 0, false, 0, "testnet".into()),
            redis_entry(1, 1, false, 10, "testnet".into()), // <-- previous intersection (safepoint)
            redis_entry(2, 2, false, 20, "testnet".into()), // <-- best intersection (tip)
            redis_entry(3, 3, true, 30, "testnet".into()),  // mempool, ignored
        ];

        let entry_map = vec![(instance0_key, instance0_val)]
            .into_iter()
            .collect::<HashMap<_, _>>();

        let ts = get_latest_network_intersection_ts(&entry_map);

        // Safepoint should be at height 1 (previous), not height 2 (tip)
        assert_eq!(ts, TiKVTimestamp::from_version(10));
    }

    /// Test that when only one height is available, we use it as the safepoint
    #[test]
    fn test_single_height_uses_best() {
        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        let instance0_val = vec![
            redis_entry(5, 5, false, 50, "testnet".into()), // only one height
        ];

        let entry_map = vec![(instance0_key, instance0_val)]
            .into_iter()
            .collect::<HashMap<_, _>>();

        let ts = get_latest_network_intersection_ts(&entry_map);

        // With only one height, use it as safepoint
        assert_eq!(ts, TiKVTimestamp::from_version(50));
    }

    /// Test multiple instances - safepoint is at previous intersection,
    /// using the earliest "latest timestamp" across all instances
    #[test]
    fn test_multiple_instances_previous_intersection() {
        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        let instance0_val = vec![
            redis_entry(0, 0, false, 0, "net".into()),
            redis_entry(1, 1, false, 10, "net".into()), // <-- previous, latest ts for this instance
            redis_entry(2, 2, false, 20, "net".into()), // <-- tip
        ];

        let instance1_key = RedisKey {
            dataplane: 1,
            instance: 1,
        };

        let instance1_val = vec![
            redis_entry(0, 0, false, 5, "net".into()),
            redis_entry(1, 1, false, 15, "net".into()), // <-- previous, latest ts for this instance
            redis_entry(2, 2, true, 25, "net".into()),  // mempool, so tip is at height 1
        ];

        let entry_map = vec![
            (instance0_key, instance0_val),
            (instance1_key, instance1_val),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let ts = get_latest_network_intersection_ts(&entry_map);

        // Best intersection is height 1 (both have it as non-mempool)
        // Previous intersection is height 0
        // Instance0's latest ts at height 0 is 0, Instance1's latest ts is 5
        // Earliest of those is 0
        assert_eq!(ts, TiKVTimestamp::from_version(0));
    }

    /// Test that refreshed timestamps are used (latest commit_ts for a block is selected)
    #[test]
    fn test_refreshed_timestamps_used() {
        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        // Block 1 has multiple entries with refreshed timestamps
        let instance0_val = vec![
            redis_entry(1, 1, false, 10, "testnet".into()), // original
            redis_entry(1, 1, false, 11, "testnet".into()), // refreshed
            redis_entry(1, 1, false, 12, "testnet".into()), // refreshed again (latest)
            redis_entry(2, 2, false, 20, "testnet".into()), // tip
        ];

        let entry_map = vec![(instance0_key, instance0_val)]
            .into_iter()
            .collect::<HashMap<_, _>>();

        let ts = get_latest_network_intersection_ts(&entry_map);

        // Safepoint should be at height 1, using the latest refreshed timestamp (12)
        assert_eq!(ts, TiKVTimestamp::from_version(12));
    }

    /// Test multiple networks - earliest safepoint across all networks is selected
    #[test]
    fn test_multiple_networks() {
        // --- network 0: safepoint at height 1 with ts 10
        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        let instance0_val = vec![
            redis_entry(0, 0, false, 5, "net0".into()),
            redis_entry(1, 1, false, 10, "net0".into()), // <-- previous
            redis_entry(2, 2, false, 20, "net0".into()), // <-- tip
        ];

        // --- network 1: safepoint at height 10 with ts 100
        let instance1_key = RedisKey {
            dataplane: 1,
            instance: 1,
        };

        let instance1_val = vec![
            redis_entry(9, 9, false, 90, "net1".into()),
            redis_entry(10, 10, false, 100, "net1".into()), // <-- previous
            redis_entry(11, 11, false, 110, "net1".into()), // <-- tip
        ];

        let entry_map = vec![
            (instance0_key, instance0_val),
            (instance1_key, instance1_val),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let ts = get_latest_network_intersection_ts(&entry_map);

        // net0's previous is height 1 (ts 10), net1's previous is height 10 (ts 100)
        // But wait - we need to recalculate:
        // net0: best=2, previous=1, ts=10
        // net1: best=11, previous=10, ts=100
        // Earliest across networks is 10
        assert_eq!(ts, TiKVTimestamp::from_version(10));
    }

    /// Test mixed mempool and non-mempool instances in the same network.
    /// Mempool-enabled instances have both mempool entries (was_mempool=true) and confirmed
    /// block entries (was_mempool=false) for the same height.
    /// Non-mempool instances only have confirmed block entries.
    /// Both types refresh their confirmed block entry timestamps.
    #[test]
    fn test_mixed_mempool_and_non_mempool_instances() {
        // Instance 0: NON-MEMPOOL instance (no mempool entries, just confirmed blocks)
        // Refreshes its block timestamps periodically
        let instance0_key = RedisKey {
            dataplane: 0,
            instance: 0,
        };

        let instance0_val = vec![
            // Block 100 - confirmed, with refreshed timestamps
            redis_entry(100, 100, false, 1000, "mainnet".into()),
            redis_entry(100, 100, false, 1001, "mainnet".into()), // refreshed
            redis_entry(100, 100, false, 1002, "mainnet".into()), // refreshed again
            // Block 101 - confirmed, with refreshed timestamps
            redis_entry(101, 101, false, 1010, "mainnet".into()),
            redis_entry(101, 101, false, 1011, "mainnet".into()), // refreshed
            redis_entry(101, 101, false, 1012, "mainnet".into()), // refreshed (latest)
            // Block 102 (tip) - confirmed, with refreshed timestamps
            redis_entry(102, 102, false, 1020, "mainnet".into()),
            redis_entry(102, 102, false, 1021, "mainnet".into()), // refreshed (latest)
        ];

        // Instance 1: MEMPOOL-ENABLED instance
        // Has mempool entries when block first seen, then confirmed entries once processed
        let instance1_key = RedisKey {
            dataplane: 1,
            instance: 1,
        };

        let instance1_val = vec![
            // Block 100 - was first mempool, then confirmed
            redis_entry(100, 100, true, 2000, "mainnet".into()), // mempool entry
            redis_entry(100, 100, false, 2001, "mainnet".into()), // confirmed
            redis_entry(100, 100, false, 2002, "mainnet".into()), // confirmed, refreshed
            // Block 101 - was first mempool, then confirmed
            redis_entry(101, 101, true, 2010, "mainnet".into()), // mempool entry
            redis_entry(101, 101, false, 2011, "mainnet".into()), // confirmed
            redis_entry(101, 101, false, 2012, "mainnet".into()), // confirmed, refreshed (latest)
            // Block 102 (tip) - was first mempool, then confirmed
            redis_entry(102, 102, true, 2020, "mainnet".into()), // mempool entry
            redis_entry(102, 102, false, 2021, "mainnet".into()), // confirmed
            redis_entry(102, 102, false, 2022, "mainnet".into()), // confirmed, refreshed (latest)
            // Block 103 - still in mempool, not yet confirmed
            redis_entry(103, 103, true, 2030, "mainnet".into()), // mempool only
        ];

        // Instance 2: Another MEMPOOL-ENABLED instance (slightly behind)
        let instance2_key = RedisKey {
            dataplane: 2,
            instance: 2,
        };

        let instance2_val = vec![
            // Block 100 - confirmed
            redis_entry(100, 100, true, 3000, "mainnet".into()), // mempool entry
            redis_entry(100, 100, false, 3001, "mainnet".into()), // confirmed
            // Block 101 - confirmed with refreshes
            redis_entry(101, 101, true, 3010, "mainnet".into()), // mempool entry
            redis_entry(101, 101, false, 3011, "mainnet".into()), // confirmed
            redis_entry(101, 101, false, 3012, "mainnet".into()), // confirmed, refreshed (latest)
            // Block 102 (tip) - confirmed
            redis_entry(102, 102, true, 3020, "mainnet".into()), // mempool entry
            redis_entry(102, 102, false, 3021, "mainnet".into()), // confirmed (latest)
            // Block 103 - still in mempool
            redis_entry(103, 103, true, 3030, "mainnet".into()), // mempool only
        ];

        let entry_map = vec![
            (instance0_key, instance0_val),
            (instance1_key, instance1_val),
            (instance2_key, instance2_val),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let ts = get_latest_network_intersection_ts(&entry_map);

        // Best intersection: height 102 (all have confirmed/non-mempool entries)
        // Previous intersection: height 101
        //
        // For height 101 (only considering was_mempool=false entries):
        // - Instance 0's latest confirmed ts: 1012
        // - Instance 1's latest confirmed ts: 2012
        // - Instance 2's latest confirmed ts: 3012
        //
        // Earliest of those: 1012
        assert_eq!(ts, TiKVTimestamp::from_version(1012));
    }
}
