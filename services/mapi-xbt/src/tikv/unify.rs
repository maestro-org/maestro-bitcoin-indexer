use std::collections::HashMap;

use tracing::warn;

use super::{
    adapter::Point,
    key_resolver::ReducerType,
    redis_entry::{RedisEntry, Timestamp},
};

/// pick an arbitrary require instance type, then starting with the best point for that instance,
/// check if the other instances also have that point, if not move then try the second best, ...
pub fn find_best_common_block(entry_map: &HashMap<ReducerType, Vec<RedisEntry>>) -> Option<Point> {
    let mut candidates = entry_map
        .values()
        .next()
        .unwrap()
        .iter()
        .filter(|entry| !entry.was_mempool)
        .collect::<Vec<_>>();

    let mut best_seen: Option<(u64, [u8; 32])> = None;

    // order candidates so that first candidate has largest height and latest tikv timestamp for a given height
    candidates.sort_by_key(|b| (b.height, Into::<Timestamp>::into(b.commit_ts.clone())));
    candidates.reverse();

    let mut network_instances_intersection = None;

    // find the best candidate point which is shared by every required instance
    'candidate_loop: for candidate in candidates {
        for instance_options in entry_map.values() {
            // track the best height option we see to check if our intersect does not match that
            if let Some(max) = instance_options
                .iter()
                .filter(|x| !x.was_mempool)
                .max_by_key(|x| x.height)
            {
                if max.height > best_seen.map(|x| x.0).unwrap_or_default() {
                    best_seen = Some((max.height, max.block_hash))
                }
            }

            // try next candidate if any instance does not have entry corresonding to candidate block
            if instance_options
                .iter()
                .filter(|x| !x.was_mempool)
                .find(|b| (b.height, b.block_hash) == (candidate.height, candidate.block_hash))
                .is_none()
            {
                continue 'candidate_loop;
            }
        }

        // if we reached here then every instance contained an entry for the candidate block and
        // thus we have found the best shared block between all instances within the network
        network_instances_intersection = Some((candidate.height, candidate.block_hash));

        // stop searching candidates, we have found the winner
        break;
    }

    if let Some(intersect) = network_instances_intersection {
        if intersect.0 != best_seen.map(|x| x.0).unwrap_or_default() {
            warn!("intersect does not match best seen: {intersect:?} {best_seen:?}")
        }
    }

    network_instances_intersection
}

pub fn find_best_common_mempool_view(
    entry_map: &HashMap<ReducerType, Vec<RedisEntry>>,
    chain_tip: Point,
) -> Option<u64> {
    let mut mempool_views = HashMap::new();

    for (instance_type, entries) in entry_map.clone() {
        let mut available_views = entries
            .into_iter()
            .filter(|x| x.was_mempool)
            .filter(|x| (x.chain_tip_height, x.chain_tip_hash) == chain_tip)
            .map(|x| x.mempool_view_ts)
            .collect::<Vec<_>>();

        available_views.sort();
        available_views.dedup();
        available_views.reverse();

        // available_views is a list of available mempool_view_ts, sorted by most recent.
        mempool_views.insert(instance_type, available_views);
    }

    // we have a map of instance -> available mempool views, now we need to find the best common

    let candidates = mempool_views
        .values()
        .next()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();

    let mut mempool_view_intersection = None;

    // find the best candidate mempool view which is shared by every required instance
    'candidate_loop: for candidate in candidates {
        for instance_options in mempool_views.values() {
            // try next candidate if any instance does not have candidate mempool view
            if !instance_options.contains(candidate) {
                continue 'candidate_loop;
            }
        }

        // if we reached here then every instance contained an entry for the candidate block and
        // thus we have found the best shared block between all instances within the network
        mempool_view_intersection = Some(*candidate);

        // stop searching candidates, we have found the winner
        break;
    }

    if mempool_view_intersection.is_none() {
        warn!("no mempool intersect found, candidates: {mempool_views:?}")
    }

    mempool_view_intersection
}

/// Find a common block at a specific height that all instances share.
/// Returns the Point (height, block_hash) if found.
pub fn find_common_block_at_height(
    entry_map: &HashMap<ReducerType, Vec<RedisEntry>>,
    height: u64,
) -> Option<Point> {
    // Get candidates at the specified height from the first instance
    let candidates: Vec<Point> = entry_map
        .values()
        .next()?
        .iter()
        .filter(|e| !e.was_mempool && e.height == height)
        .map(|e| (e.height, e.block_hash))
        .collect();

    // Find a candidate that all instances share
    for candidate in candidates {
        let all_have_it = entry_map.values().all(|entries| {
            entries
                .iter()
                .any(|e| !e.was_mempool && (e.height, e.block_hash) == candidate)
        });

        if all_have_it {
            return Some(candidate);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikv_client::{Timestamp as TiKVTimestamp, TimestampExt};

    /// Helper to create a block entry (non-mempool)
    fn block_entry(height: u64, block_hash: [u8; 32], commit_ts: u64) -> RedisEntry {
        RedisEntry {
            height,
            block_hash,
            was_mempool: false,
            commit_ts: TiKVTimestamp::from_version(commit_ts),
            network: "mainnet".into(),
            chain_tip_height: 0,
            chain_tip_hash: [0; 32],
            mempool_view_ts: 0,
        }
    }

    /// Helper to create a mempool entry
    fn mempool_entry(
        height: u64,
        block_hash: [u8; 32],
        commit_ts: u64,
        chain_tip_height: u64,
        chain_tip_hash: [u8; 32],
        mempool_view_ts: u64,
    ) -> RedisEntry {
        RedisEntry {
            height,
            block_hash,
            was_mempool: true,
            commit_ts: TiKVTimestamp::from_version(commit_ts),
            network: "mainnet".into(),
            chain_tip_height,
            chain_tip_hash,
            mempool_view_ts,
        }
    }

    fn hash(n: u8) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = n;
        h
    }

    // ==========================================================================
    // Tests for find_best_common_block
    // ==========================================================================

    #[test]
    fn test_find_best_common_block_single_instance() {
        let mut entry_map = HashMap::new();
        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
                block_entry(102, hash(3), 1002),
            ],
        );

        let result = find_best_common_block(&entry_map);
        assert_eq!(result, Some((102, hash(3))));
    }

    #[test]
    fn test_find_best_common_block_two_instances_same_blocks() {
        let mut entry_map = HashMap::new();
        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
            ],
        );
        entry_map.insert(
            ReducerType::TxsByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
            ],
        );

        let result = find_best_common_block(&entry_map);
        assert_eq!(result, Some((101, hash(2))));
    }

    #[test]
    fn test_find_best_common_block_one_instance_behind() {
        // Instance A has blocks 100, 101, 102
        // Instance B has blocks 100, 101 (behind by one)
        // Should return 101 as the intersection
        let mut entry_map = HashMap::new();
        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
                block_entry(102, hash(3), 1002),
            ],
        );
        entry_map.insert(
            ReducerType::TxsByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
            ],
        );

        let result = find_best_common_block(&entry_map);
        assert_eq!(result, Some((101, hash(2))));
    }

    // ==========================================================================
    // Tests for find_best_common_mempool_view
    // ==========================================================================

    #[test]
    fn test_find_best_common_mempool_view_single_instance() {
        let chain_tip = (100, hash(1));
        let mut entry_map = HashMap::new();
        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                mempool_entry(101, hash(10), 2000, 100, hash(1), 5000),
                mempool_entry(101, hash(11), 2001, 100, hash(1), 5001),
            ],
        );

        let result = find_best_common_mempool_view(&entry_map, chain_tip);
        // Should return the most recent mempool_view_ts
        assert_eq!(result, Some(5001));
    }

    #[test]
    fn test_find_best_common_mempool_view_two_instances_common() {
        let chain_tip = (100, hash(1));
        let mut entry_map = HashMap::new();
        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                mempool_entry(101, hash(10), 2000, 100, hash(1), 5000),
                mempool_entry(101, hash(11), 2001, 100, hash(1), 5001),
            ],
        );
        entry_map.insert(
            ReducerType::TxsByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                mempool_entry(101, hash(10), 2000, 100, hash(1), 5000),
                // Instance B doesn't have 5001 yet
            ],
        );

        let result = find_best_common_mempool_view(&entry_map, chain_tip);
        // Should return 5000 as that's the best common
        assert_eq!(result, Some(5000));
    }

    #[test]
    fn test_find_best_common_mempool_view_no_mempool_for_tip() {
        let chain_tip = (101, hash(2)); // New block just mined
        let mut entry_map = HashMap::new();
        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
                // Mempool entries are for chain_tip 100, not 101
                mempool_entry(101, hash(10), 2000, 100, hash(1), 5000),
            ],
        );

        let result = find_best_common_mempool_view(&entry_map, chain_tip);
        // No mempool view for the new tip
        assert_eq!(result, None);
    }

    // ==========================================================================
    // Tests for find_common_block_at_height
    // ==========================================================================

    #[test]
    fn test_find_common_block_at_height_found() {
        let mut entry_map = HashMap::new();
        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
            ],
        );
        entry_map.insert(
            ReducerType::TxsByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
            ],
        );

        let result = find_common_block_at_height(&entry_map, 100);
        assert_eq!(result, Some((100, hash(1))));
    }

    #[test]
    fn test_find_common_block_at_height_not_found() {
        let mut entry_map = HashMap::new();
        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
            ],
        );
        entry_map.insert(
            ReducerType::TxsByScriptHash,
            vec![
                block_entry(101, hash(2), 1001), // Missing block 100
            ],
        );

        let result = find_common_block_at_height(&entry_map, 100);
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_common_block_at_height_no_block_at_height() {
        let mut entry_map = HashMap::new();
        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
            ],
        );

        let result = find_common_block_at_height(&entry_map, 99);
        assert_eq!(result, None);
    }

    // ==========================================================================
    // Tests for fallback scenario (integration-style)
    // ==========================================================================

    /// Simulates the scenario where:
    /// - Block 101 just mined (both instances have it)
    /// - No mempool view for block 101 yet
    /// - Mempool views exist for block 100
    /// - Fallback should find block 100 with mempool
    #[test]
    fn test_fallback_scenario_new_block_no_mempool() {
        let mut entry_map = HashMap::new();

        // Both instances have blocks 100 and 101
        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
                // Mempool for block 100
                mempool_entry(101, hash(10), 2000, 100, hash(1), 5000),
            ],
        );
        entry_map.insert(
            ReducerType::TxsByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
                // Mempool for block 100
                mempool_entry(101, hash(10), 2000, 100, hash(1), 5000),
            ],
        );

        // Step 1: find_best_common_block returns 101
        let intersection_block = find_best_common_block(&entry_map).unwrap();
        assert_eq!(intersection_block, (101, hash(2)));

        // Step 2: No mempool for block 101
        let mempool_view = find_best_common_mempool_view(&entry_map, intersection_block);
        assert_eq!(mempool_view, None);

        // Step 3: Fallback - find common block at height 100
        let fallback_height = intersection_block.0 - 1;
        let fallback_tip = find_common_block_at_height(&entry_map, fallback_height);
        assert_eq!(fallback_tip, Some((100, hash(1))));

        // Step 4: Check mempool view at fallback tip
        let fallback_mempool = find_best_common_mempool_view(&entry_map, fallback_tip.unwrap());
        assert_eq!(fallback_mempool, Some(5000));
    }

    /// Simulates the scenario where:
    /// - Block 101 just mined
    /// - No mempool for 101 or 100
    /// - Should return None for fallback mempool
    #[test]
    fn test_fallback_scenario_no_mempool_anywhere() {
        let mut entry_map = HashMap::new();

        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
                // No mempool entries at all
            ],
        );
        entry_map.insert(
            ReducerType::TxsByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
            ],
        );

        let intersection_block = find_best_common_block(&entry_map).unwrap();
        assert_eq!(intersection_block, (101, hash(2)));

        let mempool_view = find_best_common_mempool_view(&entry_map, intersection_block);
        assert_eq!(mempool_view, None);

        let fallback_tip = find_common_block_at_height(&entry_map, 100);
        assert_eq!(fallback_tip, Some((100, hash(1))));

        let fallback_mempool = find_best_common_mempool_view(&entry_map, fallback_tip.unwrap());
        assert_eq!(fallback_mempool, None);
    }

    /// Simulates the happy path:
    /// - Block 101 is the tip
    /// - Mempool views exist for block 101
    /// - No fallback needed
    #[test]
    fn test_happy_path_mempool_available() {
        let mut entry_map = HashMap::new();

        entry_map.insert(
            ReducerType::UtxosByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
                mempool_entry(102, hash(20), 3000, 101, hash(2), 6000),
            ],
        );
        entry_map.insert(
            ReducerType::TxsByScriptHash,
            vec![
                block_entry(100, hash(1), 1000),
                block_entry(101, hash(2), 1001),
                mempool_entry(102, hash(20), 3000, 101, hash(2), 6000),
            ],
        );

        let intersection_block = find_best_common_block(&entry_map).unwrap();
        assert_eq!(intersection_block, (101, hash(2)));

        let mempool_view = find_best_common_mempool_view(&entry_map, intersection_block);
        assert_eq!(mempool_view, Some(6000));
        // No fallback needed
    }
}
