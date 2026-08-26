use std::collections::{HashSet, VecDeque};

use tracing::{info, warn};

use crate::{crosscut::Point, storage::StorageActions};

pub const MAX_BUFFER_LEN: usize = 16;

/// An in-memory buffer of the recently-processed points
///
/// A buffer of recently processed points (blocks) which is used so that we can
/// process rollbacked blocks in reverse to undo the effects. New blocks are added
/// to the front of the queue and old blocks are truncated when the buffer grows
/// larger than MAX_BUFFER_LEN. When we process a new block (RollForward) we
/// push it to the front of the buffer, and when we process a RollBackwards(Point)
/// we return the blocks which were processed since that point ordered by latest
/// first.
///
/// Some code is inspired by TxPipe's Pallas chainsync point rollback buffer,
/// but achieves different results. The Pallas chainsync rollback buffer pops
/// the _oldest_ points from the buffer once the buffer size reaches a
/// configurable size min-depth, where as this buffer is used to pop the most
/// recent blocks when a rollback instruction is received. Instead of storing
/// the block bytes, it the buffer is generic in the sence that it stores the
/// Point (block ID) along with some other information needed to undo the effects
/// of the block, for example consumed/produced UTxOs and storage actions.
#[derive(Debug)]
pub struct RollbackBuffer {
    blocks: VecDeque<PointWithResult>,
    size: usize,
    safe_mode: bool,
}

/// A Point with the effects which resulted from that point relevant to the
/// context of the location of the buffer
///
/// For example, each entry in the enrich stage rollback buffer will be a Point
/// along with the UTxOs which were added and removed from the enrich DB (the
/// result) as a result of processing the block. In the reducer stage the
/// result will be the StorageActions which were sent when processing the block
/// and in the storage stage the result will be the results of performaning the
/// storage actions.
#[derive(Debug, Clone)]
pub struct PointWithResult {
    pub point: Point,
    pub result: StorageActions,
}

/// If we found the given point in the buffer return all the blocks which came
/// after that point, starting with the most recent, otherwise reflect that the
/// point was not found
#[derive(Debug)]
pub enum RollbackResult {
    PointFound(Vec<PointWithResult>),
    PointNotFound,
}

impl RollbackBuffer {
    pub fn new(size: usize, safe_mode: bool) -> Self {
        Self {
            blocks: VecDeque::new(),
            size,
            safe_mode,
        }
    }

    pub fn capacity(&self) -> usize {
        self.size
    }

    /// Find the position of a point within the buffer
    pub fn position(&self, point: &Point) -> Option<usize> {
        self.blocks.iter().position(|p| p.point.eq(point))
    }

    /// Returns the number of blocks in the buffer
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    pub fn latest(&self) -> Option<&PointWithResult> {
        self.blocks.front()
    }

    pub fn oldest(&self) -> Option<&PointWithResult> {
        self.blocks.back()
    }

    /// Add a new block to the front of the rollback buffer and pop the oldest
    /// block if the length of the buffer is at capacity.
    /// Returns the removed block if one was evicted from the buffer.
    pub fn add_block(&mut self, point: Point, result: StorageActions) -> Option<PointWithResult> {
        self.blocks.push_front(PointWithResult { point, result });

        // If we exceeded capacity, pop and return the oldest block
        if self.blocks.len() > self.size {
            self.blocks.pop_back()
        } else {
            None
        }
    }

    /// Return an vector of the blocks which have been processed since the
    /// given point and remove those blocks from the buffer (most recent first)
    pub fn rollback_to_point(&mut self, point: &Point) -> Result<Vec<PointWithResult>, Point> {
        match self.position(point) {
            Some(p) => Ok(self.blocks.drain(..p).collect()),
            None => Err(point.clone()),
        }
    }

    /// Like rollback to point but does not remove the points
    pub fn points_since(&self, point: &Point) -> Result<Vec<PointWithResult>, Point> {
        match self.position(point) {
            Some(p) => Ok(self.blocks.range(..p).cloned().collect()),
            None => Err(point.clone()),
        }
    }

    /// insert new inverse actions for a point into the rollback buffer, NOTE: there must be max one
    /// action per key
    pub fn insert_actions_for_point(&mut self, point: &Point, new_actions: StorageActions) {
        match self.position(point) {
            Some(p) => {
                let entry = self.blocks.get_mut(p).unwrap();

                if self.safe_mode {
                    for action in new_actions.iter() {
                        if entry.result.iter().any(|x| x.key() == action.key()) {
                            panic!(
                                "inserting action for key already present in rb buf {:?}",
                                action
                            )
                        }
                    }
                }

                entry.result.extend(new_actions);
            }
            None => panic!("point not in rb buf"),
        }
    }

    pub fn remove_actions_for_point(&mut self, point: &Point, remove_actions: StorageActions) {
        match self.position(point) {
            Some(p) => {
                let entry = self.blocks.get_mut(p).unwrap();

                if self.safe_mode {
                    for action in remove_actions.iter() {
                        if !entry.result.contains(action) {
                            panic!(
                                "trying to remove inverse action which doesn't exist {:?}",
                                action
                            );
                        }
                    }
                }

                let remove_actions: HashSet<_> = remove_actions.into_iter().collect();

                // keep only actions which we are not removing
                entry.result.retain(|x| !remove_actions.contains(x));

                // remove if empty
                if entry.result.is_empty() {
                    info!("removing point from rb buf as all actions removed");
                    self.blocks.remove(p);
                }
            }
            None => warn!("no point found when trying to remove"),
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use bitcoin::{hashes::Hash, BlockHash};

//     use super::*;

//     fn dummy_point(i: u8) -> Point {
//         Point {
//             height: i.into(),
//             hash: BlockHash::from_byte_array([i; 32]),
//         }
//     }

//     fn build_filled_buffer(n: u8) -> RollbackBuffer<Vec<u8>> {
//         let mut buffer = RollbackBuffer::default();

//         for i in 0..n {
//             let point = dummy_point(i as u8);
//             buffer.add_block(point, [i; 64].to_vec());
//         }

//         buffer
//     }

//     #[test]
//     fn add_block_accumulates_points() {
//         assert!(3 <= MAX_BUFFER_LEN, "Test requires a larger MAX_BUFFER_LEN");
//         let buffer = build_filled_buffer(3);

//         assert!(matches!(buffer.position(&dummy_point(0)), Some(2)));
//         assert!(matches!(buffer.position(&dummy_point(1)), Some(1)));
//         assert!(matches!(buffer.position(&dummy_point(2)), Some(0)));

//         assert_eq!(buffer.oldest().unwrap().point, dummy_point(0));
//         assert_eq!(buffer.latest().unwrap().point, dummy_point(2));
//     }

//     #[test]
//     fn add_block_buffer_truncation() {
//         let buffer = build_filled_buffer((MAX_BUFFER_LEN + 5) as u8);

//         assert_eq!(buffer.oldest().unwrap().point, dummy_point(5));
//         assert_eq!(
//             buffer.latest().unwrap().point,
//             dummy_point((MAX_BUFFER_LEN + 5 - 1) as u8)
//         );
//     }

//     /// buffer: [B5, B4, B3, B2, B1, B0]
//     /// rollback to B3...
//     /// to_undo: [B5, B4]
//     /// buffer: [B3, B2, B1, B0]
//     #[test]
//     fn rollback_found() {
//         assert!(6 <= MAX_BUFFER_LEN, "Test requires a larger MAX_BUFFER_LEN");
//         let mut buffer = build_filled_buffer(6);
//         let rollback_point = dummy_point(3);

//         let to_undo = match buffer.rollback_to_point(&rollback_point) {
//             Ok(ps) => ps,
//             Err(_) => panic!("Point not found"),
//         };

//         assert_eq!(to_undo.len(), 2);
//         assert_eq!(to_undo.get(0).unwrap().point, dummy_point(5));
//         assert_eq!(to_undo.get(1).unwrap().point, dummy_point(4));
//         assert_eq!(buffer.len(), 4);
//         assert_eq!(buffer.position(&dummy_point(3)), Some(0));
//         assert_eq!(buffer.position(&dummy_point(2)), Some(1));
//         assert_eq!(buffer.position(&dummy_point(1)), Some(2));
//         assert_eq!(buffer.position(&dummy_point(0)), Some(3));
//     }

//     #[test]
//     fn rollback_not_found() {
//         assert!(6 <= MAX_BUFFER_LEN, "Test requires a larger MAX_BUFFER_LEN");
//         let mut buffer = build_filled_buffer(6);
//         let rollback_point = dummy_point(6);

//         let res = buffer.rollback_to_point(&rollback_point);

//         assert_eq!(buffer.len(), 6);
//         assert!(matches!(res, Err(_)))
//     }
// }
