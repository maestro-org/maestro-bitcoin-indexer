use tikv_client::{
    Timestamp, TimestampExt, Transaction,
    proto::kvrpcpb::{Mutation, Op},
};
use tracing::{debug, info};

use crate::model::StorageActionPayload;

pub fn compact_actions(actions: Vec<StorageActionPayload>) -> Vec<StorageActionPayload> {
    let mut stack: Vec<StorageActionPayload> = Vec::new();

    for action in actions {
        match action {
            a @ StorageActionPayload::RollForward(..) => {
                // Remove all MempoolRefresh actions from the stack, to prioritise the new chain block
                stack.retain(|x| !matches!(x, StorageActionPayload::MempoolRefresh(..)));

                // Add RollForward actions to the stack
                stack.push(a);
            }
            StorageActionPayload::RollBack(target_point, mutable) => {
                // Remove all MempoolRefresh actions from the stack, to prioritise the rollback
                stack.retain(|x| !matches!(x, StorageActionPayload::MempoolRefresh(..)));

                // if we are rolling back to point X, then find the earliest action in the stack
                // after which we are at point X, and discard any action after that point
                if let Some(index) = stack.iter().position(|a| {
                    matches!(a, StorageActionPayload::RollBack(p, _) if *p == target_point) ||
                    matches!(a, StorageActionPayload::RollForward(p, _, _) if *p == target_point)
                }) {
                    stack.truncate(index + 1);
                } else {
                    // the rollback point is not on our stack, so we must be rolling back
                    // further than the actions currently being processed. wipe all actions so
                    // far and add the rollback
                    stack = vec![StorageActionPayload::RollBack(target_point, mutable)]
                }
            }
            a @ StorageActionPayload::MempoolRefresh(..) => {
                // If there are multiple mempool refreshes in the queue, we will skip all but the
                // most recent
                stack.retain(|x| !matches!(x, StorageActionPayload::MempoolRefresh(..)));

                stack.push(a)
            }
        }
    }

    stack
}

pub async fn commit_txn_or_rollback(
    txn: &mut Transaction,
) -> Result<Option<Timestamp>, tikv_client::Error> {
    match txn.commit().await {
        Ok(ts) => {
            debug!(
                "finished committing: {ts:?} ({:?})",
                ts.as_ref().map(|t| t.version())
            );

            Ok(ts)
        }
        e @ Err(_) => {
            info!("error while committing, rolling back txn...");

            txn.rollback().await?;

            info!("rollbacked database txn successfully");

            e
        }
    }
}

pub fn delete_mutation(key: Vec<u8>) -> Mutation {
    Mutation {
        op: Op::Del.into(),
        key,
        ..Default::default()
    }
}

pub fn set_mutation(key: Vec<u8>, value: Vec<u8>) -> Mutation {
    Mutation {
        op: Op::Put.into(),
        key,
        value,
        ..Default::default()
    }
}
