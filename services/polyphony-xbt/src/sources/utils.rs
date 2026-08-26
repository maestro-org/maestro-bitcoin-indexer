use bitcoin::{BlockHash, hashes::Hash};
use tonic::{Code, transport::Channel};
use tracing::{error, info};

use crate::{
    Error,
    crosscut::{self, Point},
    sources::compressor::compressor_api::PageBlocksWithCtxRequest,
    storage,
};

use super::compressor::compressor_api::{BlockRef, sync_service_client::SyncServiceClient};

pub fn point_to_block_ref(point: Point) -> BlockRef {
    BlockRef {
        height: point.height,
        hash: point.hash.to_byte_array().to_vec(),
    }
}

pub fn block_ref_to_point(block_ref: BlockRef) -> Point {
    Point {
        hash: BlockHash::from_slice(&block_ref.hash).expect("block ref"),
        height: block_ref.height,
    }
}

pub async fn try_intersect_with_last_point_or_config(
    intersect: &crosscut::IntersectConfig,
    cursor: &mut storage::Cursor,
    client: &mut SyncServiceClient<Channel>,
) -> Result<Option<Point>, crate::Error> {
    // else try intersect using the point in the lock/cursor KV should one be present
    match cursor.last_point().await? {
        Some(point) => {
            info!("found existing cursor in storage plugin: {point:?}");

            let res = client
                .page_blocks_with_context(PageBlocksWithCtxRequest {
                    cursor: Some(point_to_block_ref(point.clone())),
                    max_items: 1,
                })
                .await;

            match res {
                Ok(_) => {
                    info!("found intersect with cursor: {point:?}");
                    return Ok(Some(point));
                }
                Err(e) if e.code() == Code::NotFound => {
                    error!("could not intersect using cursor: {point:?}");
                    return Err(Error::IntersectNotFound);
                }
                Err(e) => return Err(Error::source(e)),
            }
        }
        None => info!("no cursor found in storage plugin"),
    };

    // if no cursor KV then this must be the first time the instance is running, use the intersect
    // specified in the config
    match &intersect {
        crosscut::IntersectConfig::Origin => {
            info!("using origin as intersect");

            Ok(None)
        }
        crosscut::IntersectConfig::Tip => {
            let res = client
                .page_blocks_with_context(PageBlocksWithCtxRequest {
                    cursor: None,
                    max_items: 1,
                })
                .await
                .map_err(Error::source)?;

            let tip = res.into_inner().chain_tip;

            info!("using source tip as intersect: {tip:?}");

            Ok(tip.map(|x| block_ref_to_point(x)))
        }
        crosscut::IntersectConfig::Point(_, _) => {
            let point = intersect.get_point().expect("point value");

            let res = client
                .page_blocks_with_context(PageBlocksWithCtxRequest {
                    cursor: Some(point_to_block_ref(point.clone())),
                    max_items: 1,
                })
                .await;

            match res {
                Ok(_) => {
                    info!("found intersect with config point: {point:?}");
                    return Ok(Some(point));
                }
                Err(e) if e.code() == Code::NotFound => {
                    error!("could not intersect using config point: {point:?}");
                    return Err(Error::IntersectNotFound);
                }
                Err(e) => return Err(Error::source(e)),
            }
        }
    }
}

pub async fn try_intersect_with_rollback_buf(
    cursor: &mut storage::Cursor,
    client: &mut SyncServiceClient<Channel>,
    persistent_buf: Option<Vec<crate::rollback::PersistentBufferValue>>,
) -> Result<Option<Point>, crate::Error> {
    match persistent_buf {
        Some(buf) => {
            // sort our persistent buf points newest to oldest
            let points: Vec<Point> = buf.into_iter().map(|v| v.point.into()).rev().collect();

            info!("found persistent rollback buffer in storage plugin with points: {points:?}");

            let split_commit_lock = cursor
                .split_commit_lock()
                .await?
                .map(|x| x.lock.safe_point.map(|x| x.0))
                .flatten();

            // for each point, keep trying to request blocks from that point
            // until we get a success, which means we intersected with the chain
            // at that point
            for point in points {
                // if we may have partially committed a block, ignore associated entries in
                // rollback buffer
                if let Some(partial_after) = split_commit_lock {
                    info!("found split commit lock, won't intersect above height {partial_after}");

                    if point.height > partial_after {
                        continue;
                    }
                }

                let res = client
                    .page_blocks_with_context(PageBlocksWithCtxRequest {
                        cursor: Some(point_to_block_ref(point.clone())),
                        max_items: 1,
                    })
                    .await;

                match res {
                    Ok(_) => {
                        info!("found intersect with rb buffer: {point:?}");
                        return Ok(Some(point));
                    }
                    Err(e) if e.code() == Code::NotFound => (),
                    Err(e) => return Err(Error::source(e)),
                }
            }

            Err(Error::RollbackOutOfRange(
                "no intersect found with persistent rb buffer (maybe upstream catching up?)".into(),
            ))
        }
        None => {
            info!("no persistent rollback buffer found");

            Ok(None)
        }
    }
}
