use bitcoin::{Block, BlockHash};

pub type BlockHeight = u64;

pub type Point = (BlockHeight, BlockHash);
pub type Tip = Point;

#[derive(Debug, Clone)]
pub enum PullEvent {
    RollForward(BlockHeight, BlockHash, Block, Tip),
    RollBack(BlockHeight, BlockHash, Tip),
}

#[derive(Debug, Clone)]
pub enum HealthEvent {
    RollForward(BlockHeight, BlockHash),
    RollBack(BlockHeight, BlockHash),
}
