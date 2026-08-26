//! Protocol constants for Bitcoin and Runes.

/// UNCOMMON•GOODS rune - hardcoded genesis rune in the ord protocol.
/// This rune has no etching transaction; it's built into the protocol.
/// Rune ID: 1:0 (block 1, tx index 0)
/// Value from ordinals::Rune::UNCOMMON_GOODS.0
pub const UNCOMMON_GOODS_RUNE: u128 = 2055900680524219742;

/// Genesis rune ID (block, tx_index)
pub const GENESIS_RUNE_ID: (u64, u32) = (1, 0);

/// UNCOMMON•GOODS spacers value (spacer at position 7)
pub const UNCOMMON_GOODS_SPACERS: u32 = 128;

/// UNCOMMON•GOODS symbol (⧉)
pub const UNCOMMON_GOODS_SYMBOL: char = '⧉';

/// Bitcoin halving interval in blocks
pub const SUBSIDY_HALVING_INTERVAL: u64 = 210_000;

/// UNCOMMON•GOODS mint start height (4th halving)
pub const UNCOMMON_GOODS_START_HEIGHT: u64 = SUBSIDY_HALVING_INTERVAL * 4; // 840,000

/// UNCOMMON•GOODS mint end height (5th halving)
pub const UNCOMMON_GOODS_END_HEIGHT: u64 = SUBSIDY_HALVING_INTERVAL * 5; // 1,050,000
