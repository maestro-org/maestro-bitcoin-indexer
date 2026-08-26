pub mod adapter;
pub mod key_resolver;
pub mod scanner;
mod unify;

pub mod redis_entry;

pub use scanner::Scanner;

// when scanning many keys, scan in batches of this size
pub static KV_SCAN_BATCH_SIZE: u32 = 1000;
