use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// TiKV PD Address
    pub pd: String,
    /// Redis TiKV Timestamps DB (including database index, for example `redis://.../1`)
    pub redis: String,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[clap(subcommand)]
    Delete(DeleteArgs),
    #[clap(subcommand)]
    Write(WriteArgs),
}

#[derive(Debug, Subcommand, Clone)]
pub enum DeleteArgs {
    /// Delete keys associated with specific dataplane (all instances)
    Dataplane(DataplaneArgs),
    /// Delete keys associated with specific instance in a dataplane
    Instance(InstanceArgs),
    /// Delete keys within a range of keys (end is open)
    Range(RangeArgs),
    /// Delete a list of keys (file of hex-encoded keys)
    List(ListArgs),
}

#[derive(Debug, Subcommand)]
pub enum WriteArgs {
    /// Write a list of keys (file of colon seperated hex-encoded keys and values "hex(key):hex(value)")
    List(ListArgs),
}

#[derive(Debug, Args, Clone)]
pub struct DataplaneArgs {
    #[clap(value_name = "DATAPLANE ID")]
    pub dataplane: u8,
    /// Removes the keys from storage instead of just providing a summary of keys to be deleted
    #[arg(long, default_value_t = false)]
    pub force: bool,
    /// Number of milliseconds to wait between each database transaction (throttle)
    #[arg(long, default_value_t = 500)]
    pub pause: u64,
    /// Number of keys to scan and delete per database transaction
    #[arg(long, default_value_t = 10000)]
    pub scan_size: u32,
    /// Uses TiKV unsafe destroy range to immediately wipe the key range (https://github.com/tikv/rfcs/blob/master/text/0002-unsafe-destroy-range.md)
    #[arg(long, default_value_t = false)]
    pub destroy_mode: bool,
}

#[derive(Debug, Args, Clone)]
pub struct InstanceArgs {
    #[clap(value_name = "DATAPLANE ID")]
    pub dataplane: u8,
    #[clap(value_name = "INSTANCE ID")]
    pub instance: u16,
    /// Removes the keys from storage instead of just providing a summary of keys to be deleted
    #[arg(long, default_value_t = false)]
    pub force: bool,
    /// Number of milliseconds to wait between each database transaction (throttle)
    #[arg(long, default_value_t = 500)]
    pub pause: u64,
    /// Number of keys to scan and delete per database transaction
    #[arg(long, default_value_t = 10000)]
    pub scan_size: u32,
    /// Only delete timestamp entries from the timestamp redis DB, do not touch TiKV data
    #[arg(long, default_value_t = false)]
    pub ts_redis_only: bool,
    /// Uses TiKV unsafe destroy range to immediately wipe the key range (https://github.com/tikv/rfcs/blob/master/text/0002-unsafe-destroy-range.md)
    #[arg(long, default_value_t = false)]
    pub destroy_mode: bool,
}

#[derive(Debug, Args, Clone)]
pub struct RangeArgs {
    #[clap(value_name = "HEX(START KEY)")]
    pub start_key: String,
    #[clap(value_name = "HEX(END KEY)")]
    pub end_key: String,
    /// Removes the keys from storage instead of just providing a summary of keys to be deleted
    #[arg(long, default_value_t = false)]
    pub force: bool,
    /// Number of milliseconds to wait between each database transaction (throttle)
    #[arg(long, default_value_t = 500)]
    pub pause: u64,
    /// Number of keys to scan and delete per database transaction
    #[arg(long, default_value_t = 10000)]
    pub scan_size: u32,
    /// Uses TiKV unsafe destroy range to immediately wipe the key range (https://github.com/tikv/rfcs/blob/master/text/0002-unsafe-destroy-range.md)
    #[arg(long, default_value_t = false)]
    pub destroy_mode: bool,
}

#[derive(Debug, Args, Clone)]
pub struct ListArgs {
    #[clap(value_name = "FILE")]
    pub file_name: PathBuf,
    /// Performs the requested changes instead of just providing a summary of the changes to be made
    #[arg(long, default_value_t = false)]
    pub force: bool,
}
