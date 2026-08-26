use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

pub mod arranger;

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    GenerateOpenApi,
    GenerateOpenApiMempool,
    GenerateOpenApiWallet,
    Bitcoin,
    BitcoinTestnet,
}

#[derive(Debug, Parser, Clone)]
pub struct Options {
    #[clap(
        short = 'l',
        long = "listen-address",
        default_value = "0.0.0.0:3000",
        env = "LISTEN_ADDRESS"
    )]
    pub listen_address: SocketAddr,

    #[clap(short = 'm', long = "mode", env = "MODE")]
    pub mode: Mode,

    #[clap(flatten)]
    /// Optional external price service used to enrich responses with USD values, including base
    /// URL and different resource paths.
    pub arranger: arranger::Arranger,

    #[clap(
        long = "tikv-address",
        default_value = "127.0.0.1:2379",
        env = "TIKV_PD_CLIENT"
    )]
    /// TiKV PD client address
    pub tikv_address: String,

    #[clap(
        long = "redis",
        default_value = "redis://localhost:6379",
        env = "REDIS"
    )]
    /// Address of the redis cluster, for example: 'redis://localhost:6379'
    pub redis: String,

    #[clap(
        long = "max_redis_pool_size",
        default_value = "30",
        env = "MAX_REDIS_POOL_SIZE"
    )]
    /// Maximum size of Redis connection pool
    pub max_redis_pool_size: u32,

    #[clap(
        long = "max_tikv_pool_size",
        default_value = "30",
        env = "MAX_TIKV_POOL_SIZE"
    )]
    /// Maximum size of TiKV connection pool
    pub max_tikv_pool_size: u32,

    #[clap(long = "min_tikv_pool_size", env = "MIN_TIKV_POOL_SIZE")]
    /// Minimum size of TiKV connection pool
    pub min_tikv_pool_size: Option<u32>,

    #[clap(
        long = "info_log_interval",
        default_value = "3600",
        env = "INFO_LOG_INTERVAL"
    )]
    /// Log general information this often (seconds)
    pub info_log_interval: u32,

    #[clap(
        long = "collections_dataplane_id",
        default_value = "2",
        env = "COLLECTIONS_DATAPLANE_ID"
    )]
    /// Dataplane ID for inscription collections metadata
    pub collections_dataplane_id: u8,

    #[clap(
        long = "collections_instance_id",
        default_value = "250",
        env = "COLLECTIONS_INSTANCE_ID"
    )]
    /// Instance ID for inscription collections metadata
    pub collections_instance_id: u16,

    #[clap(
        long = "miners_metadata_dataplane_id",
        default_value = "3",
        env = "MINERS_METADATA_DATAPLANE_ID"
    )]
    /// Dataplane ID for miners metadata
    pub miners_metadata_dataplane_id: u8,

    #[clap(
        long = "miners_metadata_instance_id",
        default_value = "251",
        env = "MINERS_METADATA_INSTANCE_ID"
    )]
    /// Instance ID for miners metadata
    pub miners_metadata_instance_id: u16,
}

impl Options {
    pub fn parse() -> Self {
        <Self as clap::Parser>::parse()
    }
}
