use clap::Parser;

#[derive(Debug, Parser, Clone)]
pub struct Options {
    #[clap(
        short = 't',
        long = "tikv-address",
        default_value = "localhost:2379",
        env = "TIKV_ADDRESS"
    )]
    pub tikv_address: String,

    #[clap(
        short = 'r',
        long = "redis-address",
        default_value = "redis://localhost:6379/1",
        env = "REDIS_ADDRESS"
    )]
    pub redis_address: String,

    #[clap(
        short = 's',
        long = "safe-zone-millis",
        default_value = "600000", // 10 minutes
        env = "SAFE_ZONE_MILLIS"
    )]
    /// We will not set a new safepoint within SAFE_ZONE_INTERVAL_MILLIS of the current time
    pub safe_zone_millis: i64,

    #[clap(
        short = 'l',
        long = "cleanup-locks",
        default_value = "false",
        env = "CLEANUP_LOCKS"
    )]
    pub cleanup_locks: bool,
}

impl Options {
    pub fn parse() -> Self {
        <Self as clap::Parser>::parse()
    }
}
