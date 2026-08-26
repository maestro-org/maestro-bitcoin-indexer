// use once_cell::sync::Lazy;
use rocksdb::{statistics::StatsLevel, Cache, WriteBufferManager};
use serde::Deserialize;
use tracing::info;

#[derive(Deserialize, Debug, Clone)]
pub struct ChainDBConfig {
    pub path: std::path::PathBuf,
    pub immutable_after_confs: Option<u64>,
    db_write_buffer_size: Option<usize>,
    max_total_wal_size_init: Option<u64>,
    max_total_wal_size: Option<u64>,
    write_buffer_manager_limit_init: Option<usize>,
    write_buffer_manager_limit: Option<usize>,
    block_cache_capacity: Option<usize>,
    /// 0 means disabled (default)
    bloom_filter_bits: Option<usize>,
}

/*
// Define the global static cache:
static GLOBAL_CACHE: Lazy<Cache> = Lazy::new(|| {
    let memory_mb: usize = std::env::var("K8S_AVAILABLE_MEMORY_MB")
        .unwrap_or_else(|_| "384".to_string()) // default: 384MB
        .parse()
        .unwrap_or(384); // default: 384MB in case of invalid number.

    // Create block cache for caching uncompressed data.
    // This should be about 1/3 of your total memory budget.
    // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#block-cache-size
    Cache::new_lru_cache(512 << 20) // 512 MB
});

// Define the global write buffer manager:
static GLOBAL_WRITE_BUFFER_MANAGER: Lazy<WriteBufferManager> = Lazy::new(|| {
    let memory_mb: usize = std::env::var("K8S_AVAILABLE_MEMORY_MB")
        .unwrap_or_else(|_| "384".to_string()) // default: 384MB
        .parse()
        .unwrap_or(384); // default: 384MB in case of invalid number.

    info!("available memory = {memory_mb}");

    rocksdb::WriteBufferManager::new_write_buffer_manager((memory_mb / 3) << 20, false)
});
*/

/// Checks if statistics are enabled based on the DB_STATS_ENABLED environment variable.
pub fn stats_enabled() -> bool {
    std::env::var("DB_STATS_ENABLED")
        .map(|v| v.to_lowercase())
        .unwrap_or_else(|_| "false".to_string()) // default : disabled
        == "true"
}

/// Gets the statistics level based on the DB_STATS_LEVEL environment variable.
fn get_stats_level() -> StatsLevel {
    match std::env::var("DB_STATS_LEVEL")
        .map(|v| v.to_lowercase())
        .as_deref()
    {
        Ok("disableall") => StatsLevel::DisableAll,
        Ok("excepthistogramortimers") => StatsLevel::ExceptHistogramOrTimers,
        Ok("excepttimers") => StatsLevel::ExceptTimers,
        Ok("exceptdetailedtimers") => StatsLevel::ExceptDetailedTimers,
        Ok("excepttimeformutex") => StatsLevel::ExceptTimeForMutex,
        _ => StatsLevel::All, // Default: collect all (if enabled)
    }
}

pub fn db_options(load: bool, config: ChainDBConfig) -> rocksdb::Options {
    let cpu_cores: i32 = std::env::var("K8S_CPU_CORES")
        .unwrap_or_else(|_| "2".to_string()) // default: 2.
        .parse()
        .unwrap_or(2); // default: 2 in case of invalid number.

    info!("cpu cores = {cpu_cores}");

    let mut db_options = rocksdb::Options::default();

    // Create missing DBs and CFs:
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);

    // As suggested in the "Basic-Tuning" section of the documentation:
    // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options
    db_options.set_level_compaction_dynamic_level_bytes(true); // (default: false)
    db_options.set_max_background_jobs(std::cmp::max(cpu_cores, 2)); // default: 2 (still use two for single core)
    db_options.increase_parallelism(std::cmp::max(cpu_cores, 2)); // default: 2 (still use two for single core)
    db_options.set_bytes_per_sync(1 << 20); // Enable and set to: 1MB

    // cap max number of write buffers per CF that can build up in memory
    db_options.set_max_write_buffer_number(2);

    // cap maximum size of write buffers across *all* CFs in the database
    // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#write-buffer-size
    db_options.set_db_write_buffer_size(config.db_write_buffer_size.unwrap_or(1 << 30)); // 1GB

    if stats_enabled() {
        db_options.enable_statistics();
        db_options.set_statistics_level(get_stats_level());
    }

    // Create and configure BlockBasedOptions
    let mut block_based_options = rocksdb::BlockBasedOptions::default();

    block_based_options.set_block_size(4 * 1024); // default: 4096

    if load {
        // during initial sync we don't need a cache stuff as we will not be doing reads, besides
        // resolving utxos
        block_based_options.disable_cache();

        // high write volume should have larger WAL
        db_options.set_max_total_wal_size(config.max_total_wal_size_init.unwrap_or(2 << 30)); // db-wide, 2GB

        db_options.set_use_direct_io_for_flush_and_compaction(true);

        // dont allow write stall
        // https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager
        db_options.set_write_buffer_manager(&WriteBufferManager::new_write_buffer_manager(
            config.write_buffer_manager_limit_init.unwrap_or(1 << 30),
            false,
        )); // 1 GB
    } else {
        // we need a cache and we will share a single one for all CFs
        block_based_options.set_block_cache(&Cache::new_lru_cache(
            config.block_cache_capacity.unwrap_or(256 << 20),
        )); // 256MB, db-wide

        // slow write volume -> smaller WAL
        db_options.set_max_total_wal_size(config.max_total_wal_size.unwrap_or(128 << 20)); // 128MB, db-wide

        block_based_options.set_cache_index_and_filter_blocks(true); // default: false. Enable -> faster reads + faster writes.

        // store filter and index blocks inside block cache instead of separate memory
        block_based_options.set_pin_l0_filter_and_index_blocks_in_cache(true); // improves read performance (default: false)

        // https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager
        db_options.set_write_buffer_manager(&WriteBufferManager::new_write_buffer_manager(
            config.write_buffer_manager_limit.unwrap_or(512 << 20),
            false,
        ));
        // 512MB
    }

    let bloom_filter_bits = config.bloom_filter_bits.unwrap_or(0);

    if bloom_filter_bits > 0 {
        // Bloom Filter -> Speeds up point lookup operations (i.e. Get())
        // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#bloom-filters
        block_based_options.set_bloom_filter(bloom_filter_bits as f64, false);
    }

    // Recommended for memory saving
    // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#reducing-internal-fragmentation
    block_based_options.set_optimize_filters_for_memory(true);

    // Default format version usually lags behind the recommended version for compatibility reasons.
    block_based_options.set_format_version(6); // Default: 5

    // set_block_based_table_factory sets the block_based_options:
    db_options.set_block_based_table_factory(&block_based_options);

    let db_log_level = std::env::var("DB_LOG_LEVEL").unwrap_or_else(|_| "WARN".to_string());

    match db_log_level.to_uppercase().as_str() {
        "DEBUG" => db_options.set_log_level(rocksdb::LogLevel::Debug),
        "INFO" => db_options.set_log_level(rocksdb::LogLevel::Info),
        "WARN" => db_options.set_log_level(rocksdb::LogLevel::Warn),
        "ERROR" => db_options.set_log_level(rocksdb::LogLevel::Error),
        "FATAL" => db_options.set_log_level(rocksdb::LogLevel::Fatal),
        "HEADER" => db_options.set_log_level(rocksdb::LogLevel::Header),
        _ => db_options.set_log_level(rocksdb::LogLevel::Warn),
    }

    return db_options;
}
