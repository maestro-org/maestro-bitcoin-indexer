use args::Options;
use tikv_client::TransactionClient;
use tracing::info;
use tracing_subscriber::fmt;

mod args;
mod entry;
mod invariants;
mod types;
mod worker;

/// TiKV Garbage Collection Program
///
/// We now make use of older MVCC versions of data within TiKV to allow us to query data at
/// different points in time (blocks) instead of just the most recent data. This means we need more
/// control over the TiKV garbage collection of these old MVCC versions of data, else data we need
/// may be removed. This program implements it's own garbage collection logic which ensures we do
/// not garbage collect data which we still need.
///
/// ref: https://tikv.org/docs/dev/reference/architecture/storage/#mvcc
#[tokio::main]
async fn main() -> Result<(), ()> {
    let format = fmt::format()
        .with_level(true)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false);

    fmt().event_format(format).init();

    let options = Options::parse();

    let new_safepoint = worker::get_new_safepoint(options.clone());

    info!("sending tikv desired new safepoint...");

    // connect to tikv
    let txn_client = TransactionClient::new(vec![options.tikv_address])
        .await
        .expect("could not connect to tikv");

    // then send request to use new safepoint to tikv pd
    let successful = txn_client
        .legacy_gc(new_safepoint, options.cleanup_locks)
        .await
        .expect("tikv error");

    if successful {
        info!("tikv pd safepoint updated successfully");
    } else {
        panic!("tikv pd safepoint was not updated successfully")
    }

    Ok(())
}
