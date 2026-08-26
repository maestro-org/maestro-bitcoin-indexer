use crate::args::{DataplaneArgs, DeleteArgs, InstanceArgs, ListArgs, RangeArgs, WriteArgs};
use redis::Commands;
use std::fs::read_to_string;
use tikv_client::{Key, TransactionClient};
use timbre_xbt::Namespace;
use tokio::time::Duration;
use tracing::{info, warn};

use crate::DeleteArgs::{Dataplane, Instance, List, Range};

pub async fn handle_delete_cmd(
    pd: String,
    redis: String,
    cmd: DeleteArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let (range, force, pause, mut scan_size, destroy) = match cmd.clone() {
        Dataplane(DataplaneArgs {
            dataplane,
            force,
            pause,
            scan_size,
            destroy_mode,
        }) => {
            let namespace = Namespace::new(dataplane, 0);

            (
                namespace.dataplane_key_range(),
                force,
                pause,
                scan_size,
                destroy_mode,
            )
        }
        Instance(InstanceArgs {
            dataplane,
            instance,
            force,
            pause,
            scan_size,
            destroy_mode,
            ..
        }) => {
            let namespace = Namespace::new(dataplane, instance);

            (
                namespace.instance_key_range(),
                force,
                pause,
                scan_size,
                destroy_mode,
            )
        }
        Range(RangeArgs {
            start_key,
            end_key,
            force,
            pause,
            scan_size,
            destroy_mode,
        }) => (
            hex::decode(start_key)?..hex::decode(end_key)?,
            force,
            pause,
            scan_size,
            destroy_mode,
        ),
        List(args) => return handle_delete_list_cmd(pd, args).await,
    };

    // --- delete from timestamps redis

    let mut redis = redis::cluster::ClusterClient::new(vec![redis])
        .expect("could not connect to redis 1")
        .get_connection()
        .expect("could not connect to redis 2");

    // get all keys, one for each dataplane + instance pair
    let keys: Vec<String> = redis
        .zrangebyscore("tikv-timestamps-keys", "-inf", "+inf")
        .unwrap();

    match cmd {
        Dataplane(DataplaneArgs {
            dataplane, force, ..
        }) => {
            for key in keys {
                let mut key_parts = key.split(':');

                let _label = key_parts.next().unwrap();
                let key_dataplane: u8 = key_parts.next().unwrap().parse().unwrap();
                let key_instance: u16 = key_parts.next().unwrap().parse().unwrap();

                if key_dataplane == dataplane {
                    if force {
                        info!(
                            "deleting redis tikv timestamp db keys for ({}:{})",
                            key_dataplane, key_instance
                        );

                        let _: u32 = redis.del(&key).unwrap();
                        let _: u32 = redis.zrem("tikv-timestamps-keys", &key).unwrap();
                    } else {
                        info!(
                            "would delete redis tikv timestamp db key for ({}:{})",
                            key_dataplane, key_instance
                        )
                    }
                }
            }
        }
        Instance(InstanceArgs {
            dataplane,
            instance,
            force,
            ts_redis_only,
            ..
        }) => {
            for key in keys {
                let mut key_parts = key.split(':');

                let _label = key_parts.next().unwrap();
                let key_dataplane: u8 = key_parts.next().unwrap().parse().unwrap();
                let key_instance: u16 = key_parts.next().unwrap().parse().unwrap();

                if key_dataplane == dataplane && key_instance == instance {
                    if force {
                        info!(
                            "deleting redis tikv timestamp db keys for ({}:{})",
                            key_dataplane, key_instance
                        );

                        let _: u32 = redis.del(&key).unwrap();
                        let _: u32 = redis.zrem("tikv-timestamps-keys", &key).unwrap();
                    } else {
                        info!(
                            "would delete redis tikv timestamp db key for ({}:{})",
                            key_dataplane, key_instance
                        )
                    }
                }
            }

            if ts_redis_only {
                info!("ts redis only flag true, not deleting tikv data");
                return Ok(());
            }
        }
        _ => (),
    };

    // ---

    let txn_client = TransactionClient::new(vec![pd]).await.unwrap();

    if destroy {
        txn_client.unsafe_destroy_range(range).await?;
    } else {
        let mut range_remaining = range;
        let mut total_removed = 0;
        let mut first_seen = None;
        let mut last_seen = None;

        loop {
            let mut txn = txn_client.begin_optimistic().await?;

            let keys = match txn
                .scan_keys(range_remaining.clone(), scan_size)
                .await
                .map(|iter| iter.collect::<Vec<Key>>())
            {
                Ok(k) => k,
                Err(tikv_client::Error::Grpc(e))
                    if e.to_string().contains("Received message larger than max") =>
                {
                    warn!("scan size {scan_size} too large for range {range_remaining:?}");

                    scan_size /= 2;
                    continue;
                }
                Err(e) => {
                    warn!("{e:?}");
                    continue;
                }
            };

            let last_key = if !keys.is_empty() {
                if force {
                    info!("deleting {} keys", keys.len());
                }

                let fk = Into::<Vec<u8>>::into(keys.first().unwrap().clone());

                info!("first key: {}", hex::encode(&fk));

                if first_seen.is_none() {
                    first_seen = Some(fk)
                }

                let mut lk = Into::<Vec<u8>>::into(keys.last().unwrap().clone());
                info!("last key: {}", hex::encode(&lk));

                last_seen = Some(lk.clone());

                // get the next key after our last fetched
                lk.push(0);

                lk
            } else {
                if !force {
                    info!(
                        "{} keys will be deleted if this function is called with '--force'",
                        total_removed
                    );
                    if let Some(k) = first_seen {
                        info!("first key: {}", hex::encode(k))
                    }
                    if let Some(k) = last_seen {
                        info!("last key: {}", hex::encode(k))
                    }
                } else {
                    info!("deletion complete ({} keys removed)", total_removed);
                }

                txn.commit().await?;
                break;
            };

            for key in keys {
                // don't delete the key if we are just checking
                if force {
                    txn.delete(key).await?;
                }
                total_removed += 1;
            }

            txn.commit().await?;

            range_remaining = last_key..range_remaining.end;

            tokio::time::sleep(Duration::from_millis(pause)).await;
        }
    }

    if let Instance(InstanceArgs {
        dataplane,
        instance,
        force,
        ..
    }) = cmd
    {
        // Delete the info entry:
        info!("Delete instance info entry...");

        // <INFO_TAG><BREAK><NAMESPACE>
        let key = [vec![b'I', b'`', dataplane], instance.to_be_bytes().to_vec()].concat();

        let mut txn = txn_client.begin_optimistic().await?;
        // don't delete the key if we are just checking
        if force {
            txn.delete(key).await?;
        }
        txn.commit().await?;
    };

    Ok(())
}

pub async fn handle_delete_list_cmd(
    pd: String,
    args: ListArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let lines = read_to_string(args.file_name)?
        .lines()
        .map(|x| hex::decode(x).unwrap())
        .collect::<Vec<Vec<u8>>>();

    let txn_client = TransactionClient::new(vec![pd]).await?;

    let mut keys_num = 0;
    let mut deleted = 0;
    let mut missing = vec![];

    let mut txn = txn_client.begin_optimistic().await?;

    for line in lines {
        if line.is_empty() {
            continue;
        }

        keys_num += 1;

        if txn.get(line.clone()).await?.is_some() {
            if args.force {
                txn.delete(line).await?;
                deleted += 1;
            }
        } else {
            missing.push(hex::encode(line))
        }
    }

    txn.commit().await?;

    if args.force {
        info!("deleted {deleted} of {keys_num} keys");
    }

    if !missing.is_empty() {
        warn!("the following keys were not found in the storage: {missing:?}")
    } else {
        info!("found all provided keys in storage")
    }

    Ok(())
}

pub async fn handle_write_cmd(
    pd: String,
    cmd: WriteArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        WriteArgs::List(args) => handle_write_list_cmd(pd, args).await,
    }
}

pub async fn handle_write_list_cmd(
    pd: String,
    args: ListArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let lines = read_to_string(args.file_name)?
        .lines()
        .map(|x| {
            x.split(":")
                .collect::<Vec<&str>>()
                .into_iter()
                .map(|y| hex::decode(y).unwrap())
                .collect()
        })
        .collect::<Vec<Vec<Vec<u8>>>>();

    let txn_client = TransactionClient::new(vec![pd]).await?;

    let mut txn = txn_client.begin_optimistic().await?;

    for line in lines {
        if line.is_empty() {
            continue;
        }

        let key = line[0].clone();
        let value = line[1].clone();

        if let Some(found_value) = txn.get(key.clone()).await? {
            info!(
                "overwrite KV: {} -> ({} -> {})",
                hex::encode(&key),
                hex::encode(found_value),
                hex::encode(&value)
            );
        } else {
            info!(
                "write new KV: {} -> {}",
                hex::encode(&key),
                hex::encode(&value)
            );
        }

        if args.force {
            txn.put(key, value).await?;
        }
    }

    txn.commit().await?;

    if args.force {
        info!("performed writes");
    } else {
        info!("writes not performed, run with --force option to perform writes")
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    Ok(())
}
