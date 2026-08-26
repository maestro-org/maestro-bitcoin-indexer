# tikv-gc

The **MVCC garbage-collection safepoint manager** — the small but load-bearing piece
that makes time-travel queries safe.

TiKV retains old MVCC versions of every key until a *GC safepoint* advances past
them. This stack deliberately exploits that: [mapi-xbt](../mapi-xbt) opens snapshots
at older commit timestamps to serve state as of past blocks, and to unify reads
across indexer instances that committed at slightly different times. Letting TiKV's
default GC run would delete exactly the versions those reads depend on.

tikv-gc replaces it: a one-shot binary, run on a schedule (a looping service in
the compose stack, a CronJob in production), that computes the newest timestamp that is still safe to garbage
collect up to, and submits it to PD. "Safe" is the minimum over these invariants,
derived from the indexer cursor entries in Redis:

1. **Earliest chain tip** — never GC past the most recent *non-mempool* block of any
   instance, so every instance's chain tip remains queryable.
2. **Network intersection** — keep the versions needed to read a consistent common
   block across all instances of the network.
3. **Safe zone** — never GC anything newer than `SAFE_ZONE_MILLIS` (default 10
   minutes), regardless of what the entries say.

## Running

```
tikv-gc --tikv-address <pd:2379> --redis-address <redis://host:6379>
```

| Flag | Env | Default |
|---|---|---|
| `--tikv-address` | `TIKV_ADDRESS` | `localhost:2379` |
| `--redis-address` | `REDIS_ADDRESS` | `redis://localhost:6379/1` |
| `--safe-zone-millis` | `SAFE_ZONE_MILLIS` | `600000` |

In the compose stack it runs automatically every 10 minutes; `make gc` triggers a
manual one-shot run.

Note: how far back time-travel queries can reach is exactly "how long since the
safepoint last advanced" — run tikv-gc rarely to keep deep history (at the cost of
disk), or frequently to keep TiKV compact.
