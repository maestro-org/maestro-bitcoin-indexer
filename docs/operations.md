# Operations

The docker compose file is a faithful miniature of the production topology this
stack ran in. Scaling it up is mostly a matter of multiplying instances, not
changing code.

## From compose to production

| Compose | Production shape |
|---|---|
| One or two polyphony instances running many reducers each | One deployment **per reducer** (or small groups), each with its own `instance_id` — independent backfills, restarts, and upgrades per reducer |
| pd + tikv single nodes | A real TiKV cluster (3+ PD, 3+ TiKV) — e.g. via tidb-operator on Kubernetes |
| redis cluster-of-one | A real Redis Cluster |
| tikv-gc service looping every 10 min | A CronJob (e.g. every 10 minutes) |
| one compressor | Still one compressor (plus a warm standby if desired) — it is the only consumer of bitcoind and serves any number of indexers |
| mapi single replica | Stateless — scale horizontally behind a gateway (which is also where auth/rate limits belong) |

## Identity planning

- `dataplane_id` (u8): one per network+environment (e.g. mainnet-prod = 0).
  mapi assumes instances within a dataplane are mutually consistent.
- `instance_id` (u16): unique per polyphony instance within a dataplane. Never
  reuse a live instance's id — keys are namespaced by it.

## Upgrading a reducer (zero downtime)

The rationale and mechanism are described in
[design.md §1](design.md#1-fixing-a-broken-indexer-with-zero-downtime); the runbook:

1. Deploy a new polyphony instance with the patched reducer and a fresh
   `instance_id`, `intersect = Origin` (or a snapshot point).
2. Watch it backfill (its cursor entries advance in Redis; it is *not* yet in the
   instance registry, so the API ignores it).
3. At the tip it registers itself; mapi's per-request instance resolution starts
   selecting it.
4. Retire the old instance: stop it, `ZREM` its 3-byte member from the
   `{bitcoin:<net>:<reducer>}:scores` / `:mempool-view` sets, and delete its
   keyspace (its `<dataplane><instance>` key prefix) whenever convenient.
   Note the member is *binary* (it usually contains NUL bytes), so shell command
   substitution will mangle it — issue the removal with a binary-safe client,
   e.g. `EVAL "return redis.call('ZREM', KEYS[1], string.char(0,0,1))" 1 <key>`
   for instance `(0, 1)`.

The keyspace deletion in step 4 is what the [`daw`](../services/daw) admin CLI is
for — `daw <pd> <redis> delete instance <dataplane> <instance>` summarises the
keys, and `--force` deletes them (it also clears the instance's Redis timestamp
entries). It understands the timbre-xbt key layout, so it deletes exactly the
namespaced range and nothing else.

Step 4's registry cleanup is manual by design — the registry has no TTL. A dead
instance left registered can be selected by the API and serve stale data.

## Single-node TiKV tips (compose deployments)

- The compose file caps TiKV's block cache (`configs/tikv/tikv.toml`) — by default
  TiKV takes ~45% of visible memory.
- PD's `evict-slow-store` scheduler is counterproductive with one store: if disk IO
  stalls under backfill load it "evicts leaders" with nowhere to send them, causing
  brief unavailability (the indexer's TiKV client treats this as fatal, and the
  container restart + split-commit recovery handles it). Disable the scheduler once
  per cluster:

  ```bash
  docker compose exec pd ./pd-ctl -u http://localhost:2379 scheduler remove evict-slow-store-scheduler
  ```

## Monitoring

- **compressor**: `GET :50052/health` — upstream (bitcoind) height vs rolldb
  height; alert when the gap grows.
- **polyphony**: no listener; watch logs and the freshness of its
  `tikv-timestamps:<dp>:<id>` entries in Redis.
- **mapi**: `GET :3000/healthcheck`; 5xx rates per endpoint.
- **tikv-gc**: alert when the run fails or when the safepoint
  stops advancing (time-travel window grows unboundedly = disk grows unboundedly).
- **TiKV/PD**: standard TiKV monitoring (region count, store size, GC).

## Sizing notes

- **compressor rolldb**: dominated by the TXO resolver + metaprotocol state;
  testnet4 is a few GB, mainnet is hundreds of GB. `utxos_in_memory = true` trades
  RAM for sync speed — on mainnet leave it off unless you have the memory.
- **TiKV**: grows with enabled reducers × MVCC retention. The GC cadence is your
  main disk lever.
- **bitcoind**: standard full node requirements; no txindex needed (compressor
  resolves inputs itself).

## Backups / recovery

- The rolldb and TiKV are both rebuildable from the chain — backups are a
  time-saver, not a correctness requirement. Snapshot volumes if resync time
  matters to you.
- compressor has a `rollback <height>` subcommand for manual recovery after
  corruption or a bad deploy.
- A polyphony instance that got corrupted is simply replaced: new `instance_id`,
  backfill, swap over — same flow as an upgrade.
