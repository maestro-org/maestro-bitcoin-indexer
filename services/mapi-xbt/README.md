# mapi-xbt

The **HTTP API layer**: a stateless axum server that reads pre-indexed data out of
TiKV and serves it as a documented REST API. It performs no indexing of its own —
all data is written by [polyphony-xbt](../polyphony-xbt) instances, and mapi-xbt's
job is to (1) discover which instances to read from, (2) pick a *consistent*
snapshot across them, and (3) decode timbre-xbt keys/values into API responses.

Three OpenAPI surfaces are served from the same binary (Swagger UI at
`/swagger-ui`): the blockchain indexer API (blocks, transactions, addresses, runes,
inscriptions, BRC-20), the mempool monitoring API (fee rates from estimated next
blocks), and the wallet API (portfolio-style address views).

## How reads work

For each request, mapi-xbt:

1. **Resolves instances** — looks up the per-reducer instance registry in Redis
   (`{bitcoin:<network>:<reducer>}:scores` sorted sets) to find a healthy
   (dataplane, instance) pair for every reducer the endpoint needs.
2. **Unifies a snapshot** — fetches those instances' cursor entries
   (`tikv-timestamps:*`), finds the most recent block (or mempool view) that *all*
   required instances have committed, and opens TiKV snapshots at exactly that
   commit timestamp. Responses are therefore always internally consistent, even
   across reducers written by different instances.
3. **Time-travel** — because TiKV MVCC versions are retained (GC is controlled by
   [tikv-gc](../tikv-gc)), historical endpoints can open snapshots at older
   timestamps and serve state *as of a past block*.

## Running

```
mapi-xbt --mode bitcoin-testnet --tikv-address 127.0.0.1:2379 --redis redis://127.0.0.1:6379
```

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--mode` | `MODE` | required | `bitcoin`, `bitcoin-testnet`, or `generate-open-api{,-mempool,-wallet}` (print the OpenAPI JSON and exit) |
| `--listen-address` | `LISTEN_ADDRESS` | `0.0.0.0:3000` | HTTP listener |
| `--tikv-address` | `TIKV_PD_CLIENT` | `127.0.0.1:2379` | TiKV PD endpoint |
| `--redis` | `REDIS` | `redis://localhost:6379` | Redis (cluster mode) |
| `--arranger-base-url` | `ARRANGER_BASE_URL` | unset | Optional external price service for USD enrichment; when unset, USD fields are null |
| `--max_redis_pool_size` / `--max_tikv_pool_size` / `--min_tikv_pool_size` | same, upper-cased | 30 / 30 / – | Connection pools |

## Security note

mapi-xbt performs **no authentication or rate limiting in-process** — deploy it
behind a gateway if you expose it publicly.
