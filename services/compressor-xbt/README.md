# compressor-xbt

A Bitcoin **light node and enrichment engine**: the first stage of the indexing
pipeline. It connects directly to a bitcoind node — over the P2P protocol for block
download and the JSON-RPC interface for chain metadata — stores blocks in a local
RocksDB database (the *rolldb*), and precomputes everything downstream indexers need
but can't get from raw blocks alone:

- **Resolved inputs**: every transaction input is resolved to the full previous
  output it spends, so consumers never need their own UTXO index.
- **Ordinals**: sat ranges tracked across the UTXO set.
- **Runes**: etchings, mints, and per-output rune balances.
- **Inscriptions & BRC-20**: inscription envelope parsing and BRC-20 state.

The result — blocks bundled with their *enrichment context* — is served over gRPC
(see [`proto/sync/v1/sync.proto`](../../proto/sync/v1/sync.proto)) to any number of
downstream indexer instances:

| RPC | Purpose |
|---|---|
| `PageBlocksWithContext` | Bulk paged download of historical blocks (initial sync) |
| `StreamUpdatesWithContext` | Apply/undo/reset stream at the chain tip (rollback-aware) |
| `MempoolBlocksWithContext` | *Estimated next blocks* built from the mempool, with the same enrichment applied to unconfirmed transactions |

Because inputs are resolved and metaprotocol state is precomputed **once**, adding
another indexer instance costs no extra bitcoind or UTXO-resolution work — this is
what makes running many parallel indexer instances cheap.

## Running

```
compressor-xbt <config.toml> <subcommand>
```

| Subcommand | Meaning |
|---|---|
| `sync` | Sync the rolldb from bitcoind only |
| `serve` | Serve gRPC from an existing rolldb only |
| `daemon` | `sync` + `serve` |
| `mempool` | `sync` + `serve` + mempool block templates (recommended) |
| `rollback <height>` | Force-roll the database back to a height (recovery tool) |

## Configuration

Layered: `compressor-xbt.toml` in the working directory, then the explicit config
file argument, then environment variables prefixed `COMPR` (e.g.
`COMPR_SYNC_NODE_RPC_USER`). See [`configs/compressor/`](../../configs/compressor)
for complete examples.

| Key | Meaning |
|---|---|
| `chain_db.path` | RocksDB rolldb directory |
| `chain_db.immutable_after_confs` | Blocks deeper than this are stored immutably (rollback log kept above it) |
| `sync.node_address` | bitcoind P2P address (`host:8333`) |
| `sync.node_rpc` / `node_rpc_user` / `node_rpc_pass` | bitcoind JSON-RPC endpoint and credentials |
| `sync.network` | `bitcoin` or `bitcoin_testnet` (testnet4) |
| `sync.health_endpoint` | HTTP health listener (`/health` reports sync progress) |
| `sync.first_rune_height` / `first_inscription_height` / `jubilee_height` | Metaprotocol activation heights (0 on testnet4; 840000 / 767430 / 824544 on mainnet) |
| `sync.utxos_in_memory` | Keep the TXO resolver set in memory (faster; mainnet needs ~raw UTXO set worth of RAM) |
| `sync.mgm_address` | Optional gRPC address of a global mempool service; omit it — the built-in bitcoind `getblocktemplate` fallback is used instead |
| `serve.listen_address` | gRPC listener (default port 50051) |

## Ports

- `50051` — gRPC sync service
- `50052` — HTTP health (`curl localhost:50052/health`)

## Note on the build

compressor-xbt intentionally builds **outside** the repo's cargo workspace (own
`Cargo.lock`): its `ord` dependency pins a `bitcoin` crate version incompatible with
the TiKV-side services. It only shares the gRPC proto with them, so the split is
harmless — `make build` covers both.
