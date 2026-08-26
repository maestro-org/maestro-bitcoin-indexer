# polyphony-xbt

The **indexer**: consumes enriched blocks from [compressor-xbt](../compressor-xbt)
over gRPC, runs them through a configurable set of ~40 **reducers** (map/reduce-style
state machines, one per query pattern), and writes the resulting key/value mutations
to TiKV — plus cursor and instance-registry entries to Redis so the API layer knows
what data exists and how fresh it is.

Architecturally a descendant of TxPipe's Scrolls, built on the
[gasket](https://github.com/maestro-org/gasket-rs) staged-pipeline framework:

```
source (compressor gRPC / emulator) ──► reducers (fan-out per block) ──► storage (TiKV + Redis)
```

## Key properties

- **Multi-instance by design.** Each instance is identified by
  (`dataplane_id`, `instance_id`) and namespaces every TiKV key it writes with that
  identity (the key layout lives in [timbre-xbt](../../crates/timbre-xbt)). Any
  number of instances — different reducer sets, different code versions — can index
  the same chain into the same TiKV cluster concurrently, all fed by one compressor.
- **Registry-gated swap-over.** An instance advertises itself in the per-reducer
  Redis instance registry **only once it reaches the chain tip** (detected by the
  arrival of mempool blocks). Until then the API layer doesn't know it exists. This
  is the zero-downtime upgrade mechanism: deploy a new instance with patched
  reducers, let it backfill in parallel, and it takes over automatically when caught
  up. Set `storage.advertise_immediately = true` to bypass the gate in dev setups.
- **Rollback-aware.** Inverse actions for recent blocks are buffered (and persisted
  to TiKV), so chain reorgs unwind cleanly; `safe_mode` panics on missing data
  instead of warning.
- **Mempool-aware.** With `source.use_mempool = true`, estimated next blocks from
  compressor are indexed between real blocks and unwound when reality arrives, so
  the API can serve mempool-aware state.

## Running

```
polyphony-xbt daemon --config <config.toml> [--console plain|tui]
```

## Configuration

Layered: `/etc/polyphony-xbt/daemon.toml`, then `polyphony-xbt.toml` in the working
directory, then `--config <file>`, then env vars prefixed `POLYPHONY` with `__`
separator (e.g. `POLYPHONY__STORAGE__INSTANCE_ID=1`). Complete examples in
[`configs/polyphony/`](../../configs/polyphony).

| Section | Keys |
|---|---|
| `[general]` | `stage_timeout_secs`, `stage_message_queue`, `buffer_size` (rollback buffer depth), `safe_mode` |
| `[source]` | `type = "Compressor"` with `url`, `max_items_per_page`, `use_mempool` — or `type = "Emulate"` with synthetic apply/undo `instructions` for testing |
| `[intersect]` | Where to start: `Origin`, `Tip`, or `Point = [height, "hash"]` |
| `[storage]` | `type = "TiKV"`: `connection_params` (PD endpoint), `redis_address` (cluster-mode Redis), `network` (`mainnet`/`testnet`), `dataplane_id`, `instance_id`, `advertise_immediately`, plus commit tuning (`split_commit_*`, `tikv_commit_*`, `tikv_cleanup_locks`, `key_warnings`) |
| `[[reducers]]` | One `type = "..."` entry per reducer to run — see [docs/reducers.md](../../docs/reducers.md) |

polyphony-xbt exposes no network listener of its own; observe it through its logs,
its Redis cursor entries, or the TUI console.
