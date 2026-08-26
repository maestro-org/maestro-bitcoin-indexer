# daw

A small admin CLI for inspecting and deleting indexer key ranges directly in
TiKV, with helpers that understand the [timbre-xbt](../../crates/timbre-xbt) key
layout (dataplane/instance namespacing).

It is an operator tool, not part of the running pipeline — use it to retire a
decommissioned instance's keyspace, clean up a dataplane, or surgically remove a
key range. Every command is a **dry run by default**, printing a summary of what
*would* be deleted; pass `--force` to actually apply it.

## Usage

```
daw <PD_ADDRESS> <REDIS_URL> <COMMAND>
```

`REDIS_URL` includes the timestamp DB index, e.g. `redis://127.0.0.1:6379/1`.

Delete commands:

| Command | Deletes |
|---|---|
| `delete dataplane <DATAPLANE_ID>` | all keys for a dataplane (every instance) |
| `delete instance <DATAPLANE_ID> <INSTANCE_ID>` | all keys for one instance |
| `delete range <HEX_START> <HEX_END>` | an explicit key range (end-exclusive) |
| `delete list <FILE>` | a file of hex-encoded keys, one per line |
| `write list <FILE>` | write `hex(key):hex(value)` pairs from a file |

Shared options: `--force` (apply instead of summarise), `--pause <ms>` (throttle
between transactions, default 500), `--scan-size <n>` (keys per transaction,
default 10000). `delete instance` also accepts `--ts-redis-only` (only clear the
Redis timestamp entries, leave TiKV untouched); the range-based deletes accept
`--destroy-mode` (use TiKV `unsafe_destroy_range` for an immediate wipe).

## Examples

Summarise the keys that would be deleted for instance 1 in dataplane 0:

```
RUST_LOG=info daw 127.0.0.1:2379 redis://127.0.0.1:6379/1 delete instance 0 1
```

Actually delete them:

```
RUST_LOG=info daw 127.0.0.1:2379 redis://127.0.0.1:6379/1 delete instance 0 1 --force
```

This is the tooling behind the "retire the old instance" step of the
[zero-downtime upgrade flow](../../docs/operations.md#upgrading-a-reducer-zero-downtime).
