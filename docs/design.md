# Design

This stack indexed Bitcoin in production at [Maestro](https://www.gomaestro.org/).
This document explains how the pieces fit together and, more importantly, *why*
the system is shaped the way it is: the problems that come up operating a
multi-tenant Bitcoin indexing platform, and the mechanisms built to solve them.

## The stack in one pass

```
bitcoind ──P2P/RPC──► compressor ──gRPC──► polyphony (×N) ──► TiKV + Redis ◄── mapi ──HTTP──► clients
                                                                   ▲
                                                                tikv-gc
```

**bitcoind** is a stock Bitcoin Core node. Nothing in the stack patches or
extends it; it is only the source of raw blocks and mempool transactions.

**compressor** follows the node over the P2P protocol (blocks) and RPC (tip
tracking, mempool templates) and maintains its own RocksDB store. It is not a
block cache — its job is *enrichment*. A raw Bitcoin block does not carry the
information an indexer actually needs: inputs are just pointers to outputs of
older transactions, and metaprotocol state (ordinals, runes, inscriptions,
BRC-20) is a function of the entire chain history. Compressor resolves every
input to its full previous output, tracks satoshi ranges, and runs the global
metaprotocol state machines, then serves *enriched blocks* — raw blocks plus all
of that context — over gRPC: as paged history for backfills, and as a
follow-the-tip stream with apply/undo semantics for reorgs. It does the same for
the mempool: it assembles the estimated next blocks and enriches unconfirmed
transactions identically, so downstream nothing distinguishes pending data from
confirmed data except a flag.

Why does compressor exist at all — why not point the indexer at the node?
Because resolving inputs requires a complete TXO index, and computing
metaprotocol state requires sequential processing of the whole chain. If each
indexer did this itself, every instance would carry many gigabytes of resolver
state and hours-to-days of extra sync time — and the stack's operational model
(§1) depends on indexer instances being cheap enough to create and discard
freely. Compressor pays the expensive cost exactly once and amortises it over
any number of consumers. It is also the only component that has to care about
P2P quirks, reorg detection, and mempool assembly; everything downstream sees
one clean, ordered, reorg-aware stream.

**polyphony** is the indexer — a three-stage
[gasket](https://github.com/maestro-org/gasket-rs) pipeline: a *source* (the
compressor gRPC client, or a synthetic emulator for testing), a *reducer* stage
fanning each block out to a configurable set of reducers — functions that fold
each block into key/value updates (UTXOs by script hash, rune balances,
transaction metadata, fee histograms, …) — and a *storage* stage. Reducer output is expressed in a small storage DSL (§4) that the
storage stage merges and batches before committing to TiKV. Each polyphony
process is an *instance* identified by `(dataplane_id, instance_id)`; every key
it writes is namespaced by that identity, and after every commit it publishes a
cursor entry to Redis recording the height, block hash, and TiKV commit
timestamp. Instances are deliberately disposable: many can run against one
compressor, indexing in parallel, with completely independent keyspaces.

**timbre** is not a service but a library — the byte-level definition of every
key and value the stack stores. It is the schema contract: polyphony links it to
write, mapi links it to read, and the two never communicate directly. Data is
the interface, and §9 explains how the key layout itself is what makes a plain
key/value store queryable.

**mapi** is the stateless HTTP API. On each request it decides *which instance's
data to read*: it consults the per-reducer instance registry in Redis, selects
instances, finds the best common block across every reducer the request touches
(via the cursor entries), and opens TiKV snapshots at that block's commit
timestamp. Every response therefore reflects one exact, named chain position —
and says which (`last_updated` / `indexer_info`).

**tikv-gc** closes the loop. Reading "at a timestamp" works because TiKV is an
MVCC store that keeps old versions of values — until garbage collection. tikv-gc
owns the GC safepoint: it computes the newest timestamp that is provably safe to
collect past (§2) and submits it to TiKV's placement driver. Run it frequently
and TiKV stays compact; run it rarely and historical snapshots reach further
back. Without it, TiKV never collects at all.

**Redis** (cluster mode) is the coordination plane for all of the above: cursor
entries, the instance registry, and GC bookkeeping. It holds no indexed data; a
lost Redis is repopulated by the running instances.

### Why TiKV

Four properties drove the choice of store, and the design leans on all of them:

- **MVCC snapshots.** Reading at an arbitrary commit timestamp is a first-class
  operation. The stack never built its own snapshotting; consistent
  cross-reducer reads (§8) and historical queries are TiKV's MVCC plus
  discipline about garbage collection (§2).
- **Horizontal write scaling.** Many indexer instances commit concurrently;
  region-sharded TiKV scales writes by adding nodes rather than by sharding in
  application code.
- **Ordered keys and range scans.** The entire query model is prefix and range
  scans over carefully constructed keys (§9). A sorted KV store matches it
  exactly; nothing needs secondary indexes.
- **Multi-tenancy in one cluster.** Key-prefix namespacing gives every network,
  environment, and instance its own contiguous keyspace in a shared cluster —
  and retiring an instance is a single range deletion.

## Components at a glance

| Component | Kind | Reads | Writes | Why it exists |
|---|---|---|---|---|
| bitcoind | stock node | the network | — | source of truth |
| compressor | service (Rust) | bitcoind P2P/RPC | its RocksDB; serves gRPC | resolve inputs + metaprotocol state **once**, serve enriched blocks to N consumers |
| polyphony | service (Rust), ×N instances | compressor gRPC | TiKV (data), Redis (cursors/registry) | fold blocks into queryable k/v via reducers; disposable per-instance |
| timbre | library (Rust) | — | — | the key/value schema contract between writer and reader |
| mapi | service (Rust) | TiKV, Redis | — | stateless API; per-request instance selection + consistent snapshots |
| tikv-gc | job (Rust) | Redis | TiKV (PD safepoint) | make MVCC retention explicit: collect exactly what no reader can need |

---

# Problems and the mechanisms that solve them

## 1. Fixing a broken indexer with zero downtime

Indexers have bugs. A reducer that mis-handled an edge case has, by the time the
bug is noticed, written wrong values into the store — and the wrong values are
entangled with months of correct ones. The traditional answers are bad: take the
API down and re-index, or serve known-bad data while a fix crawls through.

The stack dissolves the problem with three decisions that work together. First,
indexed data is *derived and disposable* — nothing in TiKV is precious, because
it can always be rebuilt from the chain. Second, an indexer instance's identity
is its `(dataplane_id, instance_id)` pair, and every key it writes is prefixed
with that identity — so two instances of the same reducer coexist in the same
cluster without touching each other. Third, *which instance the API reads* is
not configuration but data: a Redis sorted set per reducer
(`{bitcoin:<network>:<reducer>}:scores`), whose members are 3-byte instance
identities and whose scores are the height each instance has indexed to.

Fixing a bug then becomes: patch the reducer, start a new instance with a fresh
`instance_id`, and let it re-index the chain in parallel with the live one. The
new instance is invisible while it backfills — instances only advertise
themselves in the registry once they reach the chain tip — so the API keeps
serving the old data uninterrupted. When the new instance registers, mapi's
per-request instance resolution starts selecting it. The old instance is then
retired: stopped, removed from the registry, its contiguous keyspace deleted at
leisure. No downtime, no flag day, no coordination beyond a sorted-set entry.

Because the registry is per-*reducer*, production ran one deployment per reducer
— roughly forty polyphony deployments per network — so a fix to one reducer
never risked the other thirty-nine. The compose file in this repository
demonstrates the full cycle with two instances (`polyphony-a`/`polyphony-b`):
start the second at any time, watch it backfill invisibly, and observe the API's
instance selection move to it once it registers at the tip.

Relevant code: registration in
`services/polyphony-xbt/src/storage/tikv.rs` (`insert_timestamp_entry`);
resolution in `services/mapi-xbt/src/tikv/key_resolver.rs` and
`services/mapi-xbt/src/tikv/adapter.rs` (`resolve_instances`).

## 2. One store serving tip, mempool, and history: MVCC and tikv-gc

The API must answer three kinds of question: what is true *now* (at the tip),
what is *about to be* true (mempool-aware queries), and what *was* true (queries
at a past block). Most systems grow three storage paths for this. Here there is
one, because TiKV's MVCC already keeps every version of every value — the only
question is when old versions may be garbage collected.

The stack turns that question into an explicit contract. Every polyphony commit
records its TiKV commit timestamp against the block height in a Redis cursor
entry, so "the state at block H" is precisely "a snapshot at H's commit
timestamp". Serving historical or consistent reads is then just opening a
snapshot in the past — provided GC has not collected it.

tikv-gc computes the greatest safepoint that preserves every timestamp a reader
could still need, as the minimum of three invariants
(`services/tikv-gc/src/invariants/`):

- **Earliest chaintip.** The safepoint never passes the commit timestamp of any
  instance's chaintip entry. Every instance's own tip-consistent snapshot stays
  readable — important because the newest data in TiKV may belong to a mempool
  view, and at least one real block must always remain behind it. The job warns
  if this earliest chaintip is more than two hours stale, since a stalled
  instance pins GC for everyone.
- **Network intersection.** For each network, the highest block present in *all*
  instances' non-mempool entries (and the intersection at the height below it)
  must remain readable. This is exactly the block mapi's snapshot unification
  (§8) will choose, so the API can always find a common point across instances
  at slightly different heights.
- **Safe zone.** Nothing newer than a configurable window (default ten minutes)
  is ever collected, regardless of the other invariants — covering the lifetime
  of in-flight transactions and long-running reads.

The operational consequence is a pleasingly simple dial: **the historical query
window equals the GC cadence.** Run tikv-gc rarely and the store retains deep
history at the cost of disk; run it every ten minutes and TiKV stays compact.
In this repository's compose stack it runs continuously on a ten-minute loop;
production ran the same binary as a CronJob.

## 3. Mempool indexing that does not melt the write path

The mempool changes every second; blocks change every ten minutes. Re-indexing
the full mempool view on every refresh would generate write traffic dwarfing the
chain itself.

Two decisions keep it cheap. First, mempool data flows through the *same*
pipeline as everything else: compressor assembles the estimated next blocks,
enriches their transactions identically to confirmed ones, and streams them as
pseudo-blocks marked mutable. Every reducer is therefore mempool-aware for free
— there is no parallel mempool implementation to write or to drift out of sync.
The reducers do not know they are processing unconfirmed data.

Second, the storage stage treats mempool views as *deltas*. Mempool writes are
mutable: their inverse actions sit in the rollback buffer (§5), and each new
view — or the arrival of a real block — first rolls the previous view back, then
applies the new state. The refresh cache
(`mempool_refresh_cache` in `services/polyphony-xbt/src/storage/tikv.rs`)
remembers the action set of the previous view keyed by storage key, diffs the
newly computed view against it, and writes only the keys that actually changed.
A largely-stable mempool costs almost nothing per refresh; a churning one costs
proportional to the churn.

On the read side, mempool-aware requests resolve instances through separate
`:mempool-view` registry keys scored by mempool view timestamp, so mapi selects
the instance with the freshest mempool view and can fall back to tip-based
selection when no mempool data is available.

## 4. The storage DSL: actions as algebra

Reducers do not write to the database. They emit *actions* — `set`, `delete`,
`increment`, and friends — and the storage stage owns what those actions become.
This indirection buys three things.

It buys optimisation. Actions over the same key compose algebraically: a set
followed by a set is the last set; a set followed by a delete is nothing;
increments fold into a single sum. The action merger
(`services/polyphony-xbt/src/storage/action_merger.rs`) applies these rules
within each commit. On metaprotocol-heavy blocks — where many reducers touch
the same keys repeatedly — this merges the raw action stream down to a fraction
of its size before anything reaches the store, with no reducer knowing or
caring.

It buys invertibility. Because every action is a small semantic operation rather
than an opaque write, each has a computable inverse (a set's inverse restores
the previous value; an increment's inverse is a decrement). The rollback
machinery of §5 is built entirely on this property.

And it buys decoupling: reducers stay pure folds from blocks to actions, unit
testable without a database, while batching, merging, retries, and commit
strategy live in one place.

## 5. Reorgs: the rollback buffer and refusing to be wrong

Bitcoin reorganises. An indexer that has applied blocks which are no longer part
of the best chain must *unapply* them — exactly, not approximately, because
downstream data (balances, UTXO sets) must be correct at every height.

As each block's actions are committed, polyphony also records their inverses in
a rollback buffer covering the last `buffer_size` blocks — held in memory and
persisted to TiKV (`services/polyphony-xbt/src/rollback/`), so it survives
process restarts. Handling a reorg is mechanical: apply the stored inverses back
to the fork point, then apply the new branch forward. The same machinery powers
the mempool lifecycle (§3), where every view is written knowing it will shortly
be rolled back.

The deliberate design decision is what happens when a reorg exceeds the buffer:
the instance panics. It does not skip, approximate, or resume from the fork
point with stale keys from the orphaned branch still in the store — it refuses
to continue, because past the buffer it can no longer guarantee correctness.
The remedy is the standard replacement flow of §1: start a fresh instance,
backfill, swap over.

Buffer depth is a per-network judgement: mainnet reorgs are almost always one
or two blocks, while testnet4 regularly reorganises tens of blocks deep. Two
settings govern the runway together: the indexer switches to buffered (mutable)
processing when it can intersect compressor's mutable stream, and that intersect
is only possible within compressor's own mutable window — so the effective
rollback depth is `min(buffer_size, immutable_after_confs)`. Raise both in
tandem; the testnet4 configs in this repository ship 128 and 150 respectively.

Reorg handling exists at both layers of the pipeline, and end to end it runs:
bitcoind reorganises; compressor's P2P sync detects the fork, rewinds its own
store (it keeps a rollback log for a mutable window of recent blocks,
`immutable_after_confs`), and emits explicit *undo* messages followed by the
replacement blocks on the gRPC stream; each polyphony instance applies its
buffered inverses in a TiKV transaction and indexes the new branch; and mapi
never notices — cursor entries only advance on committed state, so snapshot
unification always lands on a block that fully exists. Compressor additionally
offers a manual `rollback <height>` subcommand for operational recovery.

## 6. Resolve once, enrich everywhere

Section "The stack in one pass" gave the argument for compressor's existence;
this section gives the mechanics, because the enrichment contract is the
load-bearing wall of the whole design.

For every block, compressor ships: the raw block; every input resolved to the
full previous output it spends (script, value — the data a UTXO indexer needs
but a raw block lacks); ordinal satoshi ranges; rune etchings, mints, and
balance changes; and inscription and BRC-20 state transitions. Computing this
requires a full TXO resolver and sequential metaprotocol state machines —
hundreds of gigabytes of state and the better part of a chain sync to build. By
doing it once, behind a gRPC interface, the marginal cost of an additional
indexer instance drops to almost nothing: a fresh instance holds no resolver
state of its own and backfills an entire chain at whatever rate the storage
layer can absorb writes.

The gRPC contract (`proto/sync/v1/sync.proto`) has three surfaces:
`PageBlocksWithContext` for bulk history (paged, for backfills),
`StreamUpdatesWithContext` for the tip (a stream of apply/undo/reset
instructions, making reorgs explicit protocol events rather than something each
consumer detects), and `MempoolBlocksWithContext` for estimated next blocks —
with a client-side transaction cache, so a mempool view whose transactions the
client has already seen transfers only the new ones.

## 7. Committing large blocks: the split-commit protocol

A single block can produce tens of thousands of storage actions. One giant TiKV
transaction that size is slow and fragile — but naive chunking is worse, because
a crash between chunks would leave a block *half-applied* with the cursor
claiming otherwise.

The storage stage commits large blocks as a set of parallel batches wrapped in a
small two-phase protocol (`services/polyphony-xbt/src/storage/tikv.rs`). Before
the batches fly, a lock key is written recording the commit's identity: the
block point, the total action count, and whether the data is mutable
(`SplitCommitLockValue`). Each completed batch writes a completion marker; the
cursor advances and the lock clears only when every batch has landed.

Recovery is the mirror image. A restarting worker first checks for a lock key.
Finding one, it knows exactly which block was mid-commit and which batches
already completed; it skips the completed batches and finishes the rest — no
corruption, no redundant re-writes, and no operator involvement.

This protocol is also why the stack pins a fork of the TiKV Rust client. Large
batched commits can outlive the stock client's fixed lock TTLs, producing
`TxnLockNotFound` storms at exactly the worst moment — mid-commit of a large
block. The fork (`maestro-org/client-rust`) adds `CommitTTLParameters`, scaling
lock TTLs with batch size so a commit's locks live as long as the commit does.

## 8. One request, one block: consistent cross-reducer snapshots

An API request rarely touches one reducer. An address page might need UTXOs,
transaction counts, and rune balances — three reducers, possibly indexed by
different instances, each at a slightly different height at any given instant.
Reading each at "whatever it has now" produces answers from different points in
history that disagree with each other in ways users notice.

mapi refuses to do that. For each request it gathers the cursor entries of every
required reducer's selected instance and computes the best *common* block — the
highest height for which every instance has a commit — then opens each
instance's TiKV snapshot at that instance's *own* commit timestamp for that
block (different instances commit the same block at different wall-clock
moments; what is shared is the block, not the timestamp)
(`services/mapi-xbt/src/tikv/unify.rs`: `find_best_common_block`, with a
mempool-view analogue). The whole response is then assembled from one moment of
chain history, and the response names it in `last_updated`. The GC invariants of
§2 guarantee such a common point always remains readable.

This design closes a failure mode common in load-balanced indexer deployments:
successive requests landing on replicas at slightly different index heights and
returning answers that disagree with each other, each individually "current".
With per-request unification the replica-skew failure mode is structurally
impossible, no matter how many instances serve the data — and because the
response names its block, any answer can be independently verified against a
node at that exact height.

## 9. The key encoding: how a key/value store answers range queries

TiKV has no indexes, no query language, and no planner — only ordered bytes and
range scans. Timbre's job is to make byte order itself answer every query the
API needs.

Every key follows one shape:

```
<dataplane u8><instance u16> 'D' <reducer tag><BREAK> <key body…>
```

The 3-byte namespace makes multi-tenancy a property of the keyspace (§1); the
`'D'` tag separates data from the other planes that share the store — cursors
(`'C'`), rollback entries (`'R'`), the split-commit lock and batch markers
(`'L'`, `'B'`), and externally-ingested metadata (`'T'`, `'U'`) — so even the
transactional machinery of §7 lives in the same ordered keyspace. One byte then names the reducer, and everything after belongs
to that reducer's key body (`crates/timbre-xbt/src/encdec/builder.rs`).

Two rules make the body scannable. All integers encode **big-endian**, so
lexicographic byte order coincides with numeric order — a scan over a byte range
*is* a scan over a height range. And **field order is the query plan**: each
reducer's key struct orders its fields to match its endpoint's access pattern.
`UtxosByScriptHash` (`crates/timbre-xbt/src/reducers/utxos_by_script_hash.rs`)
keys are `{script_hash, height, utxo_hash, utxo_index}` — all UTXOs of a script
sit contiguously (a prefix scan), height-ordered within (a bounded scan answers
"as of height H"), with the transaction hash serving only to disambiguate. There
is no index because the schema *is* the index; designing a reducer is designing
its key order.

The `BREAK` byte after the reducer tag is what makes prefix scans cheap to
bound. To scan everything belonging to a prefix, the range is simply
`[<prefix><BREAK> .. <prefix><BREAK+1>)`: because `BREAK+1` is the next byte
value, the end key is the smallest byte string greater than *every* key under
the prefix — an exact exclusive upper bound obtained by incrementing the
delimiter rather than doing any arithmetic on the data itself. Scan bounds are
built by `reducer_key_range` (`crates/timbre-xbt/src/reducers/mod.rs`):
namespace, reducer tag, optional partition parameters, optional lower/upper key
fragments; for arbitrary prefixes without a trailing delimiter, the general
fallback increments the prefix's last non-`0xFF` byte (`prefix_key_range`,
`crates/timbre-xbt/src/lib.rs`).

Pagination then falls out for free: a page cursor is simply (a suffix of) the
last key returned, base64url-encoded and handed to the client as an opaque
token. The next page resumes the scan at exactly that key — no offsets, no skip
cost, stable under concurrent writes. The odd-looking `next_cursor` strings in
API responses are these raw key bytes.

Two disciplines keep the contract tight. Every reducer's maximum key and value
size is documented in the timbre README as part of the schema. And the
`Encode`/`Decode` derive macros (`crates/timbre-xbt/macros/`) generate the
field-order codec directly from the struct definition, so the Rust type and the
byte layout cannot drift apart.

## 10. Smaller mechanisms worth knowing about

**Tip-gated registration.** An instance only advertises itself in the instance
registry once it reaches the chain tip — detected by the arrival of mempool
blocks, which compressor only streams at the tip (an `advertise_immediately`
config override exists for development setups). Backfilling instances are
invisible by construction — there is no "draining" or "warming" state to
manage, and a half-synced instance can never be selected to serve.

**Cursor entries as universal glue.** One record type — height, block hash,
mempool flag, TiKV commit timestamp, chain tip, mempool view timestamp,
published to Redis per commit — powers three unrelated subsystems: mapi's
snapshot unification (§8), tikv-gc's safepoint invariants (§2), and the
instance registry scores (§1). The stack coordinates through this one small,
append-only vocabulary.

**The emulate source.** Polyphony can run against a synthetic chain: the
`Emulate` source turns an instruction string into apply/undo events, so a config
of `"FFF"` produces three forward blocks and mixed strings script arbitrary
reorg scenarios. Rollback logic is tested in milliseconds with no node, no
compressor, and no real chain.

**Retirement is a range delete.** Because an instance's entire output lives
under its 3-byte key prefix, decommissioning one is a registry removal plus a
single contiguous range deletion — no tombstone sweeps, no cross-key bookkeeping.

---

# Appendix: the coordination records in Redis

Everything the stack coordinates through fits in two small Redis structures
(cluster mode — the services use cluster clients, hence the cluster-of-one
arrangement in docker compose).

**Cursor entries** — written by polyphony after every commit:

```
key:    tikv-timestamps:<dataplane_id>:<instance_id>     (sorted set)
score:  TiKV commit timestamp (physical ms)
member: height,block_hash,was_mempool,commit_ts,network,
        chain_tip_height,chain_tip_hash,mempool_view_ts
```

plus `tikv-timestamps-keys`, a sorted set of those key names by most recent
activity. These entries are what mapi unifies snapshots from (§8) and what
tikv-gc derives safepoints from (§2).

**Instance registry** — the swap-over switch (§1):

```
key:    {bitcoin:<network>:<reducer-name>}:scores        (sorted set)
member: <dataplane_id: u8><instance_id: u16 BE>          (3 bytes)
score:  chain tip height this instance has indexed to

key:    {bitcoin:<network>:<reducer-name>}:mempool-view  (sorted set)
member: same 3 bytes
score:  mempool view timestamp
```

Registry entries are not garbage-collected: retiring an instance includes
removing its members (`ZREM`) — see
[operations.md](operations.md) for the full retirement runbook.
