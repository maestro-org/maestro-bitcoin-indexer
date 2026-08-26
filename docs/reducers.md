# Reducers

A reducer is one query pattern, indexed. Each one consumes enriched blocks and
emits key/value actions; enabling one is a single `[[reducers]]` entry in the
polyphony config. Because compressor resolves inputs and precomputes metaprotocol
state, most reducers are short — they read facts off the enriched block rather than
computing them.

## Catalogue

### Chain / transactions

| Reducer | Answers |
|---|---|
| `BlockInfo` | Block header/summary by height |
| `BlockByTxHash` | Which block is this tx in? |
| `HeightByBlockHash`, `HeightByTimestamp` | Height lookups |
| `TxInfo` | Full decoded transaction (inputs resolved) |
| `TxsByBlock` | Transactions in a block |
| `TxsByScriptHash` | Transaction history of an address/script |
| `TxFirstSeenTimestamp` | When a tx first appeared (mempool-aware) |
| `SpendingTxByTxo` | Which tx spent this output? |
| `SatsPerVbByBlock` | Fee-rate distribution per (estimated) block — feeds the fee-rates endpoint |

### Address / balance

| Reducer | Answers |
|---|---|
| `UtxosByScriptHash` | Current UTXOs of a script |
| `SatBalanceByScriptHash` | Balance |
| `HistoricalSatBalanceByScriptHash` | Balance at past heights |
| `SatTxsByScriptHash` | Sat movement history |
| `ScriptByScriptHash`, `ScriptHashByAddressPayloadHash` | Script/address resolution |
| `TotalOutputsByScriptHash`, `TotalUtxosByScriptHash`, `TotalTxsByScriptHash`, `TotalSatInInputsByScriptHash`, `TotalSatInOutputsByScriptHash` | Address statistics counters |

### Runes

| Reducer | Answers |
|---|---|
| `EtchingByRuneId`, `RuneIdByRuneName` | Rune definitions |
| `MintsByRuneId` | Mint history |
| `BalancesByRuneId` | Holders of a rune |
| `UtxosByRuneId` | UTXOs carrying a rune |
| `TxsByRuneId` | Rune activity |
| `RuneUtxosByScriptHash`, `RuneTxsByScriptHash` | A wallet's rune UTXOs / history |

### Inscriptions / BRC-20

| Reducer | Answers |
|---|---|
| `ContentByInscriptionId` | Inscription content |
| `TxsByInscription` | An inscription's transfer history |
| `InscriptionUtxosByScriptHash` | Inscriptions a wallet holds |
| `InscriptionActivityByScriptHash`, `InscriptionActivityByTx`, `InscriptionActivityByTxV2` | Inscription activity views |
| `TotalInscriptionsByScriptHash`, `TransferInscriptionsByScriptHash` | Wallet inscription counters |
| `Brc20TermsByTicker` | BRC-20 deployments |
| `BalancesByBrc20` | Holders of a BRC-20 ticker |
| `Brc20BalancesByScriptHash` | A wallet's BRC-20 balances |

## Writing a reducer

1. **Define the key layout** in `crates/timbre-xbt/src/reducers/<name>.rs`: pick an
   unused tag byte (see the table in the timbre-xbt README), design key bytes so
   that range scans return results in the order your queries want, and register the
   module in `crates/timbre-xbt/src/reducers/mod.rs` and the `Reducer` enum in
   `src/lib.rs`.
2. **Implement the reducer** in `services/polyphony-xbt/src/reducers/<name>.rs`: a
   `Config` struct (often empty), and a `reduce_block` producing `StorageAction`s
   from the enriched block. Register it in the `Config` enum in `reducers/mod.rs`
   (including `kebab_name()` — the name used in the Redis instance registry) and the
   plugin/reduce match arms.
3. **Read it back** in `services/mapi-xbt`: add the reducer to
   `src/tikv/key_resolver.rs` (same kebab-case name) and build endpoints on
   `src/tikv/adapter.rs` snapshots.
4. **Backfill it live** — this is the point of the architecture: run a new polyphony
   instance with the new reducer under a fresh `instance_id`; it backfills in
   parallel and enters service automatically when it reaches the tip
   ([design.md](design.md#1-fixing-a-broken-indexer-with-zero-downtime)).

Reducers must be **deterministic and invertible** — the storage stage records
inverse actions per block so rollbacks unwind exactly. Stick to
set/delete/increment actions and derive everything from the block + enrichment
context (never from wall-clock or external I/O).
