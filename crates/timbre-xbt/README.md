# timbre-xbt

The **TiKV key/value encoding schema** — a pure library crate (no I/O) defining the
byte-level layout of every key and value the indexer writes and the API reads. It is
the contract between [polyphony-xbt](../../services/polyphony-xbt) (writer) and
[mapi-xbt](../../services/mapi-xbt) (reader): both depend on this crate, so the
schema can never drift between them.

Every data key is prefixed with the writing instance's identity —
`<dataplane_id:u8><instance_id:u16><reducer_tag:u8>...` — which is what lets many
indexer instances share one TiKV cluster without touching each other's keyspace
(see the reducer tag table below). Keys are encoded so that range scans iterate in
a useful order (e.g. by height, or by script hash then outpoint); see
`src/encdec/` for the building blocks and `src/reducers/` for per-reducer layouts.

**Note:** Max Key Size counts the reducer-specific key body only. Every data key
additionally carries a fixed 6-byte prefix: dataplane ID (1), instance ID (2),
the data-plane tag `D` (1), the reducer tag (1), and an initial `BREAK` (1).
The `BREAK` delimiter is also how prefix scans are bounded: the range
`[<prefix><BREAK> .. <prefix><BREAK+1>)` covers exactly the keys under a prefix,
with the exclusive end obtained by incrementing the delimiter byte.

| Reducer  | Tag Byte | Max Key Size | Max Val Size |
| --- | --- | --- | --- |
| EtchingByRuneId | `0x61` | 12 | 163 |
| MintsByRuneId  | `0x62` | 12 | 16 |
| UtxosByRuneId  | `0x63` | 59 | 46 |
| UtxosByScriptHash  | `0x64` | 67 | 8 |
| ~~RunesByUtxo~~  | `0x65` | 37 | 8 + (number of runes * 28) |
| ~~RuneInfoByRuneId~~ | `0x66` | - | - |
| ScriptByScriptHash | `0x67` | 20 | 4 + script length |
| ScriptHashByAddressPayloadHash | `0x68` | 20 | 20 |
| RuneIdByRuneName | `0x69` | 16 | 12 |
| ~~InscriptionsByUtxo~~ | `0x6A` | - | - |
| Brc20TotalBalanceByScriptHash | `0x6B` | 27 | 16 |
| Brc20AvailableBalanceByScriptHash | `0x6C` | 27 | 16 |
| Brc20TermsByTicker | `0x6D` | 6 | 74 |
| BalancesByBrc20 | `0x6E` | 27 | 16 |
| TxsByScriptHash | `0x6F` | 65 | 3 |
| RuneBalancesByScriptHash | `0x70` | 33 | 16 |
| ~~InscriptionsByScriptHash~~ | `0x71` | - | - |
| ContentByInscriptionId | `0x72` | 36 | 27 + content type length + content body length |
| TransferInscriptionsByScriptHash | `0x73` | 64 | 77 |
| SatsPerVbByBlock | `0x74` | 8 | 26 |
| InscriptionUtxosByScriptHash | `0x75` | 89 | 35 + (number of inscriptions * 66) |
| ~~InscriptionActivityByBlock~~ | `0x76` | - | - |
| BlockByTxHash | `0x77` | 32 | 35 |
| HeightByBlockHash | `0x78` | 32 | 17 |
| InscriptionActivityByTx | `0x79` | 37 | 50 + (num of inscriptions * 101)
| TxsByInscription | `0x7A` | 20 | 106 * (num of inscription activities entries)
| TxInfo | `0x7B` | 35 | 146 + (242 * num of inputs) + (191 * num of outputs)
| BlockInfo | `0x7C` | 17 | 164 + length of script_sig in coinbase tx
| TxsByBlock | `0x7D` | 35 | 32
| BalancesByRuneId | `0x7E` | 55 | 16
| SpendingTxByTxo | `0x7F` | 50 | 35
| RuneUtxosByScriptHash | `0x81` | 89 | 22 + (number of different rune kinds * 51)
| TxsByRuneId | `0x82` | 59 | 18 + num of self-transferring addresses * 36 + num of sender addresses * 36 + num of receiver addresses * 36
| TxFirstSeenTimestamp | `0x83` | 32 | 17
| RuneTxsByScriptHash | `0x84` | 67 | 59 + number of runes with self transfers * 28 + number of runes with increased balance * 28 + number of runes with decreased balance * 28
| SatBalanceByScriptHash | `0x85` | 20 | 8
| SatTxsByScriptHash | `0x86` | 67 | 10
| InscriptionActivityByScriptHash| `0x87` | 67 | 14 + number of self-transfers * 56 + number of sent inscriptions * 80 + number of received inscriptions * 80
| InscriptionActivityByTxV2 | `0x88` | 37 | 50 + (num of inscriptions * 102)
| TotalTxsByScriptHash | `0x89` | 20 | 8
| TotalUtxosByScriptHash | `0x8A` | 20 | 8
| TotalInscriptionsByScriptHash | `0x8B` | 20 | 8
| HeightByTimestamp | `0x8C` | 4 | 8
| HistoricalSatBalanceByScriptHash | `0x8D` | 28 | 8
| TotalOutputsByScriptHash | `0x8E` | 20 | 8
| TotalSatInOutputsByScriptHash | `0x8F` | 20 | 16
| TotalSatInInputsByScriptHash | `0x90` | 20 | 16

Struck-through reducers are retired; their tag bytes remain permanently reserved
so historical data never becomes ambiguous.

**Current maximum key size:** 89 (95 including the 6-byte prefix)