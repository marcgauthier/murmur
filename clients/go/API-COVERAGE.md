# Public API coverage

`api-inventory.json` lists public functions, types, fields, enum variants, trait
methods, inherent methods, and associated constants from Murmur's Rust sources.
The scanner uses `syn` to follow the root crate's explicit exports and public
scheduler module, the replication crate's exports/public modules, and exported
Spool key inventory types. Private implementation types and third-party methods
(e.g. the complete rusqlite or quinn API) are outside this inventory.

`api-coverage.json` maps each of the 791 reviewed Rust symbols to an exported Go
binding target. The audit parses Go declarations and fails for new/removed Rust
symbols, unreviewed mappings, and missing Go targets. This checks API drift and
symbol existence; behavioral correctness is covered by integration tests and
review, not proven by a mapping entry.

| Rust surface | Go representation |
|---|---|
| `Database`, `Sql` | `DB` (`Database` alias), scoped `Sql`, owned `Statement`/`Rows` |
| Generic subscriptions | Generic package `Subscribe[T]`, `Subscription[T]`, `RowCodec[T]`; SQL convenience `SubscribeRows` |
| FTS/vector configuration structs | Tagged `SearchIndex`, constructors `FTS5Index` and `VectorIndex`, `IndexName`/`Source` accessors |
| `Result<T>` and error enums | Go result plus `error`, typed `Error`, SQLite code and network category |
| Protocol message variants | `Message{Kind, Body}`, Rust validates variants and bodies when encoding; `NewMessage`/`DecodeBody` |
| Public data/enum fields | Owned Go structs, strings/constants, `[16]byte`/`[32]byte` IDs and keys |
| Node-keyed maps | Explicit entries (`TrustEntry`, `NodeSequence`, `CounterComponent`, `SetAddition`); no lossy JSON stringification |
| `Counter.positive` / `.negative` | `Counter.Components` with database/node and independent positive/negative values |
| Replication `Handle` | `Replication`; files and bridge handles expose separate owned wrappers |
| Scheduler and tickets | `WriterScheduler`, `Ticket`, monotonic `GrantedAt`, `WriterClassCount`/`String` |
| `Backend` trait | Complete Go `Backend` interface with native-owned callback lease |
| Async transport/protocol/HTTP/FTP functions | Blocking Go methods backed by the native Tokio runtime; stream/endpoint close releases native ownership |
| Snapshot and retention helpers | Native-delegated Go functions and DTOs; storage-key functions consolidate into `StorageKey(KeyRequest)` |
| Wire `encode<T>` / `decode<T>` | `Encode(typeName, value)` / `Decode(typeName, bytes, out)` for supported exported data types listed in ABI.md |
| Unmanaged session utilities | `OpenInMemory`, `RawConnection.Run`, `StartSession`, `Session` |

Rust reference lifetimes are enforced by callback scope and thread checks. Rust
`usize` capacities use uint64 and durations use `time.Duration` nanoseconds;
invalid/out-of-range native values return errors. Arbitrary user-defined Rust
Serde types cannot cross this binding; application data can be carried as bytes
or a custom subscription codec. Control JSON requires finite real values; SQL
and SQL subscriptions use binary values and support infinities.

After a public Rust change, run the inventory command from README into a temporary
file. Implement/review the corresponding binding, update `api-inventory.json` and
`api-coverage.json`, then run the audit and relevant integration tests. Never
refresh the inventory alone to silence a mismatch. The CI job repeats this audit.
