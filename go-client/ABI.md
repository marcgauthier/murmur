# Native ABI version 1

`native/murmur.h` is the private C interface used by cgo. It exposes ABI version,
control request, scoped SQL request, buffer copy, and buffer free functions.
Operations never expose Rust struct layout, references, or raw SQLite pointers.

Native objects have monotonically allocated integer IDs in a synchronized
registry. An operation retains an Arc during execution; registry locks are released
before calling database code or Go callbacks. Explicit close/release removes the
registry owner. Go objects must not be copied after first use.

A `murmur_buffer` owns a Rust allocation (pointer, length, capacity). Only
`murmur_buffer_free` frees it. Go copies outputs before freeing them and keeps input
slices alive through synchronous calls. Rust never retains Go data pointers.
Callbacks retain only numeric `runtime/cgo.Handle` tokens. The native callback
lease invokes opcode 0 once when its final Rust owner drops, deleting the Go handle.
Callback outputs are copied into native allocations. Native request inputs are
bounded to 512 MiB; this is an ABI limit independent of Murmur storage settings.

Control requests/results use UTF-8 JSON. Byte vectors are integer arrays rather
than base64; IDs and keys have fixed lengths. Integers decode without float64
conversion. Durations use nanoseconds; Rust Serde duration objects are normalized
when copying outputs. Tuple-keyed maps have explicit DTO adapters. Error results
carry `code`, `message`, optional `sqlite_code`, and optional `network_code`.

SQL uses a little-endian binary encoding, separate from control JSON. Parameters
are a uint32 count followed by tagged values: 0 NULL, 1 int64, 2 IEEE float64 bits,
3 UTF-8 text, 4 blob. Text/blob lengths are uint32. Statement/row IDs are uint64.
SQL callbacks lock the Go goroutine to its native OS thread; Rust validates a
thread-local scope. The scope drops all statements and rows before it exits.
Scoped statements are boxed to keep addresses stable; erased internal lifetimes
never escape the scope. Statement rows are dropped before their statements.

Native entry points catch Rust panics in unwind builds. Go callbacks recover
ordinary panics. Transaction callback errors and recovered panics roll back.
`runtime.Goexit` (including `testing.T.Fatal` inside callbacks), process aborts,
and allocation failure are not recoverable callback errors: return errors from
callbacks. The release profile deliberately uses `panic = "unwind"`.

The native library uses Murmur's Rust protocol implementation. No cryptography,
postcard format, conflict logic, persistence format, or derived-index replication
is implemented independently in Go. `Encode`/`Decode` accept these Rust type names:

```text
Value Values ReplicaState Schema Version Column Table Cell RowState Tombstone
MergePolicy OpenPhase OpenProgress SqliteMetrics SpoolMetrics Status
SearchIndexStatus SchedulerConfig SchedulerSnapshot SchedulerClassStats WriterClass
SubscriptionCursor Record Records Content Transaction SignedSchema Message
Progress ProgressList Chunk ChunkManifest Manifest Incoming Pin Origin Header Bundle
FileMetadata FileAvailability FileKeyRotationStatus Role Dissemination Policy Tag
NodeId Key PeerRetention ReplicationStatus ReplicationMetrics PeerDiagnostics
Counter Set Causal Bytes String U64 I64 Bool F64
```

Names refer to Rust wire types, so Go `OriginTransaction` uses `Transaction`,
`SnapshotManifest` uses `Manifest`, `SnapshotIncoming` uses `Incoming`, and bridge
DTOs use `Origin`/`Header`/`Bundle`. Optional values use pointers. Control JSON limits
these codecs to JSON-representable values; raw SQL has its own binary boundary.
