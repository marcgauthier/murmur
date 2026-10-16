# Murmur Go client

An embedded cgo binding to Murmur's Rust implementation. Each managed database
uses one in-memory SQLite database, with persistence, search maintenance, and
replication in Rust background workers. No separate server or Go SQLite driver
is required. The native library enables both `replication` and `vector`.

Go 1.24+, a Rust toolchain supporting edition 2024, a C compiler, and libclang
are required. Linux needs a GNU toolchain and libclang development packages;
macOS needs Xcode command-line tools and libclang; Windows uses Rust's GNU target,
MinGW-w64 GCC, and LLVM/libclang (`LIBCLANG_PATH` if needed). Build on the target
OS/architecture; these scripts do not provision cross-compilers.

From the repository root:

```sh
./clients/go/build.sh test -count=1 -timeout 180s ./...
./clients/go/build.sh --release build ./...
```

Windows PowerShell:

```powershell
$env:CC = 'gcc'
$env:LIBCLANG_PATH = 'C:\Program Files\LLVM\bin'
./clients/go/build.ps1 test -count=1 -timeout 180s ./...
./clients/go/build.ps1 -Release build ./...
```

Native targets: Linux amd64/arm64, macOS amd64/arm64, Windows amd64 (GNU).
CI builds and tests each target. Native binaries are built locally into ignored
`native/lib/<goos>-<goarch>` directories. Build scripts also generate an ignored
library fingerprint so Go's build cache notices native changes. Rebuild after
changing Rust code. `CGO_ENABLED=0` is unsupported. There are no external Go modules.

Use `./clients/go/build.sh --release` (PowerShell: `./clients/go/build.ps1 -Release`)
before linking an application for realistic startup and throughput measurements.
The default build is debug. Running `go build` in a consuming application only
links the installed native archive; it does not rebuild Rust sources, even with
a local Go module replacement.

For another application, use a local module replacement until distributing a
native build with your application:

```go
require github.com/marcgauthier/murmur/clients/go v0.0.0
replace github.com/marcgauthier/murmur/clients/go => ../MURMUR/clients/go
```

Browse the [19 runnable feature examples](examples/README.md). Each creates local
resources automatically and verifies its result. Start with:

```sh
./clients/go/build.sh run ./examples/basic
./clients/go/build.sh run ./examples/replication
./clients/go/build.sh run ./examples/bridge
```

Start with `DefaultOptions()`, set `Mode = CreateNew` for a new directory, and
call `Open(path, secret32ByteKey, options)`. A zero `Options{}` is not a defaults
constructor. `Existing` reopens persisted state; `Flush` is the explicit durability
barrier. A successful application transaction alone does not guarantee persistence.

Spool's index includes fixed namespace hints for prefix searches: 32 KiB with
the default 256 shards. These contain no record values, are rebuilt on open,
and can cause extra searches when hashes collide or namespaces have been deleted.
Setting `SpoolCacheBytes` to zero disables the decoded-block cache; these fixed
index hints remain. See the [root usage guide](../../USAGE.md) for memory options.

## SQL and transactions

```go
err := db.Transaction(func(sql *murmur.Sql) error {
    _, err := sql.Exec("INSERT INTO docs(id,title) VALUES(?,?)", 2, "Second document")
    return err // an error or recovered panic rolls back
})
err = db.Read(func(sql *murmur.Sql) error {
    rows, err := sql.Query("SELECT id,title FROM docs ORDER BY id")
    if err != nil { return err }
    defer rows.Close()
    for rows.Next() {
        var id int64
        var title string
        if err := rows.Scan(&id, &title); err != nil { return err }
    }
    return rows.Err()
})
```

`Exec`, `ExecuteBatch`, `Prepare`, `Query`, `QueryRow`, `LastInsertRowID`, statement
execution, and streamed rows run in their native scope. `db.Exec` wraps a transaction;
`db.Query` returns owned materialized rows. Use `Read` for streaming large results.
SQL supports positional bindings: nil, booleans, signed integers, uint values that
fit int64, float32/64, strings, bytes, and tagged `Value`. SQL's binary boundary
preserves int64, floating-point bits, embedded NULs, empty blobs, and NULL.
Use `Value` or `any` to scan nullable columns. Errors support `errors.Is` and
`errors.As(*murmur.Error)`; SQLite extended codes are available on `Error`.

SQL scopes, statements, and row iterators belong to the callback's locked OS thread.
Do not retain them after return, share them with another goroutine, or call database
methods from inside that callback. Such native SQL access is rejected. Call methods
on the provided `Sql` instead. Owned row values can be retained. SQL statements and
iterators are not safe for concurrent use. `DB` operations serialize in Rust.

Migrate schemas with `Migrate(NewMigration(sql))`; `TableMapping` preserves stable
table/column identities through renames. Managed SQL has the same restrictions as
Rust, including primary keys, transaction control, and reserved objects. See the
[root usage guide](../../USAGE.md#sql-and-schema-restrictions).

## Features

| Feature | Go entry points | Runnable examples |
|---|---|---|
| Lifecycle, recovery, durability | `Open`, `Clone`, `Close`, `Release`, `Flush`, `RetryStorage`, `OpenProgress`, `Options.OnOpenProgress`, `Options.FaultHooks` | [basic](examples/basic/main.go), [recovery](examples/recovery/main.go) |
| Migrations and offline exchange | `Migrate`, `Schema`, `ExportState`, `MergeState`; share database ID and compatible schema, use distinct node IDs | [migrations](examples/migrations/main.go), [offline](examples/offline/main.go) |
| FTS5 and vector indexes | `FTS5Index`, `VectorIndex`, `SetSearchIndexes`, `RefreshIndexes`; source tables replicate, derived virtual tables rebuild locally | [search](examples/search/main.go), [replication](examples/replication/main.go) |
| Table replication | `SetReplication("docs", false)`, `ReplicationEnabled`; node-local settings survive reopen and migrations | [migrations](examples/migrations/main.go), [replication](examples/replication/main.go) |
| Subscriptions | `SubscribeRows`, generic `Subscribe[T]`, `RowCodec[T]`, `RecvContext`, `RecvTimeout`, `TryRecv`, `Close` | [subscriptions](examples/subscriptions/main.go) |
| Counters, sets, min/max | `sql.CounterAdd`, `SetAdd`, `SetRemove`, `Min`, `Max`; causal policies require replication configuration | [policies](examples/policies/main.go), [retention](examples/retention/main.go) |
| Network replication | `NewReplicationConfig`, `Credentials`, trust/allow lists, `Options.Replication`, `DB.Replication`; returned `Replication` exposes peer management, status, sync and retention | [replication](examples/replication/main.go) |
| Continuous replication and bounded batches | `BatchTransactions`, `BatchBytes`, opt-in `AdaptiveSync`; byte/count limits bound each batch, not transactions per second | [replication](examples/replication/main.go) |
| Streamed snapshot installation | `BuildSnapshotStream`, `FinishSnapshot`; verified pages become visible incrementally, final progress waits for completion | [snapshots](examples/snapshots/main.go) |
| Replication retention | `RetentionStatus`, `CollectRetention`, `RetirePeer`, minimum age/count and offline protection | [retention](examples/retention/main.go), [snapshots](examples/snapshots/main.go) |
| Files | `NewFileConfig`, `UploadFile`, `GetFile`, `ListFiles`, `SearchFiles`, metadata/availability/deletion/key rotation; `Objects` exposes standalone lifecycle and chunk operations | [files](examples/files/main.go), [bridge](examples/bridge/main.go) |
| Backup and key management | `Backup`, `VerifyBackup`, `Restore` with `RestoreOptions`, `RotateDataKey`, `RotateMasterKey`, `KeyInventory` | [backup](examples/backup/main.go), [keys](examples/keys/main.go) |
| Scheduling and metrics | `Scheduler`, `NewWriterScheduler`, `Admit`, `AdmitTimeout`, `Ticket.Release`, scheduler snapshots, `Status`, `SQLiteMetrics`, `SpoolMetrics` | [scheduler](examples/scheduler/main.go) |
| Batched backend reads | Optional `BatchBackend`, `BackendHandle.GetMany`, `GetManyBounded`; ordered bounded prefixes | [backend](examples/backend/main.go) |
| Blocking scheduler utility | `RunBlocking` invokes Rust; ordinary Go calls run inline | [backend](examples/backend/main.go) |
| Standalone backend | Implement `Backend`, `RegisterBackend`, `BindSigner`, `BackendHandle.Progress`, `GetMany`, `GetManyBounded`, optional `BatchBackend`, `RunBlocking`, `StartReplication`; record-only teaching implementation | [backend](examples/backend/main.go), [files](examples/files/main.go) |
| High/Low bridge | `ExporterConfig`, `ReceiverConfig`, `OpenBridge`, `Capture`, `Export`, `Stage`, `Process`, `ReleaseOwnership`, `PublishHTTP`, `PublishFTP` | [bridge](examples/bridge/main.go), [bridge-transports](examples/bridge-transports/main.go) |
| Snapshot protocol | `BuildSnapshot`, manifest `Verify`, `AcceptSnapshot`, `ReceiveSnapshot`, `MissingSnapshot`, `FinishSnapshot`, outgoing/cancel/maintenance/prune helpers | [snapshots](examples/snapshots/main.go), [retention](examples/retention/main.go) |
| Transport, crypto and wire format | `NewEndpoint`, `Connect`/`Accept`, bidirectional streams, `ReadMessage`/`WriteMessage`, `Encode`/`Decode`, signing/verification, `StorageKey`, `Constants` | [protocol](examples/protocol/main.go), [bridge-transports](examples/bridge-transports/main.go) |
| Unmanaged SQLite sessions | `OpenInMemory`, `RawConnection.Run`, `StartSession`, `Attach`, `Changeset`, `Patchset`, `SetEnabled`, `IsEmpty`; no managed persistence/replication | [sessions](examples/sessions/main.go) |

Detailed operational examples and restrictions remain in [USAGE.md](../../USAGE.md).
The [example catalog](examples/README.md) provides executable programs with
expected results and restrictions for every feature group. Their smoke tests
exercise the same scenario functions as the commands.
[API-COVERAGE.md](API-COVERAGE.md) explains the public API mapping and audit.

Subscribe to an owned row projection:

```go
sub, err := db.SubscribeRows("SELECT id,title FROM docs", nil, []int{0}, murmur.DefaultSubscriptionOptions())
if err != nil { return err }
defer sub.Close()
event, err := sub.RecvContext(ctx) // Initial, Update, or Reset
```

Subscriptions have bounded queues. Overflow, schema changes, or shutdown can
issue `Reset`; recreate the subscription to obtain fresh rows. An incompatible
resume cursor returns `ErrInvalid`. Valid resume cursors provide the current
snapshot, not historical replay. SQL subscriptions use binary value encoding.
For custom Go rows, provide stable unique keys and a `RowCodec[T]`; `JSONRowCodec[T]`
is convenient for JSON-compatible types. Callback failures/panics become errors;
background subscription errors follow Rust's subscription behavior.

Close every owned native handle explicitly. `DB.Close` closes its shared core,
including clones; `DB.Release` drops only that handle. Closing a borrowed scheduler
with `Close` stops the shared scheduler; use `Release` to drop its handle.
Replication handles and endpoints have explicit close semantics. Stream `Close`
cancels pending frame reads/writes. Blocking network operations otherwise use Rust's
timeouts; they do not accept Go contexts. Subscription context cancellation is
polled at up to 100 ms intervals. Go garbage collection does not release native
handles. Callbacks can arrive on Rust threads; backend and fault-hook implementations
must be concurrency safe. Keep callbacks short and avoid reentering blocking native
operations on the same resource.

Windows skips directory fsync where the underlying Rust store does; file contents
are synchronized, but crash durability of directory entries depends on Windows.
Keys cross an owned native control buffer and are zeroized where Rust already
zeroizes them; Go keys/control JSON cannot promise secure memory erasure.

## Validation

```sh
cd clients/go
./build.sh test -count=1 -timeout 180s ./...
go test -race -count=1 -timeout 180s ./...
GOEXPERIMENT=cgocheck2 go test -count=1 -timeout 180s ./...
go vet ./...
cargo test --locked --manifest-path native/Cargo.toml
mkdir -p build
cargo run --locked --quiet --manifest-path native/Cargo.toml --example api_inventory > build/current-api.json
go run ./internal/apiaudit -inventory build/current-api.json
```

The build script builds a direct Rust peer fixture for `go test`; the mixed Rust/Go
test exchanges persisted source rows and checks a Go FTS index. Running tests without
a fixture skips that test with an explicit reason. Set `MURMUR_RUST_FIXTURE` to the
fixture binary to choose its path. Native ABI tests cover malformed requests,
invalid scopes, unwind containment, defaults, and integer codec fidelity.

Database capture now defaults to one second. Incoming application drains continuously;
`IncomingInterval` is a recovery retry interval capped at one second, not a throughput ceiling.
`BatchTransactions` defaults to 64, `BatchBytes` to 4 MiB, and `AdaptiveSync` to false.
A larger transaction is admitted alone. `ApplyBatches`, `ApplyBytes`, and `ApplyNanos`
report completed incoming work. Optional `PagedBackend.KeysPage` avoids materializing
large prefixes; existing custom backends receive a sorted fallback over `Keys`.
`BuildSnapshotStream` persists encoded `Content` pages through Rust; its Go input slice
is materialized. Managed database exports instead stream from an immutable Spool checkpoint.
Protocol version 10 and database format 5 require new databases and upgraded peers;
older formats are rejected, and no migration is provided.

Custom backends may implement optional `CancellableBackend.CancelPending` to interrupt
pending owner/admission waits during `Replication.Close`; active durable commits must
finish. Backends that leave synchronous calls blocked cannot guarantee bounded shutdown.

Batched backend reads preserve input order and missing values (`nil` in `[]*Bytes`);
a pointer to empty bytes represents an existing empty value. `BatchBackend.GetMany`
receives a nullable byte budget: nil requests every key, otherwise return a bounded
prefix with one oversized first value allowed. Custom backends without this optional
interface fall back to `Get` inside one native callback. These are not atomic read
snapshots. The backend example implements batching under one storage lock and
checks missing/empty results. `RunBlocking` delegates to Rust's scheduler utility;
ordinary Go callers execute inline rather than launching a goroutine.
