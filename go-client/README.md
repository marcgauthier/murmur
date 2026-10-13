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
./go-client/build.sh test -count=1 -timeout 180s ./...
./go-client/build.sh --release build ./...
```

Windows PowerShell:

```powershell
$env:CC = 'gcc'
$env:LIBCLANG_PATH = 'C:\Program Files\LLVM\bin'
./go-client/build.ps1 test -count=1 -timeout 180s ./...
./go-client/build.ps1 -Release build ./...
```

Native targets: Linux amd64/arm64, macOS amd64/arm64, Windows amd64 (GNU).
CI builds and tests each target. Native binaries are built locally into ignored
`native/lib/<goos>-<goarch>` directories. Build scripts also generate an ignored
library fingerprint so Go's build cache notices native changes. Rebuild after
changing Rust code. `CGO_ENABLED=0` is unsupported. There are no external Go modules.

For another application, use a local module replacement until distributing a
native build with your application:

```go
require github.com/marcgauthier/murmur/go-client v0.0.0
replace github.com/marcgauthier/murmur/go-client => ../MURMUR/go-client
```

Run the complete example against a new path:

```sh
./go-client/build.sh run ./examples/basic /tmp/murmur-go-demo
```

Start with `DefaultOptions()`, set `Mode = CreateNew` for a new directory, and
call `Open(path, secret32ByteKey, options)`. A zero `Options{}` is not a defaults
constructor. `Existing` reopens persisted state; `Flush` is the explicit durability
barrier. A successful application transaction alone does not guarantee persistence.

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
[root usage guide](../USAGE.md#sql-and-schema-restrictions).

## Features

| Feature | Go entry points and examples |
|---|---|
| Lifecycle, recovery, durability | `Open`, `Clone`, `Close`, `Release`, `Flush`, `RetryStorage`, `OpenProgress`, `Options.OnOpenProgress`, `Options.FaultHooks` |
| Migrations and offline exchange | `Migrate`, `Schema`, `ExportState`, `MergeState`; share database ID and compatible schema, use distinct node IDs |
| FTS5 and vector indexes | `FTS5Index`, `VectorIndex`, `SetSearchIndexes`, `RefreshIndexes`; source tables replicate, derived virtual tables rebuild locally |
| Table replication | `SetReplication("docs", false)`, `ReplicationEnabled`; node-local settings survive reopen and migrations |
| Subscriptions | `SubscribeRows`, generic `Subscribe[T]`, `RowCodec[T]`, `RecvContext`, `RecvTimeout`, `TryRecv`, `Close` |
| Counters, sets, min/max | `sql.CounterAdd`, `SetAdd`, `SetRemove`, `Min`, `Max`; causal policies require replication configuration |
| Network replication | `NewReplicationConfig`, `Credentials`, trust/allow lists, `Options.Replication`, `DB.Replication`; returned `Replication` exposes peer management, status, sync and retention |
| Files | `NewFileConfig`, `UploadFile`, `GetFile`, `ListFiles`, `SearchFiles`, metadata/availability/deletion/key rotation; `Objects` exposes standalone lifecycle and chunk operations |
| Backup and key management | `Backup`, `VerifyBackup`, `Restore` with `RestoreOptions`, `RotateDataKey`, `RotateMasterKey`, `KeyInventory` |
| Scheduling and metrics | `Scheduler`, `NewWriterScheduler`, `Admit`, `AdmitTimeout`, `Ticket.Release`, scheduler snapshots, `Status`, `SQLiteMetrics`, `SpoolMetrics` |
| Standalone backend | Implement `Backend`, `RegisterBackend`, `BindSigner`, `StartReplication`; see `replication_test.go` for a test backend |
| High/Low bridge | `ExporterConfig`, `ReceiverConfig`, `OpenBridge`, `Capture`, `Export`, `Stage`, `Process`, `ReleaseOwnership`, `PublishHTTP`, `PublishFTP` |
| Snapshot protocol | `BuildSnapshot`, manifest `Verify`, `AcceptSnapshot`, `ReceiveSnapshot`, `MissingSnapshot`, `FinishSnapshot`, outgoing/cancel/maintenance/prune helpers |
| Transport, crypto and wire format | `NewEndpoint`, `Connect`/`Accept`, bidirectional streams, `ReadMessage`/`WriteMessage`, `Encode`/`Decode`, signing/verification, `StorageKey`, `Constants` |
| Unmanaged SQLite sessions | `OpenInMemory`, `RawConnection.Run`, `StartSession`, `Attach`, `Changeset`, `Patchset`, `SetEnabled`, `IsEmpty`; no managed persistence/replication |

Detailed operational examples and restrictions remain in [USAGE.md](../USAGE.md).
The integration tests contain runnable Go examples for replication credentials,
convergence, bridge bundles, snapshots, subscriptions, backups, and file streams.
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
cd go-client
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
