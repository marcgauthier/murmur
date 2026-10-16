# Runnable Murmur Go examples

These 19 programs exercise the complete Go feature catalog using real native
Murmur operations. Each verifies its result before printing `verified successfully`.
They generate temporary data, random keys, short-lived credentials, and local peers
or receivers automatically. No external service or additional Go dependency is
required for their default runs.

Build prerequisites and supported platforms are described in the
[Go client guide](../README.md). From the repository root, build once:

```sh
./clients/go/build.sh build ./...
cd clients/go
go run ./examples/basic
go run ./examples/replication
go run ./examples/bridge-transports
```

On Windows, use `./clients/go/build.ps1 build ./...` from PowerShell, then the same
`go run` commands. Alternatively, the build script can build and run any example:

```sh
./clients/go/build.sh run ./examples/search
```

All commands accept `--dir NEW_DIRECTORY`, `--keep`, and `--timeout 60s`.
`--dir` must not exist; parent directories must exist. Each run owns that directory
and removes it after closing handles unless `--keep` is supplied. Keys are
intentionally not printed or saved, so retained encrypted artifacts cannot be
reopened in a later process without separately changing the demo to supply your
own keys. The basic command also preserves its original positional database-path
invocation; that database path is retained, while temporary supporting data is
cleaned up. Repeated runs must use new paths.

```sh
go run ./examples/basic --keep --dir /tmp/murmur-example-new
go run ./examples/basic /tmp/murmur-database-new
go run ./examples/retention --timeout 90s
```

## Catalog

Run each entry with `go run ./examples/NAME` from `clients/go`.

| Example | What it demonstrates | Verified result |
|---|---|---|
| [basic](basic/main.go) | Typed bindings, prepared statements, streaming reads, NULL, empty BLOB, NUL text, int64, infinity, rollback, flush, clone/release, reopen | One row survives rollback and recovery with exact value types |
| [migrations](migrations/main.go) | Table/column renames with explicit mappings; local replication settings | Stable IDs, data, and disabled replication survive rename/reopen |
| [offline](offline/main.go) | Export/merge between disconnected replicas, conflicting edits, deletes, duplicate delivery | Both peers resolve to identical rows and retain deletion tombstones |
| [search](search/main.go) | FTS stemming; vector L2/cosine/L1, little-endian float32 embeddings, refresh, recovery, managed-index restrictions | Expected text match and nearest neighbor; virtual tables excluded from exported state and rebuilt locally |
| [subscriptions](subscriptions/main.go) | SQL and generic typed projections, codecs, diffs, cancellation, resume, bounded queues, reset/recreation | Added-row events and fresh snapshots; expired cursor rejected |
| [replication](replication/main.go) | Generated mTLS credentials, origin trust, peer/network allow lists, Gossip and Plumtree, master schema adoption, peer sync/removal, local-only tables, replicated search | Rows propagate both ways; remote FTS/vector results match; private schema stays local |
| [policies](policies/main.go) | Disconnected counter/set/min/max writes, LWW, observed removal, standalone causal helpers | Counter 7, minimum 9, maximum 91, matching tags and LWW value |
| [retention](retention/main.go) | Minimum age/count, durable collection, stale-peer snapshot catch-up, offline edits, tombstones, ACKs and retirement | History pruned; snapshot retains counter 19 and deletion; retired peer recorded |
| [files](files/main.go) | Streaming encrypted files, remote fetch, search, availability, deletion, standalone Objects, verification, fetched publication, resumed chunks, rotation/collection | Matching downloaded bytes, replicated deletion, correct offsets and new key generation |
| [backup](backup/main.go) | Point-in-time backup, verification, restore, new database/node identities | Snapshot excludes later edits; fresh identities match requested values |
| [keys](keys/main.go) | Data-key rotation, wrapping-key replacement, inventory, reopen | New master key reads intact rows; old key rejected |
| [recovery](recovery/main.go) | Controlled sync failure, read availability, write rejection, retry and startup progress | Failed mutation excluded, original row recovered, Ready progress observed |
| [scheduler](scheduler/main.go) | Shares/debt/reserve configuration, writer classes, admission timeout, ticket metadata/release, borrowed scheduler and metrics | All classes acquire tickets; blocked admission times out; database metrics reflect a durable row |
| [bridge](bridge/main.go) | Real managed Low/High domains, sealed rows/files, snapshot bundles, replay, ownership protection/release | High value protected; release exposes Low shadow; file bytes match across different encryption keys |
| [bridge-transports](bridge-transports/main.go) | Loopback HTTP PUT, FTP passive upload, private-CA FTPS, HTTPS trust restriction | Uploaded sealed bytes match; private-CA HTTPS is rejected |
| [backend](backend/main.go) | Complete custom Backend interface, callback ownership, signer binding, standalone replication, signed record journals, optional batched reads and blocking utility | Record insertion and deletion reach the other backend; batched reads distinguish missing and empty values |
| [protocol](protocol/main.go) | Transaction/schema signing, verification, tamper rejection, postcard, constants/keys, certificate identity, authenticated QUIC streams | Ping/Pong returns 42; signatures verify; changed payload and invalid certificate fail |
| [snapshots](snapshots/main.go) | Signed manifests, donor records, pin boundaries, incomplete/resumed receipt, application, cancellation, expiry | Missing chunks complete the actual payload; cancellation releases outgoing transfer; expired manifest rejected |
| [sessions](sessions/main.go) | Unmanaged SQLite, session attachment, capture enable/disable, changeset/patchset | Disabled capture stays empty; enabled capture produces both formats |

Feature-specific code is in each `main.go`; its `run` function is also exercised by
`TestSmoke`. The small [shared setup package](internal/demo/demo.go) handles
credentials, working directories, cleanup, and bounded waits. The
[custom backend](internal/demo/backend.go) demonstrates actual record application;
it does not substitute success-only mock callbacks.

## Restrictions demonstrated

- Application SQL runs in one managed in-memory SQLite database per database
  instance. Source rows persist/replicate; FTS/vector tables are disposable local
  projections. Call `RefreshIndexes` when this example needs a freshness barrier.
- SQL/statement/row handles belong to their callback and OS thread. Return errors
  to roll back. Do not call `testing.T.Fatal`, `runtime.Goexit`, or reenter database
  methods inside a native callback. Close owned handles explicitly.
- Use explicit SQL column names when schemas may be adopted/reconstructed by a
  peer; do not depend on column ordinal order across that boundary.
- Offline exchange requires the same database identity and compatible migration
  history, with different node identities. Peer certificates and transaction
  signing keys have separate trust configuration. These ephemeral certificates
  and reduced demo timings are not production configuration.
- A subscription `Reset` contains a cursor/reason, not replacement rows. Recreate
  the subscription. Valid resume returns current state, not historical replay;
  expired cursors return an error. The demo uses migration reset as a deterministic
  fallback while also exercising a one-event queue under publication pressure.
- Retention uses age **and** count limits, peer ACK/offline protection, snapshot
  coverage, and transfer pins. Its age=0/count=2 settings intentionally force a
  small demonstration; application code never deletes journal records directly.
- The standalone record backend is explicitly ephemeral, has no SQLite, supports
  a fixed record-only schema, and rejects SQL/ownership/durable-retention work.
  It is not a production Spool replacement. Managed examples use Murmur's own
  authoritative Spool and background workers.
- Bridge HTTP uses the native default HTTPS trust store and exposes no private-CA
  parameter. The local HTTPS rejection is an expected success criterion, not a
  skipped example. FTPS supports a supplied private CA. To demonstrate successful
  HTTPS against a trusted endpoint you control:

  ```sh
  go run ./examples/bridge-transports --https-url https://your-host.example/bundle
  ```

  That optional endpoint must accept PUT. The loopback FTP receiver implements
  only the commands needed for one example upload; it is not a general server.
- All waits have deadlines; blocking native operations are also subject to an
  executable watchdog. A forced timeout may leave temporary artifacts for
  inspection. Normal completion closes handles/listeners before removing data.

## Validate the catalog

```sh
./build.sh test -p 2 -count=1 -timeout 180s ./...
go test -p 2 -race -count=1 -timeout 180s ./examples/...
GOEXPERIMENT=cgocheck2 go test -p 2 -count=1 -timeout 180s ./examples/...
go vet ./...
```

The existing Linux/macOS/Windows workflow runs these smoke tests along with the
client tests. `coverage.json` maps every feature row in the client README to an
executable example; its test catches missing feature groups, broken paths, and
examples without smoke tests. The catalog covers capabilities rather than every
individual configuration field or wire enum variant.

The replication example exercises configurable continuous batches and opt-in adaptive
capture/application in both dissemination modes. The snapshots example also builds
an encoded `Content` page stream, verifies partial visibility after its first durable
installation checkpoint, then resumes to completion. The same scenarios run in their
smoke tests. Standalone snapshot pages in Go are materialized slices; managed database
exports stream from Spool. Final snapshot ACK/progress is withheld until all pages install.

The standalone backend example checks `BackendHandle.Progress` after replication.
Managed backends collect origin heads and watermarks in one serialized Spool read.
