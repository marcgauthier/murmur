# SQLite backends

The query materialization uses an in-memory SQLite database with zero disk
footprint and is rebuilt from authoritative Pebble state whenever a node
opens. Choose one of two Go drivers at build time:

- Default CGO build: `github.com/mattn/go-sqlite3`, using its bundled SQLite.
- Pure-Go build: `modernc.org/sqlite`, selected with `-tags modernc` and
  `CGO_ENABLED=0`.

Both backends use a context-aware reader/writer engine lock. A query holds the engine read
lock until its rows are closed, exhausted, or its context is canceled; a write waits for active queries,
and new queries wait while a write is running. Transaction startup and write admissions respect
context cancellation and deadlines, unblocking without deadlocks. Pebble remains the sole durable
source of truth.

## Build and test

The mattn driver needs SQLite's pre-update hook for replication capture and
FTS5 for the optional full-text index. Use the bundled SQLite build and enable
both features with Go build tags:

```sh
CGO_ENABLED=1 go build -tags 'sqlite_preupdate_hook sqlite_fts5' ./...
CGO_ENABLED=1 go vet -tags 'sqlite_preupdate_hook sqlite_fts5' ./...
CGO_ENABLED=1 go test -tags 'sqlite_preupdate_hook sqlite_fts5' ./...
CGO_ENABLED=1 go test -race -tags 'sqlite_preupdate_hook sqlite_fts5' ./...
```

Use modernc without CGO:

```sh
CGO_ENABLED=0 go build -tags modernc ./...
CGO_ENABLED=0 go vet -tags modernc ./...
CGO_ENABLED=0 go test -tags modernc ./...
```

No external SQLite library is linked. The Pebble persistent format
and replication wire protocol do not depend on which SQLite driver is selected.

## Capture and concurrency

The mattn build enables `RegisterPreUpdateHook` with the
`sqlite_preupdate_hook` tag. Its bundled SQLite is compiled with
`SQLITE_ENABLE_PREUPDATE_HOOK`. The modernc driver provides its own pre-update
hook API. Both implementations capture insert, update, and delete values and
pass them through the same transaction coalescing and Pebble commit path.

The query database uses a named shared-cache in-memory SQLite database with
a reserved write connection and a pool of read connections. The engine-level
read/write lock ensures readers see a stable materialization while writes,
rebuilds, migrations, and remote bulk apply run. This model does not provide
MVCC readers that continue while a write is in progress. The pool is capped at
32 connections, including the reserved writer.

## Supported builds

| Build | Driver | Requirements | SQL materialization |
|---|---|---|---|
| Default | mattn/go-sqlite3 | CGO, `sqlite_preupdate_hook sqlite_fts5` tags | In-memory |
| Optional | modernc.org/sqlite | `modernc` tag, `CGO_ENABLED=0` | In-memory |

The optional modernc build is the supported cross-platform path for targets
where a CGO compiler is unavailable. The default CGO backend is validated on
Linux amd64; other targets require separate validation before being claimed as
supported.

## Query store and security

`Config.QueryStore` configures remote-apply batching into the in-memory
materialization; the driver selection is independent:

| Field | Behavior / default |
|---|---|
| `RemoteApplyInterval` | Batches durable remote changes into one SQLite transaction; zero selects 1s. |
| `RemoteApplyMaxTransactions` | Flushes when this many received transactions are queued; zero selects 1,000. |

The query view is always an in-memory SQLite database: it leaves no
query files on disk, creates no temporary query directories, and is
rebuilt from authoritative encrypted Pebble state on every open.
