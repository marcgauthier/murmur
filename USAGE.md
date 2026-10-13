# Murmur usage and restrictions

Murmur runs application SQL in one in-memory SQLite database. SQLite sessions
capture source-table changes; background workers compress and encrypt them in
Spool and, when configured, replicate them between peers. Spool stores durable
conflict versions and tombstones. Managed FTS and vector indexes are disposable
local projections of source rows in the same SQLite connection.

This guide describes the current Rust API, including optional features. Rust
examples are independent functions unless described as continuations. Paths
opened with `CreateNew`, backup destinations, and restore destinations must not
already exist. Repeated-byte keys and IDs below are demonstration values;
applications must supply their own keys and identities.

## Contents

- [Go client](#go-client)
- [Installation and features](#installation-and-features)
- [Create, write, read, and reopen](#create-write-read-and-reopen)
- [SQL and schema restrictions](#sql-and-schema-restrictions)
- [Migrations and stable identities](#migrations-and-stable-identities)
- [Durability, recovery, and backpressure](#durability-recovery-and-backpressure)
- [FTS5 and vector search](#fts5-and-vector-search)
- [Reactive query subscriptions](#reactive-query-subscriptions)
- [Offline state exchange](#offline-state-exchange)
- [Network replication](#network-replication)
- [Table replication settings](#table-replication-settings)
- [Counters, sets, minimum, and maximum](#counters-sets-minimum-and-maximum)
- [Replication retention and snapshot catch-up](#replication-retention-and-snapshot-catch-up)
- [Encrypted file objects](#encrypted-file-objects)
- [One-way Low-to-High bridge](#one-way-low-to-high-bridge)
- [Backups, restore, and key rotation](#backups-restore-and-key-rotation)
- [Progress, metrics, and writer scheduling](#progress-metrics-and-writer-scheduling)
- [Configuration reference](#configuration-reference)
- [Administration CLI](#administration-cli)
- [Low-level SQLite session helpers](#low-level-sqlite-session-helpers)
- [Error handling and deployment limits](#error-handling-and-deployment-limits)

## Go client

`./clients/go` provides an embedded cgo client for the Rust API, including
replication, vector/FTS search, subscriptions, files, backups/key rotation,
standalone backends, High/Low bridges, transport/protocol/snapshot utilities,
scheduling, and unmanaged SQLite sessions. It enables the Rust `replication`
and `vector` features and uses the same background workers and storage formats.

The [19 runnable Go examples](clients/go/examples/README.md) cover the full feature
catalog, including local replication and High/Low bridges, files, retention,
subscriptions, protocol/snapshot utilities, recovery, scheduling and sessions.
They generate temporary data and credentials and verify results automatically.

```sh
./clients/go/build.sh test -count=1 -timeout 180s ./...
./clients/go/build.sh run ./examples/basic
./clients/go/build.sh run ./examples/replication
./clients/go/build.sh run ./examples/bridge-transports
```

Use `clients/go/build.ps1` on Windows. Native builds require Go 1.24+, Rust,
a C toolchain and libclang. Targets are Linux/macOS amd64 and arm64, and Windows
amd64 with MinGW. Binaries are built locally; pure Go (`CGO_ENABLED=0`) and
server/RPC access are not provided. See [Go build and feature examples](clients/go/README.md),
[API coverage](clients/go/API-COVERAGE.md), and [native ownership rules](clients/go/ABI.md).

Go `Read` and `Transaction` callbacks receive a scoped `Sql`. Statements and
rows must remain on that callback's OS thread and cannot escape its lifetime.
Return callback errors to roll back; ordinary panics become `ErrCallbackPanicked`.
Do not call `runtime.Goexit` or `testing.T.Fatal` inside native callbacks.
Close native handles explicitly; Go garbage collection does not close them.
`DefaultOptions()` obtains current Rust defaults. `Flush()` is the durability
barrier, with the same asynchronous write restrictions as Rust. Examples accept `--dir`, `--keep`, and `--timeout`; working directories must be
new and are removed after closing handles unless retained. Retained demo data
uses ephemeral keys that are not printed or saved. Bridge transport examples
run HTTP/FTP/FTPS locally; generated-CA HTTPS rejection demonstrates the current
absence of a private-CA option in `PublishHTTP`. An optional trusted external
HTTPS URL demonstrates a successful upload. Network helpers
are blocking wrappers over the native runtime; subscription receive supports
context cancellation. Control/codec JSON requires finite real numbers; SQL and
SQL subscriptions use binary values and preserve infinities.

Windows skips directory fsync for Spool, file objects, and bridge/backup
directories; file contents are synchronized, while directory-entry crash
durability follows the underlying platform.

## Installation and features

The Cargo package is `sqlite-session`; Rust imports use `sqlite_session`.
SQLite is bundled. For an application beside this checkout:

```toml
[dependencies]
sqlite-session = { path = "../MURMUR", features = ["replication", "vector"] }
# Only needed if your own code imports rusqlite helpers such as params!:
rusqlite = "0.40.2"
```

| Cargo feature | Capabilities |
| --- | --- |
| Default: no optional features | In-memory SQL, session capture, encrypted Spool, migrations, offline state exchange, table settings, FTS5, subscriptions, backups, keys, metrics, scheduling, admin CLI |
| `vector` | Managed sqlite-vec `vec0` indexes; currently sqlite-vec 0.1.6 |
| `replication` | QUIC peers, signed replication, merge policies, retention, snapshot catch-up, file objects, Low/High bridge |

Enabling `replication` does not start networking by itself: supply
`Options.replication = Some(config)`. `vector` can be used without networking.
See [VENDORS.md](VENDORS.md) for dependency attribution and
[TESTING.md](TESTING.md) for repository validation guidance.

## Create, write, read, and reopen

```rust,no_run
use sqlite_session::{Database, Migration, OpenMode, Options, Result};

fn main() -> Result<()> {
    let key = [7; 32];
    let db = Database::open("./documents-db", key, Options {
        mode: OpenMode::CreateNew,
        ..Options::default()
    })?;
    db.migrate(Migration::new(
        "CREATE TABLE documents(
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT,
            embedding BLOB
        ); CREATE INDEX documents_title ON documents(title);"
    ))?;

    let title = String::from("Murmur");
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO documents(id,title,body) VALUES(?1,?2,?3)",
            (1001_i64, title, "Replicated search"),
        )?;
        Ok(())
    })?;
    let title: String = db.read(|sql| {
        Ok(sql.query_row("SELECT title FROM documents WHERE id=?1", [1001],
            |row| row.get(0))?)
    })?;
    assert_eq!(title, "Murmur");

    db.flush()?; // Source data is durable locally after this succeeds.
    db.close()?;
    let reopened = Database::open("./documents-db", key, Options::default())?;
    reopened.close()?;
    Ok(())
}
```

`transaction` commits the callback's SQL atomically or rolls it back on error.
`read` is read-only. Both callbacks run on the SQLite owner thread and require
`Send + 'static`: move owned parameters into closures and return owned results.
`Sql` provides `execute`, `execute_batch`, `prepare`, `query_row`, and
`last_insert_rowid`; use prepared statements for multiple rows. Returned
statements and SQLite-borrowed values cannot escape a callback. Never call back
into the same `Database` from a SQL callback or perform long external work there.
Cloned `Database` handles share the same database and owner thread.

For replicated integer keys, allocate IDs uniquely across writers. Independent
SQLite-generated row IDs can collide. Ordinary tables can instead use non-null
text keys or composite primary keys.

## SQL and schema restrictions

These rules apply to the managed `Database`, rather than every capability of
unmanaged SQLite:

| Supported | Restricted or rejected |
| --- | --- |
| Tables with declared, non-null primary keys | Tables without primary keys or with nullable primary-key components |
| Single declared `INTEGER PRIMARY KEY` | `AUTOINCREMENT`; changing primary-key identity through migration |
| Composite primary keys and `WITHOUT ROWID` | Updating primary-key values in ordinary transactions |
| Ordinary nonunique indexes | Non-primary-key uniqueness constraints/indexes |
| SQLite queries, joins, aggregates, bound parameters | Application DDL outside `migrate`; application transaction-control SQL |
| Declared source columns and ordinary constraints such as `NOT NULL` | `CHECK`, foreign-key `REFERENCES`, generated/hidden columns, triggers, views |
| Managed FTS5 and optional managed `vec0` | Arbitrary application-created virtual tables or direct writes to managed indexes/shadow tables |
| Controlled SQL through `Sql` | Attaching databases, loading extensions, unsafe functions, modifying connection settings through PRAGMA |

SQLite permits a single `INTEGER PRIMARY KEY` without explicit `NOT NULL`;
Murmur accepts that form. Other primary-key columns must declare `NOT NULL`.
Do not use private `sqlite_` or `_crdt_` names for application objects. Replicated
conflict resolution does not enforce cross-row business invariants; design those
in the application. Values use SQLite's NULL, integer, real, text, and blob types.

## Migrations and stable identities

Use `Database::migrate` for schema changes. Explicit mappings preserve table and
column identities through renames, including node-local replication settings:

```rust,no_run
use sqlite_session::{Database, Migration, Result, TableMapping};

fn rename_documents(db: &Database) -> Result<()> {
    db.set_search_indexes(vec![])?; // Remove definitions referencing old names.
    db.migrate(Migration {
        sql: "ALTER TABLE documents RENAME TO articles;
              ALTER TABLE articles RENAME COLUMN title TO headline;
              ALTER TABLE articles ADD COLUMN category TEXT;".into(),
        mappings: vec![TableMapping {
            old_name: "documents".into(), new_name: "articles".into(),
            columns: vec![("title".into(), "headline".into())],
        }],
    })?;
    println!("{:?}", db.schema()?);
    Ok(())
}
```

Migrations compare before/after schemas and rows, preserve stable identities,
and create durable schema events. They can scan substantial data and pause
application work. Compatible managed indexes are validated and rebuilt; remove
or replace definitions before dropping or renaming indexed sources.

Offline merges require compatible schema history. Network schema adoption is
limited to compatible additive changes, such as new tables, nullable non-key
columns, and nonunique indexes. Coordinate renames, removals, and incompatible
changes explicitly on peers; they are not general automatic SQL migration replay.

## Durability, recovery, and backpressure

| Operation | Guarantee after success |
| --- | --- |
| `transaction` | Application changes committed in local RAM |
| `flush` | Pending source capture and queued persistence are durable in local Spool |
| `refresh_indexes` | Pending local source changes have been incorporated into managed indexes |
| `close` | Managed shutdown, including local persistence flush |

Neither `flush` nor `refresh_indexes` waits for all peers. `flush` is not a search
freshness barrier, and `refresh_indexes` is not a disk durability barrier.

By default, collection happens at a five-second interval or an 8 MiB capture
threshold, between owner-thread jobs. A crash can lose unflushed RAM writes;
the interval is not a strict upper bound if SQL callbacks or workers are delayed.
Use `flush` at application durability boundaries and close explicitly so errors
can be handled. Reopening reconstructs SQLite from durable Spool records.

Batch conflict timestamps are assigned when the session changeset is collected,
before queuing, and survive worker delays/retries. A capture can combine several
transactions. Repeated writes may collapse to a final session change; an insert
followed by delete can disappear. If a delete/reinsert must create a distinct
replicated row generation, collect the deletion first, for example with `flush`.

Queues and transaction sizes are bounded. `Error::Busy` signals pressure or a
changed merge baseline; retry replayable operations with application backoff.
`Error::Limit` requires reducing the operation size or adjusting a valid limit.
Storage failures are visible in `status().storage_error`; repair the cause and
use `retry_storage()` before attempting `flush` again. Do not interpret a storage
failure as proof that an earlier RAM commit was rolled back.

Spool uses zstd compression and authenticated AES-256-GCM encryption. Its default
64 KiB independent blocks keep point reads small; oversized individual records
are isolated and large atomic commits span blocks. A disposable, byte-bounded
hot block cache evicts blocks and wipes decoded buffers. The database, indexes,
captured sessions, and other working buffers still consume RAM; cache and queue
limits do not cap total process memory.

## FTS5 and vector search

Store text and embeddings in ordinary source tables. Only source rows and their
conflict state replicate. Each node builds its own indexes from resolved rows,
including incoming changes and deletions. Index contents, shadow tables, and
definitions are not replicated or authoritative persisted data.

FTS5 works with default Cargo features:

```rust,no_run
use sqlite_session::{Database, Fts5Index, Result};

fn search_text(db: &Database) -> Result<Vec<i64>> {
    // Requires the documents table from the first example.
    db.set_search_indexes(vec![Fts5Index::new(
        "documents_fts", "documents", vec!["title".into(), "body".into()],
    ).into()])?;
    db.refresh_indexes()?;
    db.read(|sql| {
        let mut statement = sql.prepare(
            "SELECT rowid FROM documents_fts
             WHERE documents_fts MATCH ?1 ORDER BY rank"
        )?;
        let rows = statement.query_map(["replicated"], |row| row.get(0))?;
        Ok(rows.collect::<std::result::Result<Vec<i64>, _>>()?)
    })
}
```

For vector search, enable `vector`. This example replaces the definitions with
both FTS5 and a three-dimensional vector index:

```rust,no_run
use sqlite_session::{Database, DistanceMetric, Fts5Index, Result, VectorIndex};

fn search_vectors(db: &Database) -> Result<Vec<(i64, f64)>> {
    let mut vector = VectorIndex::new("documents_vec", "documents", "embedding", 3);
    vector.distance = DistanceMetric::Cosine;
    db.set_search_indexes(vec![
        Fts5Index::new("documents_fts", "documents", vec!["body".into()]).into(),
        vector.into(),
    ])?;
    let embedding: Vec<u8> = [1.0_f32, 0.5, 0.25].into_iter()
        .flat_map(f32::to_le_bytes).collect();
    db.transaction(move |sql| {
        sql.execute("UPDATE documents SET embedding=?1 WHERE id=1001", [embedding])?;
        Ok(())
    })?;
    db.refresh_indexes()?;
    db.read(|sql| {
        let mut statement = sql.prepare(
            "SELECT rowid,distance FROM documents_vec
             WHERE embedding MATCH ?1 AND k=10 ORDER BY distance"
        )?;
        let rows = statement.query_map(["[1.0,0.5,0.25]"],
            |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(rows.collect::<std::result::Result<Vec<(i64, f64)>, _>>()?)
    })
}
```

Search restrictions and operational behavior:

- Sources need exactly one declared `INTEGER PRIMARY KEY`; FTS columns must be
  declared `TEXT`, vector columns `BLOB`. Managed search does not support text or
  composite keys, even though ordinary replicated tables do.
- Embeddings contain exactly `dimensions * 4` bytes of finite little-endian
  float32 values. Dimensions must be 1–8192. NULL excludes a row from vector
  search. Murmur does not generate embeddings.
- Vector column names are ASCII identifiers beginning with a letter, followed
  by letters, digits, or underscores. Configure source names as declared.
- Distances are L2 (default), cosine, or L1. For the current sqlite-vec version,
  use ascending `ORDER BY distance`; do not combine `k=...` with `LIMIT` in the
  same nearest-neighbor query. Queries may bind a vector BLOB or JSON vector.
- FTS tokenizers are `unicode61` (default), `ascii`, `porter`, or `trigram`.
  FTS stores its own content, adding memory beyond the source table.
- Indexes are managed in SQLite's TEMP schema with in-memory temp storage.
  Application SQL cannot mutate them or their shadow tables. Names must not
  collide with sources, other indexes, or reserved/shadow names.
- Supply `Options.search_indexes` on **every open**, including restored stores.
  Existing databases rebuild before open completes. `CreateNew` can declare
  indexes before their source tables are created or adopted.
- `set_search_indexes` replaces definitions atomically and rebuilds; an empty
  list removes them. Its definitions are local runtime configuration.
- Normal queries can lag behind source changes. Maintenance runs in bounded
  batches between SQL jobs after collection; queue overflow requests a rebuild.
  Startup/reconfiguration/rebuild work can pause SQL execution.
- Inspect `status().search_indexes` for readiness, rebuilds, pending work, and
  errors. Failed or rebuilding indexes reject reads; source writes remain
  available after runtime indexing failures. Repair source data and refresh or
  reconfigure. An invalid index at startup can fail open; reopen without that
  definition to repair it. Barriers can return `Busy` under contention.

## Reactive query subscriptions

Subscriptions re-run a query after relevant captured commits and publish owned
rows and keyed diffs. Use stable, unique row keys:

```rust,no_run
use sqlite_session::{Database, Result, SubscriptionEvent, SubscriptionOptions};

fn watch_documents(db: &Database) -> Result<()> {
    let mut subscription = db.subscribe(
        |sql| {
            let mut statement = sql.prepare("SELECT id,title FROM documents ORDER BY id")?;
            let rows = statement.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
        },
        |row: &(i64, String)| row.0.to_be_bytes().to_vec(),
        SubscriptionOptions { tables: vec!["documents".into()], ..Default::default() },
    )?;
    match subscription.recv()? {
        SubscriptionEvent::Initial { rows, cursor } => println!("{cursor:?}: {rows:?}"),
        SubscriptionEvent::Update { rows, changes, cursor } =>
            println!("{cursor:?}: {rows:?}, {changes:?}"),
        SubscriptionEvent::Reset { reason, .. } => println!("Resubscribe: {reason}"),
    }
    subscription.close();
    Ok(())
}
```

`RowChange` reports `Added`, `Updated`, or `Removed`. The default buffer holds 32
events; unchanged results are suppressed unless `emit_unchanged` is enabled.
An empty `tables` filter watches all tables. Watch an index name for notifications
when managed index maintenance publishes fresh results.

`recv_timeout` and `try_recv` return `Busy` for timeout/empty respectively.
Overflow, schema changes, and shutdown can reset a stream; recreate it and read
a fresh snapshot. `resume_from: Some(cursor)` validates the database, epoch, and
sequence, but is not durable historical event replay. Reopening changes the
subscription generation. Notifications are based on collected changes, not
every SQL statement, and multiple updates may coalesce. Index publication can
reuse a source sequence, so sequence alone is not a search-freshness token.

## Offline state exchange

Without a network transport, applications can exchange `ReplicaState` directly:

```rust,no_run
use sqlite_session::{Database, Result};

fn exchange(a: &Database, b: &Database) -> Result<()> {
    let state = a.export_state()?;
    b.merge_state(state)?;
    b.flush()?;
    a.merge_state(b.export_state()?)?;
    a.flush()?;
    Ok(())
}
```

Peers must share a database identity and compatible schema history, and have
distinct node identities. Use restore with a fresh node ID to seed a replica.
`ReplicaState` implements serde serialization; the application supplies transport
and authentication. It is a whole-state interchange, not the network protocol's
signed log/snapshot format. Export flushes local persistence and filters disabled
tables' row data and tombstones. It does not export managed indexes or encrypted
file-object payloads, and is not a substitute for exchanging causal policy
records through the configured replication transport.

Default conflicts resolve per column by ordered hybrid-clock versions:
physical milliseconds, logical counter, then node ID. Concurrent edits to
different columns can survive together; competing edits to one column have a
deterministic winner. Row generations and durable deletion tombstones prevent
old updates from reviving deleted generations. This is eventual convergence,
not distributed transactions or linearizable reads. Merge can return `Busy`
when local work changes its baseline; the caller can retry.

## Network replication

Requires `replication`. Provision a TLS certificate chain and private key per
node, trusted CA roots, and a separate Ed25519 signing seed. Certificates identify
nodes using a URI SAN `replicateddb://node/<32 hexadecimal node-id characters>`.
Use a distinct certificate/node ID for each active replica and never change the
signing key under an existing node ID.

```rust,no_run
use sqlite_session::{Database, OpenMode, Options, replication};

fn open_peer(
    path: &str, storage_key: [u8; 32], database_id: [u8; 16], node_id: [u8; 16],
    credentials: replication::Credentials, signing_seed: [u8; 32],
    peer_id: [u8; 16], peer_signing_public: [u8; 32],
) -> sqlite_session::Result<Database> {
    let mut config = replication::Config::new(
        "127.0.0.1:7400".parse().unwrap(), credentials, signing_seed,
    );
    config.trusted_origins.insert(peer_id, peer_signing_public);
    config.allowed_peers.insert(peer_id);
    config.trusted_snapshot_sources.insert(peer_id);
    config.bootstrap.push("127.0.0.1:7401".parse().unwrap());
    Database::open(path, storage_key, Options {
        mode: OpenMode::CreateNew,
        database_id: Some(database_id), node_id: Some(node_id),
        replication: Some(config), ..Options::default()
    })
}
```

`credentials.chain` is a leaf-first vector of DER certificates,
`credentials.private_key` is DER private-key bytes wrapped with `.into()`, and
`credentials.roots` is a vector of DER CA certificates. Generate the peer signing
public key with `replication::protocol::public_key(&peer_signing_seed)` during
provisioning. On the other node, swap ports, identities, credentials and trust
entries, while retaining the shared `database_id`. On reopen use `Existing`.

The handle from `db.replication()` supports `add_peer(address)`,
`remove_peer(node_id)`, `force_sync()`, `status()`, and `close()`. `force_sync`
requests anti-entropy work; it is not an all-peer durability barrier. Database
shutdown manages the attached transport. Bootstrap and membership discover
peers; `advertise` sets the address announced instead of the bind address.

Configure these controls before open:

- `trusted_origins` pins signing public keys, including origins relayed by another
  peer. TLS trust and origin signing trust are separate.
- Nonempty `allowed_peers` and `allowed_networks` restrict connections by node
  and network; empty collections do not impose those additional restrictions.
- `dissemination` selects `Gossip` or `Plumtree`; `membership` controls membership
  exchange. Fanout, repair sessions, queues, and connection limits bound work.
- `accept_remote_schema` controls remote adoption. To appoint one schema authority,
  set `schema_master = Some(master_node)` on peers, and `master = true` on that
  node with its own ID as `schema_master`. Data writes remain masterless.
- `trusted_snapshot_sources` explicitly allows snapshot donors. The default
  empty set does not authorize automatic snapshot imports.

Application commits do not wait for network I/O. Incoming application waits its
turn on the single SQLite owner. Network queues can report `Overloaded` or
transport/authentication errors; inspect replication diagnostics. Peers need
compatible protocol versions and schema configuration. The current signed
snapshot protocol is version 9; version 8 peers cannot exchange current traffic.

## Table replication settings

```rust,no_run
use sqlite_session::{Database, Migration, Result};

fn private_table(db: &Database) -> Result<()> {
    db.migrate(Migration::new("CREATE TABLE private_notes(id INTEGER PRIMARY KEY, note TEXT)"))?;
    db.set_replication("private_notes", false)?;
    assert!(!db.replication_enabled("private_notes")?);
    db.transaction(|sql| {
        sql.execute("INSERT INTO private_notes VALUES(1,'Local only')", [])?;
        Ok(())
    })?;
    db.flush()?;
    // db.SetReplication("private_notes", true)?; // Alias, when sharing is desired.
    Ok(())
}
```

Settings are node-local, durable, and keyed by stable table IDs. Missing entries
mean enabled; migrations and recovery preserve them. Local SQL and persistence
continue normally, and local search indexes still include local-only rows.
Background incoming/outgoing paths filter row data, tombstones, and table policy
records; network schema publication also filters disabled table definitions.
Set policies on every relevant node. Disabling replication does not erase data
already delivered, recall exported artifacts, or retroactively make shared data
private. Re-enabling is not a guarantee that previously ignored incoming changes
will replay automatically.

## Counters, sets, minimum, and maximum

Requires `replication` and a configured replication instance. Configure policies
for existing source columns using `config.merge_policies` before reopening:

```rust,no_run
use sqlite_session::replication::{Config, policies::Policy};

fn configure_policies(config: &mut Config) {
    for (column, policy) in [
        ("count", Policy::Counter), ("tags", Policy::Set),
        ("lowest", Policy::Min), ("highest", Policy::Max),
    ] {
        config.merge_policies.insert(("stats".into(), column.into()), policy);
    }
}
```

First create and persist this schema with ordinary options, then reopen with the
policy configuration. Use the same policies on peers:

```sql
CREATE TABLE stats(
    id INTEGER PRIMARY KEY, count INTEGER NOT NULL,
    tags BLOB, lowest INTEGER NOT NULL, highest INTEGER NOT NULL
);
INSERT INTO stats VALUES(1, 0, NULL, 100, 0);
```

Operate through explicit intents in a transaction:

```rust,no_run
use sqlite_session::{Database, Result, Value};

fn update_stats(db: &Database) -> Result<()> {
    db.transaction(|sql| {
        let key = vec![Value::Integer(1)];
        sql.counter_add("stats", key.clone(), "count", 3)?;
        sql.set_add("stats", key.clone(), "tags", Value::Text("rust".into()))?;
        sql.set_remove("stats", key.clone(), "tags", Value::Text("old".into()))?;
        sql.min("stats", key.clone(), "lowest", Value::Integer(12))?;
        sql.max("stats", key, "highest", Value::Integer(90))?;
        Ok(())
    })
}
```

Ordinary UPDATE assignments to policy-controlled columns are rejected. Policies
cannot target primary keys. Counter projections require INTEGER and reject
overflow. Set projections are NULL or a postcard-encoded `Vec<Value>` BLOB; use
`replication::decode::<Vec<Value>>(&blob)` to read them. Sets track observed adds
and removals rather than treating concurrent whole-set assignments as LWW.
Min/max require comparable values. The row must exist, keys must match declared
primary-key order, and at most 10,000 policy intents fit one transaction. Causal
state lives in Spool; no policy metadata tables or triggers are added to SQLite.

## Replication retention and snapshot catch-up

Requires configured `replication`. Example maintenance for an open peer:

```rust,no_run
use sqlite_session::{Database, Result};

fn inspect_retention(db: &Database) -> Result<()> {
    println!("{:?}", db.retention_status()?);
    println!("Removed {} retained records", db.collect_retention()?);
    Ok(())
}
```

Background cleanup enforces **both** minimum persistence age (`retention`, default
one hour) and minimum retained log count (`minimum_retained`, default 1000).
Peer ACKs, retirements, ages, snapshot coverage, and duplicate floors are durable
Spool records. Unretired peers within `maximum_offline` (default seven days),
pending bridge exports, and active snapshot transfers protect needed history.
Cleanup publishes durable snapshot coverage before deleting logs/receipts and
advancing duplicate floors. CRDT tombstones remain.

`db.retire_peer(node_id)` durably retires that identity and rejects its future
connections; use it only for a permanently removed replica. `remove_peer` on the
network handle disconnects a peer without making that retention decision.
Retention is not a hard storage quota or backup rotation policy.

If retained logs cannot cover a peer's gap, an authorized snapshot donor provides
a signed, resumable snapshot. Transfers use 64 KiB chunks, durable staging, and
bounded pages; the receiver merges concurrent local changes rather than replacing
its entire database. Completion must be durable before ACK. Snapshot payload and
per-direction aggregate staging budgets default to 512 MiB, with a seven-day
fixed transfer lifetime. Larger states require a different seeding strategy such
as backup/restore. File metadata can travel with catch-up; object payloads are
fetched separately. Snapshot catch-up needs an explicitly trusted donor.

## Encrypted file objects

Requires `replication` and `config.files`. Payloads are separate encrypted objects,
not SQLite BLOB rows. Configure their directory, key, and QUIC fetch address:

```rust,no_run
use sqlite_session::{FileConfig, replication::Config};

fn configure_files(config: &mut Config, object_key: [u8; 32]) {
    config.files = Some(FileConfig::new(
        "./objects".into(), object_key, "127.0.0.1:7500".parse().unwrap(),
    ));
}
```

```rust,no_run
use sqlite_session::{Database, Result};

fn manage_files(db: &Database) -> Result<()> {
    let metadata = db.upload_file("reports/example.txt", &b"Report contents"[..])?;
    println!("{:?}", metadata);
    println!("{:?}", db.list_files()?);
    println!("{:?}", db.search_files("report")?);
    println!("{:?}", db.file_metadata("reports/example.txt")?);
    println!("{:?}", db.file_availability("reports/example.txt")?);
    if db.file_available("reports/example.txt")? {
        let mut bytes = Vec::new();
        db.get_file("reports/example.txt", &mut bytes)?;
        assert_eq!(bytes, b"Report contents");
    }
    db.delete_file("reports/example.txt")?;
    Ok(())
}
```

Names are logical metadata keys; `search_files` searches names, not file contents.
Replicated metadata can arrive before a payload is available. Availability is
`Local`, `Remote(peer_ids)`, or `Unavailable`. Background fetching uses configured
peers and resumable staging. Objects are authenticated in 64 KiB chunks with a
whole-file length/digest check; a failing read can already have written earlier
chunks to the provided output, so publish an output file only after success.

File defaults: generation 0, maximum file size 4 GiB, staging 1 GiB, two concurrent
fetches, fetch interval 30 seconds. File and staging limits differ; advertised size
alone does not guarantee a transfer will fit available staging. Files have a
separate key lifecycle from Spool. To rotate, reopen with `generation + 1`, a new
`key`, and the old `previous_key`, then use
`db.replication().unwrap().files().unwrap().rotate_keys(batch_limit)` and inspect
`db.file_key_rotation_status()`. Rotate one generation at a time; local
`previous_key_unused_locally` does not prove every peer has finished rotation.
`Objects::collect(grace)` collects unreferenced old objects after a grace period.

Database backups include durable file metadata, **not the external object
directory**. Preserve that directory and the relevant object keys separately.

## One-way Low-to-High bridge

Requires `replication` and `config.bridge`. This transports signed, compressed,
recipient-encrypted bundles from a Low database domain to a distinct High domain.
Provision matching schemas/stable mappings and trusted origin keys on the receiver.
Configure the two sides before opening their databases:

```rust,no_run
use sqlite_session::replication::{Config, bridge};
use std::collections::{BTreeMap, BTreeSet};

fn configure_bridge(
    low: &mut Config, high: &mut Config,
    low_database: [u8; 16], high_database: [u8; 16], low_node: [u8; 16],
    low_signing_seed: [u8; 32], receiver_secret: [u8; 32],
) {
    let public = sqlite_session::replication::protocol::public_key(&low_signing_seed);
    low.bridge = Some(bridge::Config::exporter(
        low_database, "./low-outbox".into(), "feed".into(),
        low_signing_seed, bridge::recipient_public(receiver_secret),
    ));
    high.bridge = Some(bridge::Config::receiver(
        high_database, "./high-inbox".into(), "feed".into(), receiver_secret,
        BTreeSet::from([low_database]), BTreeMap::from([(low_node, public)]),
        BTreeMap::from([(low_node, public)]),
    ));
}
```

The attached `Bridge` supports `capture()` for Low exports and `process()` for
High inbox imports; background replication also services configured bridges.
Copy completed `.bundle` artifacts from the outbox to the inbox with your delivery
mechanism. `export_snapshot()` creates a snapshot artifact, and `stage(path)`
authenticates/stages an incoming artifact. HTTP PUT and FTP/FTPS publishing are
also available:

```rust,no_run
use sqlite_session::replication::{bridge::{Bridge, FtpOptions}, Result};
use std::path::Path;

async fn publish(bridge: &Bridge, artifact: &Path, pinned_ca_pem: Vec<u8>) -> Result<()> {
    bridge.publish_http(artifact, "https://receiver.example/inbox/feed.bundle").await?;
    let tls = FtpOptions { roots_pem: pinned_ca_pem, ..Default::default() };
    bridge.publish_ftp(artifact, "ftps://user:password@receiver.example/inbox/", Some(&tls)).await?;
    Ok(())
}
```

Run async publishers within a Tokio runtime. FTPS requires configured CA trust;
plain FTP/HTTP are supported transport forms but do not encrypt the transport.
Bundles themselves remain recipient encrypted and signed. Bridge roles enforce
the direction; this is not bidirectional peer replication.

High local edits establish ownership and shield those cells from later Low
updates, while Low shadow state is retained. On the High bridge, release ownership
for a row or column using an encoded primary key:

```rust,no_run
use sqlite_session::{Value, replication::{self, bridge::Bridge}};

fn release_title(bridge: &Bridge) -> replication::Result<bool> {
    let key = replication::encode(&vec![Value::Integer(1001)])?;
    bridge.release("documents", key, Some("title".into()))
}
```

Passing `None` releases row ownership. Low/High policy state is scoped to its
domain/branch, not blindly added to High-owned counters or sets. Sequence checks
and receipts prevent replay; trust the source database, bundle signer, and
embedded origins. Default bundle limit is 16 MiB, decoded payload 64 MiB, staging
1 GiB. Large file payloads can be chunked into recipient-sealed artifacts and
re-encrypted under the receiver's object key; the logical file must fit bridge
staging. An export's retained-history pin can prevent log cleanup until delivery
is resolved. `export_file(name)` explicitly exports an available object.

## Backups, restore, and key rotation

```rust,no_run
use sqlite_session::{Database, RestoreOptions, Result};

fn backup_and_seed(db: &Database, key: [u8; 32], new_node: [u8; 16]) -> Result<()> {
    db.backup("./backup-001")?;
    Database::verify_backup("./backup-001", key)?;
    Database::restore_with_options("./backup-001", "./seeded-peer", key, RestoreOptions {
        new_database_id: None, new_node_id: Some(new_node),
    })?;
    Ok(())
}
```

`backup` flushes and takes a consistent Spool checkpoint while the managed
database is open; later commits are not part of that checkpoint. Same-filesystem
checkpoints use hardlinks; cross-filesystem backups copy through a temporary
checkpoint. `verify_backup` checks without modifying the backup. Both
`Database::restore` and `restore_with_options` require a new destination.
Plain `restore` preserves identities. A peer clone needs the same database ID
and a **fresh node ID**, matching its provisioned TLS identity; an independent
database clone can also choose `new_database_id`. Do not run two active replicas
with one node identity. Supply local index/network/file settings on reopen.

Backup rotation is application-managed: choose distinct checkpoint directories
and remove old ones according to your retention policy. Keep the master key that
can open each backup; later key rotations do not update old backups.

```rust,no_run
use sqlite_session::{Database, Result};

fn rotate_storage_keys(db: &Database, new_master: [u8; 32]) -> Result<()> {
    db.rotate_data_key()?;
    println!("{:?}", db.key_inventory());
    db.rotate_master_key(new_master)?;
    db.flush()?;
    // Subsequent opens of this live store use new_master.
    Ok(())
}
```

Data-key rotation selects a new key for subsequent Spool writes. Master-key
rotation rewraps the key registry instead of rewriting every encrypted block.
Inventory reports active/retired keys; retired keys can remain necessary to read
old blocks. `data_key_max_age` optionally enables age-based rotation. Encryption
protects persisted data; application rows are plaintext in process memory.

## Progress, metrics, and writer scheduling

Startup reconstructs authoritative rows and configured local indexes. Observe
the actual progress fields rather than assuming a percentage or known total:

```rust,no_run
use sqlite_session::{Database, Options, Result};
use std::sync::Arc;

fn open_with_progress(path: &str, key: [u8; 32]) -> Result<Database> {
    Database::open(path, key, Options {
        on_open_progress: Some(Arc::new(|p| {
            eprintln!("{:?}: items={}, rows={}, table={:?}, elapsed={:?}, error={:?}",
                p.phase, p.processed_items, p.rows_inserted,
                p.current_table, p.elapsed, p.error);
        })),
        ..Options::default()
    })
}
```

Phases are `Opening`, `Rebuilding`, `Indexing`, `Finalizing`, `Ready`, `Failed`,
and `Cancelled`. `open_progress()` returns a snapshot. Progress callbacks must
return promptly and must not reenter the database.

`status()` reports committed/durable cursors, captured pending bytes, storage
errors, checkpoint pause, closed state, progress, search status, scheduler state,
and optional replication diagnostics. `pending_bytes` measures captured queued
payload, not the still-active SQLite session. `sqlite_metrics()` exposes session
memory/changeset size, SQLite pages and memory, and source row counts; it runs on
the owner thread and can scan tables, so avoid excessive polling. `spool_metrics()`
reports storage/cache/record/compression/flush/compaction activity.

```rust,no_run
use sqlite_session::{Options, SchedulerConfig};
use std::time::Duration;

fn writer_options() -> Options {
    Options {
        scheduler: SchedulerConfig {
            local_share: 80, remote_share: 20,
            max_debt: Duration::from_secs(1), maintenance_reserve_every: 100,
        },
        ..Options::default()
    }
}
```

Writer admission defaults to 90% local and 10% remote service, with bounded debt
and a maintenance reservation every 100 grants. Idle classes can lend capacity.
Shares must be positive and sum to 100; debt bound and reservation interval must
be nonzero. The scheduler cannot preempt a running SQL callback and shares are
not a latency guarantee. Inspect `db.scheduler().snapshot()` for class wait,
grants, service, and debt. The public `WriterScheduler`, `WriterClass`, and `Ticket`
APIs also support standalone admission and timed admission; the managed database
already obtains its own tickets, so do not wrap its calls in another admission
on that same scheduler. Closing that scheduler shuts down writer admission.

## Configuration reference

Supply operational options on each open. MiB/KiB below are binary units.

| `Options` field | Default | Purpose / constraint |
| --- | --- | --- |
| `mode` | `Existing` | Use `CreateNew` only for new storage |
| `database_id`, `node_id` | `None` | Create identities or validate supplied identities; peers share database ID, differ in node ID |
| `capture_interval` | 5 s | Nonzero collection interval between owner jobs |
| `capture_bytes` | 8 MiB | Positive capture threshold, at most queue capacity |
| `queue_bytes` | 64 MiB | Captured persistence queue; maximum 128 MiB |
| `transaction_bytes` | 64 MiB | Positive transaction budget, at most queue capacity |
| `segment_bytes` | 256 MiB | Segment target, greater than 64 MiB |
| `spool_block_bytes` | 64 KiB | Positive independent block target, maximum 64 MiB |
| `spool_cache_bytes` | 32 MiB | Byte-bounded hot cache; zero disables |
| `snapshot_deltas` | 128 | Positive persistence snapshot threshold |
| `snapshot_bytes` | 256 MiB | Positive persistence snapshot byte threshold, distinct from network snapshot limit |
| `compression_level` | 3 | zstd compression level |
| `storage_workers` | 2 | Positive background storage worker count |
| `data_key_max_age` | `None` | Optional automatic Spool data-key rotation age |
| `search_indexes` | Empty | Node-local definitions, supplied on every open |
| `search_queue_bytes` | 8 MiB | Positive disposable work budget, maximum 128 MiB |
| `search_batch_rows` | 256 | Maintenance turn rows, 1–65536 |
| `search_interval` | 100 ms | Nonzero index maintenance interval |
| `scheduler` | 90/10, 1 s debt, every 100 grants | Writer service configuration |
| `replication` | `None` | Optional network config, requires Cargo feature |
| `on_open_progress` | `None` | Optional short synchronous progress callback |
| `fault_hooks` | `None` | Failure injection for testing, not a production feature |

`replication::Config::new` defaults and limits:

| Setting | Default / restriction |
| --- | --- |
| Trust, bootstrap | Empty trust maps/allowlists/bootstrap; provision credentials and origin trust |
| Membership / dissemination | Enabled / `Gossip` (`Plumtree` also available) |
| Remote schema / authority | Adoption enabled; `master=false`, `schema_master=None` |
| Fanout / repairs | 4 / 2; positive, combined at most sessions |
| Sessions / connections | 32 / 64; at most 4096 each, connections at least sessions + 8 |
| Queue / per-peer queue | 64 MiB / 4 MiB; total at most 128 MiB, peer at least 1 MiB and at most total |
| Staging / transfers | 256 MiB / 4096; staging at least maximum transaction, transfers positive |
| Maximum transaction | 64 MiB; positive and at most 64 MiB |
| Snapshot payload / aggregate staging | 512 MiB each; positive and at most 512 MiB each |
| Snapshot transfer lifetime | Seven days; positive, not renewed indefinitely by reconnect |
| Incoming interval / anti-entropy | 1 s / 10 s; positive |
| Retention / cleanup interval | One hour / 60 s; cleanup interval positive |
| Minimum retained / offline window | 1000 / seven days; positive |
| Policies, files, bridge | Empty / `None` / `None`; configure explicitly |

Network and local queue budgets are separate. These limits bound particular work
buffers; they do not remove SQLite's memory requirement or guarantee a hard
on-disk size limit. Spool also imposes record/atomic-operation limits; oversized
operations must be split rather than bypassed by raising one unrelated budget.

## Administration CLI

Build with `cargo build --bin murmur-admin`. Commands accept a storage directory,
not a `.sqlite` file. Set `MURMUR_KEY_HEX` in your shell to the 64-character hex
encoding of the store's 32-byte key before running these commands:

```sh
./target/debug/murmur-admin status --key-hex="$MURMUR_KEY_HEX" ./documents-db
./target/debug/murmur-admin inspect --json --key-hex="$MURMUR_KEY_HEX" ./documents-db
./target/debug/murmur-admin keys --key-hex="$MURMUR_KEY_HEX" ./documents-db
./target/debug/murmur-admin verify --key-hex="$MURMUR_KEY_HEX" ./documents-db
./target/debug/murmur-admin backup --key-hex="$MURMUR_KEY_HEX" ./documents-db ./backup-002
./target/debug/murmur-admin verify --backup --key-hex="$MURMUR_KEY_HEX" ./backup-002
./target/debug/murmur-admin restore --fresh-node-id --key-hex="$MURMUR_KEY_HEX" ./backup-002 ./restored-db
./target/debug/murmur-admin --help
./target/debug/murmur-admin --version
```

`status` reports cursors/health; `inspect` summarizes schema and storage; `keys`
shows key inventory; `verify` validates recovery; `backup` creates a checkpoint;
`restore` recreates storage. `--json` enables machine-readable output.
Restore supports `--fresh-node-id[=<32 hex characters>]` and
`--fresh-db-id[=<32 hex characters>]` for generated or explicit identities.
Use `Database::backup` inside a running application for live backups; coordinate
CLI access with the store's exclusive lock rather than opening the same live
store independently.

`--passphrase=...` is also accepted and derives a key with BLAKE3; this is not a
password-hardening KDF. With neither key option, the CLI uses a public demo key.
Always provide the correct explicit key for real storage. CLI opens do not supply
your application's node-local search or network configuration.

## Low-level SQLite session helpers

For unmanaged local session use:

```rust,no_run
fn capture_sqlite_changes() -> rusqlite::Result<()> {
    use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
    let connection = sqlite_session::open_in_memory()?;
    connection.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY, title TEXT)")?;
    let mut session = sqlite_session::start_session(&connection)?;
    connection.execute("INSERT INTO items VALUES(1,'Captured')", [])?;
    let changeset = session.changeset()?;
    assert!(changeset.iter()?.next()?.is_some());
    Ok(())
}
```

These return rusqlite connection/session types and only provide local SQLite
operations and session capture. They do not start managed persistence, conflict
resolution, subscriptions, or networking. Sessions need declared primary keys
and do not capture virtual tables. Use `Database` for Murmur's managed guarantees.

## Error handling and deployment limits

| Error | Typical response |
| --- | --- |
| `Sql` | Fix SQL, bindings, constraints, or prohibited access |
| `Invalid` | Fix configuration, definitions, or unsupported operation |
| `WrongKey` | Supply the correct master key; do not recreate the store |
| `Corrupt` | Investigate storage/version integrity and restore a verified backup if needed |
| `SchemaMismatch` | Align database identity and schema history |
| `Busy` | Back off and retry replayable work; reduce pressure |
| `Limit` | Split work or adjust a permitted budget |
| `Storage` / `Io` | Repair storage, inspect status, retry persistence |
| `Closed` | Stop using the closed handle |
| `CallbackPanicked` | Fix the callback; its transaction was rolled back |

The whole active database and its search projections must fit available RAM.
SQLite work is serialized; long queries, callbacks, schema scans, or rebuilds can
delay other callers. Background persistence and networking cannot make SQLite
callbacks parallel or preempt them. There is no automatic embedding service,
distributed SQL transaction, all-peer commit barrier, or arbitrary virtual-table
replication. Derived FTS/vector indexes work by replicating ordinary source data.
Backups, object directories, keys, certificate provisioning, peer trust, and
application-level retry/business rules remain deployment responsibilities.
