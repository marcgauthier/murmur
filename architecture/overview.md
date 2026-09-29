# SPeD-SQL — Architecture and Implementation Plan

Fast In Memory Secure Peer-Distributed SQL with persistance.

**Status:** Architecture and implementation plan  
**Target language:** Go  
**Primary components:** SQLite + Pebble + HashiCorp memberlist + quic-go
**Replication model:** Masterless, offline-capable, per-column last-writer-wins using HLC  
**Durable source of truth:** Pebble  
**Query/search engine:** SQLite materialization (memory by default), rebuilt from Pebble
**Prepared:** 2026-09-26  
**Storage revision:** Pebble v2.1.6 with authenticated encrypted VFS; new databases only
**Membership revision:** 2026-09-27; SWIM discovery and bounded replication, with all inter-node traffic over QUIC
**Recovery revision:** 2026-09-27; safe snapshot merging/restore identities, range/chunk synchronization, optional Plumtree, and explicit overload control

---

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [1. Goal](#1-goal)
- [2. Important Design Decision: Do Not Use CR-SQLite in Version 1](#2-important-design-decision-do-not-use-cr-sqlite-in-version-1)
- [3. SQLite Role](#3-sqlite-role)
- [4. Go and CGO Boundary](#4-go-and-cgo-boundary)
- [88. Final Target Architecture](#88-final-target-architecture)

---

## 1. Goal

Build a reusable Go package that applications can embed directly. It is not a standalone database server and does not require a separate daemon, HTTP API, or database service.

The package will provide:

- SQL queries through an embedded SQLite engine.
- Very fast local search by keeping the query database in memory and building normal SQL indexes and optional FTS indexes.
- Durable state in Pebble.
- Masterless multi-writer replication between nodes.
- Offline writes on every node.
- Per-column conflict resolution similar in concept to CR-SQLite.
- HLC-based deterministic last-writer-wins merge.
- QUIC replication using `quic-go`.
- SWIM membership using HashiCorp `memberlist`, carried over authenticated QUIC.
- Bounded replication fanout, rotating peers, and periodic anti-entropy with a subset of nodes.
- Optional Plumtree dissemination, resumable transaction chunks, and byte/bandwidth budgets.
- Snapshot recovery that preserves acknowledged offline writes and explicit backup-restore identity rules.
- Encrypted Pebble storage.
- Encryption data-key rotation and package-controlled storage-key rotation.
- Pebble block caching, bounded encrypted-VFS indexes/buffers, and SQL prepared-statement caching.
- Fast startup rebuild from compact current state, not from the complete historical change log.
- Snapshot/bootstrap support for new or very stale nodes.
- Garbage collection of replication logs after they are no longer required.
- A small application-facing Go API.

The core architecture is:

```text
Application
    |
    | Go API / SQL
    v
+----------------------------------------------------+
|                 Embedded Go Package                |
|                                                    |
|  SQL API      Tx Manager      Schema Manager       |
|  HLC/CRDT     Replicator      Encryption Manager   |
|  Cache        Snapshot/GC     Diagnostics          |
+-----------------------+----------------------------+
                        |
             +----------+-----------+
             |                      |
             v                      v
     +---------------+       +---------------+
     |    SQLite     |       |    Pebble     |
     |               |       |               |
     | in-memory     |       | durable state |
     | query tables  |       | mutation log  |
     | indexes       |       | watermarks    |
     | FTS           |       | schema/meta   |
     | disposable    |       | encrypted     |
     +---------------+       +-------+-------+
                                     |
                                     | QUIC + TLS
                                     v
                                  Peers
```

The fundamental rule is:

> Pebble is authoritative. SQLite is a rebuildable materialized query database.

If the in-memory SQLite database disappears, the node recreates it solely from Pebble state.

---

## 2. Important Design Decision: Do Not Use CR-SQLite in Version 1

Do not embed CR-SQLite into the first implementation.

CR-SQLite solves replication inside SQLite. This design already has a separate replication and durable-state layer. Using CR-SQLite would create two overlapping replication systems:

```text
SQLite
   |
CR-SQLite
   |
Custom replication
   |
Pebble
```

Instead, copy the useful CR-SQLite idea:

```text
(table, row, column) -> value + version metadata
```

Implement that model directly in the package.

Benefits:

- Replication format is independent of the SQL engine.
- Pebble remains the authoritative replicated database.
- The SQLite materialization can be dropped and rebuilt.
- The query engine can be replaced later without changing the replication protocol.
- No requirement to make CR-SQLite work with multiple SQLite drivers.
- No duplicated conflict-resolution metadata.
- Easier control of tombstones, snapshots, log retention, node watermarks, and encryption.

CR-SQLite can still be studied as a reference for conflict semantics and testing.

---

## 3. SQLite Role

SQLite is the embedded SQL engine and holds the query-visible materialization
in memory by default. Pebble stores the durable current state and replication history.
SQLite is rebuilt from Pebble during open and can be discarded at any time.

The default build uses `mattn/go-sqlite3` with bundled SQLite. It requires
`sqlite_preupdate_hook` and `sqlite_fts5` build tags. The optional `modernc`
build tag selects the pure-Go `modernc.org/sqlite` driver and builds with
`CGO_ENABLED=0`. Both drivers use a context-aware reader/writer lock: active queries hold a shared
engine read lock, while writes, rebuilds, migrations, and remote apply take the
exclusive engine lock. An open result set delays writes until it is closed, exhausted,
or its query context is canceled. Transaction startup and write admissions respect
context deadlines, aborting without deadlock if reads remain blocked. Abandoned
transactions automatically rollback when their context is canceled.

The query materialization is always in-memory with zero disk footprint.
Pebble remains the authoritative database and is responsible for the
successful-write durability contract.
See [SQLite backends](sqlite-backends.md) for build and validation commands.

---

## 4. Go and CGO Boundary

Pebble and quic-go are Go libraries. The default SQLite driver uses CGO; the
optional modernc driver is pure Go.

Therefore the package will be embedded in Go, but the complete package will not be pure Go.

Use the default CGO build when the mattn driver is desired. Use `-tags modernc`
with `CGO_ENABLED=0` on systems without a C toolchain.

Recommended repository layout:

```text
SPeD-SQL/
    db.go
    config.go
    errors.go
    status.go

    sqlengine/
        engine.go
        connection.go
        capture.go
        preupdate.go
        triggers.go
        schema.go
        rebuild.go
        apply.go
        stmtcache.go
        fts.go

    state/
        store.go
        keys.go
        values.go
        log.go
        watermarks.go
        snapshot.go
        gc.go

    crdt/
        hlc.go
        version.go
        merge.go
        tombstone.go
        delta.go

    replication/
        membership.go
        scheduler.go
        manager.go
        peer.go
        session.go
        handshake.go
        sender.go
        receiver.go
        ack.go
        snapshot.go
        protocol.go

    transport/
        quic.go
        memberlist.go
        pool.go
        tls.go
        certs.go

    crypto/
        algorithms.go
        registry.go
        rotation.go
        encryptedfs.go
        encryptedfile.go
        format.go
        provider.go
        manager.go
        cache.go

    codec/
        codec.go
        mutation.go
        snapshot.go
        protocol.go

    internal/
        ids/
        binary/
        retry/
        testutil/

```

The build tag `modernc` selects the pure-Go driver. The default CGO build uses
the bundled SQLite in mattn/go-sqlite3 and enables pre-update capture plus FTS5
with `sqlite_preupdate_hook sqlite_fts5`.

---

## 88. Final Target Architecture

```text
                         Application
                              |
                    Go package SQL API
                              |
                              v
               +-----------------------------+
               |       Transaction Layer     |
               |                             |
               | write serialization         |
               | TxID                        |
               | HLC                         |
               | sequence                    |
               +-------------+---------------+
                             |
                             v
               +-----------------------------+
               |            SQLite           |
               |                             |
               | in-memory base tables       |
               | secondary indexes           |
               | FTS                         |
               | prepared statements         |
               | pre-update hook             |
               +-------------+---------------+
                             |
                        TxDelta
                             |
                             v
               +-----------------------------+
               |          CRDT Layer         |
               |                             |
               | per-cell LWW                |
               | row tombstones              |
               | deterministic merge         |
               +-------------+---------------+
                             |
                             v
               +-----------------------------+
               |           Pebble            |
               |                             |
               | current cell state          |
               | tombstones                  |
               | per-origin mutation log     |
               | peer watermarks             |
               | TxID receipts               |
               | schema/system metadata      |
               | encrypted VFS/key registry  |
               +-------------+---------------+
                             |
              +--------------+---------------+
              |                              |
              v                              v
      snapshot / rebuild           bounded QUIC replication
                                             |
                              SWIM + TLS 1.3 / mTLS
                                             |
                               +-------------+-------------+
                               |                           |
                               v                           v
                       selected Node B             selected Node C
```

The design intentionally separates:

```text
Search/query performance  -> SQLite
Durability                -> Pebble
Conflict resolution       -> CRDT/HLC layer
Replication transport     -> quic-go
Membership/discovery      -> memberlist SWIM over QUIC
Dissemination/repair       -> bounded peer scheduler and anti-entropy
At-rest protection        -> encrypted Pebble VFS/key manager
```

That separation is the core reason the package can remain embedded, fast, recoverable, and replaceable component-by-component.

---
