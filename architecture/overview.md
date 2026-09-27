# SPeD-SQL — Architecture and Implementation Plan

Fast In Memory Secure Peer-Distributed SQL with persistance.

**Status:** Architecture and implementation plan  
**Target language:** Go  
**Primary components:** LumoSQL (LMDB MVCC backend) + Pebble + HashiCorp memberlist + quic-go  
**Replication model:** Masterless, offline-capable, per-column last-writer-wins using HLC  
**Durable source of truth:** Pebble  
**Query/search engine:** LumoSQL materialized database with LMDB MVCC backend (disposable query cache)  
**Prepared:** 2026-09-26  
**Storage revision:** Pebble v2.1.6 with authenticated encrypted VFS; new databases only
**Membership revision:** 2026-09-27; SWIM discovery and bounded replication, with all inter-node traffic over QUIC
**Recovery revision:** 2026-09-27; safe snapshot merging/restore identities, range/chunk synchronization, optional Plumtree, and explicit overload control

---

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [1. Goal](#1-goal)
- [2. Important Design Decision: Do Not Use CR-SQLite in Version 1](#2-important-design-decision-do-not-use-cr-sqlite-in-version-1)
- [3. LumoSQL Role](#3-lumosql-role)
- [4. Go and CGO Boundary](#4-go-and-cgo-boundary)
- [88. Final Target Architecture](#88-final-target-architecture)

---

## 1. Goal

Build a reusable Go package that applications can embed directly. It is not a standalone database server and does not require a separate daemon, HTTP API, or database service.

The package will provide:

- SQL queries through an embedded LumoSQL engine.
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
     |    LumoSQL    |       |    Pebble     |
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

> Pebble is authoritative. LumoSQL is a rebuildable materialized query database.

If the complete LumoSQL database disappears, the node must be able to recreate it solely from Pebble state.

---

## 2. Important Design Decision: Do Not Use CR-SQLite in Version 1

Do not embed CR-SQLite into the first implementation.

CR-SQLite solves replication inside SQLite. This design already has a separate replication and durable-state layer. Using CR-SQLite would create two overlapping replication systems:

```text
LumoSQL
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
- The LumoSQL database can be dropped and rebuilt.
- The query engine can be replaced later without changing the replication protocol.
- No requirement to make CR-SQLite work with every LumoSQL backend.
- No duplicated conflict-resolution metadata.
- Easier control of tombstones, snapshots, log retention, node watermarks, and encryption.

CR-SQLite can still be studied as a reference for conflict semantics and testing.

---

## 3. LumoSQL Role

LumoSQL is the embedded SQL/query engine.

Regular SQLite is strictly a Two-Phase Locking (2PL) single-version database; even in WAL mode, it is not true MVCC and suffers from checkpoint lock contention where readers block checkpoints and writers stall. LumoSQL replaces SQLite's monolithic B-tree with pluggable key-value storage engines, notably **LMDB** (and MDBX), which provide **true Multi-Version Concurrency Control (MVCC)** via Copy-on-Write (COW) B+ trees.

For this project, the primary query engine architecture is **LumoSQL with its LMDB backend memory-mapped on disk (`QueryStoreMMap`)**.

In this design:
- Readers hold an immutable root pointer and read memory-mapped pages directly with **zero locks** on database pages or tables.
- **Readers never block writers, and writers never block readers.**
- There is **no checkpointing step**. When a write commits, it updates the root pointer in virtual memory; obsolete pages are tracked in an internal free-list and reclaimed only when older readers finish.
- The engine runs on standard disk paths (e.g. on Windows or Linux) without needing a RAM disk.
- All LumoSQL connections are explicitly configured with `PRAGMA synchronous = OFF;` (`MDB_NOSYNC | MDB_NOMETASYNC`). Because **Pebble is the authoritative durable store** (`pebble.Sync`), LumoSQL does not perform synchronous disk flushes (`FlushFileBuffers`/`fdatasync`). Write transactions commit in microseconds at RAM speed without duplicating Pebble's disk I/O.
- If LumoSQL files are corrupted or the process crashes, the entire LMDB directory is discarded and rebuilt cleanly from Pebble current state.

Do not use LMDB as the authoritative database. That would duplicate Pebble's responsibility.

### 3.1 Query-store modes

Design the package so the SQL materialization mode is configurable:

```go
type QueryStoreMode int

const (
    QueryStoreMMap QueryStoreMode = iota // Primary: LumoSQL LMDB MVCC memory-mapped engine
    QueryStoreMemory                     // Fallback: Pure-Go in-memory SQLite (modernc.org/sqlite)
)
```

#### `QueryStoreMMap` (Primary Target)

Primary production target for multi-user and high-concurrency workloads.

- Backed by LumoSQL's LMDB/MDBX engine on a local disposable file path with a large virtual memory map (`mmap`).
- True MVCC: completely non-blocking concurrent readers during write transactions.
- Zero physical disk sync wait: `PRAGMA synchronous = OFF` (`MDB_NOSYNC | MDB_NOMETASYNC`) keeps commits in memory cache at RAM speeds.
- Easy deployment on Windows and Linux: uses a standard directory without requiring RAM disk setup or third-party drivers.
- Non-authoritative: verified against Pebble's `state_generation` on startup; purged and rebuilt if stale or missing.

#### `QueryStoreMemory` (Pure-Go CGO-Free Fallback)

Optional fallback mode for lightweight deployments or environments lacking a C compiler.

- Uses pure-Go SQLite (`modernc.org/sqlite`) in memory or on tmpfs.
- CGO-free: compiles instantly with standard `go build`.
- Useful for unit tests, development, and resource-constrained environments where installing CGO toolchains is undesirable.
- Rebuilt from Pebble at startup.

---

## 4. Go and CGO Boundary

Pebble and quic-go are Go libraries. LumoSQL is C code exposed through the SQLite C API.

Therefore the package will be embedded in Go, but the complete package will not be pure Go.

Plan for CGO from the beginning.

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

    lumosql/
        amalgamation/
        driver/
        build/
```

Build tags should eventually allow:

```text
lumosql
sqlite
```

The initial supported production build should be LumoSQL only, but isolating the SQL interface will make testing easier and will prevent the rest of the architecture from being permanently coupled to one C implementation.

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
               |           LumoSQL           |
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
Search/query performance  -> LumoSQL
Durability                -> Pebble
Conflict resolution       -> CRDT/HLC layer
Replication transport     -> quic-go
Membership/discovery      -> memberlist SWIM over QUIC
Dissemination/repair       -> bounded peer scheduler and anti-entropy
At-rest protection        -> encrypted Pebble VFS/key manager
```

That separation is the core reason the package can remain embedded, fast, recoverable, and replaceable component-by-component.

---

