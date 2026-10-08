# Murmur — Architecture and Implementation Plan

Coordinated starling flock flight.

**Status:** Implementation and release qualification in progress
**Target language:** Go  
**Primary components:** RIME + encrypted Spool + HashiCorp memberlist + quic-go
**Replication model:** Masterless, offline-capable, per-field conflict resolution using HLC
**Durable source of truth:** Spool and its in-memory state index
**Query engine:** Managed RIME records rebuilt from Spool
**Build:** Go module builds with CGO disabled; production has no SQLite dependency

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [1. Goal](#1-goal)
- [2. Managed Go API](#2-managed-go-api)
- [3. RIME and Spool Roles](#3-rime-and-spool-roles)
- [4. Go and CGO Boundary](#4-go-and-cgo-boundary)
- [88. Current Architecture](#88-current-architecture)

## 1. Goal

Murmur is an embedded Go database for encrypted durable local records and
masterless replication over QUIC with mutual TLS. Every node remains writable
offline and converges after communication resumes. Replication is
asynchronous; it does not provide global serializability.

The storage layer owns durable state, transaction receipts, origin logs,
snapshots, membership metadata, encryption and recovery. Applications provide
Go record definitions and use Murmur-managed typed table and transaction
handles.

## 2. Managed Go API

Applications define schemas with `Define[T]`, supply definitions using
`Config.Tables`, and obtain managed handles using `TableOf[T]`. Writes use
`WriteTxContext` or explicit `BeginTx` transactions. Murmur stages changes,
validates them against the durable schema, commits them to Spool, then
publishes the prepared transaction to RIME. The API does not expose writable
raw RIME tables or SQL execution.

Schemas use stable table and field identities. Additive evolution is
manifest-backed and replicated. Rich Go values use the versioned canonical
record codec; unknown compatible values remain in authoritative state when an
older binary writes known fields.

## 3. RIME and Spool Roles

Spool is authoritative for replicated and persistent node-local values,
transaction history, receipts, snapshots, schema manifests and recovery
metadata. Its memory index accelerates state reads and is accounted separately
from RIME's materializer.

RIME provides immutable managed records, MVCC snapshots, indexes, typed
filters, joins, aggregates and query subscriptions. It is rebuilt from Spool
on open and after completed snapshot or schema publication. RIME data is never
a second durable source of truth.

Local writes commit to Spool before RIME publication. Remote transactions are
authenticated and committed to Spool before the managed materializer publishes
the accepted winners. Replication does not echo remotely received changes.

## 4. Go and CGO Boundary

The production module has no SQLite driver, SQL engine or mandatory build
tags. Murmur builds and tests with `CGO_ENABLED=0`; CGO is needed only when the
Go race detector or an isolated optional benchmark requires it. The normal
runtime is implemented in Go and its declared dependencies.

Historical SQLite comparison fixtures, where retained, are isolated in nested
benchmark modules and do not participate in production builds, CI production
package tests, or the public API.

## 88. Current Architecture

```text
Application: Go records and typed queries
                 |
         Murmur managed facade
          /              \
 RIME materializer      Commit coordinator
          ^                    |
          |              Encrypted Spool
          |                    |
          +------ remote apply +---- signed QUIC/TLS replication
```

The migration's remaining release gates are recorded in
[the migration plan](../MIGRATION_PLAN.md),
[capability gaps](capability-gaps.md), and
[release status](release-status.md). They include broader storage-fault,
delivery-permutation/live soak
coverage, and reproducible rich-record performance measurements.
