# Operational Tooling and CLI Architecture

This document describes the design, architecture, and operational capabilities of the Murmur SQL command-line utility (`murmur`).

---

## 1. Overview and Design Principles

The Murmur CLI is an all-in-one operational, diagnostic, and administrative tool designed for both embedded database administration and remote cluster management.

### Key Architectural Tenets

1. **Zero-CGO Portability**: The CLI compiles without requiring a C compiler or shared C libraries (`CGO_ENABLED=0`), ensuring straightforward cross-compilation and single-binary deployment across Linux (x86_64, ARM64), macOS (Intel, Apple Silicon), and Windows.
2. **Dual Operation Modes**:
   - **Offline / Direct Mode**: Opens local database directories directly via Pebble and the pure-Go SQLite driver for offline diagnostics, low-level inspection, materialization repairs, and cold/hot backup operations.
   - **Online / Remote Mode**: Communicates with live Murmur nodes over mutual TLS (mTLS) and QUIC/HTTPS to query runtime status, inspect replication lag matrices, view SWIM cluster topologies, and trigger garbage collection sweeps.
3. **Structured Automation First**: Every command provides machine-readable `--json` output alongside human-friendly ASCII tables and Markdown tables (`--markdown`), simplifying integration into CI/CD pipelines, Kubernetes health probes, and metrics collectors.
4. **Resilient Backup Lifecycle**: Provides complete backup creation, checksum verification, metadata introspection, and fresh-identity restoration without requiring external tools.

---

## 2. Command Architecture and Dispatch

The CLI uses a modular subcommand architecture located in `tool/cmd/`:

```
tool/
├── client/
│   └── client.go          # Pure-Go mTLS HTTPS/QUIC client for remote operations
├── cmd/
│   ├── root.go            # Command registry, global flag parser, exit handler
│   ├── db_helper.go       # Storage initialization, key derivation, schema management
│   ├── ddl_helper.go      # DDL statement parser & migration router
│   ├── init.go            # Database initialization command
│   ├── shell.go           # Interactive SQL REPL with dot-command suite
│   ├── query.go           # Script and query execution engine
│   ├── import.go          # CSV/JSON bulk ingest with UUID/BLOB conversion
│   ├── export.go          # CSV, JSON, and SQL table dumper
│   ├── inspect.go         # Storage and metadata inspector
│   ├── verify.go          # Deep integrity verifier (Pebble + SQLite PRAGMAs)
│   ├── repair.go          # SQLite materialization rebuilder
│   ├── schema.go          # Schema DAG and column manifest inspector
│   ├── keys.go            # KEYREGISTRY inspector & credential verifier
│   ├── doctor.go          # Multi-check automated health scorecard
│   ├── bench.go           # Microbenchmark suite (I/O, ciphers, SQLite Tx)
│   ├── cluster.go         # SWIM membership and topology inspector
│   ├── lag.go             # Replication lag matrix
│   ├── gc.go              # Garbage collection inspector & online trigger
│   └── backup.go          # Backup creation, verification, info, and restore
├── format/
│   ├── table.go           # ASCII and Markdown tabular formatters
│   ├── csv.go             # RFC 4180 CSV serializer
│   └── json.go            # JSON formatting & error structures
├── main.go                # Global entrypoint
└── USAGE.md               # User guide and operational recipes
```

---

## 3. Core Functional Domains

### 3.1 Database & Interactive Shell

- **Interactive Shell (`murmur shell` / `murmur <path>`)**:
  - Provides a complete REPL for running interactive SQL queries.
  - Supports dot-commands: `.tables`, `.schema`, `.mode`, `.headers`, `.timer`, `.read`, `.dump`, `.status`, `.help`, `.quit`.
  - Automatically handles multiline SQL statements and transaction rollbacks.
- **DDL Migration Routing**:
  - SQL `CREATE TABLE` and schema modifications are intercepted by `ddl_helper.go` and executed via `db.Migrate(...)`.
  - Active schema definitions and epochs are persisted in `schema.json` to guarantee strict epoch consistency across reopens.
- **Bulk Data Ingest & Export**:
  - `murmur import` supports CSV and JSON datasets with automatic primary key UUID-to-BLOB conversion.
  - `murmur export` and `murmur dump` generate CSV, JSON arrays, and SQL schema/insert scripts.

### 3.2 Storage Inspection and Forensics

- **Low-Level Storage Inspection (`murmur inspect`)**:
  - Reads Pebble manifests, WAL files, HLC watermarks, and key registry metadata without requiring SQLite engine locks.
- **Deep Integrity Verification (`murmur verify`)**:
  - Validates Pebble SSTable block checksums, log sequence continuity, and executes `PRAGMA integrity_check` / `quick_check` against SQLite materializations.
- **Offline Materializer Repair (`murmur repair`)**:
  - Reconstructs missing or corrupt SQLite `materialized.db` files by replaying transactions from the underlying encrypted Pebble store.
- **Cryptographic Key Inspection (`murmur keys`)**:
  - Inspects `KEYREGISTRY` headers, active/expired data keys, generations, cipher suites, and pinned backup checkpoints.

### 3.3 Remote Cluster Diagnostics

- **Liveness & Health (`murmur status`)**: Queries remote node memory usage, uptime, active connections, and HLC clock offsets over mTLS.
- **SWIM Cluster Topology (`murmur cluster`)**: Reports member list state, node states (Alive, Suspect, Dead), and incarnation numbers.
- **Replication Lag Matrix (`murmur lag`)**: Measures commit gaps, replication offsets, and pending transaction counts across cluster peers.
- **Garbage Collection Management (`murmur gc`)**: Evaluates tombstone counts, dead tuple generations, and triggers on-demand compaction sweeps.

### 3.4 Backup Lifecycle Workflows

- **Consistent Snapshot (`murmur backup create`)**:
  - Generates an encrypted, streaming gzip archive containing database state, key registry metadata, and schema manifests.
- **Archive Introspection (`murmur backup info`)**:
  - Inspects `backup-metadata.json` without extracting the archive.
- **Integrity Validation (`murmur backup verify`)**:
  - Verifies tar archive CRC checksums and validates cryptographic key derivation.
- **Safe Recovery (`murmur backup restore`)**:
  - Extracts database state into a target directory, registers a fresh writer `NodeID` to prevent same-identity rollback, and preserves schema manifests.

---

## 4. Cross-References

- [API and Configuration](api-and-configuration.md)
- [Storage and Materialization](storage.md)
- [Encryption and Key Management](encryption.md)
- [Membership and Transport](membership-and-transport.md)
- [Snapshots, Backup, and Restore](snapshots-backup-and-restore.md)
- [Usage Guide](../tool/USAGE.md)
