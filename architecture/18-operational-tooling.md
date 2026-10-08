# Operational Tooling and CLI Architecture

This document describes the design, architecture, and operational capabilities of the Murmur command-line utility (`murmur`).

---

## 1. Overview and Design Principles

The Murmur CLI is an all-in-one operational, diagnostic, and administrative tool designed for both embedded database administration and remote cluster management.

### Key Architectural Tenets

1. **Storage-Aware Operations**: Durable-state inspection, verification, repair, doctor, and key inspection open encrypted Spool directly and do not construct a query materializer. Doctor reports the materializer as unchecked because it has no application Go schema. RIME materialization is rebuilt when the application opens with its typed schema.
2. **Dual Operation Modes**:
  - **Offline / Direct Mode**: Initialization creates encrypted Spool without a query engine; metadata inspection, verification, repair, doctor, and key inspection read Spool and schema manifests directly. Backup operations checkpoint authoritative durable state.
  - **Online / Remote Mode**: Communicates with live Murmur nodes over mutual TLS (mTLS) and QUIC/HTTPS to query runtime status, retrieve the schema manifest through `GET /v1/schema`, inspect replication lag matrices, view SWIM cluster topologies, and trigger retention-aware garbage collection through `POST /v1/admin/gc`.
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
│   ├── offline_store.go   # Direct encrypted Spool access for metadata-only commands
│   ├── init.go            # Database initialization command
│   ├── inspect.go         # Storage and metadata inspector; table count from schema manifest
│   ├── verify.go          # Direct Spool replay and schema-manifest verifier
│   ├── repair.go          # Direct Spool verification and restore-intent repair
│   ├── schema.go          # Schema DAG and column manifest inspector
│   ├── keys.go            # Keyring inspector & credential verifier
│   ├── doctor.go          # Multi-check automated health scorecard
│   ├── bench.go           # Typed RIME storage and cipher microbenchmarks
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

### 3.1 Database Initialization and Application API

- **Initialization (`murmur init`)**:
  - Creates encrypted Spool and a node identity without choosing an application schema. The application binds its Go definitions on first typed `Open`.
- **Managed data access**:
  - Applications declare records through `Config.Tables` and use Murmur's typed CRUD, transaction, query, subscription, backup, and replication APIs. The CLI does not parse SQL or synthesize application table definitions.

### 3.2 Storage Inspection and Forensics

- **Low-Level Storage Inspection (`murmur inspect`)**:
  - Opens encrypted durable state directly, reads its schema manifest and watermarks, and does not instantiate the RIME query materializer.
- **Deep Integrity Verification (`murmur verify`)**:
  - Opens and replays encrypted Spool state and validates the durable schema manifest without a query engine. It reports that materializer verification was not run because the CLI has no application Go table definitions.
- **Health Scorecard (`murmur doctor`)**:
  - Local mode checks durable Spool and manifest integrity without opening a query materializer. It marks materializer health as unchecked; remote mode checks TLS setup and the live node status endpoint.
- **Durable State Repair (`murmur repair`)**:
  - Replays encrypted Spool directly and can clear a restore intent. The application rebuilds its private RIME materializer on typed Open.
- **Cryptographic Key Inspection (`murmur keys`)**:
  - Opens encrypted Spool directly and reports key inventory, active/retired data keys, and key generations without starting a query engine.

### 3.3 Remote Cluster Diagnostics

- **Liveness & Health (`murmur status`)**: Queries remote node memory usage, uptime, active connections, and HLC clock offsets over mTLS.
- **SWIM Cluster Topology (`murmur cluster`)**: Reports member list state, node states (Alive, Suspect, Dead), and incarnation numbers.
- **Replication Lag Matrix (`murmur lag`)**: Measures commit gaps, replication offsets, and pending transaction counts across cluster peers.
- **Garbage Collection Management (`murmur gc`)**: Evaluates tombstone counts, dead tuple generations, and triggers on-demand compaction sweeps.

### 3.4 Backup Lifecycle Workflows

- **Consistent Snapshot (`murmur backup create`)**:
  - Checkpoints encrypted Spool directly and streams an archive containing durable state and schema manifests without opening a query engine.
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
