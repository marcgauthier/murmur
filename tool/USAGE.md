# Murmur SQL CLI Operational Tool (`murmur`)

The `murmur` CLI is a standalone, CGO-free diagnostic, operational, and management utility for Murmur SQL databases. It combines local database management, interactive shell access, storage inspection, data repair, cryptographic key inspection, backup/restore lifecycle workflows, and remote cluster diagnostics over mTLS.

---

## Table of Contents

1. [Installation and Build](#1-installation-and-build)
2. [Global Flags & Authentication](#2-global-flags--authentication)
3. [Database Lifecycle & Querying](#3-database-lifecycle--querying)
   - [`init` - Initialize a Database](#init---initialize-a-database)
   - [`shell` - Interactive REPL & Dot-Commands](#shell---interactive-repl--dot-commands)
   - [`query` - Execute SQL Statements & Scripts](#query---execute-sql-statements--scripts)
   - [`import` - Bulk Import CSV / JSON](#import---bulk-import-csv--json)
   - [`export` - Export Tables / Views](#export---export-tables--views)
   - [`dump` - Full SQL Schema and Data Dumper](#dump---full-sql-schema-and-data-dumper)
4. [Storage, Integrity & Diagnostics](#4-storage-integrity--diagnostics)
   - [`inspect` - Low-Level Storage Inspector](#inspect---low-level-storage-inspector)
   - [`verify` - Storage & Materialization Integrity Check](#verify---storage--materialization-integrity-check)
   - [`repair` - Materializer Rebuild & State Repair](#repair---materializer-rebuild--state-repair)
   - [`schema` - Schema Manifest & Table Metadata](#schema---schema-manifest--table-metadata)
   - [`keys` - Key Registry & Cryptographic State](#keys---key-registry--cryptographic-state)
   - [`doctor` - Automated System Health Check](#doctor---automated-system-health-check)
   - [`bench` - Microbenchmarks (I/O, Ciphers, Transactions)](#bench---microbenchmarks-io-ciphers-transactions)
5. [Cluster & Replication Operations](#5-cluster--replication-operations)
   - [`status` - Node Liveness & Runtime Status](#status---node-liveness--runtime-status)
   - [`cluster` - Cluster Membership & Topology](#cluster---cluster-membership--topology)
   - [`lag` - Replication Lag Matrix](#lag---replication-lag-matrix)
   - [`gc` - Garbage Collection Inspection & Trigger](#gc---garbage-collection-inspection--trigger)
6. [Backup & Recovery](#6-backup--recovery)
   - [`backup create` - Consistent Hot Snapshot](#backup-create---consistent-hot-snapshot)
   - [`backup info` - Inspect Archive Metadata](#backup-info---inspect-archive-metadata)
   - [`backup verify` - Verify Checksum & Decryption](#backup-verify---verify-checksum--decryption)
   - [`backup restore` - Restore into Clean Directory](#backup-restore---restore-into-clean-directory)
7. [Scripting and Automation (JSON / Markdown)](#7-scripting-and-automation-json--markdown)

---

## 1. Installation and Build

The `murmur` CLI is written in pure Go and can be built without a C compiler (`CGO_ENABLED=0`), enabling cross-compilation for Linux, macOS, and Windows.

### Build from Source

```bash
# Pure-Go build using modernc.org/sqlite
CGO_ENABLED=0 go build -tags modernc -o bin/murmur ./tool

# Verify build
./bin/murmur --help
```

---

## 2. Global Flags & Authentication

Global options can be provided to any subcommand:

| Flag | Description | Default |
|---|---|---|
| `--json` | Output response in structured JSON format | `false` |
| `--markdown` | Output tables in Markdown format | `false` |
| `--passphrase=<str>` | Passphrase to derive storage/backup decryption key | `""` |
| `--key-hex=<hex>` | 32-byte (64 hex char) raw encryption key | `""` |
| `--cert=<path>` | Client TLS certificate path for mTLS remote operations | `""` |
| `--key=<path>` | Client TLS private key path for mTLS remote operations | `""` |
| `--ca=<path>` | Custom Root CA certificate path | `""` |
| `--insecure` | Skip TLS server certificate verification (testing only) | `false` |
| `--timeout=<dur>` | Network timeout for remote requests (e.g. `10s`, `1m`) | `10s` |

---

## 3. Database Lifecycle & Querying

### `init` - Initialize a Database

Creates a new Murmur database directory with storage manifests, cryptographic key registry, and a unique node ID.

```bash
# Initialize with default settings
murmur init /var/lib/murmur/data

# Initialize with custom passphrase
murmur init /var/lib/murmur/data --passphrase="super-secret-passphrase"

# Initialize with specific 32-byte hex key
murmur init /var/lib/murmur/data --key-hex="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
```

### `shell` - Interactive REPL & Dot-Commands

Starts an interactive SQL REPL session. The interactive shell supports syntax highlighting, line editing, multiline SQL queries, and a suite of dot-commands.

```bash
# Launch interactive shell
murmur shell /var/lib/murmur/data

# Shorthand invocation
murmur /var/lib/murmur/data
```

#### Supported Dot-Commands in Shell

| Command | Arguments | Description |
|---|---|---|
| `.help` | | Show help and list available dot commands |
| `.tables` | `[pattern]` | List tables in database |
| `.schema` | `[table]` | Show `CREATE` statements for tables |
| `.mode` | `table\|csv\|json` | Switch output rendering format |
| `.headers` | `on\|off` | Toggle column headers in query output |
| `.timer` | `on\|off` | Toggle execution timer display |
| `.read` | `<file.sql>` | Execute SQL statements from an external file |
| `.dump` | `[table]` | Render SQL script to reconstruct schema and data |
| `.status` | | Display node ID, schema version, and directory stats |
| `.quit` / `.exit` | | Exit the interactive shell |

#### Interactive Session Example

```sql
murmur> CREATE TABLE users (id BLOB PRIMARY KEY, username TEXT, email TEXT, score INTEGER);
Migration applied: schema updated to version 2 (1 total tables).

murmur> INSERT INTO users (id, username, email, score) VALUES (x'a0000000000000000000000000000001', 'alice', 'alice@example.com', 95);
murmur> INSERT INTO users (id, username, email, score) VALUES (x'b0000000000000000000000000000002', 'bob', 'bob@example.com', 80);

murmur> .mode table
Output mode set to table.

murmur> SELECT username, email, score FROM users ORDER BY score DESC;
+----------+-------------------+-------+
| USERNAME | EMAIL             | SCORE |
+----------+-------------------+-------+
| alice    | alice@example.com | 95    |
| bob      | bob@example.com   | 80    |
+----------+-------------------+-------+

murmur> .status
=== Murmur Database Status ===
Target:          /var/lib/murmur/data
Node ID:         node_01h7abc...
Schema Version:  2
Tables:          users

murmur> .quit
```

### `query` - Execute SQL Statements & Scripts

Run non-interactive queries, DDL migrations, or execute scripts from standard input or files against local databases or remote nodes.

```bash
# Execute query locally
murmur query /var/lib/murmur/data "SELECT username, score FROM users WHERE score >= 80"

# Output query results in JSON
murmur query /var/lib/murmur/data --json "SELECT * FROM users"

# Execute a SQL migration script
murmur query /var/lib/murmur/data --file=migrations/v1.sql

# Pipe SQL from stdin
cat migrations/seed.sql | murmur query /var/lib/murmur/data

# Execute query against a remote node over mTLS
murmur query https://node1.internal:8443 \
  --cert=client.crt --key=client.key --ca=ca.crt \
  "SELECT count(*) FROM users"
```

### `import` - Bulk Import CSV / JSON

Import structured data into a table. Automatically transforms 16-byte UUID hex strings into primary key BLOBs.

```bash
# Import CSV data
murmur import /var/lib/murmur/data users data/users.csv --format=csv

# Import JSON lines (JSONL) data
murmur import /var/lib/murmur/data events data/events.json --format=json
```

### `export` - Export Tables / Views

Export table data into CSV or JSON formats.

```bash
# Export table to CSV
murmur export /var/lib/murmur/data users --format=csv --output=backup_users.csv

# Export table to JSON array
murmur export /var/lib/murmur/data users --format=json --output=backup_users.json
```

### `dump` - Full SQL Schema and Data Dumper

Generates standard SQL `CREATE TABLE` and `INSERT` statements to recreate the database.

```bash
# Dump entire database to file
murmur dump /var/lib/murmur/data > backup.sql

# Dump only a single table
murmur dump /var/lib/murmur/data users > users.sql
```

---

## 4. Storage, Integrity & Diagnostics

### `inspect` - Low-Level Storage Inspector

Inspects storage files, WAL/Pebble directory structures, manifest versions, and on-disk payload sizes without acquiring SQLite locks.

```bash
# Inspect local database directory
murmur inspect /var/lib/murmur/data

# Output in JSON for monitoring tools
murmur inspect /var/lib/murmur/data --json
```

### `verify` - Storage & Materialization Integrity Check

Performs low-level cryptographic checksum checks, Pebble LSM-tree integrity scans, and executes SQLite `PRAGMA integrity_check` on the materialized view.

```bash
# Quick integrity verification
murmur verify /var/lib/murmur/data --quick

# Full forensic integrity check
murmur verify /var/lib/murmur/data --full
```

### `repair` - Materializer Rebuild & State Repair

Rebuilds corrupt or missing SQLite materialization databases (`materialized.db`) from scratch by replaying change records from the underlying Pebble / KV storage engine.

```bash
# Check if rebuild is necessary
murmur repair /var/lib/murmur/data

# Force rebuild of materialized SQLite database
murmur repair /var/lib/murmur/data --force
```

### `schema` - Schema Manifest & Table Metadata

Displays registered table definitions, column types, nullability, primary key constraints, and schema epoch version.

```bash
# Show schema overview
murmur schema /var/lib/murmur/data

# Show schema in Markdown format
murmur schema /var/lib/murmur/data --markdown
```

### `keys` - Key Registry & Cryptographic State

Inspects the on-disk `KEYREGISTRY` file, displays active and expired cryptographic keys, key generations, cipher algorithms, and pinned backup checkpoints.

```bash
# Inspect key registry
murmur keys /var/lib/murmur/data

# Inspect key registry with custom passphrase
murmur keys /var/lib/murmur/data --passphrase="super-secret-passphrase"
```

### `doctor` - Automated System Health Check

Runs comprehensive diagnostics on directory permissions, key registry validity, database locking, storage engine health, schema status, and SQLite materializer integrity.

```bash
# Run doctor check
murmur doctor /var/lib/murmur/data

# Run doctor check with JSON scorecard output
murmur doctor /var/lib/murmur/data --json
```

### `bench` - Microbenchmarks (I/O, Ciphers, Transactions)

Evaluates system storage I/O throughput, cryptographic ciphers (AES-256-GCM, ChaCha20-Poly1305, SHA-256), and SQLite transaction execution rates.

```bash
# Run default benchmark suite
murmur bench

# Run benchmark with 50,000 operations
murmur bench --operations=50000 --markdown
```

---

## 5. Cluster & Replication Operations

### `status` - Node Liveness & Runtime Status

Queries an active Murmur node or remote cluster endpoint over mTLS for memory usage, uptime, active connections, and commit state.

```bash
# Query remote node status
murmur status https://node1.cluster.internal:8443 \
  --cert=client.crt --key=client.key --ca=ca.crt

# Output in JSON format
murmur status https://node1.cluster.internal:8443 \
  --cert=client.crt --key=client.key --ca=ca.crt --json
```

### `cluster` - Cluster Membership & Topology

Inspects cluster members, addresses, role states (Leader, Follower, Learner), and health.

```bash
murmur cluster https://node1.cluster.internal:8443 \
  --cert=client.crt --key=client.key --ca=ca.crt
```

### `lag` - Replication Lag Matrix

Displays replication offsets, commit indices, and lag in bytes / operations across all peer nodes in the cluster.

```bash
murmur lag https://node1.cluster.internal:8443 \
  --cert=client.crt --key=client.key --ca=ca.crt
```

### `gc` - Garbage Collection Inspection & Trigger

Inspects tombstone counts, dead tuple generations, and optionally triggers a garbage collection compaction sweep.

```bash
# Inspect GC tombstone status
murmur gc https://node1.cluster.internal:8443 \
  --cert=client.crt --key=client.key --ca=ca.crt

# Trigger online GC sweep
murmur gc https://node1.cluster.internal:8443 \
  --cert=client.crt --key=client.key --ca=ca.crt --trigger
```

---

## 6. Backup & Recovery

Murmur provides point-in-point, encrypted hot snapshots that can be safely verified and restored.

### `backup create` - Consistent Hot Snapshot

Creates an encrypted, gzip-compressed tar archive containing database state, key registry metadata, and manifests.

```bash
# Create backup archive
murmur backup create /var/lib/murmur/data /var/backups/murmur_2026-10-04.tar.gz

# Create backup with custom passphrase
murmur backup create /var/lib/murmur/data /var/backups/murmur_secure.tar.gz \
  --passphrase="backup-encryption-key"
```

### `backup info` - Inspect Archive Metadata

Inspects archive headers, manifest information, node ID, database ID, schema version, creation timestamp, and file contents without uncompressing to disk.

```bash
murmur backup info /var/backups/murmur_2026-10-04.tar.gz
```

### `backup verify` - Verify Checksum & Decryption

Validates SHA-256 payload checksums and attempts key derivation to verify archive integrity.

```bash
# Verify archive
murmur backup verify /var/backups/murmur_2026-10-04.tar.gz

# Verify archive encrypted with passphrase
murmur backup verify /var/backups/murmur_secure.tar.gz \
  --passphrase="backup-encryption-key"
```

### `backup restore` - Restore into Clean Directory

Safely unpacks and restores a database archive into a target directory.

```bash
# Restore backup archive
murmur backup restore /var/backups/murmur_2026-10-04.tar.gz /var/lib/murmur/restored_data

# Restore encrypted backup archive
murmur backup restore /var/backups/murmur_secure.tar.gz /var/lib/murmur/restored_data \
  --passphrase="backup-encryption-key"
```

---

## 7. Scripting and Automation (JSON / Markdown)

All operational commands support the `--json` flag, making the CLI ideal for automated CI/CD checks, health monitors, and Kubernetes readiness/liveness probes.

### Automated Healthcheck Probe Example

```bash
#!/usr/bin/env bash
set -e

# Run doctor and verify overall status
STATUS=$(murmur doctor /var/lib/murmur/data --json | jq -r '.status')

if [ "$STATUS" != "HEALTHY" ]; then
  echo "Node health check failed: status=$STATUS"
  exit 1
fi

echo "Node is healthy."
```

### Automated Backup & Verification Pipeline

```bash
#!/usr/bin/env bash
set -euo pipefail

BACKUP_FILE="/backups/murmur-$(date +%Y%m%d_%H%M%S).tar.gz"

echo "Creating backup..."
murmur backup create /var/lib/murmur/data "$BACKUP_FILE"

echo "Verifying backup..."
murmur backup verify "$BACKUP_FILE"

echo "Backup verified successfully: $BACKUP_FILE"
```
