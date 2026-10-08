# Murmur Operational CLI (`murmur`)

The `murmur` CLI provides database initialization, durable-state inspection and verification, key diagnostics, backup and restore, and remote cluster operations. SQL shell, query, import, and export commands have been removed; applications define schemas and access records through the managed Go API.

---

## Table of Contents

1. [Installation and Build](#1-installation-and-build)
2. [Global Flags & Authentication](#2-global-flags--authentication)
3. [Database Initialization](#3-database-initialization)
   - [`init` - Initialize a Database](#init---initialize-a-database)
4. [Storage, Integrity & Diagnostics](#4-storage-integrity--diagnostics)
   - [`inspect` - Low-Level Storage Inspector](#inspect---low-level-storage-inspector)
   - [`verify` - Durable Storage & Schema Manifest Check](#verify---durable-storage--schema-manifest-check)
   - [`repair` - Durable State & Restore Intent](#repair---durable-state--restore-intent)
   - [`schema` - Schema Manifest & Table Metadata](#schema---schema-manifest--table-metadata)
   - [`keys` - Key Registry & Cryptographic State](#keys---key-registry--cryptographic-state)
   - [`doctor` - Automated System Health Check](#doctor---automated-system-health-check)
   - [`bench` - Typed Storage and Cipher Microbenchmarks](#bench---typed-storage-and-cipher-microbenchmarks)
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

The CLI commands use direct Spool access and the managed Go database API. The production module and its normal test suite build with CGO disabled and do not require SQLite build tags.

### Build from Source

```bash
# Build the operational CLI
CGO_ENABLED=0 go build -o bin/murmur ./tool

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

## 3. Database Initialization

### `init` - Initialize a Database

Creates an encrypted Spool database directory and unique node identity. The database has no bound application schema until the application first opens it with its Go table definitions. SQL seed files are not supported.

```bash
# Initialize with default settings
murmur init /var/lib/murmur/data

# Initialize with custom passphrase
murmur init /var/lib/murmur/data --passphrase="super-secret-passphrase"

# Initialize with specific 32-byte hex key
murmur init /var/lib/murmur/data --key-hex="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
```


Applications bind their Go record types and schema through `Config.Tables` and use Murmur's managed typed API. The CLI does not parse SQL or dynamically construct application tables. See the [RIME migration schema guide](../architecture/rime-migration-schema.md) for supported record and query features.
## 4. Storage, Integrity & Diagnostics

### `inspect` - Low-Level Storage Inspector

Inspects storage files, segment files, manifest versions, and on-disk payload sizes by opening encrypted Spool state directly. It does not construct the RIME query materializer. The reported table count comes from the durable schema manifest.

```bash
# Inspect local database directory
murmur inspect /var/lib/murmur/data

# Output in JSON for monitoring tools
murmur inspect /var/lib/murmur/data --json
```

### `verify` - Durable Storage & Schema Manifest Check

Opens encrypted Spool directly and verifies durable records without constructing the RIME query materializer. Before the application binds its Go schema, the result remains valid and includes a warning that no schema is bound. The result sets `materializer_checked` to `false`: reconstructing a RIME materializer requires the application's Go table definitions, which are not available to the CLI. `--deep` (or legacy `--full`) is reported in the result; opening always performs the full durable replay.

```bash
# Verify durable state and schema manifest
murmur verify /var/lib/murmur/data

# Request detailed verification metadata
murmur verify /var/lib/murmur/data --deep
```

### `repair` - Durable State & Restore Intent

Replays and checks authoritative encrypted Spool directly, or clears a pending restore intent when explicitly requested. RIME materialization is rebuilt when the application opens with its Go table definitions; the CLI cannot rebuild it without those definitions. `--dry-run` does not open storage or change files.

```bash
# Check durable state
murmur repair /var/lib/murmur/data

# Clear a restore intent after confirming recovery state
murmur repair /var/lib/murmur/data --clear-intent
```

### `schema` - Schema Manifest & Table Metadata

Displays registered table definitions from the durable schema manifest, including stable table/column IDs, types, nullability, and primary-key identity. Local mode opens encrypted Spool directly and does not construct a query materializer. Remote mode uses `GET /v1/schema`.

```bash
# Show schema overview
murmur schema /var/lib/murmur/data

# Show schema in Markdown format
murmur schema /var/lib/murmur/data --markdown
```

### `keys` - Key Registry & Cryptographic State

Inspects the on-disk key registry and opens encrypted Spool directly to display active and retired data keys, key generations, and wrapping-key identity. It does not construct the RIME query materializer.

```bash
# Inspect key registry
murmur keys /var/lib/murmur/data

# Inspect key registry with custom passphrase
murmur keys /var/lib/murmur/data --passphrase="super-secret-passphrase"
```

### `doctor` - Automated System Health Check

Local mode opens encrypted Spool directly, replays durable state, and verifies the schema manifest without constructing the RIME query materializer. It reports the materializer as unchecked because application Go table definitions are unavailable to the CLI. Remote mode checks TLS configuration and the live node status endpoint.

```bash
# Run doctor check
murmur doctor /var/lib/murmur/data

# Run doctor check with JSON scorecard output
murmur doctor /var/lib/murmur/data --json
```

### `bench` - Typed Storage and Cipher Microbenchmarks

Measures durable typed-record batch writes and point reads through Murmur's managed RIME API, plus AES-256-GCM, ChaCha20-Poly1305, and SHA-256 throughput. The record measurements include Spool durability and are not an isolated RIME engine benchmark.

```bash
# Run default benchmark suite
murmur bench

# Run benchmark for a shorter interval
murmur bench --duration=500ms --json
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

Inspects replication GC watermarks and retention state. `--trigger` calls the node's `POST /v1/admin/gc` endpoint, which runs Murmur's collector for eligible replication-log and transaction-receipt history using persisted peer acknowledgements and configured retention limits.

```bash
# Inspect GC watermarks and retention state
murmur gc https://node1.cluster.internal:8443 \
  --cert=client.crt --key=client.key --ca=ca.crt

# Trigger online replication-history collection
murmur gc https://node1.cluster.internal:8443 \
  --cert=client.crt --key=client.key --ca=ca.crt --trigger
```

---

## 6. Backup & Recovery

Murmur provides point-in-point, encrypted hot snapshots that can be safely verified and restored.

### `backup create` - Consistent Hot Snapshot

Creates an encrypted, gzip-compressed archive from an authoritative Spool checkpoint and its embedded manifests. Local backup creation does not construct the RIME query materializer and does not include a `schema.json` sidecar.

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
STATUS=$(murmur doctor /var/lib/murmur/data --json | jq -r '.overall')

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
