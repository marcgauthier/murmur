# Versioning, release, and references

Compatibility/versioning, packaging, platform support, and architecture sources.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [84. Versioning](#84-versioning)
- [86. Packaging and Release](#86-packaging-and-release)
- [90. References Used for This Plan](#90-references-used-for-this-plan)

---

## 84. Versioning

Version separately:

```text
package API version
persistent Pebble format version
replication protocol version
membership metadata/envelope version
mutation codec version
schema epoch
```

Do not assume they always change together.

Pebble metadata:

```text
format_version
minimum_reader_version
minimum_writer_version
```

QUIC handshake:

```text
protocol_version
min_protocol_version
capabilities
```

This makes rolling upgrades possible later.

Require a new replication transport protocol version for shared membership/replication stream dispatch and DATAGRAM capability negotiation. Reject legacy peers explicitly; do not silently fall back to native memberlist UDP/TCP or full-mesh replication. This network revision does not change mutation identities or conflict semantics. Persist new peer-retirement/retention metadata under versioned package-owned keys without inventing a data migration or marking it implemented in the current code.

Version the new range/chunk and Plumtree message formats, schema ancestry, snapshot manifest/generation publication, and restore identity metadata. Negotiate required capabilities before starting transfers; an old peer must not interpret observed heads or staging receipts as applied watermarks. Preserve transaction identities across chunking and existing HLC/LWW semantics. These additions describe target architecture rather than claiming the current implementation already supports the new persistent metadata or APIs.

Implementation status: fresh Pebble stores record `format_version`,
`minimum_reader_version`, and `minimum_writer_version` (all 2); open fails
closed when a store demands a newer reader or writer, while pre-marker v2
stores default the minima to the format version. The QUIC handshake
negotiates capabilities (`NegotiateCapabilities`): unknown optional bits are
ignored, unknown required bits (marked with `CapRequiredMask`) refuse the
session before any peer state exists, and sessions pin the negotiated usable
set. Capability bits 1..31 are reserved for the target subsystems above and
are defined alongside their features. Snapshot manifests already enforce
`FormatVersion == 1` on both sides; range/chunk, Plumtree, generation
publication, and restore-marker versioning land with those formats.

---

## 86. Packaging and Release

Deliver the project as a Go module.

Example:

```text
github.com/nomadsql/replicateddb
```

Target:

```go
go get github.com/nomadsql/replicateddb
```

### Supported Platform and Build Matrix

Continuous integration (GitHub Actions `.github/workflows/ci.yml`) validates the following platform and build configurations:

| Platform / Architecture | Build Mode / Driver | Compiler & Toolchain | CI Checks |
|---|---|---|---|
| **Linux amd64** | Default pure Go (`modernc.org/sqlite`) | Go 1.26.x (`CGO_ENABLED=0`) | `go vet`, unit/integration test suite |
| **Linux amd64** | CGO Native (`mattn/go-sqlite3`) | Go 1.26.x (`CGO_ENABLED=1`, gcc) | Race detector (`go test -race ./...`), smoke tests |
| **Linux amd64** | Optional LumoSQL CGO | Go 1.26.x (`-tags "lumosql sqlite_preupdate_hook"`) | LumoSQL backend tag build & adapter tests |
| **Linux arm64** | Pure Go (`modernc.org/sqlite`) | Go 1.26.x (`GOARCH=arm64`, `CGO_ENABLED=0`) | Cross-compilation build verification |
| **Windows amd64** | Pure Go (`modernc.org/sqlite`) | Go 1.26.x (`GOOS=windows`, `CGO_ENABLED=0`) | Windows native test suite & cross-build |

### Build Tags and Driver Selection

The default build produces a zero-CGO binary using `modernc.org/sqlite` for both `QueryStoreMemory` and `QueryStoreMMap`.

When building with CGO or specialized backend engines:
- **Default (no tags)**: Pure Go SQL engine (`modernc.org/sqlite`).
- **`sqlite_preupdate_hook`**: Enables native SQLite pre-update hook capture via `mattn/go-sqlite3`.
- **`lumosql`**: Enables the LumoSQL MVCC driver integration (`sqlengine/backend_lumosql.go`), switching SQL materialization to LMDB with reader snapshot isolation.
- **`lumosql_mvcc`**: Enables advanced concurrent reader-snapshot acceptance tests against external LMDB builds.

### Pinned Dependencies and Toolchains

The codebase maintains explicit pins in `go.mod`:

| Component | Pinned Version / Source | Rationale / Capability |
|---|---|---|
| **Go Toolchain** | `1.26.0+` (minimum `1.25.x`) | Primary runtime, compiler, and standard library |
| **Pebble** | `github.com/cockroachdb/pebble/v2 v2.1.6` | Authoritative LSM KV, Zstd level 3, VFS encryption, reader/writer version checks |
| **Memberlist** | `github.com/hashicorp/memberlist v0.7.0` | SWIM cluster membership and gossip |
| **quic-go** | `github.com/quic-go/quic-go v0.63.0` | QUIC transport, mTLS connection multiplexing, DATAGRAM capabilities |
| **Compress** | `github.com/klauspost/compress v1.19.1` | Zstandard payload and snapshot wire compression |
| **Aegis** | `github.com/ericlagergren/aegis v0.0.0-20250325060835-cd0defd64358` | High-performance AEGIS AEAD authenticated ciphers |
| **Modernc SQLite** | `modernc.org/sqlite v1.44.3` | Pure Go embedded SQLite engine and pre-update hook parser |
| **Google UUID** | `github.com/google/uuid v1.6.0` | 128-bit identity serialization for nodes, transactions, and snapshots |
| **Prometheus Client**| `github.com/prometheus/client_golang v1.24.1` | Standard metrics exposition types for runtime observability |

### Licensing and Redistribution Review

Before publishing release binaries, container images, or vendored amalgamations, maintain compliance with constituent open-source licenses:

1. **Apache License 2.0**:
   - `github.com/cockroachdb/pebble/v2` (CockroachDB LSM KV store)
   - `github.com/ericlagergren/aegis` (AEGIS AEAD cipher implementation)
   - `crypto/key_registry.go` (adapted key-registry mechanics based on Badger concepts)
   - *Requirement*: Include original copyright notices and Apache 2.0 license text in binary distributions and third-party notices.
2. **Mozilla Public License 2.0 (MPL-2.0)**:
   - `github.com/hashicorp/memberlist` (HashiCorp SWIM membership library)
   - *Requirement*: Distribute MPL-2.0 notice. If memberlist source files are modified, modifications must remain available under MPL-2.0.
3. **MIT License**:
   - `github.com/quic-go/quic-go` (QUIC implementation)
   - `github.com/google/uuid` (UUID library)
   - *Requirement*: Preserve copyright and permission notice in binary distributions.
4. **BSD 3-Clause License**:
   - Go standard library and `golang.org/x/*` packages (`x/crypto`, `x/sys`, `x/net`)
   - `github.com/klauspost/compress` (BSD 3-Clause)
   - *Requirement*: Retain copyright notice, conditions list, and disclaimer.
5. **Public Domain / BSD (SQLite & LumoSQL)**:
   - SQLite source code is dedicated to the public domain.
   - LumoSQL backend modifications and amalgamations follow permissive open licenses (MIT/BSD/LGPL depending on backend). Binary distributions must include applicable backend notices.

---

## 90. References Used for This Plan

Architecture references; the Pebble storage/compression design is pinned to v2.1.6, and cipher/VFS conformance must be validated during implementation.

- LumoSQL documentation and amalgamation/backend information:
  https://lumosql.org/src/lumosql/doc/trunk/README.md

- SQLite pre-update hook:
  https://sqlite.org/c3ref/preupdate_blobwrite.html

- Pebble repository and versioned storage/cache/compression options:
  https://github.com/cockroachdb/pebble
  https://github.com/cockroachdb/pebble/blob/v2.1.6/options.go

- Pebble VFS:
  https://github.com/cockroachdb/pebble/blob/v2.1.6/vfs/vfs.go

- Pebble Zstd level 3 compression profile:
  https://github.com/cockroachdb/pebble/blob/v2.1.6/sstable/block/compression.go

- Badger key-registry reference (concepts/adapted code only, Apache-2.0):
  https://github.com/dgraph-io/badger/blob/v4.9.1/key_registry.go

- Go authenticated cipher implementations:
  https://pkg.go.dev/crypto/cipher
  https://pkg.go.dev/golang.org/x/crypto/chacha20poly1305
  https://github.com/ericlagergren/aegis

- quic-go repository and stream documentation:
  https://github.com/quic-go/quic-go
  https://quic-go.net/docs/quic/streams/

- Corrosion architecture, SWIM/broadcast implementation, QUIC transport, and bootstrap configuration:
  https://github.com/superfly/corrosion
  https://github.com/superfly/corrosion/blob/main/crates/corro-agent/src/broadcast/mod.rs
  https://github.com/superfly/corrosion/blob/main/crates/corro-agent/src/transport.rs
  https://superfly.github.io/corrosion/config/gossip.html

- Corrosion Plumtree, detailed synchronization, overload tuning, schema restrictions, and reseeding references:
  https://github.com/superfly/corrosion/blob/main/crates/corro-agent/src/plumtree/mod.rs
  https://github.com/superfly/corrosion/blob/main/crates/corro-types/src/sync.rs
  https://superfly.github.io/corrosion/config/perf.html
  https://superfly.github.io/corrosion/schema.html
  https://github.com/superfly/corrosion/blob/main/doc/reseeding.md

- HashiCorp memberlist membership and custom transport/configuration interfaces:
  https://github.com/hashicorp/memberlist
  https://github.com/hashicorp/memberlist/blob/master/transport.go
  https://github.com/hashicorp/memberlist/blob/master/config.go

- quic-go unreliable DATAGRAM negotiation and APIs:
  https://quic-go.net/docs/quic/datagrams/

---

