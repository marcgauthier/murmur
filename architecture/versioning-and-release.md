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
persistent Spool storage format version
replication protocol version
membership metadata/envelope version
mutation codec version
schema epoch
```

Do not assume they always change together.

Spool metadata:

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

Require a new replication transport protocol version for shared membership/replication stream dispatch and DATAGRAM capability negotiation. Reject legacy peers explicitly; do not silently fall back to native memberlist UDP/TCP or full-mesh replication. This network revision does not change mutation identities or conflict semantics. Peer admission/retention and retirement metadata are implemented under package-owned keys; see [retention](snapshots-backup-and-restore.md).

Version the new range/chunk and Plumtree message formats, schema ancestry, snapshot manifest/generation publication, and restore identity metadata. Negotiate required capabilities before starting transfers; an old peer must not interpret observed heads or staging receipts as applied watermarks. Preserve transaction identities across chunking and existing HLC/LWW semantics. Range/chunk transfer, Plumtree, snapshot publication, and restore metadata have implementations; their evidence and acceptance limits are recorded in [release status](release-status.md#1-verified-feature-matrix).

The RIME cutover uses store format/minimum reader/minimum writer 6, replication protocol/minimum protocol 6, mutation codec 4, schema manifest encoding 3, and snapshot manifest format 3. Protocol 6 requires origin signatures and merge-policy capabilities. Format-5 SQL-era stores fail closed without rewrite; applications start from a fresh directory. See [origin signatures](origin-signatures.md) and the [migration plan](migration-plan.md).

The breaking API and format changes are summarized in the project
[release notes](../RELEASE_NOTES.md). They describe typed-table adoption,
previous-release export requirements, and the current qualification status.

Fresh Spool stores record `format_version`,
`minimum_reader_version`, and `minimum_writer_version` (all 6); open fails
closed when a store demands a newer reader or writer. Format-5 SQL-era and
legacy Pebble directories are rejected without migration. The QUIC handshake
negotiates capabilities (`NegotiateCapabilities`): unknown optional bits are
ignored, unknown required bits (marked with `CapRequiredMask`) refuse the
session before any peer state exists, and sessions pin the negotiated usable
set. Capability bits 1..31 are reserved for the target subsystems above and
are defined alongside their features. Snapshot manifests already enforce
`FormatVersion == 2` on both sides; range/chunk, Plumtree, generation
publication, and restore-marker formats are implemented alongside their feature code.

---

## 86. Packaging and Release

Deliver the project as a Go module.

Example:

```text
github.com/marcgauthier/murmur
```

Target:

```go
go get github.com/marcgauthier/murmur
```

### Supported Platform and Build Matrix

Continuous integration (GitHub Actions `.github/workflows/ci.yml`) is configured
for the following checks. These are not observed results for the latest revision:

| Platform / Architecture | Build Mode / Driver | Compiler & Toolchain | CI Checks |
|---|---|---|---|
| **Linux amd64** | RIME + encrypted Spool; no SQLite dependency | Go 1.26.x; production build supports `CGO_ENABLED=0` | CI `go vet`, race detector, benchmark modules, and live gate |

Other targets (for example Windows or Linux arm64) are not covered in CI and
need separate validation before being claimed as supported.

### Build and test contract

The production module builds and its normal tests run with CGO disabled. No
SQLite build tags or driver selection are required. Live fixtures are separate
processes; `MURMUR_RACE=1` is required to race-instrument the child node as well
as the test process.

### Release status and release commit

The [release inventory and verification record](release-status.md) separates
code presence, test coverage, and historical results. No verified release
revision is recorded there. Record the exact candidate, commands, backend,
platform, and results using its §5 procedure before making release claims;
checkout cleanliness is a transient observation, not a release identifier.

### Pinned Dependencies and Toolchains

The codebase maintains explicit pins in `go.mod`:

| Component | Pinned Version / Source | Rationale / Capability |
|---|---|---|
| **Go Toolchain** | `1.26.0` (`go.mod` directive) | Primary runtime, compiler, and standard library |
| **Immutable Radix** | `github.com/hashicorp/go-immutable-radix v1.3.1` | In-memory authoritative state store and snapshots |
| **Memberlist** | `github.com/hashicorp/memberlist v0.7.0` | SWIM cluster membership and gossip |
| **quic-go** | `github.com/quic-go/quic-go v0.63.0` | QUIC transport, mTLS connection multiplexing, DATAGRAM capabilities |
| **Google UUID** | `github.com/google/uuid v1.6.0` | 128-bit identity serialization for nodes, transactions, and snapshots |

### Licensing and Redistribution Review

Before publishing release binaries, container images, or vendored amalgamations, maintain compliance with constituent open-source licenses:

1. **Mozilla Public License 2.0 (MPL-2.0)**:
   - `github.com/hashicorp/go-immutable-radix` (HashiCorp immutable radix tree library)
   - `github.com/hashicorp/memberlist` (HashiCorp SWIM membership library)
   - *Requirement*: Distribute MPL-2.0 notice. If source files are modified, modifications must remain available under MPL-2.0.
2. **MIT License**:
   - `github.com/quic-go/quic-go` (QUIC implementation)
   - `github.com/google/uuid` (UUID library)
   - *Requirement*: Preserve copyright and permission notice in binary distributions.
3. **BSD 3-Clause License**:
   - Go standard library and `golang.org/x/*` packages (`x/crypto`, `x/sys`, `x/net`)
   - *Requirement*: Retain copyright notice, conditions list, and disclaimer.

---

## 90. References Used for This Plan

Architecture references:

- Badger key-registry reference (concepts/adapted code only, Apache-2.0):
  https://github.com/dgraph-io/badger/blob/v4.9.1/key_registry.go

- Go authenticated cipher implementations:
  https://pkg.go.dev/crypto/cipher
  https://pkg.go.dev/golang.org/x/crypto/chacha20poly1305

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
