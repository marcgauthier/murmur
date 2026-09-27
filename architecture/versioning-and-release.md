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

---

## 86. Packaging and Release

Deliver the project as a Go module.

Example:

```text
github.com/<org>/replicateddb
```

Target:

```go
go get github.com/<org>/replicateddb
```

Because LumoSQL requires CGO/native compilation, document supported platforms and toolchains.

CI matrix should eventually include at least:

```text
Linux amd64
Linux arm64
Windows amd64
```

plus any additional required deployment targets.

Pin:

```text
LumoSQL source/version
Pebble major/minor
memberlist version
quic-go version
Go toolchain range
```

Review LumoSQL, SQLite, Pebble, memberlist and quic-go licensing/redistribution requirements before publishing binaries or vendored amalgamations.

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

