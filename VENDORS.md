# External Package Vendors and Supply Chain Registry

This document lists all external dependencies and packages imported to build Murmur. For each package, it details the company or individual developer responsible for the package, country of origin, license type, and purpose within Murmur.

> [!IMPORTANT]
> **Dependency Policy**: Whenever any new external package or module is imported into the codebase or added to `go.mod`, it **must** be documented in this file with its publisher, country of origin, and license as mandated by [AGENTS.md](AGENTS.md).

---

## 1. Direct Dependencies

These packages are directly imported and utilized by the Murmur database, storage, replication, and test systems.

| Package | Version | Publisher / Organization | Country of Origin | License | Description / Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `github.com/hashicorp/go-immutable-radix` | `v1.3.1` | HashiCorp, Inc. / IBM | United States | MPL-2.0 | Immutable Radix tree implementing the in-memory authoritative state store, snapshots, and point/range reads. |
| `github.com/quic-go/quic-go` | `v0.63.0` | Marten Seemann & quic-go contributors | Germany | MIT | High-performance QUIC protocol transport for encrypted, low-latency inter-node replication. |
| `github.com/hashicorp/memberlist` | `v0.7.0` | HashiCorp, Inc. / IBM | United States | MPL-2.0 | Gossip protocol for cluster membership, node state dissemination, and failure detection. |
| `github.com/hashicorp/golang-lru/v2` | `v2.0.7` | HashiCorp, Inc. / IBM | United States | MPL-2.0 | High-throughput in-memory LRU, 2Q, and ARC cache implementations for query and block caching. |
| `github.com/google/uuid` | `v1.6.0` | Google LLC | United States | BSD-3-Clause | Universally Unique Identifier (UUID) generation for transaction, node, and replica IDs. |
| `golang.org/x/crypto` | `v0.57.0` | Google LLC / Go Authors | United States | BSD-3-Clause | Cryptographic primitives (Argon2, HKDF, BLAKE2, Poly1305, ChaCha20, etc.) for key derivation and auth. |
| `golang.org/x/sys` | `v0.48.0` | Google LLC / Go Authors | United States | BSD-3-Clause | Low-level OS primitives, memory locking, direct I/O, and platform system calls. |

---

## 2. Indirect / Transitive Dependencies

These packages are required dependencies of our direct libraries (such as Memberlist).

| Package | Version | Publisher / Organization | Country of Origin | License | Primary Upstream Consumer |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `github.com/google/btree` | `v1.1.3` | Google LLC | United States | Apache-2.0 | Memberlist (In-memory B-Tree data structures) |
| `github.com/hashicorp/errwrap` | `v1.1.0` | HashiCorp, Inc. / IBM | United States | MPL-2.0 | Memberlist (Error wrapping utilities) |
| `github.com/hashicorp/go-metrics` | `v0.7.0` | HashiCorp, Inc. / IBM | United States | MPL-2.0 | Memberlist (Gossip metrics gathering) |
| `github.com/hashicorp/go-msgpack/v2` | `v2.1.5` | HashiCorp, Inc. / IBM | United States | BSD-2-Clause | Memberlist (MessagePack network serialization) |
| `github.com/hashicorp/go-multierror` | `v1.1.1` | HashiCorp, Inc. / IBM | United States | MPL-2.0 | Memberlist (Aggregated error management) |
| `github.com/hashicorp/go-sockaddr` | `v1.0.7` | HashiCorp, Inc. / IBM | United States | MPL-2.0 | Memberlist (Socket address inspection & resolution) |
| `github.com/hashicorp/golang-lru` | `v1.0.2` | HashiCorp, Inc. / IBM | United States | MPL-2.0 | Memberlist (LRU cache v1) |
| `github.com/miekg/dns` | `v1.1.73` | Miek Gieben | Netherlands | BSD-3-Clause | Memberlist (DNS discovery & SRV record parsing) |
| `github.com/sean-/seed` | `v0.0.0-20170313163322-e2103e2c3529` | Sean Chittenden | United States | MIT | Memberlist (Secure PRNG seeding) |
| `golang.org/x/net` | `v0.58.0` | Google LLC / Go Authors | United States | BSD-3-Clause | quic-go / Memberlist (Network protocols, IP routing & DNS) |

---foun

## 3. Geographic Distribution Summary

| Country | Number of Packages | Primary Publishers / Entities |
| :--- | :--- | :--- |
| **United States** | 16 | Google, HashiCorp/IBM, individual maintainers |
| **Denmark** | 1 | Klaus Post |
| **Netherlands** | 1 | Miek Gieben (`miekg/dns`) |

---

## 4. Maintenance & Supply Chain Auditing

1. **Vendor Validation**: Every package included above must have a clear open-source license compatible with commercial distribution (e.g. Apache-2.0, MIT, BSD-2/3-Clause, MPL-2.0).
2. **Review on Version Bump**: When updating any version in `go.mod`, verify that no changes to licensing or ownership/maintainership have taken place.
3. **New Package Registration**: Any new dependency introduced to this repository requires an entry in this document before merging.
