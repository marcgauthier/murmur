// Package rime implements RIME — Rapid In-Memory Engine.
//
// RIME is a zero-dependency, pure-Go, in-memory relational engine built
// around native immutable Go objects, copy-on-write updates, sharded
// concurrent storage, MVCC transactions, specialized indexes, and
// function-based queries. See USAGE.md for the public syntax reference.
// RIME performs no record serialization and
// requires no CGO or query parsing.
//
// The primary optimization hierarchy is:
//
//	Correctness → Concurrency → Low allocation → Index efficiency →
//	Query planning → Raw throughput
//
// # Concurrency rules
//
//  1. Published records are immutable.
//  2. Multiple readers may share a published pointer.
//  3. Writers never modify published records.
//  4. Updates create new record versions.
//  5. Readers operate against an MVCC snapshot.
//  6. Writers use optimistic concurrency.
//  7. Shard locks are held only for short critical sections.
//  8. Multi-shard commits serialize on a single commit lock.
//  9. Index publication and record publication are atomic from the
//     perspective of transaction visibility.
//  10. Old versions remain valid until no active snapshot can observe them.
//
// Persistence, replication, encryption, and disk storage are intentionally
// outside the RIME core. They can be implemented through hooks or separate
// packages.
package rime
