# RIME — Rapid In-Memory Engine

<img src="logo.png" alt="RIME logo: fast in-memory engine" width="512">

RIME is a pure-Go, in-memory relational engine for native Go records. It
supports concurrent readers and writers, repeatable snapshots, indexed
queries, constraints, hooks, transactions and maintained query views. Views
cache Go query results and update synchronously with source commits. Commits
currently serialize.
RIME does not provide disk persistence, encryption, or replication.

Start with the [usage and syntax reference](USAGE.md). For implementation
details and concurrency guarantees, see [ARCHITECTURE.md](ARCHITECTURE.md).
RIME uses only Go's standard library.
