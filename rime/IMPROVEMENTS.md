# RIME improvement backlog

This file tracks deferred optimizations with correctness and measurement
constraints. RIME remains a standard-library-only package. The migration plan
uses this backlog only for work that does not gate migration completion.

## 1. Full table scan path

RIME stores records in per-key MVCC chains spread across shards. Full scans
follow map entries, chains and record pointers, which produces less cache-local
memory access than a dense row layout. A future scan representation may keep a
generation-aligned dense set of visible records while preserving immutable
snapshots and atomic publication.

Any dense layout must prove exact equivalence for snapshot visibility, pending
write overlays, deletes, ordering, pagination, joins, aggregates, GC and
concurrent publication. Measure retained heap and rebuild cost as well as scan
latency before changing storage ownership.

### 1.1. Streaming aggregate experiment

A single-pass streaming aggregate reduced temporary allocation substantially
but ran slower than materializing matches and folding the dense result slice in
the measured workload. That implementation was reverted. Revisit streaming only
with a scan layout that improves memory locality, and compare interleaved
record access with the current materialized path under the same host and GC
conditions.

## 2. Field extraction and index maintenance

The former interface-boxing cost for common scalar index keys was addressed by
kind-specialized index maps and direct typed probes. This item remains here as a
historical benchmark reference; do not reimplement it without profiles showing
a new bottleneck. Keep generic fallback handling for named and uncommon key
types.
