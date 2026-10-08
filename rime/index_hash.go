package rime

// hashIndex documents the hash-index behavior; the storage itself lives in
// indexSet (index.go) as field -> value -> key-set maps with MVCC filtering
// applied at read time by the executor.
//
// Visibility rule: the maps always reflect the latest committed state.
// Queries at the latest snapshot use them directly. Queries at older
// snapshots re-evaluate the predicate against the visible record version, so
// stale entries can only add candidates that filtering then removes; and any
// query whose snapshot predates the newest commit on an involved table falls
// back to a full scan to avoid missing records that matched historically but
// no longer do. Correctness first, speed second.
