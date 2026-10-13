# crdt-contention (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crdt-contention/`
(`README.md` + `contention_test.go`:
`TestMultiProcessDisjointAndSharedCellContention`).

## What it proves

Three encrypted, mutually authenticated nodes start from one replicated row
(`03141592-...`, name/phone `baseline`, score `0`), apply concurrent
disjoint-column updates from all nodes (name on node 1, phone on node 2,
score on node 3 — all must survive), then apply 40 competing updates per
node to the shared name cell. The shared-cell LWW winner must be identical
on every node, the untouched cells must keep their disjoint values, and the
full logical rows must match.

## Pass criteria

- `Cluster::new` opens 3 meshed nodes under
  `tests-live/runtime/crdt-contention/`; each reports 2 live peers within
  20 s.
- Baseline row inserted on node 1 is visible on all nodes within 20 s.
- After the concurrent disjoint updates, every node reads
  `("node1-name", "node2-phone", 42)` within 20 s.
- After 120 competing name updates, every node agrees on one non-empty
  winner from the `node{N}-cell-{seq:03}` write set within 30 s, with phone
  `node2-phone` and score `42` intact.
- Final full-row reads are equal across all three nodes.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`contention(id TEXT NOT NULL PRIMARY KEY, name TEXT, phone TEXT, score
  INTEGER)`) instead of the managed typed-records HTTP API
  (`TypedContentionInsert/Update/Read`); contention resolution is whatever
  the replicated-SQL layer converges to rather than the Go RIME per-column
  LWW versions, but the pass criteria (disjoint survival, single identical
  winner, full-row equality) are unchanged.
- Concurrency via `std::thread::scope` + a start `Barrier` (one worker per
  node, cloned `Database` handles) instead of goroutines + `WaitGroup`; the
  barrier only sharpens the race the Go test already intends.
- Peer-mesh wait reads `replication().status().peers_diagnostics`
  (`connected` flags) instead of `GET /v1/status connected_peers`.
- Error collection is panics-inside-workers caught by the outer
  `catch_unwind` (then `dump_forensics` + `finish`) instead of Go's `errCh`
  + `t.Fatal`.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/crdt-contention/Cargo.toml
```
