# chaos-load (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/chaos-load/`
(`README.md` + `chaos_load_test.go`:
`TestContinuousWritesAcrossPartitionAndHealing`).

## What it proves

Three encrypted, mutually authenticated QUIC nodes write concurrently
while node3 is cut off from the connected node1/node2 pair. The split
groups must show divergent row counts and ordered state digests; after
re-admitting peers while writes continue, all three nodes must converge
on identical counts and digests, and a post-heal write must replicate
everywhere.

## Pass criteria

- `Cluster::new` opens 3 meshed nodes under
  `tests-live/runtime/chaos-load/`.
- Baseline insert on node 0 reaches all 3 nodes within 20 s.
- After removing all 4 cross-partition peer links (both directions),
  the mesh reports live-peer counts `[1, 1, 0]` within 20 s.
- During the partition window, all three nodes keep writing (25 ms
  cadence each); at the measurement barrier node1/node2 hold
  `1 + writes1 + writes2` rows, node3 holds `1 + writes3` rows, and
  the two groups' ordered-name digests differ while node3 reports 0
  connected peers.
- After re-adding all 4 links under continuing load, the mesh reports
  `[2, 2, 2]` within 30 s and all 3 nodes converge on identical counts
  and digests within 60 s.
- No application write ever fails (Go `MarkFailed` parity: failures
  are collected and fail the test after the writers join).
- A post-heal insert on node 1 reaches all 3 nodes within 20 s with
  identical digests.

## Env knobs

- `MURMUR_CHAOS_PARTITION_SECONDS` (default 12): partitioned write window.
- `MURMUR_CHAOS_HEAL_SECONDS` (default 12): post-heal write window.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API (`TypedInsert` / `TypedNames`); writer
  ids are `node * 10_000_000 + seq` instead of server-generated typed
  IDs, with the digest still the sorted names joined by newline. The Go
  `AwaitUnlock` / `TypedRecords` cluster options have no Rust equivalents
  (no unlock endpoint; SQL schema instead of typed fixtures).
- Goroutine writers become `std::thread::scope` writer threads sharing
  `&Cluster`; the stop channel becomes an `AtomicBool`, and a
  drop-guard sets it on unwind so a failed phase cannot deadlock the
  scope join. Pause/drain/active-counter semantics are unchanged.
- Connected-peer counts come from replication `peers_diagnostics`
  (`connected` flags) instead of `/v1/status`.
- Directory logging shows label/dir/repl-addr (no PIDs exist in-process).

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/chaos-load/Cargo.toml
```
