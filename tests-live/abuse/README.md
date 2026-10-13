# abuse (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/abuse/`
(`abuse_test.go`: `TestAbuseMesh`, plus the `progress_test.go` reporter).

## What it proves

Parameterized adversarial validation: a live mesh is hammered through a
healthy soak, a prolonged half-split partition, an extended outage of a
quarter of the nodes with reconnect backlogs, and a simultaneous
kill/restart storm of every node, with continuous writers throughout.
The verdict is exact cross-node convergence (equal row counts plus
identical PK-ordered row contents), never a sample.

## Env knobs

- `MURMUR_ABUSE_NODES` (default 10): nodes in the mesh (clamped to 3..256).
- `MURMUR_ABUSE_DURATION_SECONDS` (default 600): total abuse time, split
  evenly across the four phases.
- `MURMUR_ABUSE_SETTLE_SECONDS` (default 120): max convergence wait before
  the verdict.
- `MURMUR_ABUSE_WRITE_INTERVAL_MS` (default 50): pacing per writer thread
  (up to 8 writers).
- `MURMUR_ABUSE_SEED` (default 1): RNG seed for writer targeting.
- `MURMUR_ABUSE_STATUS_SECONDS` (default 15): progress heartbeat interval.
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

Quick smoke (run via `tests-live/run.sh`, never directly):

```sh
MURMUR_ABUSE_NODES=3 MURMUR_ABUSE_DURATION_SECONDS=60 \
  bash tests-live/run.sh abuse
```

## Pass criteria

- The baseline row converges on every node before abuse starts.
- At least one write is acked during the run (anti-vacuous guard).
- After writers stop, every node is reachable with equal row counts and
  identical PK-ordered row contents within `MURMUR_ABUSE_SETTLE_SECONDS`.

Transient write errors during abuse are counted, never fatal.

## Deviations from the Go original

- Plain SQL (`contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL,
  phone TEXT NOT NULL, score INTEGER NOT NULL)`) replaces the
  typed-contention HTTP API; baseline/writer row shapes keep the Go
  32-hex-char ids and `writer-<n>` names.
- Convergence compares exact sorted row vectors (which imply equal
  counts and digests); the logged digest is dependency-free FNV-1a
  instead of sha256 hex.
- `KillNode`/`StopNode` are both in-process `close()` + reopen: true
  SIGKILL/SIGTERM cannot target one in-process node without freezing the
  test driver, so the alternating kill/graceful mix has no effect.
- Restarts are sequential in-process reopens followed by a single
  re-mesh (fresh ephemeral ports), so catch-up still overlaps like the
  Go barrier restart; per-node readiness is the reopen itself (opened
  under the harness unlock watchdog), with no separate unlock/wait.
- Link cut/heal fans out over 32 scoped threads (same count as Go) with
  non-fatal per-link errors; the verdict is the judge, as in Go.
- Writer targeting uses a dependency-free splitmix64 stream per writer
  (seeded `seed*1000 + writer`, as in Go) instead of `math/rand`.
- The markdown report is written to `runtime/abuse/` (retained under
  `tests-live/failures/` on FAIL via `Cluster::finish`) instead of next
  to the test, so passing runs leave the repo clean.
- Partitions are logical (`remove_peer`), not network-level, same as Go.
- One table, insert-only workload; no updates, deletes, or files
  traffic, same as Go.
