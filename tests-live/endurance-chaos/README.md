# endurance-chaos (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/endurance-chaos/`
(`endurance_chaos_test.go`: `TestEnduranceChaos`, plus the
`faults_test.go` helpers and `statfs_*_test.go` free-space probes).

## What it proves

Long-duration combined-fault validation: a live multi-node mesh absorbs
continuous writes while rolling restarts, peer flaps, disk pressure,
online key rotation, snapshot resyncs, and origin-log GC all overlap for
a configured chaos window. The verdict is exact cross-node convergence
(equal row counts plus identical PK-ordered row contents) with
counter-proven GC progress on every node, at least one proven snapshot
resync, successful key rotations, a rejoined full mesh, and a post-chaos
write that converges everywhere.

## Env knobs

Same names and defaults as Go:

- `MURMUR_ENDURANCE_NODES` (default 5): nodes in the mesh (clamped to 3..16).
- `MURMUR_ENDURANCE_DURATION_SECONDS` (default 600): chaos window.
- `MURMUR_ENDURANCE_SETTLE_SECONDS` (default 300): max convergence wait.
- `MURMUR_ENDURANCE_WRITE_INTERVAL_MS` (default 25): pacing per writer
  thread (up to 8 writers).
- `MURMUR_ENDURANCE_MAX_ROWS` (default 10000): live-id ceiling; past it
  writers recycle via updates/deletes.
- `MURMUR_ENDURANCE_SEED` (default 1): RNG seed.
- `MURMUR_ENDURANCE_STATUS_SECONDS` (default 30): heartbeat interval.
- `MURMUR_ENDURANCE_RESTART_SECONDS` (default 90): mean rolling-restart
  interval; `MURMUR_ENDURANCE_RESTART_DOWN_SECONDS` (default 8): downtime
  per restart.
- `MURMUR_ENDURANCE_ROTATE_SECONDS` (default 180): online key-rotation
  interval (round-robin).
- `MURMUR_ENDURANCE_SNAPSHOT_SECONDS` (default 300):
  `MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS` (default 75, minimum 60):
  snapshot offline windows.
- `MURMUR_ENDURANCE_IMPAIR_ON_SECONDS` (default 60) /
  `MURMUR_ENDURANCE_IMPAIR_OFF_SECONDS` (default 20): impairment phase
  length and clear gap. `MURMUR_ENDURANCE_IMPAIR_LATENCY_MS` (default 150)
  and `MURMUR_ENDURANCE_IMPAIR_LOSS_PCT` (default 3) are parsed and logged
  but have no effect (flap-only; no tc privilege model in-process).
- `MURMUR_ENDURANCE_DISK_SECONDS` (default 180) /
  `MURMUR_ENDURANCE_DISK_MB` (default 128) /
  `MURMUR_ENDURANCE_DISK_HOLD_SECONDS` (default 60): disk-pressure holds.
- `MURMUR_ENDURANCE_SKEW_SECONDS` (default 120): parsed and logged but the
  skew leg is degraded away (no daemon for libfaketime).
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

Quick smoke (run via `tests-live/run.sh`, never directly):

```sh
MURMUR_ENDURANCE_NODES=3 MURMUR_ENDURANCE_DURATION_SECONDS=180 \
MURMUR_ENDURANCE_SETTLE_SECONDS=120 \
  bash tests-live/run.sh endurance-chaos
```

## Pass criteria

- The baseline row converges on every node before chaos starts.
- At least one write is acked during the run (anti-vacuous guard).
- After chaos stops, every node is reachable with equal row counts and
  identical PK-ordered row contents within settle.
- GC ran and collected on every node (restart-proof high-water history)
  with no GC-attributed engine error ever observed.
- At least one restart, rotation, impairment cycle, disk hold (unless
  degraded for space), and snapshot window (unless the run is too short
  to fit one) executed; at least one snapshot resync proved via the
  snapshot path (see deviations).
- A full N-1 peer mesh is back, and a post-chaos write converges
  everywhere.

Transient write errors during chaos are counted, never fatal.

## Deviations from the Go original

- Plain SQL (`contention(id TEXT PRIMARY KEY, name TEXT NOT NULL,
  phone TEXT NOT NULL, score INTEGER NOT NULL)`) replaces the
  typed-contention HTTP API; writer row shapes keep the Go 32-hex-char
  ids (`%08x%024x` per writer/sequence) and `writer-<n>` names. The Go
  two-call update (name, then phone) is one SQL `UPDATE` setting both
  columns; convergence verdicts hash actual state, so the split is
  unobservable there.
- Convergence compares exact sorted row vectors (which imply equal
  counts and digests); the logged digest is dependency-free FNV-1a
  instead of sha256 hex.
- Every restart is in-process `close()` + reopen: true SIGKILL vs
  graceful cannot target one in-process node without freezing the test
  driver, so the Go alternating kill/graceful mix has no effect.
- tc/netem impairment degrades to logical peer flaps in all runs (the Go
  fallback when `CAP_NET_ADMIN` is absent): per-edge `remove_peer` holds
  cycling the latency -> loss -> both phase names, then re-add. There is
  no tc probe because ephemeral in-process QUIC ports have no stable
  shaping target.
- Clock skew is always skipped and reported as degraded: in-process
  nodes share the test driver clock, so no node can run under
  libfaketime (Go skips the same way without libfaketime or with a
  static daemon).
- Key rotation is `Database::rotate_master_key` with a fresh
  `/dev/urandom` key; the Rust product has no key IDs, so the Go active
  `ApplicationKeyID` assertion maps to staying online plus later
  restarts unlocking with the new key (the harness stored key is updated
  via the new `Cluster::set_node_key`, mirroring Go `rewriteKeyConfig`).
- Snapshot trust is configured up front (every node trusts every node)
  instead of via the Go fixture policy; without this overlay no leg
  could request a snapshot.
- No snapshot counters exist (`ReplicationMetrics` carries none), so the
  Go `snapshots_received >= 1` leg maps to a behavioral proof per
  offline window: after a 30 s catch-up delay with writers still
  running, the window counts as proven only when the victim has
  approximately caught up (row-set drift within `max(200, rows/50)`)
  while its `applied_transactions` delta stays below the outage's acked
  write count (a pure log catch-up would have to apply every missed
  transaction; snapshot application bypasses that counter, as the
  `snapshot-resync` port relies on).
- No GC-failures counter exists, and the engine multiplexes every
  subsystem error (including peer-disconnect noise from
  restarts/flaps) into one `last_error` latch — so unlike the
  quiesced `gc-balance`/`maintenance-under-load` ports, which assert the
  latch flat, this port fails only on GC-substring matches
  (`retention`/`collect`/`gc_`/`prune`), the faithful mapping of Go's
  GC-specific `spedsql_gc_failures_total` under chaos.
- The impairment cycler runs on the chaos director (test thread) rather
  than a background thread, because peer add/remove borrows the
  cluster; writer, status-sampler, and disk-pressure loops stay on
  background threads. Status/GC sampling reads through the shared
  handle table, never through the cluster.
- The markdown report is written to `runtime/endurance-chaos/`
  (retained under `tests-live/failures/` on FAIL via `Cluster::finish`)
  instead of next to the test, so passing runs leave the repo clean.
- One table, one bounded working set; no file traffic or schema
  migration during chaos, same as Go.
