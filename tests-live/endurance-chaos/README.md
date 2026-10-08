# Endurance chaos suite

Long-duration combined-fault validation for Murmur: a live multi-process
mesh absorbs continuous writes while restarts, packet loss, latency, disk
pressure, clock skew, key rotation, snapshot resyncs, and log GC all
overlap. It answers "can I make Murmur fail under realistic abuse?" at
the 24–72 hour timescale with a binary verdict backed by exact
cross-node convergence (equal row counts plus identical PK-ordered
digests), never a sample.

## Run

```sh
# Smoke: 5 nodes, ~10 min of combined chaos (default)
bash tests-live/run.sh endurance-chaos

# Short development pass: 3 nodes, 3 min of chaos
MURMUR_ENDURANCE_NODES=3 MURMUR_ENDURANCE_DURATION_SECONDS=180 \
MURMUR_ENDURANCE_SETTLE_SECONDS=120 \
  go test ./tests-live/endurance-chaos/ -v -count=1 -timeout=15m

# 24-hour acceptance (privileged box: tc + libfaketime for the full mix)
MURMUR_ENDURANCE_DURATION_SECONDS=86400 MURMUR_ENDURANCE_SETTLE_SECONDS=1800 \
  go test ./tests-live/endurance-chaos/ -v -count=1 -timeout=26h

# 72-hour acceptance
MURMUR_ENDURANCE_DURATION_SECONDS=259200 MURMUR_ENDURANCE_SETTLE_SECONDS=3600 \
  go test ./tests-live/endurance-chaos/ -v -count=1 -timeout=76h
```

`go test -timeout` must exceed duration + settle + restart/restart-down
overhead; `run.sh` sizes it automatically (76h ceiling).

## Knobs

| Variable | Default | Meaning |
|---|---|---|
| `MURMUR_ENDURANCE_NODES` | 5 | Nodes in the mesh (clamped to 3..16) |
| `MURMUR_ENDURANCE_DURATION_SECONDS` | 600 | Chaos window: all injectors overlap for this long |
| `MURMUR_ENDURANCE_SETTLE_SECONDS` | 300 | Max convergence wait before the verdict |
| `MURMUR_ENDURANCE_WRITE_INTERVAL_MS` | 25 | Pacing per writer goroutine (up to 8 writers) |
| `MURMUR_ENDURANCE_MAX_ROWS` | 10000 | Live-id ceiling; past it writers recycle via updates/deletes |
| `MURMUR_ENDURANCE_SEED` | 1 | RNG seed for writers and fault targeting |
| `MURMUR_ENDURANCE_STATUS_SECONDS` | 30 | Progress heartbeat interval |
| `MURMUR_ENDURANCE_RESTART_SECONDS` | 90 | Mean interval between rolling restarts (SIGKILL/graceful alternate) |
| `MURMUR_ENDURANCE_RESTART_DOWN_SECONDS` | 8 | Downtime per rolling restart |
| `MURMUR_ENDURANCE_ROTATE_SECONDS` | 180 | Interval between online storage-key rotations (round-robin) |
| `MURMUR_ENDURANCE_SNAPSHOT_SECONDS` | 300 | Interval between snapshot offline windows |
| `MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS` | 75 | Offline length per window (must stay >= 60: retention expiry + GC tick) |
| `MURMUR_ENDURANCE_IMPAIR_ON_SECONDS` | 60 | Impairment phase length (latency, then loss, then both, cycling) |
| `MURMUR_ENDURANCE_IMPAIR_OFF_SECONDS` | 20 | Clear gap between impairment phases |
| `MURMUR_ENDURANCE_IMPAIR_LATENCY_MS` | 150 | tc/netem added latency on replication ports |
| `MURMUR_ENDURANCE_IMPAIR_LOSS_PCT` | 3 | tc/netem packet loss on replication ports |
| `MURMUR_ENDURANCE_DISK_SECONDS` | 180 | Interval between disk-pressure holds |
| `MURMUR_ENDURANCE_DISK_MB` | 128 | Filler size per hold (auto-reduced to leave 1 GiB free) |
| `MURMUR_ENDURANCE_DISK_HOLD_SECONDS` | 60 | Filler hold length per cycle |
| `MURMUR_ENDURANCE_SKEW_SECONDS` | 120 | Clock-skew window length (+5 min on the last node, once per run) |

## Fault mix

- **writes** — bounded insert/update/delete mix against one replicated
  table. The live-id ceiling keeps a 72h run churning without unbounded
  growth, so the verdict digest stays O(ceiling).
- **restarts** — rolling SIGKILL/graceful restarts on random nodes with
  real downtime; the skewed node and the snapshot victim are excluded.
- **packet loss / latency** — tc/netem on replication ports only (API
  and metrics stay clean), cycling latency → loss → both. Without
  `CAP_NET_ADMIN` the suite degrades loudly to logical peer flaps and
  says so in the log and report.
- **disk pressure** — a filler file holds node data dirs under reduced
  free space, then releases; size auto-shrinks to protect the host.
- **clock skew** — one node runs +5 min under libfaketime for one
  window mid-run, then rejoins on a true clock. The FAKETIME syntax is
  calibrated in a fail-fast preflight; without libfaketime (or with a
  static daemon) skew is skipped and reported as degraded.
- **key rotation** — online storage-key rotation round-robins the mesh;
  each rotation is verified via encryption status and the node config
  is rewritten so later restarts unlock with the new key.
- **snapshots** — a victim node goes offline past log-retention expiry
  plus a GC tick while survivors write, forcing a real snapshot resync
  proven by `spedsql_repl_snapshots_received_total`, not just
  convergence.
- **GC** — aggressive retention keeps origin-log GC collecting under
  the sustained update churn; `spedsql_gc_runs_total` and
  `spedsql_gc_log_collected_total` must advance on every node with
  `spedsql_gc_failures_total` flat.

## Verdict

PASS requires: every node reachable with equal row counts and identical
digests after settle; GC ran and collected on every node with zero new
failures; at least one snapshot resync observed; at least one restart,
rotation, impairment cycle, disk hold (unless degraded for space),
snapshot window and skew window (unless the run is too short to fit
them, or skew is degraded for libfaketime) executed; a full
N-1 peer mesh; and a post-chaos write converging everywhere.
Transient write errors during chaos are counted, never fatal.

Progress streams to the console under `-v`: fault events, a heartbeat
line every `MURMUR_ENDURANCE_STATUS_SECONDS`, and the final verdict.
Each run writes `endurance-report-<unixtime>.md` next to the test; on
FAIL the harness preserves node logs and `DumpForensics` captures
status, peers, metrics, and goroutine stacks.

## Limits

- One table, one bounded working set; no file traffic or schema
  migration during chaos (covered by dedicated suites).
- Snapshot windows need room for a full offline window plus 60s of
  reconvergence; runs too short for that skip the leg loudly (log +
  report) instead of failing on coverage they could never produce.
- 72h runs need a stable box (dozens of GB free for daemon logs and
  filler churn); start with the 10-minute smoke and scale up.
