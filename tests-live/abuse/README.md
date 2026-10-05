# Abuse suite

Parameterized adversarial validation for Murmur: a live multi-process mesh
hammered through prolonged partitions, extended outages with reconnect
backlogs, and a simultaneous reconnect storm, with continuous writers
throughout. It answers "can I make Murmur fail under realistic abuse?"
with a binary verdict backed by exact cross-node convergence (row counts
plus PK-ordered digests), never a sample.

## Run

```sh
# Quick smoke: 3 nodes, ~60 s of abuse
MURMUR_ABUSE_NODES=3 MURMUR_ABUSE_DURATION_SECONDS=60 \
  go test -tags "sqlite_preupdate_hook sqlite_fts5" ./tests-live/abuse/ -v -count=1 -timeout=10m

# Default: 10 nodes, 10 minutes of abuse
bash tests-live/run.sh abuse

# Large-scale: 100 nodes, 2 hours of abuse
MURMUR_ABUSE_NODES=100 MURMUR_ABUSE_DURATION_SECONDS=7200 \
  MURMUR_ABUSE_SETTLE_SECONDS=600 \
  go test -tags "sqlite_preupdate_hook sqlite_fts5" ./tests-live/abuse/ -v -count=1 -timeout=3h
```

## Knobs

| Variable | Default | Meaning |
|---|---|---|
| `MURMUR_ABUSE_NODES` | 10 | Nodes in the mesh (clamped to 3..256) |
| `MURMUR_ABUSE_DURATION_SECONDS` | 600 | Total abuse time, split evenly across the four phases |
| `MURMUR_ABUSE_SETTLE_SECONDS` | 120 | Max convergence wait before the verdict |
| `MURMUR_ABUSE_WRITE_INTERVAL_MS` | 50 | Pacing per writer goroutine (up to 8 writers) |
| `MURMUR_ABUSE_SEED` | 1 | RNG seed for writer targeting |
| `MURMUR_ABUSE_STATUS_SECONDS` | 15 | Progress heartbeat interval |

## Phases

1. **mesh-soak** — healthy full mesh, writers running, nothing breaks.
2. **partition** — the mesh splits into halves with no cross-links for a
   full quarter of the run; both sides keep accepting writes. Healed at
   the end of the phase. Link cut/heal is O(N^2) API calls, fanned out
   over 32 workers.
3. **outage** — a quarter of the nodes go down (alternating SIGKILL and
   graceful stop) while writers hammer the survivors, building the
   reconnect backlog. All victims restart behind one barrier. Outage
   length is the "days offline" knob: stretch the duration to simulate
   long absences.
4. **storm** — every node is killed and restarted near-simultaneously
   while writers keep firing into the churn.
5. **verdict** — writers stop; the mesh gets `SETTLE` seconds to reach
   exact convergence. PASS requires every node reachable with equal row
   counts and identical digests.

Transient write errors during abuse are counted, never fatal: broken
individual writes are expected; only divergence fails the run.

## Progress and reports

Progress streams to the console under `-v`: phase transitions, a
heartbeat line every `MURMUR_ABUSE_STATUS_SECONDS` (elapsed time, write
totals, min/max row counts, unreachable nodes), and the final verdict.
Each run also writes `abuse-report-<unixtime>.md` next to the test with
the same timeline; on FAIL the cluster is marked failed so node logs
are preserved for forensics.

## Limits

- Partitions are logical (`RemovePeer`), not network-level (iptables/tc).
- One table, insert-only workload; no updates, deletes, or files traffic.
- 200-node runs need a large box (tens of GB RAM for 200 daemons);
  start at 10/50 nodes and scale up.
