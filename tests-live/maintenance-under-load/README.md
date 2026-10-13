# maintenance-under-load (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/maintenance-under-load/`
(`maintenance_under_load_test.go`, `TestMaintenanceUnderSustainedLoad`).

## What it proves

Three nodes take sustained concurrent writes with **zero failed writes
allowed** while tight retention forces origin-log GC. GC passes and batch
collection must advance on **every** node with writers provably still
progressing (overlap), GC failures must stay flat, and all nodes must
converge on exact counts plus equal id-ordered digests, followed by a
post-maintenance write that replicates everywhere.

Only counter-observed maintenance is claimed (same inventory as Go):
origin-log GC is claimed; receipt collection, durability sync, FilesGC,
snapshot resync, and spool compaction are NOT claimed.

## Env knobs

- `MURMUR_MAINT_LOAD_SEED` (default 120): seed rows inserted before load.
- `MURMUR_MAINT_LOAD_MAX_INSERTS` (default 4000): per-writer insert cap;
  writers then switch to updates cycling their own rows.
- `MURMUR_MAINT_LOAD_GC_TIMEOUT` (default 150s; accepts `150`, `150s`,
  `2m`, `1h`): maintenance observation window.

## Pass criteria

1. `retention_runs` and `collected_transactions` advance past baseline on
   every node while writers run, and every writer had `ok > 0` when GC was
   observed.
2. Zero failed writer ops; replication `last_error` unchanged on every node.
3. Exact row counts plus identical PK-ordered contents on all nodes, then a
   post-maintenance insert on node3 converges everywhere.

## Deviations from the Go original

- Plain SQL (`contention` table via `Database::transaction`/`read`) replaces
  the typed-contention HTTP API; same key space (`rowID` format, writer
  ranges at `1<<20*(w+1)`, post-maintenance id at `seed + 1<<22`).
- Metrics come from in-process APIs instead of `/metrics`:
  `retention_runs` (was `spedsql_gc_runs_total`),
  `retention_status().collected_transactions` (was
  `spedsql_gc_log_collected_total`); there is no GC-failures counter, so
  replication `last_error` staying flat stands in for
  `spedsql_gc_failures_total` (same substitution as the `gc-balance` port).
- Digests: PK-ordered row vectors are compared directly instead of
  SHA-256-hashing (equivalent equality proof, no extra dep).
- In-process `Cluster` nodes instead of `testnode` daemons; no
  `/v1/status` diagnostics (forensics via `dump_forensics`).
