# gc-balance (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/gc-balance/`
(`README.md` + `gc_balance_test.go`: `TestLogGCKeepsUpWithChurn`).

## What it proves

Three encrypted, mutually authenticated nodes mesh with aggressive log
retention (retention 1s, minimum retained 10, maximum offline 20s, 1s
collection cadence) while node 1 updates a fixed 200-key set with eight
parallel updaters over disjoint ranges. After a 20s warmup, a 40s window
(stretched to a 150s cap until a 2000-commit floor is met) must show GC
collecting at least 90% of the origin-log batches the window produced,
i.e. the retained set does not grow; GC runs with zero failures. The
suite ends with exact row counts and identical digests on all nodes (no
loss).

Scope (same as Go): version churn only; deletes are out of scope
(delete pruning is unimplemented).

## Pass criteria

- `Cluster::new` opens 3 meshed nodes under
  `tests-live/runtime/gc-balance/` with aggressive retention overlays.
- Node 1 seeds 200 rows (`v-0`); all nodes converge on 200 rows within
  60s.
- Eight updaters churn disjoint 25-key ranges on node 1; zero update
  errors and at least 5 full sweeps per updater.
- The measurement window produces at least 2000 commits (else stretched
  in 5s steps to a 150s cap); the local writer scheduler serves
  admissions during the window (no starvation).
- At least one GC run is observed in the window; the replication
  `last_error` is unchanged across the window (zero GC failures).
- Balance: `commits - collected <= commits / 10` after the post-stop
  drain (one post-stop run observed, then 2 stable 5s collected polls).
- A follower observes a churned value for key 1 within 30s
  (end-to-end update flow).
- All nodes hold exactly 200 rows with identical ordered
  `(id, name, phone, score)` contents (polled 60s, then asserted exactly).

## Env knobs

- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.
- No scenario-specific knobs (same as Go: 200 keys, 8 updaters, fixed
  window bounds).

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`contention(id TEXT PRIMARY KEY, name TEXT, phone TEXT,
  score INTEGER)`) via `Database::transaction`/`Database::read` instead
  of the managed typed-contention HTTP API
  (`TypedContentionInsert/Update/Read/Rows`); keys keep the Go 1-based
  `%032x` format; only the `name` column churns (as in Go).
- Concurrency via `std::thread::scope` (8 workers on cloned node-1
  handles, `AtomicBool` stop flag, hang-guard deadline) instead of
  goroutines + `WaitGroup`; per-op `flush()` publishes each write to
  replication; per-op errors are counted (`update_errs`) rather than
  failing fast, as in Go.
- Aggressive retention maps onto the harness
  `ReplicationOptions { retention: 1s, minimum_retained: 10,
  maximum_offline: 20s }` overlays (Go: `MinLogRetentionMs`,
  `MinRetainedBatches`, `MaxOfflineLogRetentionMs`), plus a 1s
  `retention_interval` so background passes run inside the 40s window
  (the product default is 60s; see below).
- Counters are sampled from product APIs on node 1 instead of the
  `/metrics` endpoint: `Status.committed` replaces
  `spedsql_local_commits_total`,
  `retention_status().collected_transactions` replaces
  `spedsql_gc_log_collected_total`, and
  `status().metrics.retention_runs` replaces `spedsql_gc_runs_total`.
  The test also logs its own successful-update count as a cross-check.
- No GC-failures counter exists in Rust, so the zero-failures check
  asserts the replication `last_error` is unchanged across the window
  (collection errors land there); a repeated identical error string
  would be missed, unlike Go's counter.
- No maintenance-class check exists on the Rust collection path
  (retention runs in the replication engine, bypassing the
  `WriterScheduler`; the maintenance class only serves
  migrate/rekey). The scheduler-liveness check instead asserts local
  admissions advanced during the window.
- Digest agreement compares full ordered row vectors directly instead
  of SHA-256 hex digests (equivalent, strictly stronger).

## Shared-harness addition

One optional overlay field, same pattern as the adjacent retention
knobs: `ReplicationOptions::retention_interval` (collection cadence,
`Config::retention_interval`) in `harness/src/cluster.rs`, applied in
`Cluster::new`. Required: with the product default (60s), no
background pass would reliably fall inside the 40s measurement window,
so the scenario could not observe GC. All 10 pre-existing
`ReplicationOptions` literals use `..Default::default()`, so the
addition breaks no other scenario.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/gc-balance/Cargo.toml
```
