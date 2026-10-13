# SQLite insert benchmark

Runs the same three single-row autocommit cases: plain SQLite, a table marked
with `crsql_as_crr`, and SQLite with a session attached to record changes.
Each timed insert uses a fresh SQL string instead of a reused prepared
statement. Session teardown is outside the timed section.

From this directory, run:

```sh
cargo run --release -- --duration 30
```

Build the local loadable extension once from this directory with
`sh build-crsqlite.sh`. The CRR case loads `./libcrsqlite.so`. Pass
`--extension PATH_WITHOUT_SUFFIX` to use a different extension file; SQLite
adds the platform suffix.

## Persistence comparison

Run `cargo run --release --bin persistence -- 8` in this directory for eight
seconds per case. This compares plain SQLite, session capture, managed session
capture with collection deferred, and background Spool persistence. The workload
uses repeated updates to 1,000 rows, with 100 updates per transaction. It reports
throughput, transaction p99, extraction pause, final flush, and recovery time.

The managed cases include callback/thread dispatch overhead. Background work uses
shared CPU and memory bandwidth, so this measures its effect rather than assuming
zero overhead. Conflict metadata lives in Spool and batch versions are assigned
at collection time. No second SQLite database is used.

## Replication workload

Run `cargo run --release --bin replbench -- 20 15 100 240` in this directory
for twenty seconds of paced medium load (100 insert+update+select
iterations/s), fifteen seconds of unpaced insert burst (20 rows per
transaction), then catch-up until the replica converges (240s budget). It
reports per-op latency for the medium phase, average inserts/s for the burst,
convergence time, and per-node RAM (SQLite page image) vs on-disk (Spool
segments) sizes with the disk/RAM ratio.

Non-default options, required for burst load: `capture_interval` 10ms and
`queue_bytes` 128 MiB (see the source comments). The 5s default collection
lets a fast burst outgrow the queue, after which writes wedge on `Busy` and
checkpoints can never be admitted. Large replication transactions also stall
catch-up (multi-MB chunked transfers never complete; spool caps single values
at 16 MiB while transactions allow 64 MiB). If the replica does not converge
in time, partial state and sizes are still reported with NOT CONVERGED.

## Raw in-memory speed

Run `cargo run --release --bin rawbench -- 5` for five seconds per case with
persistence deferred (no replication, no checkpoint during the timed section):
raw rusqlite+session inserts, `Database` single-row inserts, `Database`
20-row batched inserts, point selects by PK, and full scans. Databases are
leaked instead of closed because `close` flushes the whole unflushed session
first, which is pathological after a deferred run.
