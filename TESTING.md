# Testing Murmur

The database, replication, and Spool packages have separate Cargo manifests.
Running tests at the repository root does not run the dependencies' unit tests.

Run the routine suites:

```sh
cargo test
cargo test --features replication
cargo test --all-features
cargo test --manifest-path replication/Cargo.toml
cargo test --manifest-path spool/Cargo.toml
```

Managed search tests cover FTS5 and optional sqlite-vec maintenance, read-only
virtual/shadow tables, source-only exports, queue overflow, migration rollback,
conflict resolution, replication settings, subscriptions, recovery, and restore.
The replication and snapshot suites also exercise local index publication after
QUIC delivery, peer schema adoption, and snapshot application. Run the focused
suite with `cargo test --all-features --test search_indexes`.

For a manual comparison of source-write cost with and without managed indexes,
run `cargo test --test search_indexes application_write_cost_with_indexes --
--ignored --nocapture`. It reports the median of three runs with capture and
index maintenance deferred; throughput is diagnostic, not a pass/fail threshold.

The new failure tests cover retention limits and durable floors, encrypted file
boundaries and corruption, upload failures, rotation and collection, resumed peer
fetches, bridge gaps/replays and failed completion receipts, local table settings,
seeded partitioned edits, concurrent shutdown, and cache/parser resource bounds.
File and bridge recovery tests reopen actual encrypted Spool stores. Retention
policy tests use a controlled clock and a recording backend; database retention
tests exercise the real serialized Spool worker and recovery snapshots.

The additional retention process-crash matrix is separate from routine runs:

```sh
cargo test --features replication --lib process_crashes_between_retention -- --ignored
```

It terminates child processes after logical snapshot publication, after durable
coverage publication, and after atomic pruning. Recovery must retain SQL state
and tombstones; log deletion and duplicate-floor advancement must agree.

Seeded randomized tests use fixed seeds and compare against independent expected
SQL/counter/set values. Report the test name and seed when a failure occurs.
Network tests use ephemeral loopback ports and poll explicit state with deadlines.
Spool's interoperability suite also exercises Go compatibility when its external
tools/fixtures are available; see `spool/tests/interop.rs` for its requirements.

Snapshot tests cover signed manifests, chunk conflicts and corruption, paginated
resume, deferred/failed apply, durable receipts, staging limits and retention
pins. Database tests reopen both encrypted stores and exercise pruning, local-only
tables and pruned-history catch-up with counters, sets and deletions.

Run the additional snapshot crash matrix separately:

```sh
cargo test --features replication --lib process_crashes_during_snapshot_transfer -- --ignored
```

It exits child processes after outgoing publication, partial receiving, durable
application before acknowledgement, and durable catch-up-target publication.
Reopening must preserve missing-chunk progress, completed state and retention pins.
