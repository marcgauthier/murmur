# RIME live qualification

Run the real in-memory engine with concurrent readers, contending writers,
MVCC snapshots, full scans, indexed queries, events, delete/reinsert commits,
and background GC:

```sh
go run -race ./tests-live/rime -duration=30m -rows=1000 -readers=4 -writers=4 -seed=127 > rime-live.jsonl
# From the repository root:
MURMUR_RACE=1 RIME_DURATION=30m bash tests-live/run.sh rime
```

The command exits unsuccessfully on an invariant violation or unexpected
error. It emits JSON measurements periodically and after cleanup, including
latency, heap/RSS, GC, goroutines, conflict counts, and retained versions.
Use `-shards=1` to concentrate all keys in one shard. Run `-help` for options.
This scenario uses one standalone process and does not use the Murmur daemon.

See [qualification and release gates](../../architecture/rime-testing.md)
for measurement definitions, memory checks, fuzzing, CI, and longer runs.

To include maintained query views:

```sh
go run -race ./tests-live/rime -views -duration=30m -rows=1000 -shards=4 -readers=4 -writers=4 -seed=127 > rime-views-live.jsonl
```

This maintains a filtered view incrementally and reevaluates a two-table join
on each source commit. Readers compare cached results to fresh queries at the
reported view commit while writers and GC continue. JSON output includes
`view_checks`; an enabled run must complete at least one comparison.
The ordinary command leaves views disabled to preserve existing workload costs.
