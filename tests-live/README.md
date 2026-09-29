# MURMUR-SQL Live Multi-Process Node Tests

Run `bash tests-live/run.sh all` or an individual scenario runner:

- `bash tests-live/run.sh encryption`
- `bash tests-live/run.sh api-mtls`
- `bash tests-live/run.sh crash-recovery`
- `bash tests-live/run.sh three-node-sync`
- `bash tests-live/run.sh partition`
- `bash tests-live/run.sh chaos-load`
- `bash tests-live/run.sh addrpolicy`
- `bash tests-live/run.sh allow-nodes`
- `bash tests-live/run.sh crdt-contention`
- `bash tests-live/run.sh soak-slo`
- `bash tests-live/run.sh long-running-five-node`

The allow-nodes scenario writes concurrently for three minutes by default; use
`SPEDSQL_ALLOW_NODES_WRITE_SECONDS` to select a shorter development run.

Or run via standard Go test (tags required):
```sh
go test -tags "sqlite_preupdate_hook sqlite_fts5" -v ./tests-live/...
```

`run.sh` and the test harness build the Murmur-SQL daemon with
`SPEDSQL_TAGS` (default `"sqlite_preupdate_hook sqlite_fts5"`; use
`SPEDSQL_TAGS=modernc` with `CGO_ENABLED=0` for the pure-Go backend).
`run.sh` rebuilds `tests-live/bin/testnode` on every invocation; bare
`go test` reuses the existing `tests-live/bin/testnode` as-is, so
rebuild it (or delete it and let the harness rebuild) after changing
product code, or a stale daemon will silently test old behavior.
Set `SPEDSQL_RACE=1` with `run.sh` to build the child node with `go build -race`
and run scenario tests with `go test -race`; instrumenting only the test process
does not instrument the separately built node.

CI runs `bash tests-live/run.sh gate` (release acceptance: API mTLS, smoke,
encryption, backup/restore, partitions, High/Low, files, upgrades,
snapshot resync, crash recovery, discovery mesh, migration crash,
pause/resume) on every push/PR, plus pure-Go (`modernc`) and `-race`
matrices over `three-node-sync`, `rolling-restart`, and `partition`.
`bash tests-live/run.sh soak` (ten-minute file soak, two-hour write SLO,
one-hour five-node mesh) and `bash tests-live/run.sh stress`
(repeated restart/discovery/crash/partition/snapshot runs plus a race
leg; `SPEDSQL_STRESS_COUNT` overrides the repeat count) run on a
weekly schedule plus manual dispatch.

Do not run two `run.sh`/`go test ./tests-live/...` invocations against
the same checkout at the same time: suites share `tests-live/runtime/`
and `tests-live/bin/testnode`, so concurrent runs corrupt each other's
node configs and fail with misleading startup/unlock errors. To run
suites while another invocation is active, isolate both the runtime
root and the daemon binary with a private `SPEDSQL_LIVE_RUNTIME`
directory and a private `SPEDSQL_BIN` path. Ephemeral ports need no isolation:
the harness coordinates them across processes (and checkouts) with
claim files under `${TMPDIR:-/tmp}/spedsql-portclaims` (4h TTL).

Each scenario spins up discrete node instances (e.g. `./node1`, `./node2`, `./node3`, `./node4`) running the internal test fixture binary (`tests-live/harness/testnode`) with isolated Pebble storage, certificates, log files (`node.log`), and HTTPS service/admin endpoints. API requests use a CA-signed client certificate; only `GET /healthz` permits a client without a certificate. Encrypted nodes start with `await-unlock = true` and receive their encryption key through the HTTPS Remote Unlock API (`/v1/admin/unlock`).

Successful runtime directories under `tests-live/runtime/` are automatically removed. Failures are preserved under `tests-live/failures/<timestamp>-<scenario>/` with full node directories and log files for inspection.

Suites should capture diagnostics through the shared helper
`Cluster.DumpForensics(reason)` (`tests-live/harness/forensics.go`):
it fetches status, peer states, metrics, and goroutine stacks from
every node with bounded timeouts (a wedged daemon must never hang a
suite past its own deadline), logs summaries, and writes full bodies
to per-node `forensics-<reason>-*` files preserved with failure
artifacts. Do not hand-roll endpoint fetching in new suites.

Harness cleanup stops every daemon (SIGTERM, SIGKILL after 5s) and
then sweeps recorded PIDs for stragglers; `run.sh` additionally traps
exit/interrupt to reap daemons rooted at its own runtime dir. An
external SIGKILL (`kill -9`, OOM) bypasses all cleanup by definition:
find leftovers with `pgrep -af 'testnode agent --config <root>'`.
