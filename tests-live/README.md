# SPeD-SQL Live Multi-Process Node Tests

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

`run.sh` and the test harness build the `spedsql` daemon with
`SPEDSQL_TAGS` (default `"sqlite_preupdate_hook sqlite_fts5"`; use
`SPEDSQL_TAGS=modernc` with `CGO_ENABLED=0` for the pure-Go backend).
`run.sh` rebuilds `bin/spedsql` on every invocation; bare `go test`
reuses the existing `bin/spedsql` as-is, so rebuild it (or delete it
and let the harness rebuild) after changing product code, or a stale
daemon will silently test old behavior.

CI runs `bash tests-live/run.sh gate` (release acceptance: API mTLS, smoke,
encryption, backup/restore, partitions, High/Low, files, upgrades,
snapshot resync, crash recovery) on every push/PR, and
`bash tests-live/run.sh soak` (ten-minute file soak, two-hour write SLO,
one-hour five-node mesh) on a weekly schedule plus manual dispatch.

Do not run two `run.sh`/`go test ./tests-live/...` invocations against
the same checkout at the same time: suites share `tests-live/runtime/`,
so concurrent runs corrupt each other's node configs and fail with
misleading startup/unlock errors. To run suites while another
invocation is active, isolate the runtime root with a private
`SPEDSQL_LIVE_RUNTIME` directory. Ephemeral ports need no isolation:
the harness coordinates them across processes (and checkouts) with
claim files under `${TMPDIR:-/tmp}/spedsql-portclaims` (4h TTL).

Each scenario spins up discrete node instances (e.g. `./node1`, `./node2`, `./node3`, `./node4`) running the standalone daemon binary (`cmd/spedsql`) with isolated Pebble storage, certificates, log files (`node.log`), and HTTPS service/admin endpoints. API requests use a CA-signed client certificate; only `GET /healthz` permits a client without a certificate. Encrypted nodes start with `await-unlock = true` and receive their encryption key through the HTTPS Remote Unlock API (`/v1/admin/unlock`).

Successful runtime directories under `tests-live/runtime/` are automatically removed. Failures are preserved under `tests-live/failures/<timestamp>-<scenario>/` with full node directories and log files for inspection.
