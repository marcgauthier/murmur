# MURMUR-SQL Live Multi-Process Node Tests

The separate [RIME workload](rime/README.md) runs one standalone in-memory
engine process: `RIME_DURATION=30m bash tests-live/run.sh rime`. It is
qualified by its own nightly workflow and is excluded from the daemon
`all` and `gate` scenarios.

Run `bash tests-live/run.sh all` or an individual scenario runner:

- `bash tests-live/run.sh abuse`
- `bash tests-live/run.sh resource-exhaustion`
- `bash tests-live/run.sh endurance-chaos`
- `bash tests-live/run.sh open-progress`
- `bash tests-live/run.sh churn-retirement`
- `bash tests-live/run.sh addrpolicy`
- `bash tests-live/run.sh encryption`
- `bash tests-live/run.sh api-mtls`
- `bash tests-live/run.sh crash-recovery`
- `bash tests-live/run.sh three-node-sync`
- `bash tests-live/run.sh partition`
- `bash tests-live/run.sh chaos-load`
- `bash tests-live/run.sh allow-nodes`
- `bash tests-live/run.sh crdt-contention`
- `bash tests-live/run.sh graceful-shutdown`
- `bash tests-live/run.sh plaintext-audit`
- `bash tests-live/run.sh delete-pruning`
- `bash tests-live/run.sh typed-records`
- `bash tests-live/run.sh backup-restore`
- `bash tests-live/run.sh version-skew`
- `bash tests-live/run.sh tls-floor`
- `bash tests-live/run.sh cert-lifecycle`
- `bash tests-live/run.sh dbid-isolation`
- `bash tests-live/run.sh merge-policies`
- `bash tests-live/run.sh origin-signatures`
- `bash tests-live/run.sh file-permissions`
- `bash tests-live/run.sh files-bridge`
- `bash tests-live/run.sh files-soak`
- `bash tests-live/run.sh files-crash`
- `bash tests-live/run.sh files-corrupt-source`
- `bash tests-live/run.sh maintenance-under-load`
- `bash tests-live/run.sh snapshot-resync`
- `bash tests-live/run.sh tail-repair`
- `bash tests-live/run.sh three-way-heal`
- `bash tests-live/run.sh migration-concurrency`
- `bash tests-live/run.sh migration-crash`
- `bash tests-live/run.sh rekey`
- `bash tests-live/run.sh soak-slo`
- `bash tests-live/run.sh long-running-five-node`
- `bash tests-live/run.sh spool`

The allow-nodes scenario writes concurrently for three minutes by default; use
`MURMUR_ALLOW_NODES_WRITE_SECONDS` to select a shorter development run.

The [`migration-crash` scenario](migration-crash/README.md) exercises exact
before/after schema-manifest crash recovery on an isolated node in a three-node
cluster, followed by peer convergence.

All maintained database scenarios use managed typed records and RIME. The
`sqli-api` scenario verifies that removed SQL endpoints reject requests without
changing typed records. Run one scenario at a time while qualifying the shared
runtime and fixture binary:

```sh
CGO_ENABLED=0 GOMAXPROCS=2 bash tests-live/run.sh typed-records
```

RIME scenarios (`typed-records`, `typed-bridge`, `views`,
`three-node-sync`, `open-progress`, `crash-recovery`, `graceful-shutdown`,
`plaintext-audit`, `delete-pruning`, `crdt-contention`, `schema-evolution`, `merge-policies`,
`origin-signatures`, `rekey`, `highlow`,
`file-permissions`, `backup-restore`, `version-skew`, `tls-floor`, `cert-lifecycle`,
`churn-retirement`, `addrpolicy`, `dbid-isolation`, `allow-nodes`, `files-bridge`,
`files-soak`, `files-crash`, `files-corrupt-source`,
`maintenance-under-load`, `partition`, `snapshot-resync`,
`tail-repair`,
`three-way-heal`,
`migration-concurrency`, `migration-crash`,
`backup-under-fire`, `tampered-backup`, and `bridge-two-streams`)
compile with CGO disabled and no extra build tags. Their child
fixture is cached separately as
`tests-live/bin/testnode-typed`; set `MURMUR_TYPED_TAGS` only when a native
fixture needs additional build tags. `MURMUR_RACE=1` enables CGO for Go's race
detector. This ensures typed live rehearsals also qualify the CGO-free build.

`run.sh` builds the test fixture with `MURMUR_TAGS` (default `""`). For daemon
scenarios, `run.sh` rebuilds
`tests-live/bin/testnode` on each invocation; bare `go test` reuses that binary
only while it is fresh. A `testnode.tags` stamp records the tags, and the
harness rebuilds when any `.go`/`go.mod`/`go.sum` input is newer than the
binary, logging the reason. Typed runs use their separate no-tag binary and
freshness stamp.
Set `MURMUR_RACE=1` with `run.sh` to build the child node with `go build -race`
and run scenario tests with `go test -race`; instrumenting only the test process
does not instrument the separately built node.

CI runs `bash tests-live/run.sh gate` (release acceptance: API mTLS, smoke,
encryption, backup/restore, partitions, High/Low, files, upgrades,
snapshot resync, crash recovery, discovery mesh, typed migration concurrency,
pause/resume, graceful shutdown, plus delete/crash/discovery/partition
hardening, backup integrity, security posture, and
subscription/migration suites — see the `GATE_SCENARIOS` comment in
`run.sh` for the full list) on every push/PR, plus a `-race`
matrix over `three-node-sync`, `rolling-restart`, and `partition`.
A privileged CI step runs the env-gated suites (`clock-skew` with
libfaketime, `impaired-network` with tc/netem, `diskfull-live` with a
tmpfs mount); locally they skip with a message when the capability is
absent.
`bash tests-live/run.sh soak` (ten-minute file soak, two-hour write SLO,
one-hour five-node mesh) and `bash tests-live/run.sh stress`
(repeated restart/discovery/crash/partition/snapshot runs plus a race
leg; `MURMUR_STRESS_COUNT` overrides the repeat count) run on a
weekly schedule plus manual dispatch.

Do not run two `run.sh`/`go test ./tests-live/...` invocations against
the same checkout at the same time: suites share `tests-live/runtime/`
and `tests-live/bin/testnode`, so concurrent runs corrupt each other's
node configs and fail with misleading startup/unlock errors. To run
suites while another invocation is active, isolate both the runtime
root and the daemon binary with a private `MURMUR_LIVE_RUNTIME`
directory and a private `MURMUR_BIN` path. Ephemeral ports need no isolation:
the harness coordinates them across processes (and checkouts) with
claim files under `${TMPDIR:-/tmp}/spedsql-portclaims` (4h TTL).

Daemon scenarios spin up discrete node instances (e.g. `./node1`, `./node2`, `./node3`, `./node4`) running the internal test fixture binary (`tests-live/harness/testnode`) with isolated Spool storage, certificates, log files (`node.log`), and HTTPS service/admin endpoints. API requests use a CA-signed client certificate; only `GET /healthz` permits a client without a certificate. Encrypted nodes start with `await-unlock = true` and receive their encryption key through the HTTPS Remote Unlock API (`/v1/admin/unlock`).

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

## Embedded startup progress

`open-progress` populates encrypted Spool, then opens it in fresh embedded
processes to validate progress callbacks, processed cells without a counting pass, committed rows,
indexes, cancellation, failure reporting, and recovery through later reopens.
It is included in `all` and `gate` and uses its own test worker binary.

The `origin-signatures` scenario verifies Ed25519 proofs through a live relay,
rejection of origin impersonation, and restart/idempotency. The shared harness
provisions independent random signing keys and explicit snapshot sources.
See [scenario details](origin-signatures/README.md).
