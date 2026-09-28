# SPeD-SQL Live Multi-Process Node Tests

Run `bash tests-live/run.sh all` or an individual scenario runner:

- `bash tests-live/run.sh encryption`
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

Or run via standard Go test:
```sh
go test -v ./tests-live/...
```

Each scenario spins up discrete node instances (e.g. `./node1`, `./node2`, `./node3`, `./node4`) running the standalone daemon binary (`cmd/spedsql`) with isolated Pebble storage, certificates, log files (`node.log`), and HTTP service/admin endpoints. Encrypted nodes start with `await-unlock = true` and receive their encryption key through the local Remote Unlock HTTP API (`/v1/admin/unlock`).

Successful runtime directories under `tests-live/runtime/` are automatically removed. Failures are preserved under `tests-live/failures/<timestamp>-<scenario>/` with full node directories and log files for inspection.
