# Multi-peer schema migration crash recovery

Run with `CGO_ENABLED=0 GOMAXPROCS=1 bash tests-live/run.sh migration-crash`.

The scenario starts three typed Murmur nodes and seeds converged records. It
isolates one node from the other two, then injects process exit immediately
before and immediately after the durable schema-manifest store. The peers stay
on epoch 1 during each crash. Before-store recovery reopens at epoch 1 and
retries the migration; after-store recovery reopens at epoch 2 with the v2
application descriptor. In both cases, peers adopt epoch 2, rebind their typed
handles, and converge with the recovered node, including a write to the new
field.

The testnode's crash hook is available only under the `murmur_testhooks` build
tag, which the scenario runner adds for this scenario. The root Murmur test
`TestTypedSchemaMigrationProcessCrashBoundary` separately covers these local
durability boundaries.
