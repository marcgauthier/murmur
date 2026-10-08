# High/Low bridge live scenarios

Run with `CGO_ENABLED=0 go test -count=1 ./tests-live/highlow` or
`CGO_ENABLED=0 bash tests-live/run.sh highlow`. Real Low and High
Murmur-SQL daemon processes (distinct DBIDs, no mesh between them) move
logical writes across a directory drop, driven only through HTTP: disconnected
transfer with one-way role gating, gap delivery with duplicate/reordered
recovery, two-receiver convergence, forgery/corruption/misnaming rejection
without partial apply, outbox/inbox restart resumption, schema-hold survival
across restart with release after a local High migration (no DDL crosses),
signer rotation plus outage catch-up, and High-owned-field reorder
convergence across two meshed High peers with opposite import/override
orders (plus release convergence back to the latest Low value). This suite
uses managed typed records and runs without SQLite or CGO.

Cross-peer policy/value interleavings with hostile HLC ordering are pinned
at the DB layer by `TestBridgeShadowReorderConvergence` (see the root
`bridge_shadow_test.go`); single-peer override/release is covered in
`bridge/ownership_test.go`. File-object transfer has its own suite at
`tests-live/files-bridge/`. This suite asserts functional outage catch-up,
not staging-byte budgets, which remain unasserted.
