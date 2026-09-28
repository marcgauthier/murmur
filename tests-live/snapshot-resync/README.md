# Stale-node snapshot resync (multi-process)

Run with `go test -count=1 ./tests-live/snapshot-resync`. Three `spedsql`
daemon processes mesh with aggressive log retention (1s log retention, 2s
offline pin, 10 retained batches). Node3 writes acknowledged rows and the
mesh converges; node3 stops; the survivors write 60 rows; the test waits
out retention expiry plus one 30s log-GC pass; node3 restarts with the same
durable directory.

Node3's needed log ranges are unrecoverable, so it must rejoin by merging
a snapshot. The test requires full digest convergence on all three nodes,
its five pre-stop rows intact, and `repl_snapshots_received_total >= 1` on
node3 scraped from `/metrics` — proving the snapshot path ran rather than
plain log catch-up.
