# Stale-node snapshot resync (multi-process)

Run with `go test -count=1 ./tests-live/snapshot-resync`. Three `spedsql`
daemon processes mesh with aggressive log retention (1s log retention, 20s
offline pin, 10 retained batches). Node3 writes acknowledged rows and the
mesh converges; node3 stops; the survivors write 60 rows; the test waits
out retention expiry plus one 30s log-GC pass; node3 restarts with the same
durable directory.

Node3's needed log ranges are unrecoverable, so it must rejoin by merging
a snapshot. The test requires full digest convergence on all three nodes,
its five pre-stop rows intact, and `spedsql_repl_snapshots_received_total
>= 1` on node3 scraped from `/metrics` — proving the snapshot path ran
rather than plain log catch-up.

A second test stops two nodes, writes the survivor past retention, and
restarts both stale nodes back-to-back so their snapshot requests overlap
on the survivor. Both must converge with snapshot counters `>= 1`, and the
progress/deferral series (`spedsql_repl_snapshots_busy_deferred_total`,
`spedsql_repl_snapshot_busy_received_total`,
`spedsql_peer_awaiting_snapshot`,
`spedsql_peer_snapshot_chunks_received/total`) must be present in
`/metrics`.
