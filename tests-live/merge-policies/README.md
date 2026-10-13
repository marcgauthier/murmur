# merge-policies (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/merge-policies/`
(`merge_policies_test.go`, `README.md`).

## What it proves

Three in-process nodes share one typed row (`live_merge_policies`, TEXT
primary key, PN_COUNTER / OR_SET / MAX / MIN policy columns); one `#[test]`
fn proves the Go pass criteria:

| Rust test | Go source |
|---|---|
| `disconnected_policies_forward_and_restart` | `TestDisconnectedPoliciesForwardAndRestart` |

Pass criteria: while disconnected, node 0 writes counter +10 / set
{shared, red} / extrema (10, 30), node 1 writes counter +7 / unobserved
remove of "shared" / set {blue} / extrema (20, 15), node 2 writes counter
-2 / set {green} / extrema (5, -7); after nodes 0+1 connect both converge
counter 17; node 0 stops and node 1 forwards to node 2, converging counter
15, set {blue, green, red, shared} (the unobserved remove erased nothing),
peak 20, floor -7 on nodes 1+2; node 2 restarts and recovers counter 15 and
the full projection; a later observed remove of "shared" from node 1
propagates to node 2 ({blue, green, red}, extrema unchanged).

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).
Convergence waits are 30 s with 50 ms polls, matching the Go timeouts.

## Deviations from the Go original

- Plain SQL via `Database::transaction`/`Database::read` plus `policy.rs` ops
  (`counter_add`, `set_add`/`set_remove`, `min`, `max`) replaces the managed
  typed HTTP API; there is no HTTP server in the Rust product.
- Merge policies are bound at open through the harness
  `ReplicationOptions::merge_policies` overlay (added by the `typed-records`
  port; no new shared-harness change here).
- `TypedInsertWithID` with one shared row id maps to the same SQL `INSERT`
  of `merge-policy-row` on every node while `manual_peers` keeps them
  disconnected (established `typed-records` offline-row pattern); the TEXT
  primary key is the row identity.
- `UnlockNode` + `WaitNodeReady` after `StartNode` collapse into the harness
  `start_node` (same dir + key reopen under the open watchdog); QUIC/mTLS
  and the origin-key trust the Go README mentions are provisioned by the
  harness (`rcgen` CA + per-node certs, mutual `trusted_origins`).
- The node 2 restart rebinds a fresh ephemeral port, so the 1 <-> 2 edges
  are re-added explicitly after `start_node` (peers still dial the old
  address otherwise).

## Shared-harness addition

None. The `ReplicationOptions::merge_policies` overlay this scenario needs
already exists in `harness/src/cluster.rs` (added by the `typed-records`
port).

## Compile gate (only gate; never run)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/merge-policies/Cargo.toml
```
