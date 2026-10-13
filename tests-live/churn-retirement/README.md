# churn-retirement (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/churn-retirement/`
(`churn_retirement_test.go`).

## What it proves

Three nodes mesh; node3 is excluded on the survivors. The port keeps every
Go pass criterion:

1. Baseline: five rows written on node1 converge identically on all nodes.
2. Sessions to node3 drop on both survivors; each survivor sees exactly one
   peer (the other survivor).
3. Node3's writes stop arriving (survivors keep only the five baseline rows
   across a 5 s hold).
4. Exclusion survives a survivor restart with no rediscovery re-mesh
   (restarted node1 re-meshes only its fellow survivor).
5. Explicit re-adding heals replication (all nodes converge on six rows).
6. The retired peer releases its GC retention pin (non-retired gating count
   drops below baseline; node3's retention record is marked retired).

## Leg mapping (Go -> Rust)

| Go | Rust |
|---|---|
| `Cluster.TypedInsert` / `TypedNames` (HTTP API) | Plain SQL `records(name TEXT PRIMARY KEY)` via `Database::transaction` / `Database::read` + `flush()` |
| `RemovePeer` (exclusion legs) | `Cluster::remove_peer` (durable `07/replication/excluded/*` record, reversible via `add_peer`) |
| Retention-pin release inside `RemovePeer` | `Database::retire_peer` (durable `retired=true` retention record) |
| `spedsql_peer_connected{peer=...}` | `Handle::status().peers_diagnostics` (`node_id` + `connected`) |
| `spedsql_peer_count` | `Handle::status().peers` |
| `spedsql_gating_members` | `Database::retention_status().peers` entries with `retired == false` |
| `StopNode` / `StartNode` / `UnlockNode` / `WaitNodeReady` | `Cluster::stop_node` (`close()`) / `Cluster::start_node` (reopen same dir+key under the unlock watchdog); explicit `add_peer` re-mesh to the fellow survivor only (fresh ephemeral port) |
| `AddPeer` re-add | `Cluster::add_peer` (deletes the exclusion record + dials) |

## Deviations

- **Retention-release leg runs after the re-add heal**, not immediately after
  exclusion. Rust splits Go's single `RemovePeer` into reversible exclusion
  (`remove_peer`) and permanent retirement (`retire_peer`): session setup
  rejects retired peers (`engine.rs`) and no unretire API exists, so retiring
  before the re-add leg would make the heal leg impossible. Every assertion
  is preserved; only the order changes.
- SWIM suspect/dead/refutation is not covered, same as the Go original
  (memberlist suspicion is not wired into the runtime).
- Timeouts: 60 s convergence / peer waits (Go parity for saturated parallel
  boxes), 30 s retention-pin appearance, 5 s isolation hold.

## Env knobs

None beyond the standard harness knobs (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/churn-retirement/Cargo.toml
```
