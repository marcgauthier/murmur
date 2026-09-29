# Partial-mesh forwarding and explicit peer addition (multi-process)

Run with `bash tests-live/run.sh partial-mesh`.

1. **Chain forwarding (`TestChainPeersConvergeViaForwarding`)**: Three internal test nodes peer only as a chain (node1-node2, node2-node3). Writes on node1 converge on node3 through forwarding by node2; the test checks convergence and static peer counts without direct node1-node3 peering.
2. **Explicit peer addition (`TestDynamicBootstrapDiscovery`)**: Despite its name, this test sets `ManualPeers: true` and calls `AddPeer` to connect nodes to node0. It checks replication convergence on that topology, not SWIM bootstrap discovery.

Runtime SWIM wiring exists in `db.go` and `replication/membership.go`. Live
acceptance of discovery still requires configured bootstrap seeds, no explicit
`AddPeer` calls, and assertions for discovered membership and replication.
