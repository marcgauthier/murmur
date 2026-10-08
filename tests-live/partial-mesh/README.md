# Partial-mesh forwarding and bootstrap discovery (multi-process)

Run with `bash tests-live/run.sh partial-mesh`.

1. **Chain forwarding (`TestChainPeersConvergeViaForwarding`)**: Three internal test nodes peer only as a chain (node1-node2, node2-node3). Writes on node1 converge on node3 through forwarding by node2; the test checks convergence and static peer counts without direct node1-node3 peering.
2. **Bootstrap discovery (`TestDynamicBootstrapDiscovery`)**: Three nodes use node 0 as a bootstrap seed, wait for all members to discover one another without explicit `AddPeer` calls, write distinct typed records on each node, and verify exact convergence.

Both scenarios write and read through the managed typed RIME API; neither starts the legacy SQL materializer.

The first scenario retains explicit chain links to isolate forwarding behavior.
