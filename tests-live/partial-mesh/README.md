# Partial-mesh forwarding and SWIM dynamic discovery (multi-process)

Run with `go test -count=1 ./tests-live/partial-mesh`.

1. **Chain forwarding (`TestChainPeersConvergeViaForwarding`)**: Three `spedsql` daemons peer only as a chain (node1-node2, node2-node3). Writes on node1 converge on node3 through origin forwarding by node2; the test verifies convergence and the exact static peer counts without direct node1-node3 peering.
2. **SWIM Dynamic Discovery (`TestDynamicBootstrapDiscovery`)**: Nodes join the cluster dynamically through SWIM bootstrap discovery over QUIC without requiring a static full-mesh configuration.

