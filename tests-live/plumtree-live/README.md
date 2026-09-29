# Plumtree dissemination and mixed-mode refusal (multi-process)

Run with `go test -count=1 ./tests-live/plumtree-live`.

`TestPlumtreeMeshConverges` meshes three Murmur-SQL daemons in Plumtree
dissemination mode (see the harness `Replication.Dissemination` /
`DisseminationByNode` options and the daemon `replication.dissemination`
setting) and requires end-to-end write convergence with equal digests.

`TestMixedModeRefusesGossipPeer` meshes two Plumtree nodes with one
gossip-mode node: the Plumtree pair must converge while the gossip node
stays isolated in both directions, with handshake capability refusals
recorded.
