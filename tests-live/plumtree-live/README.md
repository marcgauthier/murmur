# Plumtree dissemination and mixed-mode refusal (multi-process)

Run with `CGO_ENABLED=0 bash tests-live/run.sh plumtree-live`.

`TestPlumtreeMeshConverges` meshes three Murmur daemons in Plumtree
dissemination mode (see the harness `Replication.Dissemination` /
`DisseminationByNode` options and the daemon `replication.dissemination`
setting) and requires end-to-end convergence through typed RIME records.

`TestMixedModeRefusesGossipPeer` meshes two Plumtree nodes with one
gossip-mode node: the Plumtree pair must converge while the gossip node
stays isolated in both directions, with handshake capability refusals
recorded. Both checks use the managed typed test API and run without SQLite
tags or CGO.
