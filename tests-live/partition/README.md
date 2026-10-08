# Partition and healing smoke test

Run with `CGO_ENABLED=0 bash tests-live/run.sh partition`. The test uses
managed typed records without SQLite or CGO. Four encrypted QUIC nodes
form a mesh, split into two isolated pairs, and write independently. The test
checks each side remains isolated, restores cross-partition peers, verifies
equal ordered state digests, then confirms a post-heal write replicates.

This is a bounded convergence smoke test. It does not exercise SWIM discovery,
multi-hop forwarding, churn, or large-cluster connection/fanout limits.
