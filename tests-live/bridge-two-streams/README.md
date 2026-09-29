# Two Low domains into one High cluster (multi-process)

Run with `go test -count=1 ./tests-live/bridge-two-streams`.

`TestTwoLowDomainsIntoOneHigh` runs two independent Low domains
(distinct DBIDs, CAs, keys, streams, staging directories) and one
two-node High cluster where each node imports a different stream (see
the harness `BridgeByNode` option). Rows from both domains must converge
on both High nodes with Low-owned provenance, and a stream-a-only second
round must arrive with stream-b progress byte-identical.

`TestCrossDomainIdentityCollisionFailsClosed` lands a row from domain A
first, then imports the same row ID from domain B: the import must fail
with an identity-collision error while first-writer state stands
untouched on both High nodes.
