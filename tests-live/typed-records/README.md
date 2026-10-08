# Typed RIME record replication

This multi-process scenario starts two nodes with the same `Config.Tables`
definition, inserts through Murmur's managed typed API on one process, and
waits until the peer materializes the record from replicated Spool state. It
then verifies PN_COUNTER, OR_SET, and numeric MIN/MAX updates converge. It
restarts the peer and checks that its RIME materializer rebuilds the record,
counter, set, and extrema from durable state. Typed subscriptions also observe
the remote update. The scenario also commits a row with `BeginTx` and
verifies that an explicitly rolled-back row is absent on both nodes. A second
rehearsal migrates one node to an additive typed
schema at runtime, verifies the older peer adopts the manifest, writes a new
field from the newer node, then updates a known field from the older peer. The
new field must survive replication and the older peer's restart.
The first rehearsal also invokes operator-triggered GC through the mTLS admin
API, exercising the same retention-aware collector used by the periodic loop.
An offline-partition rehearsal starts two nodes without peers, independently
updates one shared record's PN_COUNTER and OR_SET fields on both nodes, then
connects them and verifies that both native materializers converge to the
summed counter and unioned set.
An empty typed peer also joins after the source's configured log retention
expires; the test checks the snapshot-received metric and the reconstructed
RIME record count, proving snapshot transfer rather than ordinary log catch-up.
A schema-branch rehearsal first shares a row, partitions the peers, and has each
node add a different compatible field and write it locally. After reconnect,
both nodes must publish the deterministic merged schema and retain both field
values after binding the union record type.

The typed-files rehearsal enables object storage on both nodes, uploads bytes
through a native `Config.Tables` database, verifies Spool metadata replication,
fetches and verifies the content-addressed object on the peer, then checks that
a typed-node delete tombstone replicates. It also round-trips and deletes a
traversal-shaped file name and confirms that the name never becomes a filesystem
path.

The node-local rehearsal writes a persistent `TableScopeNodeLocal` row on one
node, then waits for a replicated barrier row on its peer. The private row is
readable on its writer and remains absent from the peer's local table.

The typed-mode isolation rehearsal sends hostile, oversized, malformed,
stacked, and UNION-shaped SQL through the HTTP query and mutation endpoints.
All requests are rejected without leaking schema or stack details, and the
replicated typed control row remains intact on both nodes. File-route traversal
probes are also rejected or treated as opaque object keys; none escape into the
filesystem.

Run with `bash tests-live/run.sh typed-records`.
