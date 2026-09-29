# Two-Low/two-High encrypted file bridge (multi-process)

Run with `go test -count=1 ./tests-live/files-bridge`. Four Murmur-SQL
daemon processes (two Low mesh peers, two High mesh peers) run in discrete
`node1`/`node2` directories per domain; the test drives them only through
their HTTPS APIs with client certificates, replicating how the application works.

Flow:

1. Upload a 384 KiB file to Low-1 (`POST /v1/files/upload`).
2. Low-2 converges on the metadata over mesh replication, then fetches
   object bytes from Low-1's peer endpoint; SHA-256 must match.
3. Low-1 exports a signed bundle plus recipient-sealed file chunks to a
   shared staging directory (`POST /v1/admin/bridge/export`).
4. Staged artifacts must contain no document plaintext (sampled windows).
5. High-1 imports the staging directory (`POST /v1/admin/bridge/import`),
   installing the object re-encrypted under the High object key; its
   at-rest bytes must differ from Low's for identical verified content.
6. High-2 converges on the metadata over High mesh replication, fetches
   bytes from High-1, and verifies SHA-256.
7. File search over HTTP returns the replicated metadata.
8. Delete cascade: the Low tombstone replicates, exports, and imports, and
   the file reads deleted on all four nodes.

The Low and High domains use separate DBIDs, CAs, storage keys, object
keys, and directories, with one source stream and one file transfer. It is
an integration acceptance scenario, not an impaired-network or throughput
test.

Node ports come from the shared harness allocator, which claims each
ephemeral port once per test process: parallel daemon suites churn enough
sockets that bare bind-and-close allocation can hand two nodes the same
replication port and silently break the mesh.
