# plaintext-audit (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/plaintext-audit/`
(`README.md` + `plaintext_audit_test.go`:
`TestPlaintextAuditMarkersAndKeysAbsentOnDisk`).

## What it proves

Eight random per-run markers are written through the typed table on node 1,
surrounded by 200 filler rows on both nodes, replicated, and crash-cycled.
Every marker round-trips byte-identically through the typed API (positive
control), then the entire directory of both nodes (spool, segments, keys,
tmp, logs, `tls/`) and a backup artifact are scanned: marker plaintext and
key material must be absent everywhere. A restart + reconverge leg proves
the audit disturbs nothing.

## Env knobs

Standard harness knobs only: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS` (120),
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS` (1800).

## Pass criteria

- All 8 markers round-trip through the typed table on node 1.
- No marker bytes in any regular file under either node's whole directory.
- No key material (raw 32 bytes, lowercase or uppercase hex) in any regular
  file under either node's whole directory.
- No marker plaintext or key hex in the backup artifact.
- Both nodes restart and reconverge on all 208 rows.

## Deviations from the Go original

- Plain SQL (`CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)`,
  `Database::transaction` / `Database::read`) replaces the Go managed
  typed-records HTTP API; the Rust product has no HTTP server.
- In-process `stop_node` + `start_node` + `mesh` replaces the mid-life
  SIGKILL; signals cannot target one in-process node, and the audited
  property is the quiescent on-disk image either way.
- The backup is the product's online `Database::backup` on the running
  node 1 instead of Go's offline open + backup; the audited property (no
  marker plaintext or key in the artifact) is identical.
- No config file is skipped during the key-material scan: the Rust
  harness provisions no key-bearing config file (the key lives only in
  test memory), so the whole node tree is audited.
- Per-run randomness uses the `rand 0.8` registry crate (same precedent
  as the `compression` scenario); hex encoding is a small local helper.
