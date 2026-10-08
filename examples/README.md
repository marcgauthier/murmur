# Murmur examples

Runnable programs in increasing order of complexity. Each uses temporary
directories and cleans up after itself. `basic`, `intermediate`,
`transactions`, `subscriptions`, `prepared-statements`, `durability-async`,
`schema-migrate`, `key-rotation`, `backup-restore`, `observability`,
`spool-tuning`, `advanced`, `offline-partition`, `allowed-peers`,
`peer-exclusion`, `plumtree`, `retention-snapshot`, `prefix-search`, and
`limits` use native Go records and RIME and run without CGO.

```sh
go run ./examples/basic
```

```sh
for ex in basic intermediate durability-async transactions \
    schema-migrate subscriptions prepared-statements observability \
    limits spool-tuning key-rotation backup-restore advanced offline-partition \
    allowed-peers peer-exclusion retention-snapshot plumtree prefix-search; do
  go run ./examples/$ex || break
done
# Note: retention-snapshot takes about a minute (it waits for one 30s GC pass).
```

| Example | Replication | What it shows |
|---|---|---|
| `basic` | no | One embedded node: typed schema, managed transaction, ordered query, status. |
| `intermediate` | no | AES-GCM configuration, a local RIME secondary index, an atomic typed batch, and close/reopen durability with a stable NodeID. |
| `durability-async` | no | Opt-in scheduled Spool syncs with typed writes, graceful-close sync, and durable reopen. |
| `prefix-search` | no | Indexed RIME string-prefix matching, which does not include full-text tokenization or ranking. |
| `transactions` | no | Managed typed transactions: atomic record batches and rollback when the application callback fails. |
| `schema-migrate` | no | Additive typed evolution with `DB.MigrateRecords`: old records intact, optional new field writable. |
| `subscriptions` | no | Reactive typed queries: receive an initial record snapshot and updates with observer cursors. |
| `prepared-statements` | no | Compile a typed query once and execute it repeatedly with positional parameters. |
| `observability` | no | Diagnostics: `slog`-backed `Config.Logger` plus `Status` and `Metrics` snapshots. |
| `limits` | no | Fail-closed typed value and atomic-batch limits; rejected writes leave no rows, normal writes pass. |
| `spool-tuning` | no | Custom `SpoolConfig`: small blocks, disabled compression, reopen recovery. |
| `key-rotation` | no | Live `RotateDataKey` with `EncryptionStatus` before/after; rows stay readable. |
| `backup-restore` | no | Online `backup.CreateBackup` to a local dir, clone `Restore` under a fresh writer identity. |
| `advanced` | 3-node mesh | Shared cluster DBID, generated mTLS CA with per-node certificates, typed writes converging everywhere, and a conflicting typed update resolving to one deterministic winner on all nodes. |
| `offline-partition` | 3-node mesh | Partition tolerance: `RemovePeer` splits a node off, both sides commit offline writes, `AddPeer` rejoins and everything converges. |
| `allowed-peers` | 3-node mesh | NodeID admission: allow-listed pair meshes while the third node stays isolated both directions. |
| `peer-exclusion` | 2-node mesh | Persistent retirement: `RemovePeer` survives a restart; `AddPeer` readmits and replication resumes. |
| `retention-snapshot` | 2-node mesh | Tiny log retention forces a stale rejoin through snapshot resync, proven by the receiver counter. |
| `plumtree` | 3-node mesh | Opt-in `DisseminationPlumtree` on all members; concurrent writes converge over gossip. |

Examples provision temporary random Ed25519 signing identities with
`examples/internal/demoidentity` and explicitly trust their configured peers for
snapshot recovery. Production applications must persist private keys and
provision public bindings administratively; see
[origin signatures](../architecture/origin-signatures.md).
