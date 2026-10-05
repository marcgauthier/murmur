# Murmur-SQL examples

Runnable programs in increasing order of complexity. Each uses temporary
directories and cleans up after itself. The mattn backend needs the SQLite
feature tags; the pure-Go backend needs `-tags modernc` with `CGO_ENABLED=0`.

```sh
TAGS="sqlite_preupdate_hook sqlite_fts5"
for ex in basic intermediate durability-async local-ddl-fts transactions \
    schema-migrate subscriptions driver-sql prepared-statements observability \
    limits pebble-tuning key-rotation backup-restore advanced offline-partition \
    allowed-peers peer-exclusion retention-snapshot plumtree; do
  go run -tags "$TAGS" ./examples/$ex || break
done
# Note: retention-snapshot takes about a minute (it waits for one 30s GC pass).
```

| Example | Replication | What it shows |
|---|---|---|
| `basic` | no | One embedded node: schema, insert, query, status. |
| `intermediate` | no | Explicit cipher options, a local-only secondary index, an explicit multi-statement transaction, and close/reopen durability (the NodeID must be stable across restarts). |
| `durability-async` | no | Opt-in scheduled disk syncs: fast acknowledged commits, graceful-close sync, durable reopen. |
| `local-ddl-fts` | no | Local-only objects: a secondary index, a standalone FTS5 table with a MATCH query, and a scratch cache table. |
| `transactions` | no | Explicit transactions: atomic multi-statement commit, rollback with no trace, transaction ID. |
| `schema-migrate` | no | Additive evolution with `DB.Migrate`: old rows intact, new column writable. |
| `subscriptions` | no | Reactive queries: subscribe to a SELECT and receive an event per result change, with cursors. |
| `driver-sql` | no | The `database/sql` wrapper: standard `sql.Open`/`Exec`/`Query` against a registered `*DB`. |
| `prepared-statements` | no | Explicit `PrepareContext`: prepare once, execute many times; statement-cache sizing. |
| `observability` | no | Diagnostics: `slog`-backed `Config.Logger` plus `Status` and `Metrics` snapshots. |
| `limits` | no | Fail-closed write limits: oversized values and batches rejected, normal writes pass. |
| `pebble-tuning` | no | Custom `PebbleConfig`: small block cache, disabled compression, reopen recovery. |
| `key-rotation` | no | Live `RotateDataKey` with `EncryptionStatus` before/after; rows stay readable. |
| `backup-restore` | no | Online `backup.CreateBackup` to a local dir, clone `Restore` under a fresh writer identity. |
| `advanced` | 3-node mesh | Shared cluster DBID, generated mTLS CA with per-node certificates, static peers, 30 concurrent rows converging everywhere, and a conflicting concurrent update resolving to one deterministic winner on all nodes. |
| `offline-partition` | 3-node mesh | Partition tolerance: `RemovePeer` splits a node off, both sides commit offline writes, `AddPeer` rejoins and everything converges. |
| `allowed-peers` | 3-node mesh | NodeID admission: allow-listed pair meshes while the third node stays isolated both directions. |
| `peer-exclusion` | 2-node mesh | Persistent retirement: `RemovePeer` survives a restart; `AddPeer` readmits and replication resumes. |
| `retention-snapshot` | 2-node mesh | Tiny log retention forces a stale rejoin through snapshot resync, proven by the receiver counter. |
| `plumtree` | 3-node mesh | Opt-in `DisseminationPlumtree` on all members; concurrent writes converge over gossip. |

The original single-node demo remains at `example/` (`go run ./example` needs
the same `-tags` prefix).

Examples provision temporary random Ed25519 signing identities with
`examples/internal/demoidentity` and explicitly trust their configured peers for
snapshot recovery. Production applications must persist private keys and
provision public bindings administratively; see
[origin signatures](../architecture/origin-signatures.md).
