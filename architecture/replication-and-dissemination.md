# Replication and dissemination

Bounded peer connections, handshakes, multi-origin forwarding, and optional Plumtree.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [28. QUIC Connection Model](#28-quic-connection-model)
- [29. Replication Handshake](#29-replication-handshake)
- [30. Multi-Origin Replication](#30-multi-origin-replication)

---

## 28. QUIC Connection Model

### 28.1 Shared endpoint and bounded connection pool

Reuse at most one established QUIC connection per contacted peer for membership and replication. Keep the membership view separate from the connection pool: knowing a member must not create a dial loop, permanent connection, or replication worker for it.

The shared pool has a default hard cap of 32 connections, including inbound/outbound handshakes, and reserves eight slots for membership work. At most eight replication sessions may be active, counting inbound sessions and temporary anti-entropy or `ForceSync` sessions. Reuse existing connections first, evict idle unpinned connections, and defer/retry work when capacity is unavailable. Do not enable permanent keepalives for every known member.

Reserve up to `Fanout` replication-session slots for selected targets and `MaxConcurrentRepairs` slots (default one) for scheduled repair; inbound replication cannot consume those reservations unless it serves that selected target or repair. Reject/defer excess inbound replication streams without stopping membership on an authenticated shared connection. This prevents inbound load from permanently starving outbound dissemination or anti-entropy.

Bound pending handshakes, concurrent dials, stream counts, and queues. Coalesce concurrent dials to the same identity and resolve duplicate connections deterministically. Protect connections during active probes or streams; recycle them after work completes when under pressure. Prioritize membership and control work over bulk transfers while retaining bounded fairness for replication. Budget exhaustion must not bypass limits or create unbounded waiters.

### 28.2 Peer selection and repair

In default bounded-gossip mode, select up to `min(Fanout, eligible remote members)` distinct random members for ongoing outbound mutation dissemination. With defaults, a commit wakes at most three selected peer senders; incoming sessions do not automatically become additional push targets. Keep selected sessions reusable, replace failed selections promptly, and every 30 seconds rotate one selection toward a member outside the current subset when one exists. Optional Plumtree mode uses the eager/lazy selection rules in [Section 30](replication-and-dissemination.md#30-multi-origin-replication) instead; both modes retain the same connection/session caps and anti-entropy scheduler.

Every 10 seconds, with jitter, initiate one anti-entropy synchronization with a randomly selected eligible member outside the current subset when possible. Exchange per-origin watermarks, request missing retained batches, and request a snapshot when history is unavailable. Incoming sessions serve requested synchronization within the same budgets. Queue `ForceSync` through this scheduler without adding a permanent link. Retry rejected or deferred repairs with jitter.

No fixed subset is guaranteed to stay connected. Rotation, bootstrap retry, membership repair, and repeated anti-entropy provide opportunities for partitions to reconnect. Eventual convergence assumes communication eventually succeeds and membership becomes connected again.

The caps limit simultaneous connections and per-round target selection, not the number of distinct nodes contacted over the cluster's lifetime. SWIM maintains O(N) membership metadata and may contact every member over time; do not promise constant total memory or constant total bandwidth as membership grows.

### 28.3 Stream model

Use long-lived control streams plus transient bulk streams.

Suggested logical protocol:

```text
membership datagrams
    SWIM probes, acknowledgements, membership gossip

membership bidi streams
    join/push-pull and reliable fallback probes

control bidi stream
    handshake
    capabilities
    watermarks
    ping/status
    errors

mutation uni streams
    replication batches/chunks (one ordered sending stream per origin per peer session)

ack uni stream
    watermark updates

snapshot streams
    snapshot manifest
    current-state chunks
```

QUIC streams are reliable and independently multiplexed.

An ordered origin stream preserves that sender's byte order; it cannot order deliveries of the same origin from different peers. QUIC retransmits lost stream data. Application-level gap handling, duplicate detection, chunk staging, and contiguous durable watermarks remain necessary across sessions and stream resets; see [Section 31](synchronization-and-overload.md#31-sequence-and-gap-handling).

Avoid creating one stream per cell.

Batch mutation records.

---

## 29. Replication Handshake

Handshake request:

```go
type Hello struct {
    ProtocolVersion    uint16
    NodeID             NodeID
    DBID               [16]byte
    SchemaEpoch        uint64
    SchemaHash         [32]byte
    SchemaAuthorNode   NodeID
    SchemaTimeCreated  uint64 // HLC
    Capabilities       uint64
    Dissemination      DisseminationMode
    MaxTransactionBytes uint64

    Have []OriginWatermark
}
```

Where:

```go
type OriginWatermark struct {
    Origin   NodeID
    Sequence uint64
}
```

Handshake validation and State-Based Schema Exchange:

1. **Verify Identity & Cluster:**
   - TLS identity matches NodeID.
   - Same database/cluster ID (`DBID`).
   - Compatible protocol version.
   - Same dissemination mode and supported transaction-chunk/range synchronization capabilities; compatible advertised receive limits.
   - Peer allowed in ACL.

2. **Schema Verification & Pre-Replication Sync:**
   - **Case A: Schemas Match:** If `SchemaEpoch` and `SchemaHash` match, the handshake completes and data mutation streaming starts immediately.
   - **Case B: Different Epochs or Hashes:** Exchange complete manifests and required ancestry. A higher epoch alone does not prove that the remote schema contains local additions. Validate declarations, adopt a compatible descendant, or compute the deterministic union in [Section 50](schema.md#50-schema-migration-strategy). Persist/rebuild the accepted schema and exchange `MsgSchemaAck` for the same canonical version/hash before mutations flow.
   - **Case C: Equal Epoch, Different Hash:** Use the same ancestry/compatibility procedure as Case B; never bypass the comparison or silently select a definition by clock/NodeID.
   - **Case D: Refusal or Conflict:** With `AcceptRemoteSchema == false`, refuse a different manifest. In either policy, incompatible definitions return `ErrSchemaMismatch`, preserve local state, and leave affected mutation watermarks unchanged until explicitly resolved.

Peers advertising optional `CapProgressPages` exchange sorted, cursor-paginated progress records. The handshake carries at most 128 origin watermarks; each progress record reports applied and observed heads plus contiguous retained-log bounds. Observed is the highest durable staged sequence seen for an origin, or applied when staging has no newer transaction. Peers advertising `CapTransactionChunks` also exchange cursor-paginated chunk bitmaps for durable incomplete transactions. Advertisements are hints, never applied acknowledgements or GC watermarks. If a peer cannot serve requested chunks it returns `ErrRangeUnavailable`; the receiver tries another advertised or retained source and requests a current snapshot when none remains. Peers without these optional capabilities keep legacy ACK behavior.

`CapTransactionChunks` enables oversized transaction transfer with canonical 64 KiB `TXCH` frames. Receivers validate and durably stage each fragment before acknowledging anything, request missing indexes from the supplier and alternate peers, then verify the complete digest and apply the original transaction through the normal atomic commit path. Staging inherits Spool's AES-256-GCM storage encryption and has a 256 MiB global byte ceiling. A count-complete set that fails verification is dropped as a whole and every chunk is re-requested (throttled, alternates included): per-chunk bytes are not covered by the origin signature, so a first-arriving forged fragment must never wedge the transfer against the later valid one. Peers without this optional capability receive ordinary batches only; a transaction larger than the frame limit cannot be sent to them.

Do not exchange the at-rest storage encryption key.

---

## 30. Multi-Origin Replication

Protocol v4 requires Ed25519 origin signatures independently of the relay's mTLS identity. Forwarding and repair preserve complete signed transactions. See [origin signatures](origin-signatures.md) for the exact format and trust boundaries.

Every node keeps mutation logs by original creator:

```text
origin A: 1,2,3,4,...
origin B: 1,2,3,...
origin C: 1,2,...
```

If Node C learned A's mutation from B, it still stores it under:

```text
origin A
```

not:

```text
origin B
```

This permits forwarding and anti-entropy without creating new mutation identities.

Disseminate local and durably received batches through the selected peers in [Section 28](replication-and-dissemination.md#28-quic-connection-model), not every known member. Never put SQL mutation payloads in memberlist user broadcasts: membership gossip remains independent of database data volume. Watermark exchange and retained-log scans repair missed wakeups, failed forwarding, and rotations. Snapshots repair gaps after log collection. A mutation learned through an inbound session wakes the selected senders only after durable apply.

### Split-Horizon Forwarding (Preventing Echo Loops)

To prevent forwarding mutations back to the origin node or peers that already hold them:
1. **Never Echo to Creator:** A node never sends origin $A$'s mutations back to Node $A$.
2. **Watermark Filter:** Before sending sequence $S$ of origin $A$ to Peer $P$, the sender checks:
   ```text
   if S <= peerAck(P, A):
       skip sending (peer already acknowledged this sequence)
   ```
3. This suppresses acknowledged duplicates; concurrent forwarding before acknowledgement can still duplicate payloads. Duplicate detection and optional Plumtree pruning handle that case.

A peer can request:

```text
A > 4810
B > 812
C > 92
```

The sender performs state store range scans by origin.

### Optional Plumtree dissemination

Keep bounded gossip as the default. `ReplicationConfig.Dissemination = DisseminationPlumtree` enables eager/lazy epidemic broadcast, distinct from memberlist's membership gossip. All members of a cluster must use the same configured mode. The handshake advertises a required `CapPlumtree` bit and refuses a mismatch rather than silently changing modes. Plumtree requires `Fanout >= 2`.

- Identify each broadcast with DBID, schema identity, origin NodeID/sequence, TxID, and canonical transaction payload digest. Chunk announcements also identify their chunk range. Reject conflicting payloads for the same mutation identity rather than overwriting an accepted transaction.
- Eager peers receive payloads; lazy peers receive small `IHAVE` announcements. If a payload is missing after the announcement repair delay, issue `GRAFT` to request it and promote that route. Duplicate valid payloads may trigger `PRUNE`, moving an unlocked eager peer to the lazy set.
- Bound eager push targets by `Fanout` and lazy announcement targets by `Fanout`, selecting distinct eligible peers when possible. Require `Fanout >= 2` for Plumtree configuration. Keep the predecessor/successor in the stable NodeID membership ring eager and protected from pruning when available; deduplicate them in tiny clusters. Other eager slots may rotate or be pruned/grafted. Reconcile these sets on membership changes, without assuming inconsistent membership views already form a connected tree.

The `plumtree` package implements the bounded eager/lazy state machine and `replication.Manager` connects it to framed QUIC delivery. Committed batches use eager `MsgPlumtreeData` frames or lazy `MsgPlumtreeIHave` announcements. Receivers wait 75 ms before `GRAFT`, cancel a queued graft if eager data arrives, and answer cache hits with the retained batch. Cache misses request the origin/sequence through ordinary `Need` repair; anti-entropy and snapshots remain active as fallback. Duplicate eager deliveries send `PRUNE`. The current payload cache is bounded to 256 entries, 32 MiB, and one minute. The active stable-NodeID ring predecessor/successor are prioritized as protected eager neighbors; selected replication peers follow them. Both paths use established sessions and keep the same transport limits. A three-node test verifies that a batch reaches a non-selected neighbor through Plumtree forwarding.
- Promotions obey the existing replication-session/connection limits. Replace an unlocked eager route or defer the promotion when the eager budget is full; do not grow connections for every lazy member. Requests and lazy announcements use transient/reused authenticated QUIC control streams and the same work admission/bandwidth limits.
- Bound the seen-identity and payload caches by entries, bytes, and age. A seen entry alone cannot suppress retrieval/application of an incomplete transaction. Cached payload eviction permits lookup from retained origin logs; if unavailable, request missing ranges from another peer or use snapshot recovery. `IHAVE`, `GRAFT`, `PRUNE`, and staging receipts never count as durable transaction acknowledgements.
- Prefer retaining locally originated pending notifications over redundant forwarding or optimization messages during shedding. Accepted local mutations remain durable in Spool regardless of dissemination-cache eviction. Rotation and periodic anti-entropy continue to repair missed announcements and broken eager routes.

---

Current schema-level counter, set and extrema behavior, causal storage, signed wire formats, bridge ownership and upgrade requirements are specified in [merge policies](merge-policies.md). LWW remains the default.
