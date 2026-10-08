# Membership and transport

SWIM/memberlist over QUIC, discovery, transport configuration, and authentication.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [26. Replication Transport](#26-replication-transport)
- [27. Peer Identity and Authentication](#27-peer-identity-and-authentication)
- [IP and CIDR admission policy](#ip-and-cidr-admission-policy)

---

## 26. Replication Transport

Use both:

```text
github.com/hashicorp/memberlist
github.com/quic-go/quic-go
```

All inter-node communication uses QUIC with TLS 1.3 and mutual authentication: SWIM probes, membership gossip, membership push/pull, mutation batches, acknowledgements, and snapshots. Do not open memberlist's default UDP/TCP listeners.

Use raw QUIC streams, not HTTP/3, unless an HTTP-compatible protocol becomes a requirement.

### 26.1 Corrosion reference and membership service

Follow Corrosion's separation of SWIM membership, change dissemination, and periodic synchronization with a subset of nodes. Corrosion uses Foca for SWIM and Quinn for QUIC; this Go implementation uses memberlist and quic-go. Preserve this package's existing HLC/LWW conflict resolution, Spool authority, and durable acknowledgements rather than adopting CR-SQLite.

Create an internal membership service starting from `memberlist.DefaultWANConfig()`, overriding `GossipNodes` and `IndirectChecks` to three. Use the durable NodeID string as the memberlist name. Advertise compact, versioned metadata containing DBID, protocol capabilities, and the reachable QUIC endpoint. SWIM provides discovery, direct/indirect failure detection, suspicion, refutation, and membership repair; it is not consensus and does not track durable database progress.

Membership callbacks only enqueue work to the scheduler; never perform SQL, state store scans, dialing, or bulk replication inside them. Bound callback queues and reconcile against the current membership view if events are coalesced or dropped. Suspect nodes may refute; replace unavailable selected peers without permanently retiring them. Restart preserves NodeID and uses memberlist's incarnation/refutation behavior. Address changes update candidates and recycle stale connections.

Bootstrap from an overlapping partial seed list, resolving DNS to reachable endpoints. Retry failures with capped exponential backoff and jitter; unsuccessful bootstrap does not prevent local reads/writes after normal startup. Retain the distinction between a standalone node and an isolated member. A node joining an existing cluster must use its shared configured or persisted DBID; never silently adopt a seed's DBID. A fresh standalone node may still create its own DBID.

Current runtime wiring: `db.go` starts `replication.NewMembershipService` when
`Replication.Membership.Bootstrap` is nonempty, falling back to
`Replication.Bootstrap`. It attaches the service to the replication manager;
`replication/membership.go` calls `memberlist.Create`. With no seeds this startup
path does not create the service. Transport/service initialization errors are
currently not propagated from this block, so a successful database open alone
does not prove discovery started. The seed-only live discovery acceptance gap
is recorded in [release status](release-status.md#1-verified-feature-matrix).
Serving file-object nodes advertise their bound fetch endpoint as an optional
suffix in the same SWIM metadata (version 1 decoders ignore trailing bytes,
so no version bump was needed); fetch sources are the union of static
`Files.FetchPeers` and live members advertising an endpoint.

### 26.2 Memberlist transport over QUIC

Implement `memberlist.NodeAwareTransport`, including its underlying `Transport` interface, on the shared QUIC endpoint:

- `WriteTo`/`WriteToAddress` send membership packets as QUIC DATAGRAMs; `PacketCh` receives bounded packets with source address and receive timestamp. Return a send timestamp close to actual transmission, after any connection establishment.
- `DialTimeout`/`DialAddressTimeout` open a dedicated bidirectional membership stream; `StreamCh` delivers incoming streams. Wrap streams as `net.Conn`, implementing read/write deadlines and local/remote addresses. Closing the wrapper closes only that stream.
- `FinalAdvertiseAddr` returns the reachable advertised IP/port; wildcard listen addresses require a concrete advertised address. Bootstrap names may resolve through DNS, while memberlist advertises the resolved IP/port.
- Enable QUIC DATAGRAM negotiation and reject peers without support. Use a versioned membership envelope and stream header to distinguish membership, replication control, and bulk traffic. Authenticate and validate DBID before dispatching either datagrams or streams.
- Set memberlist's packet budget to a conservative 1,000 bytes including its label, leaving room within a 1,200-byte QUIC packet for encryption and the envelope. Enforce the negotiated datagram limit too; return an explicit oversized-packet error rather than fragmenting or silently switching to reliable delivery.
- Keep membership receive queues and per-peer pending sends bounded. Apply deadlines to connection setup and reliable exchanges; avoid blocking memberlist behind database transfers. Memberlist's reliable fallback probes use QUIC streams despite its legacy TCP configuration names.
- Membership sessions use the pool's reserved connection allowance and do not consume replication session slots. This keeps SWIM admission available while selected-target, repair, and inbound bulk replication sessions are saturated; the sessions-plus-8-reserved default arrangement is covered by the QUIC responsiveness test in [testing acceptance](testing.md#57-network-partition-tests).
- `Shutdown` stops membership delivery and closes its streams without closing the shared endpoint still used by replication. The replication manager owns the endpoint's final shutdown.

Pin a memberlist release compatible with the repository's Go version during implementation and verify its transport API in the feasibility spike. Do not fork memberlist or implement a second SWIM state machine.

### 26.3 Public configuration additions

Add these fields to the existing replication configuration; retain its batching, TLS, allow-list, acknowledgement, and retention settings:

```go
type MembershipConfig struct {
    Enabled        bool     // start SWIM even with empty Bootstrap (seed nodes)
    Bootstrap      []string // QUIC seed addresses; NodeID initially optional
    AdvertiseAddr  string   // reachable host:port; derive from concrete ListenAddr
    ProbeInterval  time.Duration
    ProbeTimeout   time.Duration
    GossipInterval time.Duration
    GossipNodes    int
    IndirectChecks int
}

type ReplicationConfig struct {
    // Existing replication fields remain; these fields are additions.
    // MaxTransactionBytes lives on top-level Config, not here.
    // Overload budgets are fixed package constants, not configuration.
    Membership             MembershipConfig
    Fanout                 int
    PeerRotationInterval   time.Duration
    AntiEntropyInterval    time.Duration
    MaxConcurrentRepairs   int
    MaxReplicationSessions int
    MaxQUICConnections     int
    Dissemination          DisseminationMode // zero selects bounded gossip
}

type DisseminationMode string

const (
    DisseminationGossip   DisseminationMode = "gossip"
    DisseminationPlumtree DisseminationMode = "plumtree"
)
```

Zero values select defaults: WAN membership timings, three gossip targets, three indirect checks, replication fanout four, peer rotation every 30 seconds, anti-entropy every 10 seconds, two concurrent repairs, 32 replication sessions, and 64 total QUIC connections. Existing `Peers []Peer` entries provide additional seeds with expected identities. Replication-enabled nodes require a listening QUIC endpoint; with no listener and no seeds, retain single-node mode.

Validate positive resolved timings and budgets, `ProbeTimeout < ProbeInterval`, `Fanout + MaxConcurrentRepairs <= MaxReplicationSessions` to leave reserved repair slots, and `MaxQUICConnections >= MaxReplicationSessions + 8` to reserve membership capacity. Negative values are errors. These are package-owned settings; do not expose memberlist implementation types in the public API.

Default top-level `Config.MaxTransactionBytes` to 64 MiB; validate it against the existing maximum replicated value and framing/chunk overhead. Advertise receive limits and required chunk/dissemination capabilities during handshake. Unknown dissemination modes are errors. Overload budgets are fixed package constants (see [Section 32](synchronization-and-overload.md#32-mutation-batching)), not configuration. A transaction within the configured local limit may still require rejection by a peer with a smaller advertised limit; expose this incompatibility rather than silently acknowledging or retrying it forever.

---

## 27. Peer Identity and Authentication

Transport identity does not authorize a forwarded transaction's claimed origin. Protocol v4 additionally verifies administrator-provisioned Ed25519 origin keys. See [origin signatures](origin-signatures.md) for the exact format and trust boundaries.

Default to mutual certificate authentication.

Each node has:

```text
NodeID
certificate
private key
trusted CA / trust policy
```

The authenticated certificate must map to the expected NodeID.

Do not accept a claimed NodeID from the application protocol without binding it to the authenticated QUIC/TLS identity.

Possible certificate identity:

```text
URI SAN: replicateddb://node/<uuid>
```

or another deterministic SAN encoding.

Add an allow-list option:

```go
AllowedPeers []NodeID
```

and optionally an address allow-list.

`ReplicationConfig.AllowedPeers` sets allowed peer NodeIDs. Entries are validated before
database open; malformed IDs fail startup. The live suite exercises
admission in `tests-live/allow-nodes/`.

Apply the same CA trust and `AllowedPeers` policy to membership and replication. For address-only bootstrap, verify the certificate chain, extract its NodeID SAN, enforce the allow-list, and then validate DBID in the authenticated protocol. Subsequent connections with a known identity must match that expected NodeID. Advertised metadata is not authorization.

Relayed SWIM records legitimately describe nodes other than the authenticated sender. Treat those records as discovery hints rather than requiring every relayed NodeID to equal the sender's certificate. When contacting a discovered endpoint directly, verify that endpoint's certificate matches its claimed identity before admitting its application traffic or durable GC obligation.

---

---

## IP and CIDR admission policy

Configure optional allowed IP addresses/networks with
`ReplicationConfig.AllowedNetworks`, parsed into package-owned
`transport.AddressPolicy` values and validated in `Config.validate` before any
listener opens. An absent address filter preserves the certificate/NodeID-only
policy. A configured filter must match in addition to CA, NodeID, and DBID
authorization; network location never substitutes for identity.

The multi-process acceptance topology is documented in
[`architecture/testing.md`](testing.md) and exercised by
`tests-live/addrpolicy/`.

Filtering applies to inbound QUIC remote addresses (checked in `Accept` before
certificate parsing and any application work) and outbound resolved addresses
(resolved and filtered in `Dial` before dialing; hostnames re-resolve on every
dial so DNS changes and reconnects are rechecked, with mixed allowed/denied
answers filtered to the allowed subset). Sessions recheck the current remote
address on every stream open, closing the session instead of serving traffic
that migrated (or rebound) onto a denied address. IPv4-mapped IPv6 addresses
are normalized before matching. Advertised membership endpoints are discovery
hints, not evidence that an actual socket address is permitted.

The single shared QUIC endpoint applies the same policy to membership,
mutation traffic, snapshots, and file fetches. Rejections return the
`ErrAddressNotAllowed` sentinel through existing accept/dial diagnostics with
bounded messages; IPv6 addresses are redacted to the /64 prefix. Acceptance
covers IPv4/IPv6 CIDRs, invalid configuration, DNS with mixed allowed/denied
answers, denied inbound and outbound peers, address changes, and allowed
addresses with unauthorized NodeIDs (see `transport/addrs_test.go`).
