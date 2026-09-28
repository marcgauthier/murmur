package replicateddb

import (
	"log/slog"
	"time"

	"github.com/nomadsql/replicateddb/replication"
	"github.com/nomadsql/replicateddb/transport"
)

// Attr is a structured log attribute.
type Attr = slog.Attr

// PeerStatus is a point-in-time peer status snapshot.
type PeerStatus = replication.PeerStatus

// PoolStats is a point-in-time connection pool status snapshot.
type PoolStats = transport.PoolStats

// MembershipStats is a point-in-time SWIM membership status snapshot.
type MembershipStats = replication.MembershipStats

// Logger is the package logging interface. The standard library's
// *slog.Logger satisfies it, so applications can pass slog directly.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// DiscardLogger drops all output. It is the default when Config.Logger is nil.
type DiscardLogger struct{}

func (DiscardLogger) Debug(string, ...any) {}
func (DiscardLogger) Info(string, ...any)  {}
func (DiscardLogger) Warn(string, ...any)  {}
func (DiscardLogger) Error(string, ...any) {}

// DBState is the node lifecycle state.
type DBState uint8

const (
	StateOpening DBState = iota
	StateRebuilding
	StateReady
	StateMaterializerDirty
	StateRotatingKey
	StateMaintenance
	StateClosing
	StateClosed
	StateFailed
)

func (s DBState) String() string {
	switch s {
	case StateOpening:
		return "opening"
	case StateRebuilding:
		return "rebuilding"
	case StateReady:
		return "ready"
	case StateMaterializerDirty:
		return "materializer-dirty"
	case StateRotatingKey:
		return "rotating-key"
	case StateMaintenance:
		return "maintenance"
	case StateClosing:
		return "closing"
	case StateClosed:
		return "closed"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// WritesAllowed reports whether the state permits new application writes.
func (s DBState) WritesAllowed() bool { return s == StateReady }

// Status is a point-in-time diagnostic snapshot. It never contains secrets.
//
// Membership fields describe the static mesh until SWIM discovery lands:
// MembershipCount equals known peers (configured plus inbound-discovered)
// and SelectedPeers equals connected sessions (every connected peer is a
// replication target; bounded selection is pending).
type Status struct {
	State  DBState
	NodeID NodeID
	DBID   DBID

	StateGeneration uint64
	// MaterializedGeneration is the in-memory SQL view's progress. It is
	// initialized after rebuilding from Pebble on every open.
	MaterializedGeneration uint64

	HLC          uint64
	LocalSeq     uint64
	SchemaEpoch  uint64
	SchemaHash   [32]byte
	FormatFormat uint64

	PeerCount       int
	ConnectedPeers  int
	MembershipCount int
	SelectedPeers   int
	QUICConnections int
	PendingDials    int

	// PendingApply is the current in-flight remote Pebble-apply count.
	// PendingSend is queued outbound control/need/schema frames.
	PendingApply int
	PendingSend  int

	// Peers holds one diagnostic record per known peer.
	Peers []PeerDiagnostics
	// Replication holds aggregate replication counters and queue depths.
	Replication replication.StatsSnapshot
	// Pool holds connection and session pooling diagnostics.
	Pool PoolStats
	// Membership holds SWIM membership and discovery diagnostics.
	Membership MembershipStats
	// Metrics holds node-local writer, apply, GC, and schema counters.
	Metrics MetricsSnapshot

	PebbleSizeBytes     uint64
	PebbleCacheHits     int64
	PebbleCacheMisses   int64
	PebbleMemTableBytes uint64

	Uptime time.Duration
}

// PeerDiagnostics is the per-peer diagnostic record: session state,
// schema compatibility, watermarks, per-origin lag, traffic totals, queue
// depths, and persisted retirement/exclusion state. It never contains
// secrets.
type PeerDiagnostics struct {
	NodeID             NodeID
	Addrs              []string
	Connected          bool
	Dynamic            bool
	SchemaAgreed       bool
	SnapshotRequired   bool
	AwaitingSnapshot   bool
	Retired            bool
	Excluded           bool
	Selected           bool
	MembershipState    string
	RetirementDeadline time.Time
	RTT                time.Duration
	LastSeen           time.Time
	LastHandshake      time.Time
	LastSend           time.Time
	LastRecv           time.Time
	LastAntiEntropy    time.Time
	RemoteSchemaEpoch  uint64
	RemoteSchemaHash   [32]byte
	BytesSent          uint64
	BytesReceived      uint64
	QueuedNeed         int
	QueuedCtrl         int
	QueuedSchema       int
	Have               map[NodeID]uint64
	Sent               map[NodeID]uint64
	LagByOrigin        map[NodeID]uint64
}
