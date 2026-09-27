package replicateddb

import (
	"log/slog"
	"time"
)

// Attr is a structured log attribute.
type Attr = slog.Attr

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
type Status struct {
	State  DBState
	NodeID NodeID
	DBID   DBID

	StateGeneration        uint64
	MaterializedGeneration uint64

	HLC          uint64
	LocalSeq     uint64
	SchemaEpoch  uint64
	SchemaHash   [32]byte
	FormatFormat uint64

	PeerCount      int
	ConnectedPeers int

	PendingApply int
	PendingSend  int

	PebbleSizeBytes     uint64
	PebbleCacheHits     int64
	PebbleCacheMisses   int64
	PebbleMemTableBytes uint64

	Uptime time.Duration
}
