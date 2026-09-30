// Package bridge implements one-way Low-to-High logical replication.
//
// Each domain keeps its own DBID, schema administration, identities, storage
// keys, and ordinary QUIC mesh. The bridge transports logical records — never
// raw Pebble files or cross-DBID mesh frames — from a Low exporter to a High
// receiver. High may redistribute accepted imports among its own mesh peers;
// it never exports database changes back to Low through this bridge.
//
// One-way flow is enforced by construction: Exporter exposes only publishing
// entry points and Receiver only receiving ones, each constructor requires
// its matching Role, and every handle is bound to its domain's DBID (a Low
// exporter cannot be wired to a High database and vice versa). Roles do not
// designate leaders and do not replace SWIM within either domain.
//
// Provenance and High-owned-field policy arrive as a later slice; this
// file defines roles, domain binding, the logical record carrier, and the
// directional transport interfaces the bundle, queue, hold, and status
// slices build on.
package bridge

import (
	"errors"
	"fmt"
	"time"

	"github.com/marcgauthier/murmur/ids"
)

// Role selects the bridge behavior of a node. The zero value disables the
// bridge.
type Role int

const (
	// RoleDisabled disables all bridge activity. It is the default.
	RoleDisabled Role = iota
	// RoleLowExporter publishes logical changes for High receivers.
	RoleLowExporter
	// RoleHighReceiver accepts Low exports into the High domain.
	RoleHighReceiver
)

// String renders the role for status and diagnostics.
func (r Role) String() string {
	switch r {
	case RoleDisabled:
		return "disabled"
	case RoleLowExporter:
		return "low-exporter"
	case RoleHighReceiver:
		return "high-receiver"
	default:
		return "unknown"
	}
}

// Common bridge errors.
var (
	// ErrDisabled indicates bridge use while RoleDisabled is configured.
	ErrDisabled = errors.New("bridge: bridge is disabled")
	// ErrRoleMismatch indicates an exporter/receiver constructed for the
	// wrong role, or a cross-direction operation.
	ErrRoleMismatch = errors.New("bridge: role mismatch")
	// ErrDomainMismatch indicates a bridge handle wired to a database
	// outside its configured domain.
	ErrDomainMismatch = errors.New("bridge: database is outside the bridge domain")
)

// Limits bounds bridge staging, bundles, and queues. Zero values select
// conservative defaults at validation.
type Limits struct {
	// MaxBundleBytes caps one encoded artifact.
	MaxBundleBytes int
	// MaxPayloadBytes caps one decoded payload.
	MaxPayloadBytes int
	// MaxTransactions caps transactions per bundle.
	MaxTransactions int
	// MaxStagingBytes caps durable outbox/inbox staging.
	MaxStagingBytes int64
	// MaxStagingEntries caps durable outbox/inbox entries.
	MaxStagingEntries int
	// MaxQueue caps pending in-memory work items.
	MaxQueue int
	// MaxFileChunkBytes caps one sealed file-object chunk's plaintext.
	MaxFileChunkBytes int
}

func (l Limits) withDefaults() Limits {
	if l.MaxBundleBytes <= 0 {
		l.MaxBundleBytes = 16 << 20
	}
	if l.MaxPayloadBytes <= 0 {
		l.MaxPayloadBytes = 64 << 20
	}
	if l.MaxTransactions <= 0 {
		l.MaxTransactions = 1024
	}
	if l.MaxStagingBytes <= 0 {
		l.MaxStagingBytes = 1 << 30
	}
	if l.MaxStagingEntries <= 0 {
		l.MaxStagingEntries = 1 << 16
	}
	if l.MaxQueue <= 0 {
		l.MaxQueue = 256
	}
	if l.MaxFileChunkBytes <= 0 {
		l.MaxFileChunkBytes = 1 << 20
	}
	return l
}

// Config is the package-owned bridge configuration.
type Config struct {
	// Role enables exporter or receiver behavior. Zero disables the bridge.
	Role Role
	// Domain is this node's bridge domain identity. It must equal the
	// bound database's DBID: Low and High domains never share storage.
	Domain ids.DBID
	// Stream identifies this exporter's source stream (exporters only).
	// Receivers use it to track contiguous per-stream progress.
	Stream string
	// Limits bounds staging and transfer sizes.
	Limits Limits
}

func (c Config) validated() (Config, error) {
	switch c.Role {
	case RoleDisabled:
		// Disabled ignores the remaining fields.
		return c, nil
	case RoleLowExporter:
		if c.Domain.IsZero() {
			return c, fmt.Errorf("bridge: low-exporter requires a domain DBID")
		}
		if err := checkNameChars(c.Stream, 64); err != nil {
			return c, fmt.Errorf("bridge: low-exporter requires a valid stream identity: %w", err)
		}
	case RoleHighReceiver:
		if c.Domain.IsZero() {
			return c, fmt.Errorf("bridge: high-receiver requires a domain DBID")
		}
	default:
		return c, fmt.Errorf("bridge: unknown role %d", int(c.Role))
	}
	c.Limits = c.Limits.withDefaults()
	if c.Limits.MaxBundleBytes <= 0 || c.Limits.MaxPayloadBytes <= 0 ||
		c.Limits.MaxTransactions <= 0 || c.Limits.MaxStagingBytes <= 0 ||
		c.Limits.MaxStagingEntries <= 0 || c.Limits.MaxQueue <= 0 {
		return c, fmt.Errorf("bridge: limits must be positive")
	}
	return c, nil
}

// Status reports role, domain binding, and transfer progress. It never
// contains key material or plaintext payloads.
type Status struct {
	Role   string `json:"role"`
	Domain string `json:"domain"`
	Stream string `json:"stream,omitempty"`
	// Exported is the exporter's highest published Low sequence (exporters).
	Exported uint64 `json:"exported_seq,omitempty"`
	// Applied is the receiver's highest contiguous applied Low sequence
	// (maximum across streams when several are tracked).
	Applied uint64 `json:"applied_seq,omitempty"`
	// Observed is the receiver's highest observed Low sequence
	// (maximum across streams; gaps allowed).
	Observed uint64 `json:"observed_seq,omitempty"`
	// GeneratedAt timestamps the snapshot.
	GeneratedAt time.Time `json:"generated_at"`
	// Export details exporter backlog and publication failures.
	Export *ExportStatus `json:"export,omitempty"`
	// Import details receiver stream progress, gaps, and holds.
	Import *ImportStatus `json:"import,omitempty"`
	// Replay details quarantined bundles and their retryability.
	Replay *ReplayStatus `json:"replay,omitempty"`
	// Keys lists trusted public key identities.
	Keys *KeyStatus `json:"keys,omitempty"`
}

// Bridge is the shared role/domain core. Exporter and Receiver build on it;
// neither can cross into the other's direction.
type Bridge struct {
	db     Database
	cfg    Config
	limits Limits
}

// Role returns the configured role.
func (b *Bridge) Role() Role { return b.cfg.Role }

// Domain returns the bound domain DBID.
func (b *Bridge) Domain() ids.DBID { return b.cfg.Domain }

// Limits returns the effective transfer limits.
func (b *Bridge) Limits() Limits { return b.limits }

// Status reports the current role/domain/progress snapshot.
func (b *Bridge) Status() Status {
	return Status{Role: b.cfg.Role.String(), Domain: b.cfg.Domain.String(),
		Stream: b.cfg.Stream, GeneratedAt: time.Now().UTC()}
}
