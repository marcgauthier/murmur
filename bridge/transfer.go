package bridge

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
)

// RecordOp distinguishes row upserts from row deletes.
type RecordOp int

const (
	// RecordPut carries column values for a row.
	RecordPut RecordOp = iota + 1
	// RecordDelete removes a row.
	RecordDelete
)

// ColumnValue is one logical column effect.
type ColumnValue struct {
	Column string
	Value  codec.Value
}

// Record is one logical row effect. Records carry replication-level values,
// never storage encodings: the bundle codec (a later slice) serializes them
// canonically.
type Record struct {
	Table   string
	Row     ids.RowID
	Op      RecordOp
	Columns []ColumnValue // empty for RecordDelete
}

// Batch is one source transaction's logical effects. It preserves the source
// transaction identity so forwarding and replay cannot duplicate exports.
type Batch struct {
	TxID     ids.TxID
	Origin   ids.NodeID
	Sequence uint64 // Low bridge sequence (progress, not a mesh watermark)
	HLC      uint64
	Records  []Record
}

// Validate checks batch well-formedness against limits.
func (b Batch) Validate(limits Limits) error {
	if b.TxID.IsZero() {
		return fmt.Errorf("bridge: batch requires a transaction identity")
	}
	if b.Origin.IsZero() {
		return fmt.Errorf("bridge: batch requires an origin identity")
	}
	if len(b.Records) == 0 {
		return fmt.Errorf("bridge: batch carries no records")
	}
	if len(b.Records) > limits.MaxTransactions {
		return fmt.Errorf("bridge: batch of %d records exceeds limit %d", len(b.Records), limits.MaxTransactions)
	}
	for i, r := range b.Records {
		if r.Table == "" {
			return fmt.Errorf("bridge: record %d has no table", i)
		}
		if r.Row.IsZero() {
			return fmt.Errorf("bridge: record %d has no row identity", i)
		}
		switch r.Op {
		case RecordPut:
			if len(r.Columns) == 0 {
				return fmt.Errorf("bridge: record %d upsert has no columns", i)
			}
			for _, c := range r.Columns {
				if c.Column == "" {
					return fmt.Errorf("bridge: record %d has an unnamed column", i)
				}
			}
		case RecordDelete:
			if len(r.Columns) != 0 {
				return fmt.Errorf("bridge: record %d delete carries columns", i)
			}
		default:
			return fmt.Errorf("bridge: record %d has unknown op %d", i, int(r.Op))
		}
	}
	return nil
}

// Artifact is one opaque transfer unit. Names and bytes are untrusted until
// the bundle layer validates them.
type Artifact struct {
	Name string
	Data []byte
}

// ValidateArtifact enforces the filename policy and size bound shared by
// every transport: names are confined to a portable charset (no separators,
// parent references, or hidden files) and payloads respect MaxBundleBytes.
func ValidateArtifact(a Artifact, limits Limits) error {
	if err := checkNameChars(a.Name, 256); err != nil {
		return fmt.Errorf("bridge: invalid artifact name %q", a.Name)
	}
	if len(a.Data) == 0 {
		return fmt.Errorf("bridge: artifact %q has no payload", a.Name)
	}
	if len(a.Data) > limits.MaxBundleBytes {
		return fmt.Errorf("bridge: artifact %q of %d bytes exceeds limit %d",
			a.Name, len(a.Data), limits.MaxBundleBytes)
	}
	return nil
}

// checkNameChars enforces the portable name policy shared by artifact names
// and stream identities (streams embed into artifact names): bounded
// length, no separators, parent references, or hidden files.
func checkNameChars(name string, max int) error {
	if name == "" || len(name) > max {
		return fmt.Errorf("invalid name length %d", len(name))
	}
	if strings.HasPrefix(name, ".") || strings.Contains(name, "..") {
		return fmt.Errorf("invalid name")
	}
	for _, r := range name {
		if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' ||
			r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("invalid name")
	}
	return nil
}

// Publisher sinks artifacts toward High. Exporters hold only this direction.
type Publisher interface {
	Publish(ctx context.Context, a Artifact) error
}

// ArtifactSource sources artifacts from Low. Receivers hold only this
// direction; Next blocks until an artifact is available or ctx ends.
type ArtifactSource interface {
	Next(ctx context.Context) (Artifact, error)
}

// Database is the minimal surface the bridge needs from either domain's DB.
// The concrete *replicateddb.DB satisfies it.
type Database interface {
	DBID() ids.DBID
}

// Exporter publishes logical changes for High receivers. It has no receive
// path: a Low node cannot import through this handle, and a High node cannot
// construct one (role + domain binding reject it).
type Exporter struct {
	*Bridge
	out Publisher
}

// NewExporter binds an exporter to a Low database.
func NewExporter(database Database, cfg Config, out Publisher) (*Exporter, error) {
	if out == nil {
		return nil, errors.New("bridge: exporter requires a publisher")
	}
	b, err := bindDB(database, cfg, RoleLowExporter)
	if err != nil {
		return nil, err
	}
	return &Exporter{Bridge: b, out: out}, nil
}

// Receiver accepts Low exports into High. It has no publish path.
type Receiver struct {
	*Bridge
	in ArtifactSource
}

// NewReceiver binds a receiver to a High database.
func NewReceiver(database Database, cfg Config, in ArtifactSource) (*Receiver, error) {
	if in == nil {
		return nil, errors.New("bridge: receiver requires a receiver transport")
	}
	b, err := bindDB(database, cfg, RoleHighReceiver)
	if err != nil {
		return nil, err
	}
	return &Receiver{Bridge: b, in: in}, nil
}

func bindDB(database Database, cfg Config, want Role) (*Bridge, error) {
	if database == nil {
		return nil, errors.New("bridge: database is required")
	}
	cfg, err := cfg.validated()
	if err != nil {
		return nil, err
	}
	if cfg.Role == RoleDisabled {
		return nil, ErrDisabled
	}
	if cfg.Role != want {
		return nil, fmt.Errorf("%w: configured %s, want %s", ErrRoleMismatch, cfg.Role, want)
	}
	if got := database.DBID(); got != cfg.Domain {
		return nil, fmt.Errorf("%w: database %s is not in domain %s", ErrDomainMismatch, got, cfg.Domain)
	}
	return &Bridge{db: database, cfg: cfg, limits: cfg.Limits}, nil
}

// PublishArtifact validates and publishes one artifact toward High.
func (e *Exporter) PublishArtifact(ctx context.Context, a Artifact) error {
	if err := ValidateArtifact(a, e.limits); err != nil {
		return err
	}
	return e.out.Publish(ctx, a)
}

// NextArtifact receives the next artifact from Low, enforcing the same
// filename/size policy before the bundle layer sees it.
func (r *Receiver) NextArtifact(ctx context.Context) (Artifact, error) {
	a, err := r.in.Next(ctx)
	if err != nil {
		return Artifact{}, err
	}
	if err := ValidateArtifact(a, r.limits); err != nil {
		return Artifact{}, err
	}
	return a, nil
}
