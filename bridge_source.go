package replicateddb

import (
	"context"
	"errors"
	"fmt"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/state"
)

// ErrBridgeLogGone indicates a bridge exporter's source log was
// garbage-collected past its resume point. Export stalls loudly: gaps must
// never be silently skipped.
var ErrBridgeLogGone = errors.New("replicateddb: source log garbage-collected past bridge resume")

// BridgeLogSource abstracts the origin logs a bridge exporter drains.
// KnownOrigins lists candidate origins; ScanLog streams up to maxBatches
// (within maxBytes) from fromSeq, returning the last sequence handed to fn.
// ProtectResume registers the lowest uncaptured sequence for an origin so that
// log GC does not collect uncaptured changes before the capturer drains them.
type BridgeLogSource interface {
	KnownOrigins(ctx context.Context) ([]ids.NodeID, error)
	ScanLog(ctx context.Context, origin ids.NodeID, fromSeq uint64, maxBatches int, maxBytes int, fn func(*codec.MutationBatch) error) (uint64, error)
	ProtectResume(origin ids.NodeID, resumeSeq uint64) error
	ReleaseProtection(origin ids.NodeID) error
}

// BridgeSchemaResolver maps replicated table/column IDs to logical names.
// Replicated schemas are additive, so a current registry resolves
// historical IDs.
type BridgeSchemaResolver interface {
	TableName(tableID uint32) (string, error)
	ColumnName(tableID, columnID uint32) (string, error)
}

// BridgeLogSource exposes this node's origin logs to a bridge exporter.
// The bridge package cannot reach db.store directly; this adapter is the
// only seam, and it carries no write path.
func (db *DB) BridgeLogSource() BridgeLogSource {
	return bridgeLogSource{db: db}
}

type bridgeLogSource struct {
	db *DB
}

func (s bridgeLogSource) KnownOrigins(ctx context.Context) ([]ids.NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.db.store.KnownOrigins()
}

func (s bridgeLogSource) ScanLog(ctx context.Context, origin ids.NodeID, fromSeq uint64, maxBatches int, maxBytes int, fn func(*codec.MutationBatch) error) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return fromSeq, err
	}
	last, err := s.db.store.LogScan(origin, fromSeq, maxBatches, maxBytes, func(mb *codec.MutationBatch) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		filtered := *mb
		filtered.Mutations = make([]codec.Mutation, 0, len(mb.Mutations))
		for _, mutation := range mb.Mutations {
			if mutation.TableID != BridgePolicyTableID {
				filtered.Mutations = append(filtered.Mutations, mutation)
			}
		}
		if len(filtered.Mutations) == 0 {
			return nil // High policy metadata is not a Low export event.
		}
		return fn(&filtered)
	})
	if errors.Is(err, state.ErrLogGone) {
		return last, fmt.Errorf("%w: origin %s from %d", ErrBridgeLogGone, origin, fromSeq)
	}
	return last, err
}

func (s bridgeLogSource) ProtectResume(origin ids.NodeID, resumeSeq uint64) error {
	s.db.store.SetBridgeExportResume(origin, resumeSeq)
	return nil
}

func (s bridgeLogSource) ReleaseProtection(origin ids.NodeID) error {
	s.db.store.ClearBridgeExportResume(origin)
	return nil
}

// BridgeSchema snapshots the current schema registry as a bridge name
// resolver. Exporters rebuild it after local migration.
func (db *DB) BridgeSchema() (BridgeSchemaResolver, error) {
	reg := db.schemaRegistry()
	if reg == nil {
		return nil, fmt.Errorf("replicateddb: schema is not ready")
	}
	tables := make(map[uint32]string, len(reg.Tables))
	cols := make(map[uint32]map[uint32]string, len(reg.Tables))
	for _, t := range reg.Tables {
		tables[t.ID] = t.Name
		m := make(map[uint32]string, len(t.Columns))
		for _, c := range t.Columns {
			m[c.ID] = c.Name
		}
		cols[t.ID] = m
	}
	// The reserved file metadata table lives outside the application
	// registry but crosses the bridge as ordinary records, so it resolves
	// here too (identically with or without local files enabled).
	fids, err := resolveFileIDs()
	if err != nil {
		return nil, fmt.Errorf("replicateddb: file metadata schema: %w", err)
	}
	tables[fids.table] = fileTableName
	cols[fids.table] = map[uint32]string{
		fids.id:     fileColID,
		fids.name:   fileColName,
		fids.digest: fileColDigest,
		fids.size:   fileColSize,
	}
	return bridgeSchema{tables: tables, cols: cols}, nil
}

type bridgeSchema struct {
	tables map[uint32]string
	cols   map[uint32]map[uint32]string
}

func (s bridgeSchema) TableName(tableID uint32) (string, error) {
	name, ok := s.tables[tableID]
	if !ok {
		return "", fmt.Errorf("replicateddb: unknown table %d", tableID)
	}
	return name, nil
}

func (s bridgeSchema) ColumnName(tableID, columnID uint32) (string, error) {
	cols, ok := s.cols[tableID]
	if !ok {
		return "", fmt.Errorf("replicateddb: unknown table %d", tableID)
	}
	name, ok := cols[columnID]
	if !ok {
		return "", fmt.Errorf("replicateddb: unknown column %d of table %d", columnID, tableID)
	}
	return name, nil
}
