// Bridge file transfer surface.
//
// File metadata crosses the High/Low bridge as ordinary bundle records under
// the reserved file table name; object bytes cross as recipient-sealed chunk
// artifacts. This file is the root-owned seam the bridge package uses: name
// constants, provenance accessors for file rows (the policy store is keyed
// by numeric IDs, so file rows fit the same ownership model as SQL rows),
// atomic metadata import with Low provenance, and object byte access.
//
// The bridge package cannot import these names from files.go's private
// constants (same package, but the bridge needs a stable contract), so the
// Bridge-prefixed names below are the contract; they alias the file
// subsystem's single source of truth.
package replicateddb

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/objectstore"
	"github.com/marcgauthier/spedsql/replication"
)

// Bridge file record contract: table and column names as they appear in
// bundle records.
const (
	BridgeFileTableName = fileTableName
	BridgeFileColID     = fileColID
	BridgeFileColName   = fileColName
	BridgeFileColDigest = fileColDigest
	BridgeFileColSize   = fileColSize
)

// BridgeFilesEnabled reports whether this node runs the file subsystem.
func (db *DB) BridgeFilesEnabled() bool { return db.files != nil }

// BridgeFileRowID derives a file metadata row ID from its name, identically
// on every node. The bridge uses it to correlate records with local state.
func BridgeFileRowID(name string) ids.RowID { return fileRowID(name) }

// BridgeFileObjects exposes the node-local object store to a bridge
// exporter for chunking object bytes. It returns nil when files are
// disabled; metadata-only export still works.
func (db *DB) BridgeFileObjects() *objectstore.Store {
	if db.files == nil {
		return nil
	}
	return db.files.objects
}

// bridgeFileIDs resolves the reserved file IDs or fails closed.
func (db *DB) bridgeFileIDs() (fileIDs, error) {
	fids, err := resolveFileIDs()
	if err != nil {
		return fileIDs{}, fmt.Errorf("replicateddb: file metadata schema: %w", err)
	}
	return fids, nil
}

// BridgeFileProvenance returns the persisted bridge provenance for a file
// row, if one exists.
func (db *DB) BridgeFileProvenance(row ids.RowID) (BridgeProvenance, bool, error) {
	fids, err := db.bridgeFileIDs()
	if err != nil {
		return BridgeProvenance{}, false, err
	}
	return db.bridgePolicy(fids.table, row, 0)
}

// BridgeFileFieldProvenance returns the ownership for one file metadata
// field (one of the BridgeFileCol* names). Like SQL fields, ownership
// derives from set-shadows so reordered imports cannot mask it.
func (db *DB) BridgeFileFieldProvenance(row ids.RowID, columnName string) (BridgeProvenance, bool, error) {
	fids, err := db.bridgeFileIDs()
	if err != nil {
		return BridgeProvenance{}, false, err
	}
	col, err := bridgeFileColumnID(fids, columnName)
	if err != nil {
		return BridgeProvenance{}, false, err
	}
	policy, ok, err := db.bridgePolicy(fids.table, row, col)
	if err != nil || !ok || policy.SourceDomain.IsZero() {
		return policy, ok, err
	}
	shadowed, err := db.bridgeFieldShadowed(fids.table, row, col)
	if err != nil {
		return BridgeProvenance{}, false, err
	}
	if shadowed {
		policy.Owner = BridgeOwnerHigh
	} else {
		policy.Owner = BridgeOwnerLow
	}
	return policy, true, nil
}

// BridgeFileHighOwnedColumns lists file metadata fields protected from Low
// updates and deletes.
func (db *DB) BridgeFileHighOwnedColumns(row ids.RowID) ([]string, error) {
	fids, err := db.bridgeFileIDs()
	if err != nil {
		return nil, err
	}
	var out []string
	for _, col := range []struct {
		name string
		id   uint32
	}{
		{BridgeFileColID, fids.id},
		{BridgeFileColName, fids.name},
		{BridgeFileColDigest, fids.digest},
		{BridgeFileColSize, fids.size},
	} {
		shadowed, err := db.bridgeFieldShadowed(fids.table, row, col.id)
		if err != nil {
			return nil, err
		}
		if shadowed {
			out = append(out, col.name)
		}
	}
	return out, nil
}

// BridgeFileRowExists reports whether a file row is currently visible
// (stored cells not hidden by a tombstone).
func (db *DB) BridgeFileRowExists(row ids.RowID) (bool, error) {
	fids, err := db.bridgeFileIDs()
	if err != nil {
		return false, err
	}
	if err := db.requireRead(); err != nil {
		return false, err
	}
	cells, err := db.store.GetRow(fids.table, row)
	if err != nil {
		return false, err
	}
	if len(cells) == 0 {
		return false, nil
	}
	tomb, hasTomb, err := db.store.GetTombstone(fids.table, row)
	if err != nil {
		return false, err
	}
	_, visible, err := effectiveFileVisible(db, cells, tomb, hasTomb)
	if err != nil {
		return false, err
	}
	return visible, nil
}

// ReleaseBridgeFileOwnership explicitly returns a High-overridden imported
// file field to Low ownership, clearing its shadow so the Low value reads
// back. The change replicates like any policy commit.
func (db *DB) ReleaseBridgeFileOwnership(ctx context.Context, row ids.RowID, columnName string) error {
	policy, ok, err := db.BridgeFileFieldProvenance(row, columnName)
	if err != nil {
		return err
	}
	if !ok || policy.Owner != BridgeOwnerHigh || policy.SourceDomain.IsZero() {
		return fmt.Errorf("replicateddb: file field has no releasable Low ownership")
	}
	fids, err := db.bridgeFileIDs()
	if err != nil {
		return err
	}
	col, err := bridgeFileColumnID(fids, columnName)
	if err != nil {
		return err
	}
	policy.Owner = BridgeOwnerLow
	policy.OverrideTxID = ids.TxID{}
	policy.Release = true
	clear, err := bridgeShadowClearFor(db, fids.table, row, col)
	if err != nil {
		return err
	}
	return db.commitBridgePolicy(ctx, bridgePolicyMutation(fids.table, row, col, policy), clear)
}

// ReleaseBridgeFileRowOwnership explicitly lets the source stream reclaim
// an imported file row after a High-local delete decision.
func (db *DB) ReleaseBridgeFileRowOwnership(ctx context.Context, row ids.RowID) error {
	policy, ok, err := db.BridgeFileProvenance(row)
	if err != nil {
		return err
	}
	if !ok || policy.Owner != BridgeOwnerHigh || policy.SourceDomain.IsZero() {
		return fmt.Errorf("replicateddb: file row has no releasable Low ownership")
	}
	fids, err := db.bridgeFileIDs()
	if err != nil {
		return err
	}
	policy.Owner = BridgeOwnerLow
	policy.OverrideTxID = ids.TxID{}
	policy.Release = true
	clears, err := bridgeShadowClearsForRow(db, fids.table, row)
	if err != nil {
		return err
	}
	return db.commitBridgePolicy(ctx, append([]codec.Mutation{bridgePolicyMutation(fids.table, row, 0, policy)}, clears...)...)
}

// bridgeFileDeleteReleases returns High-owned file columns to Low ownership
// when an accepted Low delete resolves a policy hold, mirroring the SQL
// release in policyMutationsForTx.
func bridgeFileDeleteReleases(db *DB, fids fileIDs, deletes []ids.RowID, lastSeq uint64) ([]codec.Mutation, error) {
	var out []codec.Mutation
	for _, row := range deletes {
		for _, col := range []uint32{fids.id, fids.name, fids.digest, fids.size} {
			old, ok, err := db.bridgePolicy(fids.table, row, col)
			if err != nil {
				return nil, err
			}
			if ok && old.Owner == BridgeOwnerHigh {
				old.Owner = BridgeOwnerLow
				old.OverrideTxID = ids.TxID{}
				old.LastSeq = lastSeq
				out = append(out, bridgePolicyMutation(fids.table, row, col, old))
				clear, err := bridgeShadowClearFor(db, fids.table, row, col)
				if err != nil {
					return nil, err
				}
				out = append(out, clear)
			}
		}
	}
	return out, nil
}

func bridgeFileColumnID(fids fileIDs, columnName string) (uint32, error) {
	switch {
	case strings.EqualFold(columnName, BridgeFileColID):
		return fids.id, nil
	case strings.EqualFold(columnName, BridgeFileColName):
		return fids.name, nil
	case strings.EqualFold(columnName, BridgeFileColDigest):
		return fids.digest, nil
	case strings.EqualFold(columnName, BridgeFileColSize):
		return fids.size, nil
	default:
		return 0, fmt.Errorf("replicateddb: unknown file column %q", columnName)
	}
}

// BridgeFilePut is one imported file-metadata row.
type BridgeFilePut struct {
	Row    ids.RowID
	Name   string
	Digest objectstore.Digest
	Size   int64
}

// BridgeApplyFileMetadata commits imported file metadata (puts plus row
// tombstones) atomically with its Low provenance, and notifies High mesh
// replication so accepted files redistribute as ordinary replicated writes.
// Object bytes install separately; metadata without bytes stays pending.
func (db *DB) BridgeApplyFileMetadata(ctx context.Context, source ids.DBID, stream string, bundle ids.TxID, first, last uint64, allowHighDelete bool, puts []BridgeFilePut, deletes []ids.RowID) error {
	if source.IsZero() || stream == "" || len(stream) > 1024 || first == 0 || last < first {
		return fmt.Errorf("replicateddb: invalid bridge import provenance")
	}
	if len(puts) == 0 && len(deletes) == 0 {
		return nil
	}
	fs, err := db.filesForWrite()
	if err != nil {
		return err
	}
	for _, p := range puts {
		if p.Name == "" {
			return fmt.Errorf("replicateddb: bridge file put has no name")
		}
		if len(p.Name) > db.cfg.MaxReplicatedValueBytes {
			return fmt.Errorf("%w: bridge file name %d bytes", ErrValueTooLarge, len(p.Name))
		}
		if p.Size < 0 || p.Size > db.cfg.Files.MaxFileBytes {
			return fmt.Errorf("%w: bridge file %q size %d", ErrFileTooLarge, p.Name, p.Size)
		}
	}
	txID := ids.NewTxID()
	mutations := make([]codec.Mutation, 0, 4*len(puts)+len(deletes))
	for _, p := range puts {
		row := p.Row[:]
		mutations = append(mutations,
			codec.Mutation{TableID: fs.ids.table, RowID: p.Row, ColumnID: fs.ids.id, Value: codec.Blob(row)},
			codec.Mutation{TableID: fs.ids.table, RowID: p.Row, ColumnID: fs.ids.name, Value: codec.Text(p.Name)},
			codec.Mutation{TableID: fs.ids.table, RowID: p.Row, ColumnID: fs.ids.digest, Value: codec.Blob(p.Digest[:])},
			codec.Mutation{TableID: fs.ids.table, RowID: p.Row, ColumnID: fs.ids.size, Value: codec.Int(p.Size)},
		)
	}
	for _, row := range deletes {
		mutations = append(mutations, codec.Mutation{TableID: fs.ids.table, RowID: row, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone})
	}
	info := &bridgeImportInfo{SourceDomain: source, Stream: stream, BundleID: bundle, FirstSeq: first, LastSeq: last, AllowHighDelete: allowHighDelete}
	policyMutations, err := policyMutationsForTx(db, &Tx{txID: txID, bridgeImport: info}, mutations)
	if err != nil {
		return fmt.Errorf("replicateddb: bridge policy: %w", err)
	}
	mutations = append(mutations, policyMutations...)
	// The shared policy builder releases High-owned columns through the
	// SQL registry, which has no file table: release file columns here
	// with identical semantics.
	if allowHighDelete {
		released, err := bridgeFileDeleteReleases(db, fs.ids, deletes, last)
		if err != nil {
			return fmt.Errorf("replicateddb: bridge policy: %w", err)
		}
		mutations = append(mutations, released...)
	}
	ticket, err := db.sched.Admit(ctx, WriterRemote)
	if err != nil {
		return err
	}
	defer ticket.Release()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	identity := db.schemaIdentity()
	batch := &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		TxID:            txID,
		OriginNode:      db.cfg.NodeID,
		HLC:             db.store.ClockNow(),
		SchemaEpoch:     identity.Epoch,
		SchemaHash:      identity.Hash,
		Mutations:       mutations,
	}
	result, err := db.store.CommitLocal(ctx, batch)
	if err != nil {
		return err
	}
	if len(db.remoteRows) == 0 {
		db.materializedGeneration.Store(result.Generation)
	}
	if manager := db.replManager(); manager != nil {
		manager.NotifyLocal()
	}
	return nil
}

// BridgePutFileObject streams plaintext bytes into the node-local object
// store (re-encrypting under the local object key) and returns the verified
// digest and length. Importers compare both against the replicated metadata.
func (db *DB) BridgePutFileObject(ctx context.Context, src io.Reader) (objectstore.Digest, int64, error) {
	var zero objectstore.Digest
	fs, err := db.filesForWrite()
	if err != nil {
		return zero, 0, err
	}
	if src == nil {
		return zero, 0, fmt.Errorf("replicateddb: file content reader is required")
	}
	maxBytes := db.cfg.Files.MaxFileBytes
	info, err := fs.objects.Put(ctx, io.LimitReader(src, maxBytes+1))
	if err != nil {
		return zero, 0, err
	}
	if info.Length > maxBytes {
		return zero, 0, fmt.Errorf("%w: bridge object exceeds %d bytes", ErrFileTooLarge, maxBytes)
	}
	return info.Digest, info.Length, nil
}
