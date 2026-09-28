package replicateddb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/crdt"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/replication"
	"github.com/nomadsql/replicateddb/schema"
)

const (
	BridgePolicyTableID uint32 = 0xFFFFFFFE
	bridgePolicyColumn  uint32 = 1
	bridgePolicyVersion        = 2
)

type BridgeFieldOwner uint8

const (
	BridgeOwnerNone BridgeFieldOwner = iota
	BridgeOwnerLow
	BridgeOwnerHigh
)

// BridgeProvenance is durable metadata for a High row or field imported from
// one authenticated Low stream. It is replicated as hidden state mutations.
type BridgeProvenance struct {
	Owner        BridgeFieldOwner
	SourceDomain ids.DBID
	Stream       string
	BundleID     ids.TxID
	FirstSeq     uint64
	LastSeq      uint64
	Deleted      bool
	OverrideTxID ids.TxID
	Release      bool
	TargetTable  uint32
	TargetRow    ids.RowID
	TargetColumn uint32
	Version      crdt.Version
}

type bridgeImportInfo struct {
	SourceDomain    ids.DBID
	Stream          string
	BundleID        ids.TxID
	FirstSeq        uint64
	LastSeq         uint64
	AllowHighDelete bool
}

func (db *DB) bridgeTable(tableName string) (*schema.TableSchema, error) {
	reg := db.schemaRegistry()
	if reg == nil {
		return nil, fmt.Errorf("replicateddb: schema is not ready")
	}
	t := reg.Table(tableName)
	if t == nil {
		return nil, fmt.Errorf("replicateddb: unknown table %q", tableName)
	}
	return t, nil
}

// BridgeRowProvenance returns the persisted Low source for a row, if one
// exists. Row IDs use the replicated BLOB primary-key identity.
func (db *DB) BridgeRowProvenance(tableName string, row ids.RowID) (BridgeProvenance, bool, error) {
	t, err := db.bridgeTable(tableName)
	if err != nil {
		return BridgeProvenance{}, false, err
	}
	return db.bridgePolicy(t.ID, row, 0)
}

// BridgeFieldProvenance returns the current ownership and Low provenance for
// one replicated field. Ownership derives from the field's shadow cell (a
// set-shadow means High owns the field even when a reordered Low import
// overwrote the policy cell); provenance lineage still comes from the
// policy cell.
func (db *DB) BridgeFieldProvenance(tableName string, row ids.RowID, columnName string) (BridgeProvenance, bool, error) {
	t, err := db.bridgeTable(tableName)
	if err != nil {
		return BridgeProvenance{}, false, err
	}
	var col *schema.ColumnSchema
	for i := range t.Columns {
		if strings.EqualFold(t.Columns[i].Name, columnName) {
			col = &t.Columns[i]
			break
		}
	}
	if col == nil {
		return BridgeProvenance{}, false, fmt.Errorf("replicateddb: unknown column %q of table %q", columnName, tableName)
	}
	policy, ok, err := db.bridgePolicy(t.ID, row, col.ID)
	if err != nil || !ok || policy.SourceDomain.IsZero() {
		return policy, ok, err
	}
	shadowed, err := db.bridgeFieldShadowed(t.ID, row, col.ID)
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

// BridgeHighOwnedColumns lists fields protected from Low updates and deletes.
// Protection derives from set-shadows, so reordered Low imports cannot mask
// High ownership by overwriting policy cells.
func (db *DB) BridgeHighOwnedColumns(tableName string, row ids.RowID) ([]string, error) {
	t, err := db.bridgeTable(tableName)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, column := range t.Columns {
		shadowed, err := db.bridgeFieldShadowed(t.ID, row, column.ID)
		if err != nil {
			return nil, err
		}
		if shadowed {
			out = append(out, column.Name)
		}
	}
	return out, nil
}

func (db *DB) bridgePolicy(table uint32, row ids.RowID, column uint32) (BridgeProvenance, bool, error) {
	if err := db.requireRead(); err != nil {
		return BridgeProvenance{}, false, err
	}
	key := bridgePolicyRow(table, row, column)
	cell, ok, err := db.store.GetCell(BridgePolicyTableID, key, bridgePolicyColumn)
	if err != nil || !ok {
		return BridgeProvenance{}, ok, err
	}
	if cell.Value.Type != codec.TypeBlob {
		return BridgeProvenance{}, false, fmt.Errorf("replicateddb: corrupt bridge policy value")
	}
	policy, err := decodeBridgeProvenance(cell.Value.B)
	if err == nil {
		policy.Version = cell.Version
		if policy.TargetTable != table || policy.TargetRow != row || policy.TargetColumn != column {
			return BridgeProvenance{}, false, fmt.Errorf("replicateddb: bridge policy target mismatch")
		}
	}
	return policy, err == nil, err
}

// BeginBridgeImportTx begins a High-side SQL transaction tagged with the
// verified Low bundle identity. Its row/field provenance mutations are
// committed atomically with imported values.
func (db *DB) BeginBridgeImportTx(ctx context.Context, txID ids.TxID, source ids.DBID, stream string, bundle ids.TxID, first, last uint64, allowHighDelete bool, opts *TxOptions) (*Tx, error) {
	if source.IsZero() || stream == "" || len(stream) > 1024 || first == 0 || last < first {
		return nil, fmt.Errorf("replicateddb: invalid bridge import provenance")
	}
	tx, err := db.BeginTxWithID(ctx, txID, opts)
	if err != nil {
		return nil, err
	}
	tx.bridgeImport = &bridgeImportInfo{SourceDomain: source, Stream: stream, BundleID: bundle, FirstSeq: first, LastSeq: last, AllowHighDelete: allowHighDelete}
	return tx, nil
}

// ReleaseBridgeOwnership explicitly returns a High-overridden imported field
// to Low ownership. The policy change is replicated and durable.
func (db *DB) ReleaseBridgeOwnership(ctx context.Context, tableName string, row ids.RowID, columnName string) error {
	policy, ok, err := db.BridgeFieldProvenance(tableName, row, columnName)
	if err != nil {
		return err
	}
	if !ok || policy.Owner != BridgeOwnerHigh || policy.SourceDomain.IsZero() {
		return fmt.Errorf("replicateddb: field has no releasable Low ownership")
	}
	policy.Owner = BridgeOwnerLow
	policy.OverrideTxID = ids.TxID{}
	policy.Release = true
	table, err := db.bridgeTable(tableName)
	if err != nil {
		return err
	}
	var col uint32
	for _, c := range table.Columns {
		if strings.EqualFold(c.Name, columnName) {
			col = c.ID
			break
		}
	}
	clear, err := bridgeShadowClearFor(db, table.ID, row, col)
	if err != nil {
		return err
	}
	return db.commitBridgePolicy(ctx, bridgePolicyMutation(table.ID, row, col, policy), clear)
}

// ReleaseBridgeRowOwnership explicitly lets the source stream reclaim an
// imported row after a High-local delete or resurrection decision.
func (db *DB) ReleaseBridgeRowOwnership(ctx context.Context, tableName string, row ids.RowID) error {
	policy, ok, err := db.BridgeRowProvenance(tableName, row)
	if err != nil {
		return err
	}
	if !ok || policy.Owner != BridgeOwnerHigh || policy.SourceDomain.IsZero() {
		return fmt.Errorf("replicateddb: row has no releasable Low ownership")
	}
	table, err := db.bridgeTable(tableName)
	if err != nil {
		return err
	}
	policy.Owner = BridgeOwnerLow
	policy.OverrideTxID = ids.TxID{}
	policy.Release = true
	clears, err := bridgeShadowClearsForRow(db, table.ID, row)
	if err != nil {
		return err
	}
	return db.commitBridgePolicy(ctx, append([]codec.Mutation{bridgePolicyMutation(table.ID, row, 0, policy)}, clears...)...)
}

func (db *DB) commitBridgePolicy(ctx context.Context, mutations ...codec.Mutation) error {
	if err := db.requireWrite(); err != nil {
		return err
	}
	ticket, err := db.sched.Admit(ctx, WriterLocal)
	if err != nil {
		return err
	}
	defer ticket.Release()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	if err := db.flushRemoteLocked(); err != nil {
		return err
	}
	identity := db.schemaIdentity()
	batch := &codec.MutationBatch{ProtocolVersion: replication.ProtocolVersion, TxID: ids.NewTxID(), OriginNode: db.cfg.NodeID, HLC: db.store.ClockNow(), SchemaEpoch: identity.Epoch, SchemaHash: identity.Hash, Mutations: mutations}
	result, err := db.store.CommitLocal(ctx, batch)
	if err != nil {
		return err
	}
	// Shadow clears/sets change effective SQL state without SQL writes of
	// their own: materialize the resolved winners so local reads converge
	// immediately (release flips to Low bytes, row release resurrects).
	if len(result.Winners) > 0 {
		resolved, rerr := resolveBridgeWinners(db, result.Winners)
		if rerr != nil {
			db.log.Warn("bridge policy apply failed; rebuilding materializer", "err", rerr.Error())
			if rerr := db.rebuildLocked(); rerr != nil {
				return rerr
			}
		} else if len(resolved) > 0 {
			if err := db.engine.ApplyWinners(db.shadowReader(), resolved); err != nil {
				db.log.Warn("bridge policy apply failed; rebuilding materializer", "err", err.Error())
				if rerr := db.rebuildLocked(); rerr != nil {
					return rerr
				}
			}
		}
	}
	db.materializedGeneration.Store(result.Generation)
	if manager := db.replManager(); manager != nil {
		manager.NotifyLocal()
	}
	return nil
}

func bridgePolicyRow(table uint32, row ids.RowID, column uint32) ids.RowID {
	h := sha256.New()
	h.Write([]byte("spedsql-bridge-policy-v1"))
	var target [8]byte
	binary.BigEndian.PutUint32(target[:4], table)
	binary.BigEndian.PutUint32(target[4:], column)
	h.Write(target[:])
	h.Write(row[:])
	sum := h.Sum(nil)
	var out ids.RowID
	copy(out[:], sum[:len(out)])
	return out
}

func bridgePolicyMutation(table uint32, row ids.RowID, column uint32, policy BridgeProvenance) codec.Mutation {
	policy.TargetTable, policy.TargetRow, policy.TargetColumn = table, row, column
	return codec.Mutation{TableID: BridgePolicyTableID, RowID: bridgePolicyRow(table, row, column), ColumnID: bridgePolicyColumn, Value: codec.Blob(encodeBridgeProvenance(policy))}
}

func encodeBridgeProvenance(policy BridgeProvenance) []byte {
	b := make([]byte, 1+1+16+2+len(policy.Stream)+16+8+8+1+16+1+4+16+4)
	b[0], b[1] = bridgePolicyVersion, byte(policy.Owner)
	copy(b[2:18], policy.SourceDomain[:])
	binary.BigEndian.PutUint16(b[18:20], uint16(len(policy.Stream)))
	off := 20
	copy(b[off:], policy.Stream)
	off += len(policy.Stream)
	copy(b[off:], policy.BundleID[:])
	off += 16
	binary.BigEndian.PutUint64(b[off:], policy.FirstSeq)
	off += 8
	binary.BigEndian.PutUint64(b[off:], policy.LastSeq)
	off += 8
	if policy.Deleted {
		b[off] = 1
	}
	off++
	copy(b[off:], policy.OverrideTxID[:])
	off += 16
	if policy.Release {
		b[off] = 1
	}
	off++
	binary.BigEndian.PutUint32(b[off:], policy.TargetTable)
	off += 4
	copy(b[off:], policy.TargetRow[:])
	off += 16
	binary.BigEndian.PutUint32(b[off:], policy.TargetColumn)
	return b
}

func decodeBridgeProvenance(b []byte) (BridgeProvenance, error) {
	var policy BridgeProvenance
	if len(b) < 1+1+16+2+16+8+8+1+16+1+4+16+4 || b[0] != bridgePolicyVersion {
		return policy, fmt.Errorf("replicateddb: malformed bridge policy value")
	}
	policy.Owner = BridgeFieldOwner(b[1])
	copy(policy.SourceDomain[:], b[2:18])
	streamLen := int(binary.BigEndian.Uint16(b[18:20]))
	if streamLen > len(b)-20-74 || len(b) != 20+streamLen+74 {
		return BridgeProvenance{}, fmt.Errorf("replicateddb: malformed bridge policy stream")
	}
	off := 20
	policy.Stream = string(b[off : off+streamLen])
	off += streamLen
	copy(policy.BundleID[:], b[off:off+16])
	off += 16
	policy.FirstSeq = binary.BigEndian.Uint64(b[off:])
	off += 8
	policy.LastSeq = binary.BigEndian.Uint64(b[off:])
	off += 8
	policy.Deleted = b[off] != 0
	off++
	copy(policy.OverrideTxID[:], b[off:off+16])
	off += 16
	policy.Release = b[off] != 0
	off++
	policy.TargetTable = binary.BigEndian.Uint32(b[off:])
	off += 4
	copy(policy.TargetRow[:], b[off:off+16])
	off += 16
	policy.TargetColumn = binary.BigEndian.Uint32(b[off:])
	return policy, nil
}

func policyMutationsForTx(db *DB, tx *Tx, mutations []codec.Mutation) ([]codec.Mutation, error) {
	var out []codec.Mutation
	if tx.bridgeImport == nil {
		seenRows := make(map[string]bool)
		for _, mutation := range mutations {
			if mutation.TableID == BridgePolicyTableID {
				continue
			}
			var target [20]byte
			binary.BigEndian.PutUint32(target[:4], mutation.TableID)
			copy(target[4:], mutation.RowID[:])
			rowKey := string(target[:])
			if !seenRows[rowKey] {
				rowPolicy, exists, err := db.bridgePolicy(mutation.TableID, mutation.RowID, 0)
				if err != nil {
					return nil, err
				}
				cells, err := db.store.GetRow(mutation.TableID, mutation.RowID)
				if err != nil {
					return nil, err
				}
				if !exists && (mutation.IsTombstone() || len(cells) == 0) {
					rowPolicy = BridgeProvenance{Owner: BridgeOwnerHigh, Deleted: mutation.IsTombstone(), OverrideTxID: tx.txID}
					out = append(out, bridgePolicyMutation(mutation.TableID, mutation.RowID, 0, rowPolicy))
				} else if exists && rowPolicy.Owner == BridgeOwnerLow && (rowPolicy.Deleted || mutation.IsTombstone()) {
					rowPolicy.Owner = BridgeOwnerHigh
					rowPolicy.Deleted = mutation.IsTombstone()
					rowPolicy.OverrideTxID = tx.txID
					out = append(out, bridgePolicyMutation(mutation.TableID, mutation.RowID, 0, rowPolicy))
				}
				if exists && !rowPolicy.SourceDomain.IsZero() && mutation.IsTombstone() {
					// A High delete releases field protection with the row:
					// lingering shadows would suppress this tombstone on
					// every peer.
					clears, err := bridgeShadowClearsForRow(db, mutation.TableID, mutation.RowID)
					if err != nil {
						return nil, err
					}
					out = append(out, clears...)
				}
				seenRows[rowKey] = true
			}
			if mutation.IsTombstone() {
				continue
			}
			policy, ok, err := db.bridgePolicy(mutation.TableID, mutation.RowID, mutation.ColumnID)
			if err != nil {
				return nil, err
			}
			if !ok || policy.SourceDomain.IsZero() {
				continue // never imported: High-native field, no shadow needed
			}
			if policy.Owner == BridgeOwnerLow {
				policy.Owner = BridgeOwnerHigh
				policy.OverrideTxID = tx.txID
				out = append(out, bridgePolicyMutation(mutation.TableID, mutation.RowID, mutation.ColumnID, policy))
			}
			// Every High write to an imported field (re)asserts its shadow
			// so reads resolve the latest High bytes regardless of mesh
			// order; Low imports never write shadows, so a set-shadow
			// always wins against newer-HLC Low values at read time.
			shadow, err := bridgeShadowMutationFor(db, mutation.TableID, mutation.RowID, mutation.ColumnID, mutation.Value)
			if err != nil {
				return nil, err
			}
			out = append(out, shadow)
		}
		return out, nil
	}
	info := tx.bridgeImport
	seenRows := make(map[string]bool)
	for _, mutation := range mutations {
		if mutation.TableID == BridgePolicyTableID {
			continue
		}
		var target [20]byte
		binary.BigEndian.PutUint32(target[:4], mutation.TableID)
		copy(target[4:], mutation.RowID[:])
		rowKey := string(target[:])
		if !seenRows[rowKey] {
			policy := BridgeProvenance{Owner: BridgeOwnerLow, SourceDomain: info.SourceDomain, Stream: info.Stream, BundleID: info.BundleID, FirstSeq: info.FirstSeq, LastSeq: info.LastSeq, Deleted: mutation.IsTombstone()}
			old, ok, err := db.bridgePolicy(mutation.TableID, mutation.RowID, 0)
			if err != nil {
				return nil, err
			}
			if ok && old.SourceDomain == info.SourceDomain && old.Stream == info.Stream {
				policy.FirstSeq = old.FirstSeq
			}
			out = append(out, bridgePolicyMutation(mutation.TableID, mutation.RowID, 0, policy))
			seenRows[rowKey] = true
		}
		if mutation.IsTombstone() {
			if info.AllowHighDelete {
				if reg := db.schemaRegistry(); reg != nil {
					if table := reg.TableByID(mutation.TableID); table != nil {
						for _, column := range table.Columns {
							old, ok, err := db.bridgePolicy(mutation.TableID, mutation.RowID, column.ID)
							if err != nil {
								return nil, err
							}
							if ok && old.Owner == BridgeOwnerHigh {
								old.Owner = BridgeOwnerLow
								old.OverrideTxID = ids.TxID{}
								old.LastSeq = info.LastSeq
								out = append(out, bridgePolicyMutation(mutation.TableID, mutation.RowID, column.ID, old))
								clear, err := bridgeShadowClearFor(db, mutation.TableID, mutation.RowID, column.ID)
								if err != nil {
									return nil, err
								}
								out = append(out, clear)
							}
						}
					}
				}
			}
			continue
		}
		policy := BridgeProvenance{Owner: BridgeOwnerLow, SourceDomain: info.SourceDomain, Stream: info.Stream, BundleID: info.BundleID, FirstSeq: info.FirstSeq, LastSeq: info.LastSeq}
		old, ok, err := db.bridgePolicy(mutation.TableID, mutation.RowID, mutation.ColumnID)
		if err != nil {
			return nil, err
		}
		if ok && old.SourceDomain == info.SourceDomain && old.Stream == info.Stream {
			policy.FirstSeq = old.FirstSeq
		}
		out = append(out, bridgePolicyMutation(mutation.TableID, mutation.RowID, mutation.ColumnID, policy))
	}
	return out, nil
}

// bridgeShadowMutationFor builds a set-shadow for one High field write,
// failing closed when the column cannot take a shadow (reserved mapping or
// a legacy schema where the shadow ID collides with an app column).
func bridgeShadowMutationFor(db *DB, table uint32, row ids.RowID, column uint32, v codec.Value) (codec.Mutation, error) {
	return bridgeShadowMutationChecked(db, table, row, column, true, v)
}

// bridgeShadowClearFor builds a clear-shadow for one field.
func bridgeShadowClearFor(db *DB, table uint32, row ids.RowID, column uint32) (codec.Mutation, error) {
	return bridgeShadowMutationChecked(db, table, row, column, false, codec.Value{})
}

func bridgeShadowMutationChecked(db *DB, table uint32, row ids.RowID, column uint32, set bool, v codec.Value) (codec.Mutation, error) {
	s, ok := bridgeShadowColumn(column)
	if !ok {
		return codec.Mutation{}, fmt.Errorf("replicateddb: column %d of table %d cannot take a bridge shadow", column, table)
	}
	if cols := db.bridgeColumnsFor(table); cols != nil && cols[s] {
		return codec.Mutation{}, fmt.Errorf("replicateddb: column %d of table %d collides with a bridge shadow", column, table)
	}
	return bridgeShadowMutation(table, row, column, set, v)
}

// bridgeShadowClearsForRow clears every shadow of one row (High delete,
// row release). Unknown tables fail closed: silently skipping would leave
// protection lingering over a deleted row on every peer.
func bridgeShadowClearsForRow(db *DB, table uint32, row ids.RowID) ([]codec.Mutation, error) {
	cols := db.bridgeColumnsFor(table)
	if cols == nil {
		return nil, fmt.Errorf("replicateddb: cannot enumerate columns of table %d for shadow clear", table)
	}
	out := make([]codec.Mutation, 0, len(cols))
	for c := range cols {
		m, err := bridgeShadowMutationChecked(db, table, row, c, false, codec.Value{})
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}
