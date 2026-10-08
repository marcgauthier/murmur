// Bridge ownership shadow cells.
//
// A High override and a Low import for the same field commit as independent
// LWW cells (value + policy), so under reordered mesh delivery a newer-HLC
// Low value can overwrite a High-owned field on every peer while the policy
// cell independently converges to Low: the High bytes are lost and no peer
// can tell. Import-time filtering cannot fix this: the peer that imported
// before seeing the policy already committed the Low value, and the mesh
// replicates that commit everywhere.
//
// Shadow cells reconcile the reorder deterministically. Every High write to
// an imported field additionally commits its value into a same-row shadow
// column; Low imports never write shadows. Merge stays plain LWW everywhere,
// so all peers converge on identical cells; reads resolve each field to its
// shadow value while a set-shadow is the shadow column's winner, and to the
// value cell otherwise. Releases, High deletes, and accepted Low deletes
// clear shadows, returning the field to Low visibility. No custom merge, no
// repair transactions, no resurrection races: the value cell preserves the
// latest Low state for post-release reads, and the shadow preserves the
// latest unreleased High state.
//
// Same-row placement (rather than a separate table) keeps shadows on the
// already-fetched row: resolution adds no point reads to rebuild, repair, or
// file scans. The shadow column of app column c is c^0x80000000 (invertible,
// schema-independent); schema validation rejects ambiguous pairs so a stored
// column is never both an app column and a shadow.
package murmur

import (
	"fmt"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/state"
)

// bridgeShadowXOR maps an app column to its same-row shadow column and back.
const bridgeShadowXOR uint32 = 0x80000000

// Shadow payload tags. A set-shadow wraps the High value; a clear-shadow is
// the bare tag. Values ride the wrapped form so clears stay distinguishable
// from every value (including NULL and empty blobs).
const (
	bridgeShadowClear byte = 0x00
	bridgeShadowSet   byte = 0x01
)

// bridgeShadowColumn returns the shadow column for an app column. It reports
// false when the mapping lands on a reserved column ID (legacy columns
// 0x80000000/0x7FFFFFFF, rejected for new schemas); callers fail the write
// closed.
func bridgeShadowColumn(column uint32) (uint32, bool) {
	s := column ^ bridgeShadowXOR
	if s == 0 || s == codec.ColumnTombstone {
		return 0, false
	}
	return s, true
}

// bridgeShadowOf reports whether stored column s is the shadow of a known
// app column: s itself is not an app column but s^K is. Legacy schemas with
// ambiguous pairs fail shadow writes closed, so readers treat the app
// column as authoritative there.
func bridgeShadowOf(s uint32, cols map[uint32]bool) (uint32, bool) {
	if cols[s] {
		return 0, false
	}
	c := s ^ bridgeShadowXOR
	if !cols[c] {
		return 0, false
	}
	return c, true
}

// encodeBridgeShadowSet wraps a High field value as a shadow-cell value.
func encodeBridgeShadowSet(v codec.Value) codec.Value {
	inner := codec.EncodeCellState(nil, codec.CellState{Value: v})
	out := make([]byte, 1+len(inner))
	out[0] = bridgeShadowSet
	copy(out[1:], inner)
	return codec.Blob(out)
}

// encodeBridgeShadowClear returns the cleared-shadow cell value.
func encodeBridgeShadowClear() codec.Value {
	return codec.Blob([]byte{bridgeShadowClear})
}

// decodeBridgeShadow parses a shadow cell: set reports a High-owned field
// with value v; !set is a clear. Malformed cells error so readers fail
// closed, matching corrupt-cell handling elsewhere.
func decodeBridgeShadow(cell codec.CellState, lim codec.Limits) (set bool, v codec.Value, err error) {
	if cell.Value.Type != codec.TypeBlob || len(cell.Value.B) == 0 {
		return false, codec.Value{}, fmt.Errorf("murmur: malformed bridge shadow value")
	}
	switch cell.Value.B[0] {
	case bridgeShadowClear:
		if len(cell.Value.B) != 1 {
			return false, codec.Value{}, fmt.Errorf("murmur: malformed bridge shadow clear")
		}
		return false, codec.Value{}, nil
	case bridgeShadowSet:
		inner, err := codec.DecodeCellState(cell.Value.B[1:], lim)
		if err != nil {
			return false, codec.Value{}, fmt.Errorf("murmur: malformed bridge shadow payload: %w", err)
		}
		return true, inner.Value, nil
	default:
		return false, codec.Value{}, fmt.Errorf("murmur: malformed bridge shadow tag")
	}
}

// bridgeShadowMutation builds the shadow-column mutation for one field write.
// lim only sizes the corruption check on read; writes cannot fail on size.
func bridgeShadowMutation(table uint32, row ids.RowID, column uint32, set bool, v codec.Value) (codec.Mutation, error) {
	s, ok := bridgeShadowColumn(column)
	if !ok {
		return codec.Mutation{}, fmt.Errorf("murmur: column %d has no shadow mapping", column)
	}
	val := encodeBridgeShadowClear()
	if set {
		val = encodeBridgeShadowSet(v)
	}
	return codec.Mutation{TableID: table, RowID: row, ColumnID: s, Value: val}, nil
}

// resolveBridgeRow strips shadow columns from raw row cells and substitutes
// set-shadow values for their fields, returning effective cells. Versions
// stay on the value timeline (tombstone comparison must not see shadow
// versions); a set-shadow without a value cell falls back to the shadow's
// own version. cols carries the table's app column IDs.
func resolveBridgeRow(raw map[uint32]codec.CellState, cols map[uint32]bool, lim codec.Limits) (map[uint32]codec.CellState, error) {
	if len(raw) == 0 {
		return raw, nil
	}
	var shadows map[uint32]codec.CellState
	out := make(map[uint32]codec.CellState, len(raw))
	for id, st := range raw {
		if c, ok := bridgeShadowOf(id, cols); ok {
			if shadows == nil {
				shadows = make(map[uint32]codec.CellState)
			}
			shadows[c] = st
			continue
		}
		out[id] = st
	}
	if len(shadows) == 0 {
		return out, nil
	}
	for c, sh := range shadows {
		set, v, err := decodeBridgeShadow(sh, lim)
		if err != nil {
			return nil, err
		}
		if !set {
			continue
		}
		if cur, ok := out[c]; ok {
			cur.Value = v
			out[c] = cur
			continue
		}
		out[c] = codec.CellState{Version: sh.Version, Value: v}
	}
	return out, nil
}

// bridgeRowShadowed reports whether any field of a raw row carries a
// set-shadow. Callers suppress Low tombstones while true.
func bridgeRowShadowed(raw map[uint32]codec.CellState, cols map[uint32]bool, lim codec.Limits) (bool, error) {
	for id, st := range raw {
		if _, ok := bridgeShadowOf(id, cols); !ok {
			continue
		}
		set, _, err := decodeBridgeShadow(st, lim)
		if err != nil {
			return false, err
		}
		if set {
			return true, nil
		}
	}
	return false, nil
}

// bridgeColumnsFor returns the app column set of a materialized table: the
// schema registry for app tables, the static file IDs for the reserved file
// table, nil for anything else (policy table, unknown tables) so resolution
// passes them through untouched.
func (db *DB) bridgeColumnsFor(table uint32) map[uint32]bool {
	if db.files != nil || table == bridgeFileTableID() {
		if fids, err := resolveFileIDs(); err == nil && fids.table == table {
			return map[uint32]bool{fids.id: true, fids.name: true, fids.digest: true, fids.size: true}
		}
	}
	reg := db.schemaRegistry()
	if reg == nil {
		return nil
	}
	t := reg.TableByID(table)
	if t == nil {
		return nil
	}
	cols := make(map[uint32]bool, len(t.Columns))
	for _, c := range t.Columns {
		cols[c.ID] = true
	}
	return cols
}

// bridgeFileTableID resolves the reserved file table ID without requiring
// local files (a High without files still resolves file rows delivered by
// mesh or snapshot).
func bridgeFileTableID() uint32 {
	fids, err := resolveFileIDs()
	if err != nil {
		return 0
	}
	return fids.table
}

// bridgeShadowLimits sizes shadow-payload decoding: inner values obey the
// replicated cap plus envelope slack.
func (db *DB) bridgeShadowLimits() codec.Limits {
	maxV := db.cfg.MaxReplicatedValueBytes + 4096
	if maxV < 16<<20 {
		maxV = 16 << 20
	}
	return codec.Limits{MaxValueBytes: maxV, MaxMutations: 1, MaxTransactionBytes: int64(maxV)}
}

// bridgeFieldShadowed reports whether one field currently resolves High
// (a set-shadow wins its shadow column). Absent or cleared shadows resolve
// Low. Malformed shadows error (fail closed).
func (db *DB) bridgeFieldShadowed(table uint32, row ids.RowID, column uint32) (bool, error) {
	s, ok := bridgeShadowColumn(column)
	if !ok {
		return false, nil
	}
	cell, present, err := db.store.GetCell(table, row, s)
	if err != nil || !present {
		return false, err
	}
	set, _, err := decodeBridgeShadow(cell, db.bridgeShadowLimits())
	if err != nil {
		return false, err
	}
	return set, nil
}

// batchTouchesBridgePolicy reports whether a mutation batch carries policy
// or shadow markers. Policy mutations pair with every import/release/accept
// commit and shadow mutations pair with every High write to an imported
// field, so a marker-free batch cannot change effective bridged state and
// skips winner resolution (the remote-apply hot path stays a CPU-only scan
// for non-bridge traffic).
func batchTouchesBridgePolicy(db *DB, mutations []codec.Mutation) bool {
	for i := range mutations {
		m := &mutations[i]
		if m.TableID == BridgePolicyTableID {
			return true
		}
		if m.IsTombstone() {
			continue
		}
		cols := db.bridgeColumnsFor(m.TableID)
		if cols == nil {
			continue
		}
		if _, ok := bridgeShadowOf(m.ColumnID, cols); ok {
			return true
		}
	}
	return false
}

// resolveBridgeWinners translates merged winners into effective winners for
// the materializer: shadow columns strip out, shadowed fields substitute
// their High bytes, tombstones over shadowed rows drop (with a full-row
// re-synthesis so SQL converges to effective state), and lone shadow
// winners synthesize their field's effective value (a late override whose
// value lost LWW still flips SQL to the High bytes; a clear flips back to
// the Low bytes). Non-bridge rows pass through unchanged.
func resolveBridgeWinners(db *DB, winners []state.WinningChange) ([]state.WinningChange, error) {
	lim := db.bridgeShadowLimits()
	groups := make(map[struct {
		table uint32
		row   ids.RowID
	}][]int)
	var order []struct {
		table uint32
		row   ids.RowID
	}
	for i, w := range winners {
		if w.TableID == BridgePolicyTableID {
			continue // hidden policy: never materialized
		}
		k := struct {
			table uint32
			row   ids.RowID
		}{w.TableID, w.RowID}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], i)
	}
	out := make([]state.WinningChange, 0, len(winners))
	type cellKey struct {
		table uint32
		row   ids.RowID
		col   uint32
		tomb  bool
	}
	synth := make(map[cellKey]bool)
	emit := func(w state.WinningChange) {
		k := cellKey{table: w.TableID, row: w.RowID, col: w.ColumnID, tomb: w.Tombstone}
		if synth[k] {
			return
		}
		synth[k] = true
		out = append(out, w)
	}
	for _, k := range order {
		cols := db.bridgeColumnsFor(k.table)
		if cols == nil {
			for _, i := range groups[k] {
				emit(winners[i])
			}
			continue
		}
		raw, err := db.store.GetRow(k.table, k.row)
		if err != nil {
			return nil, err
		}
		eff, err := resolveBridgeRow(raw, cols, lim)
		if err != nil {
			return nil, err
		}
		shadowed, err := bridgeRowShadowed(raw, cols, lim)
		if err != nil {
			return nil, err
		}
		var synthCols map[uint32]bool
		tomb := false
		for _, i := range groups[k] {
			w := winners[i]
			if w.Tombstone {
				tomb = true
				continue
			}
			if c, ok := bridgeShadowOf(w.ColumnID, cols); ok {
				if synthCols == nil {
					synthCols = make(map[uint32]bool)
				}
				synthCols[c] = true
				continue
			}
		}
		// A shadow change can flip tombstone effectiveness (a clear lets
		// a stored Low tombstone hide the row again): re-drive the stored
		// tombstone instead of synthesizing values the tomb hides.
		if !shadowed && (tomb || len(synthCols) > 0) {
			storedTomb, hasTomb, terr := db.store.GetTombstone(k.table, k.row)
			if terr != nil {
				return nil, terr
			}
			if hasTomb {
				var newest crdt.Version
				for _, st := range eff {
					if crdt.CompareVersion(st.Version, newest) > 0 {
						newest = st.Version
					}
				}
				if crdt.CompareVersion(storedTomb, newest) > 0 {
					emit(state.WinningChange{TableID: k.table, RowID: k.row, Tombstone: true, Version: storedTomb})
					continue
				}
			}
		}
		for _, i := range groups[k] {
			w := winners[i]
			if w.Tombstone {
				continue
			}
			if _, ok := bridgeShadowOf(w.ColumnID, cols); ok {
				continue
			}
			if st, ok := eff[w.ColumnID]; ok {
				w.Value = st.Value
				w.Version = st.Version
			}
			emit(w)
		}
		switch {
		case tomb && shadowed:
			// A Low tombstone cannot hide a High-owned row: drop it and
			// re-assert every effective cell so SQL converges even when
			// the row was missing or showed pre-shadow Low bytes.
			for col, st := range eff {
				if !cols[col] {
					continue
				}
				emit(state.WinningChange{TableID: k.table, RowID: k.row, ColumnID: col, Value: st.Value, Version: st.Version})
			}
		case tomb:
			for _, i := range groups[k] {
				if winners[i].Tombstone {
					emit(winners[i])
				}
			}
		}
		for c := range synthCols {
			st, ok := eff[c]
			if !ok {
				continue
			}
			emit(state.WinningChange{TableID: k.table, RowID: k.row, ColumnID: c, Value: st.Value, Version: st.Version})
		}
	}
	return out, nil
}

// shadowResolvingReader decorates durable state with effective bridged
// values for the materializer (rebuild, repair, apply readbacks). It strips
// shadow columns, substitutes High bytes, and suppresses tombstones over
// shadowed rows. Non-bridge rows cost a column-membership scan only.
type shadowResolvingReader struct {
	db   *DB
	cols map[uint32]map[uint32]bool
}

func (db *DB) shadowReader() *shadowResolvingReader {
	return &shadowResolvingReader{db: db, cols: make(map[uint32]map[uint32]bool)}
}

// BridgeStoredRowExists reports whether the authoritative state materializes a
// live row, without consulting either query engine. Bridge import uses this
// before acquiring its write coordinator, so the check cannot deadlock on a
// query-engine reader held behind that coordinator.
func (db *DB) BridgeStoredRowExists(table string, key ids.RowID) (bool, error) {
	if db == nil {
		return false, ErrClosed
	}
	if err := db.requireRead(); err != nil {
		return false, err
	}
	definition := db.reg.Table(table)
	if definition == nil {
		return false, fmt.Errorf("murmur: bridge row table %q is not registered: %w", table, ErrUnsupportedSchema)
	}
	rows, err := db.store.GetRows([]state.RowRef{{Table: definition.ID, ID: key}})
	if err != nil {
		return false, err
	}
	if len(rows) != 1 || len(rows[0].Cells) == 0 {
		return false, nil
	}
	row, err := db.resolveTypedBridgeRow(rows[0])
	if err != nil {
		return false, err
	}
	if _, ok := row.Cells[definition.PK]; !ok {
		return false, nil
	}
	return !row.Tomb.Present || crdt.CompareVersion(row.Newest, row.Tomb.Version) > 0, nil
}

func (db *DB) resolveTypedBridgeRow(row *state.Row) (*state.Row, error) {
	if row == nil {
		return nil, fmt.Errorf("murmur: nil typed bridge row")
	}
	cols := db.bridgeColumnsFor(row.Table)
	if cols == nil {
		return row, nil
	}
	effective, err := resolveBridgeRow(row.Cells, cols, db.bridgeShadowLimits())
	if err != nil {
		return nil, err
	}
	shadowed, err := bridgeRowShadowed(row.Cells, cols, db.bridgeShadowLimits())
	if err != nil {
		return nil, err
	}
	resolved := &state.Row{Table: row.Table, ID: row.ID, Cells: effective, Tomb: row.Tomb}
	if shadowed {
		resolved.Tomb = crdt.TombstoneState{}
	}
	for _, cell := range effective {
		if crdt.CompareVersion(cell.Version, resolved.Newest) > 0 {
			resolved.Newest = cell.Version
		}
	}
	return resolved, nil
}

func (r *shadowResolvingReader) columns(table uint32) map[uint32]bool {
	if cols, ok := r.cols[table]; ok {
		return cols
	}
	cols := r.db.bridgeColumnsFor(table)
	r.cols[table] = cols
	return cols
}

// GetRow implements state.Reader.
func (r *shadowResolvingReader) GetRow(table uint32, row ids.RowID) (map[uint32]codec.CellState, error) {
	raw, err := r.db.store.GetRow(table, row)
	if err != nil {
		return nil, err
	}
	cols := r.columns(table)
	if cols == nil {
		return raw, nil
	}
	return resolveBridgeRow(raw, cols, r.db.bridgeShadowLimits())
}

// GetTombstone implements state.Reader.
func (r *shadowResolvingReader) GetTombstone(table uint32, row ids.RowID) (crdt.Version, bool, error) {
	tomb, present, err := r.db.store.GetTombstone(table, row)
	if err != nil || !present {
		return tomb, present, err
	}
	cols := r.columns(table)
	if cols == nil {
		return tomb, true, nil
	}
	raw, err := r.db.store.GetRow(table, row)
	if err != nil {
		return crdt.Version{}, false, err
	}
	shadowed, err := bridgeRowShadowed(raw, cols, r.db.bridgeShadowLimits())
	if err != nil {
		return crdt.Version{}, false, err
	}
	if shadowed {
		return crdt.Version{}, false, nil
	}
	return tomb, true, nil
}

// IterateTable implements state.Reader.
func (r *shadowResolvingReader) IterateTable(tableID uint32, fn func(*state.Row) error) error {
	cols := r.columns(tableID)
	if cols == nil {
		return r.db.store.IterateTable(tableID, fn)
	}
	lim := r.db.bridgeShadowLimits()
	return r.db.store.IterateTable(tableID, func(row *state.Row) error {
		eff, err := resolveBridgeRow(row.Cells, cols, lim)
		if err != nil {
			return err
		}
		shadowed, err := bridgeRowShadowed(row.Cells, cols, lim)
		if err != nil {
			return err
		}
		resolved := &state.Row{Table: row.Table, ID: row.ID, Cells: eff, Tomb: row.Tomb}
		var newest crdt.Version
		for _, st := range eff {
			if crdt.CompareVersion(st.Version, newest) > 0 {
				newest = st.Version
			}
		}
		resolved.Newest = newest
		if shadowed {
			resolved.Tomb = crdt.TombstoneState{}
		}
		return fn(resolved)
	})
}

// fileColumnsFor resolves the static file column set (or nil).
func fileColumnsFor() map[uint32]bool {
	fids, err := resolveFileIDs()
	if err != nil {
		return nil
	}
	return map[uint32]bool{fids.id: true, fids.name: true, fids.digest: true, fids.size: true}
}

// effectiveFileCells resolves raw file metadata cells to effective cells:
// shadow columns strip out and set-shadows substitute their High bytes.
func effectiveFileCells(db *DB, raw map[uint32]codec.CellState) (map[uint32]codec.CellState, error) {
	cols := fileColumnsFor()
	if cols == nil {
		return raw, nil
	}
	return resolveBridgeRow(raw, cols, db.bridgeShadowLimits())
}

// effectiveFileVisible reports row visibility over effective cells: newest
// is computed post-resolution (shadow versions must not leak into it) and
// a Low tombstone cannot hide a shadowed row.
func effectiveFileVisible(db *DB, raw map[uint32]codec.CellState, tomb crdt.Version, hasTomb bool) (map[uint32]codec.CellState, bool, error) {
	eff, err := effectiveFileCells(db, raw)
	if err != nil {
		return nil, false, err
	}
	if len(eff) == 0 {
		return eff, false, nil
	}
	cols := fileColumnsFor()
	shadowed := false
	if cols != nil {
		shadowed, err = bridgeRowShadowed(raw, cols, db.bridgeShadowLimits())
		if err != nil {
			return nil, false, err
		}
	}
	var newest crdt.Version
	for _, st := range eff {
		if crdt.CompareVersion(st.Version, newest) > 0 {
			newest = st.Version
		}
	}
	if shadowed {
		return eff, true, nil
	}
	var tombState crdt.TombstoneState
	if hasTomb {
		tombState = crdt.TombstoneState{Present: true, Version: tomb}
	}
	return eff, crdt.Visible(true, newest, tombState), nil
}
