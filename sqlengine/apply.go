package sqlengine

import (
	"context"
	"fmt"
	"strings"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/crdt"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/schema"
	"github.com/nomadsql/replicateddb/state"
)

// StateReader is the durable state needed for rebuild and apply.
type StateReader interface {
	IterateTable(tableID uint32, fn func(*state.Row) error) error
	GetRow(table uint32, row ids.RowID) (map[uint32]codec.CellState, error)
	GetTombstone(table uint32, row ids.RowID) (crdt.Version, bool, error)
}

// ApplyWinners materializes remotely-won changes. Capture is suppressed so
// remote apply never generates new local replication events (echo prevention).
//
// For rows missing from the query database, the full row is re-read from
// durable state so partial winner sets (resurrections, snapshot chunks)
// always produce complete, constraint-valid rows.
func (e *Engine) ApplyWinners(src StateReader, winners []state.WinningChange) error {
	if len(winners) == 0 {
		return nil
	}
	return e.WriteSection(func(ctx context.Context) error {
		e.SetCaptureMode(CaptureSuppressed)
		defer e.SetCaptureMode(CaptureLocal)
		if _, err := e.write.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
			return err
		}
		commit := false
		defer func() {
			if !commit {
				_, _ = e.write.ExecContext(context.Background(), "ROLLBACK")
			}
		}()
		// Group by row to minimize existence checks.
		type rowGroup struct {
			tableID uint32
			row     ids.RowID
			tomb    bool
			tombVer crdt.Version
			cells   map[uint32]codec.Value
			newest  crdt.Version
		}
		groups := make(map[RowKey]*rowGroup)
		var order []RowKey
		for _, w := range winners {
			k := RowKey{TableID: w.TableID, RowID: w.RowID}
			g, ok := groups[k]
			if !ok {
				g = &rowGroup{tableID: w.TableID, row: w.RowID, cells: make(map[uint32]codec.Value)}
				groups[k] = g
				order = append(order, k)
			}
			if w.Tombstone {
				g.tomb = true
				if crdt.CompareVersion(w.Version, g.tombVer) > 0 {
					g.tombVer = w.Version
				}
				continue
			}
			g.cells[w.ColumnID] = w.Value
			if crdt.CompareVersion(w.Version, g.newest) > 0 {
				g.newest = w.Version
			}
		}
		// Batched existence probe: one SELECT ... WHERE pk IN (...) per
		// table (chunked) instead of one SELECT per row.
		byTable := make(map[uint32][]ids.RowID)
		for _, k := range order {
			// Tomb-only groups only delete (no probe needed); groups with
			// cells may fall through to insert/update and need probing.
			if g := groups[k]; !g.tomb || len(g.cells) > 0 {
				byTable[k.TableID] = append(byTable[k.TableID], k.RowID)
			}
		}
		exists := make(map[RowKey]bool, len(order))
		for tableID, rows := range byTable {
			table := e.reg.TableByID(tableID)
			if table == nil {
				continue // unknown table: unmaterializable, skipped below
			}
			found, err := e.rowsExist(ctx, table, rows)
			if err != nil {
				return err
			}
			for _, r := range found {
				exists[RowKey{TableID: tableID, RowID: r}] = true
			}
		}
		for _, k := range order {
			g := groups[k]
			table := e.reg.TableByID(g.tableID)
			if table == nil {
				continue // unknown table (corrupt batch): skip, stay converged
			}
			if g.tomb {
				// A tombstone winner deletes only when it actually hides
				// the row: a stale tomb (older than the newest cell) must
				// not remove a visible row.
				stored, err := src.GetRow(g.tableID, g.row)
				if err != nil {
					return err
				}
				newest := newestOfCells(stored)
				if crdt.CompareVersion(g.newest, newest) > 0 {
					newest = g.newest
				}
				if crdt.CompareVersion(g.tombVer, newest) > 0 {
					if err := e.deleteRow(ctx, table, g.row); err != nil {
						return err
					}
					continue
				}
				if len(g.cells) == 0 {
					continue // stale tomb; row stays as-is
				}
				// else: cells beat the tomb; fall through to insert/update.
			}
			if !exists[k] {
				// A row missing from SQL normally has no stored cells, so
				// every incoming cell won and the winners carry the full
				// row: insert directly. Only a partial winner set (e.g., a
				// resurrection update over older stored cells) needs a
				// durable readback. Either way the row stays deleted when
				// a newer tombstone hides it.
				if coversAllColumns(table, g.cells) {
					hidden, err := tombHidden(src, g.tableID, g.row, g.newest)
					if err != nil {
						return err
					}
					if hidden {
						continue
					}
					if err := e.insertWinnerRow(ctx, table, g.row, g.cells); err != nil {
						return err
					}
					continue
				}
				cells, err := src.GetRow(g.tableID, g.row)
				if err != nil {
					return err
				}
				if !rowMaterializable(table, cells) {
					continue // unmaterializable (partial/corrupt batch): skip, stay up
				}
				hidden, err := tombHidden(src, g.tableID, g.row, newestOfCells(cells))
				if err != nil {
					return err
				}
				if hidden {
					continue
				}
				if err := e.insertFullRow(ctx, table, g.row, cells); err != nil {
					return err
				}
				continue
			}
			for colID, v := range g.cells {
				if table.ColumnByID(colID) == nil {
					continue // unknown column (corrupt batch): skip
				}
				if err := e.updateCell(ctx, table, g.row, colID, v); err != nil {
					return err
				}
			}
		}
		if _, err := e.write.ExecContext(ctx, "COMMIT"); err != nil {
			return fmt.Errorf("sqlengine: apply commit: %w", err)
		}
		commit = true
		return nil
	})
}

// RepairRows re-converges listed rows with durable state after a local
// commit raced remote applies: a concurrent remote tombstone may have
// deleted a just-written row from SQL (or hidden it while SQL still shows
// it). For each row, SQL presence is reconciled with durable (Pebble) visibility.
// Capture is suppressed: everything repaired is already durable.
func (e *Engine) RepairRows(src StateReader, rows []RowKey) error {
	if len(rows) == 0 {
		return nil
	}
	return e.WriteSection(func(ctx context.Context) error {
		e.SetCaptureMode(CaptureSuppressed)
		defer e.SetCaptureMode(CaptureLocal)
		byTable := make(map[uint32][]ids.RowID)
		for _, k := range rows {
			byTable[k.TableID] = append(byTable[k.TableID], k.RowID)
		}
		present := make(map[RowKey]bool, len(rows))
		for tableID, rws := range byTable {
			table := e.reg.TableByID(tableID)
			if table == nil {
				continue
			}
			found, err := e.rowsExist(ctx, table, rws)
			if err != nil {
				return err
			}
			for _, r := range found {
				present[RowKey{TableID: tableID, RowID: r}] = true
			}
		}
		if _, err := e.write.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
			return err
		}
		commit := false
		defer func() {
			if !commit {
				_, _ = e.write.ExecContext(context.Background(), "ROLLBACK")
			}
		}()
		for _, k := range rows {
			table := e.reg.TableByID(k.TableID)
			if table == nil {
				continue
			}
			cells, err := src.GetRow(k.TableID, k.RowID)
			if err != nil {
				return err
			}
			tomb, hasTomb, err := src.GetTombstone(k.TableID, k.RowID)
			if err != nil {
				return err
			}
			visible := crdt.Visible(len(cells) > 0, newestOfCells(cells),
				crdt.TombstoneState{Present: hasTomb, Version: tomb})
			switch {
			case visible && !present[k]:
				if !rowMaterializable(table, cells) {
					continue
				}
				if err := e.insertFullRow(ctx, table, k.RowID, cells); err != nil {
					return err
				}
			case !visible && present[k]:
				if err := e.deleteRow(ctx, table, k.RowID); err != nil {
					return err
				}
			}
		}
		if _, err := e.write.ExecContext(ctx, "COMMIT"); err != nil {
			return fmt.Errorf("sqlengine: repair commit: %w", err)
		}
		commit = true
		return nil
	})
}

// Rebuild drops and recreates the query database from durable state, then
// re-applies local-only objects (indexes, FTS). Capture is suppressed.
func (e *Engine) Rebuild(src StateReader) error {
	return e.WriteSection(func(ctx context.Context) error {
		e.SetCaptureMode(CaptureSuppressed)
		defer e.SetCaptureMode(CaptureLocal)
		// DROP invalidates prepared statements; purge caches first.
		e.writeStmts.invalidate()
		e.readStmts.invalidate()
		for _, t := range e.reg.Tables {
			if _, err := e.write.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdent(t.Name)); err != nil {
				return fmt.Errorf("sqlengine: drop %s: %w", t.Name, err)
			}
		}
		if err := e.createSchema(ctx, e.ddl, nil); err != nil {
			return err
		}
		if err := e.validateOrdinals(ctx); err != nil {
			return err
		}
		for _, t := range e.reg.Tables {
			if err := e.rebuildTable(ctx, src, t); err != nil {
				return err
			}
		}
		for _, s := range e.localDDL {
			if _, err := e.write.ExecContext(ctx, s); err != nil {
				return fmt.Errorf("sqlengine: local schema %q: %w", trunc(s, 120), err)
			}
		}
		return nil
	})
}

func (e *Engine) rebuildTable(ctx context.Context, src StateReader, t *schema.TableSchema) error {
	insertSQL := fullInsertSQL(t)
	stmt, err := e.writeStmts.prepare(ctx, e.write, insertSQL)
	if err != nil {
		return err
	}
	const rowsPerTxn = 5000
	inTxn := false
	n := 0
	flush := func() error {
		if !inTxn {
			return nil
		}
		inTxn = false
		if _, err := e.write.ExecContext(ctx, "COMMIT"); err != nil {
			_, _ = e.write.ExecContext(context.Background(), "ROLLBACK")
			return fmt.Errorf("sqlengine: rebuild commit: %w", err)
		}
		return nil
	}
	begin := func() error {
		if _, err := e.write.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
			return err
		}
		inTxn = true
		return nil
	}
	err = src.IterateTable(t.ID, func(r *state.Row) error {
		if !r.Visible() {
			return nil
		}
		if !inTxn {
			if err := begin(); err != nil {
				return err
			}
		}
		if !rowMaterializable(t, r.Cells) {
			return nil // unmaterializable (partial/corrupt batch): skip, stay up
		}
		args := make([]any, len(t.Columns))
		for i, c := range t.Columns {
			st, ok := r.Cells[c.ID]
			if !ok {
				args[i] = nil
				continue
			}
			args[i] = st.Value.ToAny()
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("sqlengine: rebuild insert %s: %w", t.Name, err)
		}
		n++
		if n%rowsPerTxn == 0 {
			return flush()
		}
		return nil
	})
	if err != nil {
		_, _ = e.write.ExecContext(context.Background(), "ROLLBACK")
		return err
	}
	return flush()
}

// rowsExist probes row existence in chunks with one SELECT per chunk,
// returning the subset of rows present.
func (e *Engine) rowsExist(ctx context.Context, t *schema.TableSchema, rows []ids.RowID) ([]ids.RowID, error) {
	const chunk = 500
	pk := t.PKColumn()
	var found []ids.RowID
	for i := 0; i < len(rows); i += chunk {
		end := i + chunk
		if end > len(rows) {
			end = len(rows)
		}
		q := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
			quoteIdent(pk.Name), quoteIdent(t.Name), quoteIdent(pk.Name), placeholders(end-i))
		stmt, err := e.writeStmts.prepare(ctx, e.write, q)
		if err != nil {
			return nil, err
		}
		args := make([]any, 0, end-i)
		for _, r := range rows[i:end] {
			args = append(args, r[:])
		}
		rs, err := stmt.QueryContext(ctx, args...)
		if err != nil {
			return nil, err
		}
		for rs.Next() {
			var raw []byte
			if err := rs.Scan(&raw); err != nil {
				rs.Close()
				return nil, err
			}
			if len(raw) == 16 {
				var id ids.RowID
				copy(id[:], raw)
				found = append(found, id)
			}
		}
		if err := rs.Err(); err != nil {
			rs.Close()
			return nil, err
		}
		rs.Close()
	}
	return found, nil
}

func placeholders(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("?")
	}
	return sb.String()
}

// rowMaterializable reports whether stored cells can form a valid SQL row:
// the primary key and every NOT NULL column must be present. Rows that fail
// this (partial/corrupt batches, never well-formed local writes) are skipped
// by apply and rebuild so one bad batch cannot wedge the node. The cells
// stay in Pebble and materialize if completed later.
func rowMaterializable(t *schema.TableSchema, cells map[uint32]codec.CellState) bool {
	for _, c := range t.Columns {
		if c.ID == t.PK || !c.Nullable {
			if _, ok := cells[c.ID]; !ok {
				return false
			}
		}
	}
	return true
}

// tombHidden reports whether a stored tombstone strictly newer than newest
// hides the row.
func tombHidden(src StateReader, table uint32, row ids.RowID, newest crdt.Version) (bool, error) {
	tomb, present, err := src.GetTombstone(table, row)
	if err != nil {
		return false, err
	}
	return present && crdt.CompareVersion(tomb, newest) > 0, nil
}

// newestOfCells returns the newest cell version (zero when empty).
func newestOfCells(cells map[uint32]codec.CellState) crdt.Version {
	var newest crdt.Version
	for _, st := range cells {
		if crdt.CompareVersion(st.Version, newest) > 0 {
			newest = st.Version
		}
	}
	return newest
}

// coversAllColumns reports whether cells carry every registry column.
func coversAllColumns(t *schema.TableSchema, cells map[uint32]codec.Value) bool {
	if len(cells) < len(t.Columns) {
		return false
	}
	for _, c := range t.Columns {
		if _, ok := cells[c.ID]; !ok {
			return false
		}
	}
	return true
}

// insertWinnerRow inserts a row fully covered by winners (no readback).
func (e *Engine) insertWinnerRow(ctx context.Context, t *schema.TableSchema, row ids.RowID, cells map[uint32]codec.Value) error {
	stmt, err := e.writeStmts.prepare(ctx, e.write, fullInsertSQL(t))
	if err != nil {
		return err
	}
	args := make([]any, len(t.Columns))
	for i, c := range t.Columns {
		v, ok := cells[c.ID]
		if !ok {
			return fmt.Errorf("sqlengine: winner row %s missing column %s", row, c.Name)
		}
		args[i] = v.ToAny()
	}
	_, err = stmt.ExecContext(ctx, args...)
	return err
}

func (e *Engine) deleteRow(ctx context.Context, t *schema.TableSchema, row ids.RowID) error {
	pk := t.PKColumn()
	q := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdent(t.Name), quoteIdent(pk.Name))
	stmt, err := e.writeStmts.prepare(ctx, e.write, q)
	if err != nil {
		return err
	}
	_, err = stmt.ExecContext(ctx, row[:])
	return err
}

func (e *Engine) updateCell(ctx context.Context, t *schema.TableSchema, row ids.RowID, colID uint32, v codec.Value) error {
	pk := t.PKColumn()
	col := t.ColumnByID(colID)
	if col == nil {
		return fmt.Errorf("sqlengine: unknown column %d in table %s", colID, t.Name)
	}
	q := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?",
		quoteIdent(t.Name), quoteIdent(col.Name), quoteIdent(pk.Name))
	stmt, err := e.writeStmts.prepare(ctx, e.write, q)
	if err != nil {
		return err
	}
	_, err = stmt.ExecContext(ctx, v.ToAny(), row[:])
	return err
}

func (e *Engine) insertFullRow(ctx context.Context, t *schema.TableSchema, row ids.RowID, cells map[uint32]codec.CellState) error {
	stmt, err := e.writeStmts.prepare(ctx, e.write, fullInsertSQL(t))
	if err != nil {
		return err
	}
	args := make([]any, len(t.Columns))
	for i, c := range t.Columns {
		st, ok := cells[c.ID]
		if !ok {
			if !c.Nullable {
				return fmt.Errorf("sqlengine: apply row %s missing NOT NULL column %s", row, c.Name)
			}
			args[i] = nil
			continue
		}
		args[i] = st.Value.ToAny()
	}
	_, err = stmt.ExecContext(ctx, args...)
	return err
}

func fullInsertSQL(t *schema.TableSchema) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(quoteIdent(t.Name))
	sb.WriteString(" (")
	for i, c := range t.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdent(c.Name))
	}
	sb.WriteString(") VALUES (")
	for i := range t.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("?")
	}
	sb.WriteString(")")
	return sb.String()
}
