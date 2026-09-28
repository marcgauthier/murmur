//go:build !lumosql

package sqlengine

import (
	"database/sql"
	"fmt"
	"strings"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/nomadsql/replicateddb/codec"
)

// preupdateRegistrar matches modernc's *conn hook registration.
type preupdateRegistrar interface {
	RegisterPreUpdateHook(sqlite.PreUpdateHookFn)
}

// registerPreUpdateHook installs capture on the write connection. It fails
// when the hook is unavailable so the build/test suite cannot silently run
// without change capture.
func (e *Engine) registerPreUpdateHook(conn *sql.Conn) error {
	installed := false
	err := conn.Raw(func(dc any) error {
		reg, ok := dc.(preupdateRegistrar)
		if !ok {
			return fmt.Errorf("sqlengine: driver conn %T lacks RegisterPreUpdateHook", dc)
		}
		reg.RegisterPreUpdateHook(e.onPreUpdate)
		installed = true
		return nil
	})
	if err != nil {
		return err
	}
	if !installed {
		return fmt.Errorf("sqlengine: pre-update hook not installed")
	}
	return nil
}

// onPreUpdate is the sqlite3_preupdate_hook callback. It runs on the
// connection's call path while e.wmu (or the caller's write section) is
// held, so capMu only guards mode/pending against SetCaptureMode.
func (e *Engine) onPreUpdate(d sqlite.SQLitePreUpdateData) {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	if e.mode != CaptureLocal || !e.inTx {
		return
	}
	table := e.tables[strings.ToLower(d.TableName)]
	if table == nil {
		return // local-only object (index, FTS shadow, temp): never replicate
	}
	var op OpType
	switch d.Op {
	case sqlite3.SQLITE_INSERT:
		op = OpInsert
	case sqlite3.SQLITE_UPDATE:
		op = OpUpdate
	case sqlite3.SQLITE_DELETE:
		op = OpDelete
	default:
		return
	}
	ncols := d.Count()
	if ncols != len(table.Columns) {
		// Schema drift inside the engine: record nothing; validation at
		// open plus epoch gating make this unreachable in practice.
		return
	}
	ch := RawChange{Table: table, Op: op}
	if op != OpInsert {
		dest := make([]any, ncols)
		if err := d.Old(dest...); err != nil {
			return
		}
		old, err := anyRowToValues(dest)
		if err != nil {
			return
		}
		ch.Old = old
	}
	if op != OpDelete {
		dest := make([]any, ncols)
		if err := d.New(dest...); err != nil {
			return
		}
		newVals, err := anyRowToValues(dest)
		if err != nil {
			return
		}
		ch.New = newVals
	}
	e.pending = append(e.pending, ch)
}

func anyRowToValues(row []any) ([]codec.Value, error) {
	out := make([]codec.Value, len(row))
	for i, v := range row {
		cv, err := codec.FromAny(v)
		if err != nil {
			return nil, err
		}
		out[i] = cv
	}
	return out, nil
}

// SetCaptureMode switches capture (echo prevention during apply/rebuild).
func (e *Engine) SetCaptureMode(m CaptureMode) {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	e.mode = m
}

// CaptureMode returns the current mode.
func (e *Engine) CaptureMode() CaptureMode {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	return e.mode
}

// beginCapture starts collecting for a new SQL transaction.
func (e *Engine) beginCapture() {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	e.inTx = true
	e.pending = e.pending[:0]
}

// finishCapture returns the collected events and resets collection.
func (e *Engine) finishCapture() []RawChange {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	e.inTx = false
	ev := e.pending
	e.pending = nil
	return ev
}

// resetCapture discards collection (after rollback or failed commit).
func (e *Engine) resetCapture() {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	e.inTx = false
	e.pending = e.pending[:0]
}
