//go:build !modernc && sqlite_preupdate_hook

package sqlengine

import (
	"database/sql"
	"fmt"
	"strings"

	sqlite "github.com/mattn/go-sqlite3"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/schema"
)

type preupdateRegistrar interface {
	RegisterPreUpdateHook(func(sqlite.SQLitePreUpdateData))
}

func (e *Engine) registerPreUpdateHook(conn *sql.Conn) error {
	installed := false
	err := conn.Raw(func(dc any) error {
		reg, ok := dc.(preupdateRegistrar)
		if !ok {
			return fmt.Errorf("sqlengine: mattn SQLite conn %T lacks RegisterPreUpdateHook; build with sqlite_preupdate_hook", dc)
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

func (e *Engine) onPreUpdate(d sqlite.SQLitePreUpdateData) {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	if e.mode != CaptureLocal || !e.inTx {
		return
	}
	table := e.tables[strings.ToLower(d.TableName)]
	if table == nil {
		return
	}
	var op OpType
	switch d.Op {
	case sqlite.SQLITE_INSERT:
		op = OpInsert
	case sqlite.SQLITE_UPDATE:
		op = OpUpdate
	case sqlite.SQLITE_DELETE:
		op = OpDelete
	default:
		return
	}
	ncols := d.Count()
	if ncols != len(table.Columns) {
		return
	}
	change := RawChange{Table: table, Op: op}
	if op != OpInsert {
		old := make([]any, ncols)
		if err := d.Old(old...); err != nil {
			return
		}
		values, err := mattnRowToValues(old, table)
		if err != nil {
			return
		}
		change.Old = values
	}
	if op != OpDelete {
		newValues := make([]any, ncols)
		if err := d.New(newValues...); err != nil {
			return
		}
		values, err := mattnRowToValues(newValues, table)
		if err != nil {
			return
		}
		change.New = values
	}
	e.pending = append(e.pending, change)
}

// mattn/go-sqlite3 exposes SQLite TEXT values from pre-update callbacks as
// []byte, unlike database/sql row scans. Restore the declared schema type
// before encoding so text does not become a replicated BLOB.
func mattnRowToValues(row []any, table *schema.TableSchema) ([]codec.Value, error) {
	if len(row) != len(table.Columns) {
		return nil, fmt.Errorf("sqlengine: mattn SQLite pre-update column count mismatch")
	}
	values := make([]codec.Value, len(row))
	for i, raw := range row {
		if table.Columns[i].Type == schema.ColText {
			if b, ok := raw.([]byte); ok {
				raw = string(b)
			}
		}
		value, err := codec.FromAny(raw)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func anyRowToValues(row []any) ([]codec.Value, error) {
	out := make([]codec.Value, len(row))
	for i, v := range row {
		value, err := codec.FromAny(v)
		if err != nil {
			return nil, err
		}
		out[i] = value
	}
	return out, nil
}

func (e *Engine) SetCaptureMode(m CaptureMode) {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	e.mode = m
}

func (e *Engine) CaptureMode() CaptureMode {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	return e.mode
}

func (e *Engine) beginCapture() {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	e.inTx = true
	e.pending = e.pending[:0]
}

func (e *Engine) finishCapture() []RawChange {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	e.inTx = false
	out := append([]RawChange(nil), e.pending...)
	e.pending = e.pending[:0]
	return out
}

func (e *Engine) resetCapture() {
	e.capMu.Lock()
	defer e.capMu.Unlock()
	e.inTx = false
	e.pending = e.pending[:0]
}
