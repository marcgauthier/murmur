//go:build !modernc && !sqlite_preupdate_hook

package sqlengine

import (
	"database/sql"
	"fmt"
)

func (e *Engine) registerPreUpdateHook(conn *sql.Conn) error {
	return fmt.Errorf("sqlengine: mattn SQLite requires build tag sqlite_preupdate_hook for replication capture (or compile with -tags modernc)")
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
