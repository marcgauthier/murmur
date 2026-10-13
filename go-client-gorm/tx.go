package murmurgorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"

	murmur "github.com/marcgauthier/murmur/go-client"
)

// errTxRollback is the sentinel a pump callback returns to roll back. It never
// escapes to callers; Rollback maps it to nil.
var errTxRollback = errors.New("murmurgorm: rollback")

type txOp int

const (
	txOpExec txOp = iota
	txOpQuery
	txOpCommit
	txOpRollback
)

// txRequest is one statement shuttled into the pump. The pump owns the native
// *murmur.Sql scope; callers never touch it. resp is buffered so the pump
// never blocks answering. Commit/rollback requests carry no resp waiter: the
// pump answers by exiting, which Commit/Rollback observe via done.
type txRequest struct {
	op    txOp
	query string
	args  []any
	resp  chan txResponse
}

type txResponse struct {
	rowsAffected int64
	lastInsertID int64
	rows         *bufferedRows
	err          error
}

// driverTx is a driver.Tx implemented as a pump: one goroutine sits inside a
// Murmur Transaction callback and executes shuttled statements until commit
// (callback returns nil) or rollback (callback returns errTxRollback).
// SAVEPOINT statements from GORM nested transactions are plain SQL in the same
// native transaction, so they work unchanged.
type driverTx struct {
	c   *conn
	req chan txRequest
	// done is closed when the pump exits; exitErr holds the cause, stored
	// before the close so receivers can read it race-free.
	done    chan struct{}
	mu      sync.Mutex
	exitErr error
	// finished guards Commit/Rollback against double completion.
	finished bool
}

// beginPump opens a native transaction and blocks until its callback is
// entered (pump alive) or the open fails.
func beginPump(ctx context.Context, c *conn, db *murmur.DB) (*driverTx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t := &driverTx{c: c, req: make(chan txRequest), done: make(chan struct{})}
	ready := make(chan struct{})
	go func() {
		err := db.Transaction(func(s *murmur.Sql) error {
			close(ready)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case r := <-t.req:
					switch r.op {
					case txOpCommit:
						return nil
					case txOpRollback:
						return errTxRollback
					default:
						r.resp <- t.runStatement(s, r)
					}
				}
			}
		})
		t.mu.Lock()
		t.exitErr = err
		t.mu.Unlock()
		close(t.done)
	}()
	select {
	case <-ready:
		return t, nil
	case <-t.done:
		return nil, t.exitCause()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// runStatement executes one shuttled request on the pump thread. Statement
// errors are per-request: the transaction stays open, matching SQLite
// semantics where a failed statement does not abort the transaction.
func (t *driverTx) runStatement(s *murmur.Sql, r txRequest) (resp txResponse) {
	defer func() {
		// A panic here would otherwise unwind the native callback and kill
		// the transaction as CallbackPanicked; report it per-request instead.
		if p := recover(); p != nil {
			resp = txResponse{err: errTxPanicked(p)}
		}
	}()
	switch r.op {
	case txOpExec:
		out, err := s.Exec(r.query, r.args...)
		return txResponse{rowsAffected: out.RowsAffected, lastInsertID: out.LastInsertRowID, err: err}
	case txOpQuery:
		out := &bufferedRows{}
		err := bufferQuery(s, r.query, r.args, out)
		return txResponse{rows: out, err: err}
	default:
		return txResponse{err: errTxBadOp(r.op)}
	}
}

// send shuttles one statement to a live pump. If the pump already exited
// (commit, rollback, context cancel), the caller gets ErrTxDone instead of
// blocking forever on an unreceived channel.
func (t *driverTx) send(ctx context.Context, r txRequest) txResponse {
	select {
	case t.req <- r:
	case <-t.done:
		return txResponse{err: sql.ErrTxDone}
	case <-ctx.Done():
		return txResponse{err: ctx.Err()}
	}
	select {
	case resp := <-r.resp:
		return resp
	case <-t.done:
		// Pump died after receiving (context cancel racing the statement).
		return txResponse{err: sql.ErrTxDone}
	case <-ctx.Done():
		return txResponse{err: ctx.Err()}
	}
}

func (t *driverTx) exec(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	vals, err := namedArgsToPositional(args)
	if err != nil {
		return nil, err
	}
	resp := t.send(ctx, txRequest{op: txOpExec, query: query, args: vals, resp: make(chan txResponse, 1)})
	if resp.err != nil {
		return nil, resp.err
	}
	return result{rowsAffected: resp.rowsAffected, lastInsertID: resp.lastInsertID}, nil
}

func (t *driverTx) query(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	vals, err := namedArgsToPositional(args)
	if err != nil {
		return nil, err
	}
	resp := t.send(ctx, txRequest{op: txOpQuery, query: query, args: vals, resp: make(chan txResponse, 1)})
	if resp.err != nil {
		return nil, resp.err
	}
	return resp.rows, nil
}

func (t *driverTx) markFinished() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return false
	}
	t.finished = true
	return true
}

func (t *driverTx) exitCause() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.exitErr
}

// finish delivers a commit/rollback request, or no-ops if the pump already
// exited (context cancel won the race).
func (t *driverTx) finish(op txOp) {
	select {
	case t.req <- txRequest{op: op}:
	case <-t.done:
	}
}

// Commit ends the pump with success. A second Commit reports sql.ErrTxDone.
func (t *driverTx) Commit() error {
	if !t.markFinished() {
		return sql.ErrTxDone
	}
	defer t.c.detachTx(t)
	t.finish(txOpCommit)
	<-t.done
	err := t.exitCause()
	if err == nil {
		return nil
	}
	if errors.Is(err, errTxRollback) {
		return sql.ErrTxDone
	}
	return err
}

// Rollback ends the pump, rolling back. It is idempotent: rolling back an
// already-finished transaction (including one killed by context cancel) is nil.
func (t *driverTx) Rollback() error {
	if !t.markFinished() {
		return nil
	}
	defer t.c.detachTx(t)
	t.finish(txOpRollback)
	<-t.done
	err := t.exitCause()
	if err == nil || errors.Is(err, errTxRollback) {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil // already rolled back by context cancel
	}
	return err
}
