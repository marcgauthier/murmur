package murmurgorm

import (
	"database/sql/driver"
	"fmt"
	"io"
)

// result is a driver.Result backed by Murmur's Exec outcome.
type result struct {
	rowsAffected int64
	lastInsertID int64
}

func (r result) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r result) RowsAffected() (int64, error) { return r.rowsAffected, nil }

// bufferedRows is a driver.Rows over a fully materialized result set. Murmur
// row cursors live only inside their Read/Transaction callback scope, so every
// query buffers its rows before the callback returns. Values are converted
// lazily per Next call; blobs are cloned per row.
type bufferedRows struct {
	columns []string
	rows    [][]driver.Value
	pos     int
	closed  bool
}

func (r *bufferedRows) Columns() []string {
	return append([]string(nil), r.columns...)
}

func (r *bufferedRows) Close() error {
	r.closed = true
	r.rows = nil
	return nil
}

func (r *bufferedRows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.pos]
	r.pos++
	if len(dest) < len(row) {
		return fmt.Errorf("murmurgorm: short destination buffer (%d < %d)", len(dest), len(row))
	}
	for i, v := range row {
		if b, ok := v.([]byte); ok {
			v = append([]byte(nil), b...)
		}
		dest[i] = v
	}
	return nil
}
