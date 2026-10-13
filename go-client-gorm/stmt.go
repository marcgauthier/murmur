package murmurgorm

import (
	"context"
	"database/sql/driver"
)

// stmt is a driver.Stmt capturing query text. Murmur statements cannot outlive
// their callback scope, so every Exec/Query re-prepares natively; correctness
// is unaffected, at one prepare per call.
type stmt struct {
	c     *conn
	query string
}

func (s *stmt) Close() error { return nil }

// NumInput returns -1: argument counts are validated by Murmur at bind time.
func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamed(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.c.ExecContext(ctx, s.query, args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamed(args))
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.c.QueryContext(ctx, s.query, args)
}

func valuesToNamed(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, v := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}
