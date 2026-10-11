package murmur

import (
	"context"
	"github.com/marcgauthier/murmur/q"
	"github.com/marcgauthier/murmur/rime"
)

// ReadTx pins a consistent local record snapshot. Use it from one goroutine
// and close it promptly; it does not block writes.
type ReadTx = recordReadTx

// ReadTx opens a native read snapshot across all registered models.
func (db *DB) ReadTx(ctx context.Context) (*ReadTx, error) { return db.readTxContext(ctx) }

// Query selects records from the snapshot, including its original model bindings.
func (tx *recordReadTx) Query(model any, filters ...q.Matcher) *ItemQuery {
	query := &ItemQuery{limit: -1}
	if tx == nil || tx.done || tx.inner == nil {
		query.err = rime.ErrTxClosed
		return query
	}
	query.db, query.read, query.ctx = tx.db, tx, tx.ctx
	binding, err := resolveItemBindingFrom(model, tx.itemBindings, tx.itemAmbiguous)
	if err != nil {
		query.err = err
		return query
	}
	query.binding = binding
	for _, filter := range filters {
		if err := binding.validateMatcher(filter); err != nil {
			query.err = err
			return query
		}
	}
	query.matchers = append(query.matchers, filters...)
	return query
}
