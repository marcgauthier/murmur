package murmur

import (
	"context"
	"fmt"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/state"
	"strings"
)

// ColumnMergePolicy returns the merge policy for a replicated schema field.
func (db *DB) ColumnMergePolicy(table, column string) (schema.MergePolicy, error) {
	r := db.schemaRegistry()
	if r != nil {
		t := r.Table(table)
		if t != nil {
			for _, c := range t.Columns {
				if strings.EqualFold(c.Name, column) {
					return c.MergePolicy, nil
				}
			}
		}
	}
	return 0, fmt.Errorf("murmur: unknown column %s.%s", table, column)
}

// CRDTRecords inspects the underlying causal state from one durable read cut.
// It returns copied records, including retained set removals.
func (db *DB) CRDTRecords(ctx context.Context, table, column string, row ids.RowID) ([]codec.CRDTRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := db.requireRead(); err != nil {
		return nil, err
	}
	r := db.schemaRegistry()
	t := r.Table(table)
	if t != nil {
		for _, c := range t.Columns {
			if strings.EqualFold(c.Name, column) {
				if c.MergePolicy != schema.PN_COUNTER && c.MergePolicy != schema.OR_SET {
					return nil, fmt.Errorf("murmur: column has no causal records")
				}
				return db.store.CRDTRecords(t.ID, row, c.ID)
			}
		}
	}
	return nil, fmt.Errorf("murmur: unknown CRDT column")
}

// MergePolicyStats reports retained metadata and process merge counters.
type MergePolicyStats = state.MergePolicyStats

// MergePolicyStats scans causal metadata on demand. Prefer infrequent polling
// for large databases; the read cut includes underlying Low and High branches.
func (db *DB) MergePolicyStats(ctx context.Context) (MergePolicyStats, error) {
	if err := ctx.Err(); err != nil {
		return MergePolicyStats{}, err
	}
	if err := db.requireRead(); err != nil {
		return MergePolicyStats{}, err
	}
	return db.store.MergePolicyStats(ctx)
}
