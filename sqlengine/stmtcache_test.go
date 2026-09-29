package sqlengine

import (
	"context"
	"testing"
)

// Repeat queries must hit the statement cache; distinct queries and
// post-invalidation lookups must miss. Deltas (not absolutes) keep the
// test immune to setup prepares during Open.
func TestStmtCacheHitMissCounters(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	query := func(q string) {
		t.Helper()
		rows, err := e.Query(ctx, q)
		if err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		rows.Close()
	}
	assertDelta := func(h0, m0, wantH, wantM uint64, what string) {
		t.Helper()
		h, m := e.StmtCacheStats()
		if h-h0 != wantH || m-m0 != wantM {
			t.Fatalf("%s: delta hits=%d misses=%d, want %d/%d",
				what, h-h0, m-m0, wantH, wantM)
		}
	}

	h0, m0 := e.StmtCacheStats()
	query("SELECT id, name FROM contacts")
	assertDelta(h0, m0, 0, 1, "first query")
	query("SELECT id, name FROM contacts")
	assertDelta(h0, m0, 1, 1, "repeat query")
	query("SELECT COUNT(*) FROM contacts")
	assertDelta(h0, m0, 1, 2, "distinct query")
	e.readStmts.invalidate()
	query("SELECT id, name FROM contacts")
	assertDelta(h0, m0, 1, 3, "after invalidate")
}
