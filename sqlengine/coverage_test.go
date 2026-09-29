package sqlengine

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/crdt"
	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/schema"
)

func queryName(t *testing.T, e *Engine, id ids.RowID) (string, bool) {
	t.Helper()
	var name string
	err := e.QueryRowContext(context.Background(),
		`SELECT name FROM contacts WHERE id = ?`, []any{id[:]},
		func(r *sql.Row) error { return r.Scan(&name) })
	if err != nil {
		return "", false
	}
	return name, true
}

// TestApplyRowsPaths covers bulk row apply: empty input, unknown tables,
// fresh inserts, conflict upserts, and tombstone-driven deletes.
func TestApplyRowsPaths(t *testing.T) {
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tableID, nameID := colID(t, e, "contacts", "name")
	_, phoneID := colID(t, e, "contacts", "phone")
	_, idID := colID(t, e, "contacts", "id")

	if err := e.ApplyRows(&fakeReader{}, nil); err != nil {
		t.Fatalf("ApplyRows(nil) = %v", err)
	}
	src := &fakeReader{
		tables: map[uint32]map[ids.RowID]map[uint32]codec.CellState{},
		tombs:  map[uint32]map[ids.RowID]crdt.Version{},
	}
	if err := e.ApplyRows(src, []RowKey{{TableID: 0xBEEF, RowID: ids.NewRowID()}}); err != nil {
		t.Fatalf("ApplyRows(unknown table) = %v", err)
	}

	ver := crdt.Version{HLC: 100, NodeID: ids.NewNodeID()}
	row := ids.NewRowID()
	src.tables[tableID] = map[ids.RowID]map[uint32]codec.CellState{
		row: {
			idID:    {Version: ver, Value: codec.Blob(row[:])},
			nameID:  {Version: ver, Value: codec.Text("ann")},
			phoneID: {Version: ver, Value: codec.Text("111")},
		},
	}
	if err := e.ApplyRows(src, []RowKey{{TableID: tableID, RowID: row}}); err != nil {
		t.Fatal(err)
	}
	if got, ok := queryName(t, e, row); !ok || got != "ann" {
		t.Fatalf("applied name = %q/%v, want ann/true", got, ok)
	}

	// Re-apply with a changed cell: exercises the ON CONFLICT UPDATE path.
	src.tables[tableID][row][nameID] = codec.CellState{Version: ver, Value: codec.Text("bob")}
	if err := e.ApplyRows(src, []RowKey{{TableID: tableID, RowID: row}}); err != nil {
		t.Fatal(err)
	}
	if got, ok := queryName(t, e, row); !ok || got != "bob" {
		t.Fatalf("re-applied name = %q/%v, want bob/true", got, ok)
	}

	// A newer tombstone hides the row: the SQL copy is deleted.
	src.tombs[tableID] = map[ids.RowID]crdt.Version{
		row: {HLC: 200, NodeID: ids.NewNodeID()},
	}
	if err := e.ApplyRows(src, []RowKey{{TableID: tableID, RowID: row}}); err != nil {
		t.Fatal(err)
	}
	if got, ok := queryName(t, e, row); ok {
		t.Fatalf("tombstoned row still visible = %q", got)
	}
}

// TestApplyRowsSingleColumnTable covers the upsert fast path for tables
// with only a primary key (ON CONFLICT DO NOTHING).
func TestApplyRowsSingleColumnTable(t *testing.T) {
	reg, err := schema.BuildRegistry(1, []schema.TableSchema{
		{Name: "flags", Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(reg, nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tb := reg.Table("flags")
	pk := tb.PKColumn()
	ver := crdt.Version{HLC: 50, NodeID: ids.NewNodeID()}
	row := ids.NewRowID()
	src := &fakeReader{tables: map[uint32]map[ids.RowID]map[uint32]codec.CellState{
		tb.ID: {row: {pk.ID: {Version: ver, Value: codec.Blob(row[:])}}},
	}}
	keys := []RowKey{{TableID: tb.ID, RowID: row}}
	if err := e.ApplyRows(src, keys); err != nil {
		t.Fatal(err)
	}
	// Second apply hits the conflict branch and must stay a no-op.
	if err := e.ApplyRows(src, keys); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := e.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM flags WHERE id = ?`, []any{row[:]},
		func(r *sql.Row) error { return r.Scan(&n) }); err != nil || n != 1 {
		t.Fatalf("flags count = %d/%v, want 1/nil", n, err)
	}
}

// TestRepairRowsPaths covers post-race reconciliation: durable-visible rows
// missing from SQL are inserted, SQL rows hidden durably are deleted, and
// consistent rows are left alone.
func TestRepairRowsPaths(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tableID, nameID := colID(t, e, "contacts", "name")
	_, phoneID := colID(t, e, "contacts", "phone")
	_, idID := colID(t, e, "contacts", "id")

	if err := e.RepairRows(&fakeReader{}, nil); err != nil {
		t.Fatalf("RepairRows(nil) = %v", err)
	}

	ver := crdt.Version{HLC: 100, NodeID: ids.NewNodeID()}
	missing := ids.NewRowID() // durable-visible, absent from SQL
	stale := ids.NewRowID()   // present in SQL, hidden durably
	steady := ids.NewRowID()  // consistent both sides
	cells := func(row ids.RowID, name string) map[uint32]codec.CellState {
		return map[uint32]codec.CellState{
			idID:    {Version: ver, Value: codec.Blob(row[:])},
			nameID:  {Version: ver, Value: codec.Text(name)},
			phoneID: {Version: ver, Value: codec.Text("000")},
		}
	}
	src := &fakeReader{
		tables: map[uint32]map[ids.RowID]map[uint32]codec.CellState{
			tableID: {
				missing: cells(missing, "fresh"),
				steady:  cells(steady, "kept"),
				stale:   cells(stale, "gone"),
			},
		},
		tombs: map[uint32]map[ids.RowID]crdt.Version{
			tableID: {stale: {HLC: 200, NodeID: ids.NewNodeID()}},
		},
	}
	// Seed SQL directly: stale (doomed) and steady (consistent).
	tx, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.SetCaptureMode(CaptureSuppressed)
	for _, seed := range []struct {
		id   ids.RowID
		name string
	}{{stale, "gone"}, {steady, "kept"}} {
		if _, err := tx.Exec(`INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
			seed.id[:], seed.name, "000"); err != nil {
			t.Fatal(err)
		}
	}
	e.SetCaptureMode(CaptureLocal)
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	keys := []RowKey{
		{TableID: tableID, RowID: missing},
		{TableID: tableID, RowID: stale},
		{TableID: tableID, RowID: steady},
		{TableID: 0xBEEF, RowID: ids.NewRowID()}, // unknown table: skipped
	}
	if err := e.RepairRows(src, keys); err != nil {
		t.Fatal(err)
	}
	if got, ok := queryName(t, e, missing); !ok || got != "fresh" {
		t.Fatalf("repaired missing = %q/%v, want fresh/true", got, ok)
	}
	if got, ok := queryName(t, e, stale); ok {
		t.Fatalf("repaired stale still visible = %q", got)
	}
	if got, ok := queryName(t, e, steady); !ok || got != "kept" {
		t.Fatalf("repaired steady = %q/%v, want kept/true", got, ok)
	}
}

// TestMigrateToAdditive proves an additive registry migration creates the
// new table and installs the new registry.
func TestMigrateToAdditive(t *testing.T) {
	old := testRegistry(t)
	e, err := Open(old, nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	next, err := schema.BuildRegistry(2, []schema.TableSchema{
		{
			Name: "contacts",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
				{Name: "phone", Type: schema.ColText, Nullable: true},
			},
		},
		{
			Name:    "flags",
			Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var oldTables []schema.TableSchema
	for _, tb := range old.Tables {
		oldTables = append(oldTables, *tb)
	}
	var nextTables []schema.TableSchema
	for _, tb := range next.Tables {
		nextTables = append(nextTables, *tb)
	}
	ddl, err := schema.MigrationDDL(oldTables, nextTables)
	if err != nil {
		t.Fatal(err)
	}
	if len(ddl) == 0 {
		t.Fatal("MigrationDDL produced no statements")
	}
	if err := e.MigrateTo(next, ddl); err != nil {
		t.Fatal(err)
	}
	if e.Registry().Table("flags") == nil {
		t.Fatal("migrated registry lacks flags table")
	}
	if err := e.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM flags`, nil,
		func(r *sql.Row) error { var n int; return r.Scan(&n) }); err != nil {
		t.Fatalf("flags table unusable after migration: %v", err)
	}
}

// TestMigrateToDDLFailure proves a failing migration reports the statement
// (long statements are truncated) while still installing the new registry.
func TestMigrateToDDLFailure(t *testing.T) {
	mkEngine := func(t *testing.T) (*Engine, *schema.Registry) {
		t.Helper()
		old := testRegistry(t)
		e, err := Open(old, nil, nil, 64)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close() })
		next, err := schema.BuildRegistry(2, []schema.TableSchema{
			{
				Name: "contacts",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "name", Type: schema.ColText, Nullable: true},
					{Name: "phone", Type: schema.ColText, Nullable: true},
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return e, next
	}
	t.Run("long", func(t *testing.T) {
		e, next := mkEngine(t)
		bad := "CREATE TABLE " + strings.Repeat("x", 200) + " (oops"
		err := e.MigrateTo(next, []string{bad})
		if err == nil || !strings.Contains(err.Error(), "migration") {
			t.Fatalf("MigrateTo err = %v, want migration error", err)
		}
		// Truncation marker: the raw 200-char identifier must not leak whole.
		if strings.Contains(err.Error(), strings.Repeat("x", 200)) {
			t.Fatal("error embeds the full long statement, want truncation")
		}
		if e.Registry().Epoch != 2 {
			t.Fatalf("registry epoch = %d, want 2 (swap on failure)", e.Registry().Epoch)
		}
	})
	t.Run("short", func(t *testing.T) {
		e, next := mkEngine(t)
		err := e.MigrateTo(next, []string{"NOT VALID SQL"})
		if err == nil || !strings.Contains(err.Error(), "NOT VALID SQL") {
			t.Fatalf("MigrateTo err = %v, want short statement quoted", err)
		}
	})
}

// TestTxQueryRowAndPending covers the single-row transaction reads and the
// pre-commit capture preview.
func TestTxQueryRowAndPending(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id := ids.NewRowID()
	tx, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		id[:], "ann", "111"); err != nil {
		t.Fatal(err)
	}
	pending := tx.Pending()
	row, err := tx.QueryRow(`SELECT name FROM contacts WHERE id = ?`, id[:])
	if err != nil {
		t.Fatal(err)
	}
	var name string
	if err := row.Scan(&name); err != nil || name != "ann" {
		t.Fatalf("QueryRow = %q/%v", name, err)
	}
	row2, err := tx.QueryRowContext(ctx, `SELECT phone FROM contacts WHERE id = ?`, id[:])
	if err != nil {
		t.Fatal(err)
	}
	var phone string
	if err := row2.Scan(&phone); err != nil || phone != "111" {
		t.Fatalf("QueryRowContext = %q/%v", phone, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := tx.QueryRowContext(cancelled, `SELECT 1`); err == nil {
		t.Fatal("QueryRowContext(cancelled) = nil, want error")
	}
	changes, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != len(changes) {
		t.Fatalf("Pending = %d changes, committed = %d", len(pending), len(changes))
	}
	if _, err := tx.QueryRow(`SELECT 1`); err == nil {
		t.Fatal("QueryRow after commit = nil, want tx-done error")
	}
}

// TestRowsErrAndEmptyResult pins the small query-result helpers.
func TestRowsErrAndEmptyResult(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	rows, err := e.Query(ctx, `SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err = %v, want nil", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	var res EmptyResult
	if id, err := res.LastInsertId(); id != 0 || err != nil {
		t.Fatalf("LastInsertId = %d/%v", id, err)
	}
	if n, err := res.RowsAffected(); n != 0 || err != nil {
		t.Fatalf("RowsAffected = %d/%v", n, err)
	}
}

// TestStmtCacheLen proves the statement cache count tracks prepares.
func TestStmtCacheLen(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if n := e.readStmts.Len(); n != 0 {
		t.Fatalf("fresh read cache Len = %d, want 0", n)
	}
	rows, err := e.Query(ctx, `SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if n := e.readStmts.Len(); n != 1 {
		t.Fatalf("read cache Len = %d, want 1", n)
	}
}

// TestCaptureModeRoundTrip pins the capture-mode accessor pair.
func TestCaptureModeRoundTrip(t *testing.T) {
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := e.CaptureMode(); got != CaptureLocal {
		t.Fatalf("default CaptureMode = %v, want CaptureLocal", got)
	}
	e.SetCaptureMode(CaptureSuppressed)
	if got := e.CaptureMode(); got != CaptureSuppressed {
		t.Fatalf("CaptureMode = %v, want CaptureSuppressed", got)
	}
}
