package sqlengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/state"
)

// fakeReader is an in-memory StateReader.
type fakeReader struct {
	tables map[uint32]map[ids.RowID]map[uint32]codec.CellState
	tombs  map[uint32]map[ids.RowID]crdt.Version
	calls  int // GetRow call count (fast-path verification)
}

func TestRebuildContextCancellationAndCommittedProgress(t *testing.T) {
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeReader{}
	err = e.RebuildContext(ctx, r, func(p RebuildProgress) {
		if p.CurrentTable != "" {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("rebuild cancellation=%v", err)
	}
	var final RebuildProgress
	if err := e.RebuildContext(context.Background(), r, func(p RebuildProgress) { final = p }); err != nil {
		t.Fatal(err)
	}
	if !final.Indexing || final.RowsInserted != 0 || final.RowsSkipped != 0 {
		t.Fatalf("empty progress=%+v", final)
	}
}

func (f *fakeReader) IterateTable(tableID uint32, fn func(*state.Row) error) error {
	for id, cells := range f.tables[tableID] {
		r := &state.Row{Table: tableID, ID: id, Cells: cells}
		for _, st := range cells {
			if crdt.CompareVersion(st.Version, r.Newest) > 0 {
				r.Newest = st.Version
			}
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeReader) GetRow(table uint32, row ids.RowID) (map[uint32]codec.CellState, error) {
	f.calls++
	return f.tables[table][row], nil
}

func (f *fakeReader) GetTombstone(table uint32, row ids.RowID) (crdt.Version, bool, error) {
	v, ok := f.tombs[table][row]
	return v, ok, nil
}

func colID(t *testing.T, e *Engine, table, col string) (uint32, uint32) {
	t.Helper()
	tb := e.Registry().Table(table)
	if tb == nil {
		t.Fatalf("no table %s", table)
	}
	c := tb.ColumnByID(0)
	for i := range tb.Columns {
		if tb.Columns[i].Name == col {
			c = &tb.Columns[i]
		}
	}
	if c == nil || c.ID == 0 {
		t.Fatalf("no column %s.%s", table, col)
	}
	return tb.ID, c.ID
}

func TestApplyWinnersPaths(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tableID, nameID := colID(t, e, "contacts", "name")
	_, phoneID := colID(t, e, "contacts", "phone")
	_, idID := colID(t, e, "contacts", "id")
	ver := crdt.Version{HLC: 100, NodeID: ids.NewNodeID()}

	newRow := ids.NewRowID()
	existing := ids.NewRowID()
	resurrected := ids.NewRowID()

	// Seed one existing row directly (suppressed capture).
	tx, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.SetCaptureMode(CaptureSuppressed)
	if _, err := tx.Exec(`INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		existing[:], "old", "000"); err != nil {
		t.Fatal(err)
	}
	e.SetCaptureMode(CaptureLocal)
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	src := &fakeReader{tables: map[uint32]map[ids.RowID]map[uint32]codec.CellState{
		tableID: {
			// Partial-coverage row: durable has all cells, winners carry one.
			resurrected: {
				idID:    {Version: ver, Value: codec.Blob(resurrected[:])},
				nameID:  {Version: ver, Value: codec.Text("rez")},
				phoneID: {Version: ver, Value: codec.Text("999")},
			},
		},
	}}
	winners := []state.WinningChange{
		// Full-coverage new row: fast path, no GetRow.
		{TableID: tableID, RowID: newRow, ColumnID: idID, Value: codec.Blob(newRow[:]), Version: ver},
		{TableID: tableID, RowID: newRow, ColumnID: nameID, Value: codec.Text("new"), Version: ver},
		{TableID: tableID, RowID: newRow, ColumnID: phoneID, Value: codec.Text("111"), Version: ver},
		// Partial update of an existing row.
		{TableID: tableID, RowID: existing, ColumnID: phoneID, Value: codec.Text("222"), Version: ver},
		// Partial-coverage missing row: readback path.
		{TableID: tableID, RowID: resurrected, ColumnID: phoneID, Value: codec.Text("999"), Version: ver},
		// Unknown table/column: skipped without error.
		{TableID: 0xBEEF, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(1), Version: ver},
		{TableID: tableID, RowID: existing, ColumnID: 0xBEEF, Value: codec.Int(1), Version: ver},
	}
	if err := e.ApplyWinners(src, winners); err != nil {
		t.Fatal(err)
	}
	if src.calls != 1 {
		t.Fatalf("GetRow called %d times, want 1 (readback path only)", src.calls)
	}
	rows, err := e.Query(ctx, `SELECT name, phone FROM contacts ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var got [][2]string
	for rows.Next() {
		var name, phone string
		if err := rows.Scan(&name, &phone); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		got = append(got, [2]string{name, phone})
	}
	rows.Close() // release the read lock before the next write section
	if len(got) != 3 {
		t.Fatalf("got %v", got)
	}
	want := map[string]string{"new": "111", "old": "222", "rez": "999"}
	for _, r := range got {
		if want[r[0]] != r[1] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	// Full-coverage winners hidden by a newer tombstone stay deleted.
	hidden := ids.NewRowID()
	src.tombs = map[uint32]map[ids.RowID]crdt.Version{
		tableID: {hidden: {HLC: 200, NodeID: ver.NodeID}},
	}
	if err := e.ApplyWinners(src, []state.WinningChange{
		{TableID: tableID, RowID: hidden, ColumnID: idID, Value: codec.Blob(hidden[:]), Version: ver},
		{TableID: tableID, RowID: hidden, ColumnID: nameID, Value: codec.Text("ghost"), Version: ver},
		{TableID: tableID, RowID: hidden, ColumnID: phoneID, Value: codec.Text("000"), Version: ver},
	}); err != nil {
		t.Fatal(err)
	}
	rows, err = e.Query(ctx, `SELECT COUNT(*) FROM contacts WHERE name = 'ghost'`)
	if err != nil {
		t.Fatal(err)
	}
	var ghosts int
	for rows.Next() {
		_ = rows.Scan(&ghosts)
	}
	rows.Close()
	if ghosts != 0 {
		t.Fatal("tombstone-hidden row was materialized")
	}

	// A stale tombstone (older than the stored cells) must not delete a
	// visible row.
	src.tables[tableID][existing] = map[uint32]codec.CellState{
		idID:    {Version: ver, Value: codec.Blob(existing[:])},
		nameID:  {Version: ver, Value: codec.Text("old")},
		phoneID: {Version: ver, Value: codec.Text("222")},
	}
	stale := crdt.Version{HLC: 50, NodeID: ver.NodeID}
	if err := e.ApplyWinners(src, []state.WinningChange{
		{TableID: tableID, RowID: existing, Tombstone: true, Version: stale},
	}); err != nil {
		t.Fatal(err)
	}
	rows, err = e.Query(ctx, `SELECT phone FROM contacts WHERE name = 'old'`)
	if err != nil {
		t.Fatal(err)
	}
	var oldPhone string
	for rows.Next() {
		_ = rows.Scan(&oldPhone)
	}
	rows.Close()
	if oldPhone != "222" {
		t.Fatalf("stale tombstone deleted a visible row (phone=%q)", oldPhone)
	}

	// Tombstone deletes.
	if err := e.ApplyWinners(src, []state.WinningChange{
		{TableID: tableID, RowID: newRow, Tombstone: true, Version: ver},
	}); err != nil {
		t.Fatal(err)
	}
	rows2, err := e.Query(ctx, `SELECT COUNT(*) FROM contacts`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows2.Close()
	var n int
	for rows2.Next() {
		_ = rows2.Scan(&n)
	}
	if n != 2 {
		t.Fatalf("count = %d after tombstone", n)
	}
}

func TestRebuildUsesBoundedMultiRowInserts(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tableID, nameID := colID(t, e, "contacts", "name")
	_, phoneID := colID(t, e, "contacts", "phone")
	_, idID := colID(t, e, "contacts", "id")
	version := crdt.Version{HLC: 1, NodeID: ids.NewNodeID()}
	const rows = 1200 // crosses several 900-bind batches for this three-column table
	reader := &fakeReader{tables: map[uint32]map[ids.RowID]map[uint32]codec.CellState{tableID: {}}}
	for i := 0; i < rows; i++ {
		id := ids.NewRowID()
		reader.tables[tableID][id] = map[uint32]codec.CellState{
			idID:    {Version: version, Value: codec.Blob(id[:])},
			nameID:  {Version: version, Value: codec.Text(fmt.Sprintf("row-%04d", i))},
			phoneID: {Version: version, Value: codec.Text("555")},
		}
	}
	if err := e.Rebuild(reader); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	var count int
	if err := e.QueryRowContext(ctx, `SELECT count(*) FROM contacts`, nil, func(row *sql.Row) error {
		return row.Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	if count != rows {
		t.Fatalf("rebuilt rows=%d, want %d", count, rows)
	}
	var name, phone string
	if err := e.QueryRowContext(ctx, `SELECT name, phone FROM contacts WHERE name = ?`, []any{"row-1199"}, func(row *sql.Row) error {
		return row.Scan(&name, &phone)
	}); err != nil {
		t.Fatal(err)
	}
	if name != "row-1199" || phone != "555" {
		t.Fatalf("last rebuilt row=(%q,%q), want (row-1199,555)", name, phone)
	}
}
