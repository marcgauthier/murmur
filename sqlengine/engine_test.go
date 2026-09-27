package sqlengine

import (
	"context"
	"fmt"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/schema"
	"github.com/nomadsql/replicateddb/state"
)

func testRegistry(t *testing.T) *schema.Registry {
	t.Helper()
	reg, err := schema.BuildRegistry(1, []schema.TableSchema{
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
	return reg
}

func TestPreUpdateCaptureInsertUpdateDelete(t *testing.T) {
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
	if _, err := tx.Exec(`INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`, id[:], "ann", "111"); err != nil {
		t.Fatal(err)
	}
	ev, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(ev) != 1 || ev[0].Op != OpInsert {
		t.Fatalf("expected 1 insert event, got %+v", ev)
	}
	if len(ev[0].New) != 3 {
		t.Fatalf("expected 3 new values, got %d", len(ev[0].New))
	}

	// Read-your-write across the read connection.
	rows, err := e.Query(ctx, `SELECT name, phone FROM contacts WHERE id = ?`, id[:])
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("row not visible on read conn")
	}
	var name, phone string
	if err := rows.Scan(&name, &phone); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if name != "ann" || phone != "111" {
		t.Fatalf("bad row: %q %q", name, phone)
	}

	// Multi-row update produces per-row events; unchanged columns are still
	// delivered raw (coalescing happens in Delta).
	tx2, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Exec(`UPDATE contacts SET phone = ?`, "222"); err != nil {
		t.Fatal(err)
	}
	ev2, err := tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(ev2) != 1 || ev2[0].Op != OpUpdate {
		t.Fatalf("expected 1 update event, got %+v", ev2)
	}

	tx3, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx3.Exec(`DELETE FROM contacts`); err != nil {
		t.Fatal(err)
	}
	ev3, err := tx3.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(ev3) != 1 || ev3[0].Op != OpDelete {
		t.Fatalf("expected 1 delete event, got %+v", ev3)
	}
}

func TestCaptureSuppressed(t *testing.T) {
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
	e.SetCaptureMode(CaptureSuppressed)
	if _, err := tx.Exec(`INSERT INTO contacts (id) VALUES (?)`, id[:]); err != nil {
		t.Fatal(err)
	}
	e.SetCaptureMode(CaptureLocal)
	ev, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(ev) != 0 {
		t.Fatalf("expected no events under suppression, got %d", len(ev))
	}
}

func TestDeltaCoalescing(t *testing.T) {
	reg := testRegistry(t)
	table := reg.Table("contacts")
	ord := map[string]int{}
	for i, c := range table.Columns {
		ord[c.Name] = i
	}
	row := ids.NewRowID()
	mkrow := func(id ids.RowID, name, phone string) []codec.Value {
		v := make([]codec.Value, 3)
		v[ord["id"]] = codec.Blob(id[:])
		v[ord["name"]] = codec.Text(name)
		v[ord["phone"]] = codec.Text(phone)
		return v
	}
	add := func(d *Delta, op OpType, old, new []codec.Value) {
		t.Helper()
		if err := d.Add(RawChange{Table: table, Op: op, Old: old, New: new}); err != nil {
			t.Fatal(err)
		}
	}

	// UPDATE then UPDATE emits only the final value.
	d := NewDelta()
	add(d, OpUpdate, mkrow(row, "a", "1"), mkrow(row, "a", "2"))
	add(d, OpUpdate, mkrow(row, "a", "2"), mkrow(row, "a", "3"))
	muts, err := d.Build(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(muts) != 1 || muts[0].Value.S != "3" {
		t.Fatalf("expected single phone=3 mutation, got %+v", muts)
	}

	// INSERT then DELETE in one txn emits nothing.
	d2 := NewDelta()
	add(d2, OpInsert, nil, mkrow(row, "a", "1"))
	old := mkrow(row, "a", "1")
	add(d2, OpDelete, old, nil)
	muts2, err := d2.Build(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(muts2) != 0 {
		t.Fatalf("expected no mutations, got %+v", muts2)
	}
	if !d2.IsEmpty() {
		t.Fatal("expected empty delta")
	}

	// UPDATE then DELETE emits only the tombstone.
	d3 := NewDelta()
	add(d3, OpUpdate, mkrow(row, "a", "1"), mkrow(row, "a", "2"))
	add(d3, OpDelete, mkrow(row, "a", "2"), nil)
	muts3, err := d3.Build(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(muts3) != 1 || !muts3[0].IsTombstone() {
		t.Fatalf("expected single tombstone, got %+v", muts3)
	}

	// DELETE then INSERT resurrects with final state.
	d4 := NewDelta()
	add(d4, OpDelete, mkrow(row, "a", "1"), nil)
	add(d4, OpInsert, nil, mkrow(row, "b", "2"))
	muts4, err := d4.Build(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(muts4) != 3 {
		t.Fatalf("expected 3 resurrected cells, got %+v", muts4)
	}
	for _, m := range muts4 {
		if m.IsTombstone() {
			t.Fatalf("unexpected tombstone in %+v", muts4)
		}
	}

	// Value restored to before-image is omitted.
	d5 := NewDelta()
	add(d5, OpUpdate, mkrow(row, "a", "1"), mkrow(row, "a", "2"))
	add(d5, OpUpdate, mkrow(row, "a", "2"), mkrow(row, "a", "1"))
	muts5, err := d5.Build(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(muts5) != 0 {
		t.Fatalf("expected no mutations, got %+v", muts5)
	}
}

func TestTxCommitFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e.failCommit = func() error { return fmt.Errorf("injected commit failure") }
	id := ids.NewRowID()
	tx, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO contacts (id) VALUES (?)`, id[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(); err == nil {
		t.Fatal("expected injected commit failure")
	}
	e.failCommit = nil
	// Rolled back and reusable.
	rows, err := e.Query(ctx, `SELECT COUNT(*) FROM contacts`)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	for rows.Next() {
		_ = rows.Scan(&n)
	}
	rows.Close()
	if n != 0 {
		t.Fatalf("count = %d after failed commit", n)
	}
	tx2, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Exec(`INSERT INTO contacts (id) VALUES (?)`, id[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestFTS5Available(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tx, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// FTS5 virtual tables must be creatable (local-only search structures).
	if _, err := tx.Exec(`CREATE VIRTUAL TABLE ft USING fts5(x)`); err != nil {
		t.Fatalf("FTS5 unavailable: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO ft (x) VALUES ('hello world')`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(`SELECT x FROM ft WHERE ft MATCH 'hello'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("FTS5 match returned nothing")
	}
}

// Ensure *state.Store satisfies StateReader at compile time.
var _ StateReader = (*state.Store)(nil)
