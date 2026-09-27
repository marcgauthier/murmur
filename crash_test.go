package replicateddb

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/replication"
	"github.com/nomadsql/replicateddb/state"
)

// dumpSQL returns the query-visible rows keyed by row-id hex.
func dumpSQL(t *testing.T, db *DB) map[string]string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT id, name, phone, score FROM contacts`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var id []byte
		var name, phone, score any
		if err := rows.Scan(&id, &name, &phone, &score); err != nil {
			t.Fatal(err)
		}
		out[fmt.Sprintf("%x", id)] = fmt.Sprintf("%v|%v|%v", name, phone, score)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// dumpBadger returns the Badger-visible rows keyed by row-id hex.
func dumpBadger(t *testing.T, db *DB) map[string]string {
	t.Helper()
	table := db.reg.Table("contacts")
	if table == nil {
		t.Fatal("no contacts table")
	}
	out := make(map[string]string)
	cols := map[string]uint32{}
	for _, c := range table.Columns {
		cols[c.Name] = c.ID
	}
	str := func(cells map[uint32]codec.CellState, name string) string {
		st, ok := cells[cols[name]]
		if !ok {
			return "<nil>"
		}
		return fmt.Sprintf("%v", st.Value.ToAny())
	}
	if err := db.store.IterateTable(table.ID, func(r *state.Row) error {
		if !r.Visible() {
			return nil
		}
		out[fmt.Sprintf("%x", r.ID[:])] = str(r.Cells, "name") + "|" + str(r.Cells, "phone") + "|" + str(r.Cells, "score")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// assertConverged requires SQL-visible state to equal Badger-visible state
// and the generations to agree.
func assertConverged(t *testing.T, db *DB) {
	t.Helper()
	sqlRows, badgerRows := dumpSQL(t, db), dumpBadger(t, db)
	if fmt.Sprintf("%v", sqlRows) != fmt.Sprintf("%v", badgerRows) {
		t.Fatalf("diverged:\nsql=%v\nbadger=%v", sqlRows, badgerRows)
	}
	st := db.Status()
	if st.State != StateReady {
		t.Fatalf("state = %s", st.State)
	}
	if st.StateGeneration != st.MaterializedGeneration {
		t.Fatalf("gen %d != materialized %d", st.StateGeneration, st.MaterializedGeneration)
	}
}

func crashDB(t *testing.T) (*DB, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, ctx
}

func TestCrashBeforeSQLCommit(t *testing.T) {
	db, ctx := crashDB(t)
	id0 := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id0[:], "base"); err != nil {
		t.Fatal(err)
	}
	db.crash = &crashHooks{beforeSQLCommit: func() error { return errors.New("injected: before sql commit") }}
	id1 := NewRowID()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id1[:], "lost"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("expected injected failure")
	}
	db.crash = nil
	// Nothing durable, nothing visible; node still usable.
	assertConverged(t, db)
	if n := len(dumpSQL(t, db)); n != 1 {
		t.Fatalf("want 1 row, got %d", n)
	}
	idOK := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, idOK[:], "ok"); err != nil {
		t.Fatal(err)
	}
	assertConverged(t, db)
}

func TestCrashAfterSQLCommit(t *testing.T) {
	db, ctx := crashDB(t)
	id0 := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id0[:], "base"); err != nil {
		t.Fatal(err)
	}
	genBefore := db.Status().StateGeneration
	db.crash = &crashHooks{afterSQLCommit: func() error { return errors.New("injected: after sql commit") }}
	id1 := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id1[:], "lost"); err == nil {
		t.Fatal("expected injected failure")
	}
	db.crash = nil
	// SQL ran ahead and was rebuilt from Badger: the failed write is gone
	// everywhere and generations agree.
	if gen := db.Status().StateGeneration; gen != genBefore {
		t.Fatalf("generation moved %d -> %d", genBefore, gen)
	}
	assertConverged(t, db)
	if n := len(dumpSQL(t, db)); n != 1 {
		t.Fatalf("want 1 row, got %d", n)
	}
}

func TestCrashBeforeBadger(t *testing.T) {
	db, ctx := crashDB(t)
	db.crash = &crashHooks{beforeDurable: func() error { return errors.New("injected: before badger") }}
	idLost := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`,
		idLost[:], "lost"); err == nil {
		t.Fatal("expected injected failure")
	}
	db.crash = nil
	assertConverged(t, db)
	if n := len(dumpSQL(t, db)); n != 0 {
		t.Fatalf("want 0 rows, got %d", n)
	}
}

func TestCrashAfterBadgerAmbiguous(t *testing.T) {
	db, ctx := crashDB(t)
	db.crash = &crashHooks{afterDurable: func() error { return errors.New("injected: ack lost") }}
	id := NewRowID()
	_, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "kept")
	if !errors.Is(err, ErrAmbiguousCommit) {
		t.Fatalf("expected ErrAmbiguousCommit, got %v", err)
	}
	db.crash = nil
	// Durable and materialized despite the lost acknowledgement.
	assertConverged(t, db)
	got := dumpSQL(t, db)
	if len(got) != 1 {
		t.Fatalf("want 1 row, got %d", len(got))
	}
	// Application retry of the same logical INSERT fails on the PK (the
	// data is already durable); retry of an UPDATE is a value-level no-op.
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "kept"); err == nil {
		t.Fatal("expected PK conflict on re-insert")
	}
	genBefore := db.Status().StateGeneration
	if _, err := db.ExecContext(ctx, `UPDATE contacts SET name = ? WHERE id = ?`, "kept", id[:]); err != nil {
		t.Fatal(err)
	}
	if gen := db.Status().StateGeneration; gen != genBefore {
		t.Fatalf("idempotent retry bumped generation %d -> %d", genBefore, gen)
	}
	assertConverged(t, db)
}

func TestRemoteMaterializeFailureRebuilds(t *testing.T) {
	db, ctx := crashDB(t)
	peer := NewNodeID()
	row := NewRowID()
	batch := remoteBatchCells(db, peer, 1, 100, "contacts", row, map[string]codec.Value{
		"id":   codec.Blob(row[:]),
		"name": codec.Text("remotename"),
	})
	db.crash = &crashHooks{remoteMaterialize: func() error { return errors.New("injected: apply failed") }}
	// The batch is durable, so ApplyRemote still reports success (ack).
	if err := db.ApplyRemote(ctx, batch); err != nil {
		t.Fatal(err)
	}
	db.crash = nil
	// Converged via rebuild: the remote row is query-visible.
	assertConverged(t, db)
	if n := len(dumpSQL(t, db)); n != 1 {
		t.Fatalf("want 1 row, got %d", n)
	}
}

// remoteBatchCells crafts a remote batch for tests (white-box: uses the
// registry for table/column IDs).
func remoteBatchCells(db *DB, origin ids.NodeID, seq uint64, hlc uint64,
	table string, row ids.RowID, cells map[string]codec.Value) *codec.MutationBatch {
	tb := db.reg.Table(table)
	byName := make(map[string]uint32, len(tb.Columns))
	for _, c := range tb.Columns {
		byName[c.Name] = c.ID
	}
	batch := &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		TxID:            ids.NewTxID(),
		OriginNode:      origin,
		Sequence:        seq,
		HLC:             hlc,
		SchemaEpoch:     db.reg.Epoch,
	}
	for name, v := range cells {
		batch.Mutations = append(batch.Mutations, codec.Mutation{
			TableID: tb.ID, RowID: row, ColumnID: byName[name], Value: v,
		})
	}
	return batch
}

// TestPartialBatchSkippedNotWedged proves a corrupt batch missing the primary
// key cannot wedge apply or restart-rebuild: it stays in Badger, invisible.
func TestPartialBatchSkippedNotWedged(t *testing.T) {
	path := t.TempDir()
	ctx := context.Background()
	cfg := testConfig(path)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	peer := NewNodeID()
	row := NewRowID()
	batch := remoteBatchCells(db, peer, 1, 100, "contacts", row, map[string]codec.Value{
		"name": codec.Text("nopk"),
	})
	if err := db.ApplyRemote(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if st := db.Status().State; st != StateReady {
		t.Fatalf("state = %s", st)
	}
	if n := len(dumpSQL(t, db)); n != 0 {
		t.Fatalf("partial row query-visible: %d rows", n)
	}
	_ = db.Close()
	// Restart rebuild skips it too: the node still opens.
	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if n := len(dumpSQL(t, db2)); n != 0 {
		t.Fatalf("partial row query-visible after reopen: %d rows", n)
	}
}
