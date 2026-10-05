package murmur

import (
	"context"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
)

func TestRemoteMaterializationFlushesAtTransactionCount(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.QueryStore.RemoteApplyInterval = time.Hour
	cfg.QueryStore.RemoteApplyMaxTransactions = 3
	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	peer, row := NewNodeID(), NewRowID()
	for seq, name := range []string{"first", "second", "third"} {
		values := map[string]codec.Value{"name": codec.Text(name)}
		if seq == 0 {
			values["id"] = codec.Blob(row[:])
		}
		batch := remoteBatchCells(db, peer, uint64(seq+1), uint64(100+seq), "contacts", row, values)
		if err := applyRemoteFixture(db, context.Background(), batch); err != nil {
			t.Fatal(err)
		}
		if seq < 2 {
			if len(dumpSQL(t, db)) != 0 {
				t.Fatal("remote row materialized before the count threshold")
			}
			status := db.Status()
			if status.StateGeneration <= status.MaterializedGeneration {
				t.Fatal("pending remote rows were marked materialized")
			}
		}
	}
	waitForRemoteMaterialization(t, db)
	if got := len(dumpSQL(t, db)); got != 1 {
		t.Fatalf("row count after count-triggered flush = %d, want 1", got)
	}
	assertConverged(t, db)
}

func TestRemoteMaterializationGroupCountsTransactions(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.QueryStore.RemoteApplyInterval = time.Hour
	cfg.QueryStore.RemoteApplyMaxTransactions = 3
	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	peer, row := NewNodeID(), NewRowID()
	var batches []*codec.MutationBatch
	for seq, name := range []string{"first", "second", "third"} {
		values := map[string]codec.Value{"name": codec.Text(name)}
		if seq == 0 {
			values["id"] = codec.Blob(row[:])
		}
		batches = append(batches, remoteBatchCells(db, peer, uint64(seq+1), uint64(100+seq), "contacts", row, values))
	}
	if err := applyRemoteGroupFixture(db, context.Background(), batches); err != nil {
		t.Fatal(err)
	}
	waitForRemoteMaterialization(t, db)
	if got := len(dumpSQL(t, db)); got != 1 {
		t.Fatalf("group flush row count = %d, want 1", got)
	}
	assertConverged(t, db)
}

func TestRemoteMaterializationCoalescesDeleteAndResurrection(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.QueryStore.RemoteApplyInterval = time.Hour
	cfg.QueryStore.RemoteApplyMaxTransactions = 3
	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	peer, row := NewNodeID(), NewRowID()
	insert := remoteBatchCells(db, peer, 1, 100, "contacts", row, map[string]codec.Value{
		"id": codec.Blob(row[:]), "name": codec.Text("first"),
	})
	deleted := remoteBatchCells(db, peer, 2, 101, "contacts", row, map[string]codec.Value{"name": codec.Text("unused")})
	deleted.Mutations = []codec.Mutation{{TableID: db.reg.Table("contacts").ID, RowID: row, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone}}
	resurrected := remoteBatchCells(db, peer, 3, 102, "contacts", row, map[string]codec.Value{
		"id": codec.Blob(row[:]), "name": codec.Text("resurrected"),
	})
	for _, batch := range []*codec.MutationBatch{insert, deleted, resurrected} {
		if err := applyRemoteFixture(db, context.Background(), batch); err != nil {
			t.Fatal(err)
		}
	}
	waitForRemoteMaterialization(t, db)
	var name string
	if err := db.QueryRowContext(context.Background(), "SELECT name FROM contacts WHERE id=?", row[:]).Scan(&name); err != nil || name != "resurrected" {
		t.Fatalf("coalesced row name=%q err=%v", name, err)
	}
	assertConverged(t, db)
}

func TestRemoteMaterializationDefaults(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.withDefaults()
	if cfg.QueryStore.RemoteApplyInterval != time.Second || cfg.QueryStore.RemoteApplyMaxTransactions != 1_000 {
		t.Fatalf("query-store remote defaults = %v, %d", cfg.QueryStore.RemoteApplyInterval, cfg.QueryStore.RemoteApplyMaxTransactions)
	}
}

func TestRemoteMaterializationConfigRejectsNegativeThresholds(t *testing.T) {
	for _, configure := range []func(*Config){
		func(c *Config) { c.QueryStore.RemoteApplyInterval = -time.Second },
		func(c *Config) { c.QueryStore.RemoteApplyMaxTransactions = -1 },
	} {
		cfg := testConfig(t.TempDir())
		configure(&cfg)
		cfg.withDefaults()
		if err := cfg.validate(); err == nil {
			t.Fatal("negative remote materialization threshold accepted")
		}
	}
}

func TestRemoteRowCountAloneDoesNotWakeFlush(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.QueryStore.RemoteApplyInterval = time.Hour
	cfg.QueryStore.RemoteApplyMaxTransactions = 1_000
	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	peer, first := NewNodeID(), NewRowID()
	batch := remoteBatchCells(db, peer, 1, 100, "contacts", first, map[string]codec.Value{
		"id": codec.Blob(first[:]), "name": codec.Text("row"),
	})
	table := db.reg.Table("contacts")
	var idColumn, nameColumn uint32
	for _, column := range table.Columns {
		switch column.Name {
		case "id":
			idColumn = column.ID
		case "name":
			nameColumn = column.ID
		}
	}
	for i := 0; i < 10_000; i++ {
		row := NewRowID()
		batch.Mutations = append(batch.Mutations,
			codec.Mutation{TableID: table.ID, RowID: row, ColumnID: idColumn, Value: codec.Blob(row[:])},
			codec.Mutation{TableID: table.ID, RowID: row, ColumnID: nameColumn, Value: codec.Text("row")},
		)
	}
	if err := applyRemoteFixture(db, context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM contacts").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("row-count-only flush made %d rows visible", count)
	}
	if db.Status().StateGeneration <= db.Status().MaterializedGeneration {
		t.Fatal("bulk receive was marked materialized without time/count trigger")
	}
}

func TestRemoteMaterializationFlushesAtInterval(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.QueryStore.RemoteApplyInterval = 20 * time.Millisecond
	cfg.QueryStore.RemoteApplyMaxTransactions = 1_000
	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := NewRowID()
	batch := remoteBatchCells(db, NewNodeID(), 1, 100, "contacts", row, map[string]codec.Value{
		"id": codec.Blob(row[:]), "name": codec.Text("remote"),
	})
	if err := applyRemoteFixture(db, context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	waitForRemoteMaterialization(t, db)
	assertConverged(t, db)
}

func waitForRemoteMaterialization(t *testing.T, db *DB) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status := db.Status()
		if status.StateGeneration == status.MaterializedGeneration {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("remote SQLite materialization did not flush")
}

func TestRemotePebbleCommitContinuesDuringSQLiteRead(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.QueryStore.RemoteApplyInterval = time.Hour
	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	local := NewRowID()
	if _, err := db.ExecContext(context.Background(), "INSERT INTO contacts (id, name) VALUES (?, ?)", local[:], "local"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(context.Background(), "SELECT id, name FROM contacts")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatal("read cursor has no row")
	}
	remote := NewRowID()
	batch := remoteBatchCells(db, NewNodeID(), 1, 100, "contacts", remote, map[string]codec.Value{
		"id": codec.Blob(remote[:]), "name": codec.Text("remote"),
	})
	result := make(chan error, 1)
	go func() { result <- applyRemoteFixture(db, context.Background(), batch) }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		_ = rows.Close()
		t.Fatal("remote Pebble commit blocked on the active SQLite reader")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), "UPDATE contacts SET name=name WHERE id=?", local[:]); err != nil {
		t.Fatal(err)
	}
	if got := len(dumpSQL(t, db)); got != 2 {
		t.Fatalf("rows after local commit = %d, want 2", got)
	}
	assertConverged(t, db)
}

func TestLocalWriteFlushesPendingRemoteRows(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.QueryStore.RemoteApplyInterval = time.Hour
	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := NewRowID()
	batch := remoteBatchCells(db, NewNodeID(), 1, 100, "contacts", row, map[string]codec.Value{
		"id": codec.Blob(row[:]), "name": codec.Text("remote"),
	})
	if err := applyRemoteFixture(db, context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(dumpSQL(t, db)) != 0 {
		t.Fatal("remote row visible before flush")
	}
	if _, err := db.ExecContext(context.Background(), "UPDATE contacts SET name=? WHERE id=?", "local", row[:]); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.QueryRowContext(context.Background(), "SELECT name FROM contacts WHERE id=?", row[:]).Scan(&name); err != nil || name != "local" {
		t.Fatalf("local update after pending remote row: name=%q err=%v", name, err)
	}
	assertConverged(t, db)
}
