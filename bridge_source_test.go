package replicateddb

import (
	"context"
	"fmt"
	"testing"

	"github.com/marcgauthier/spedsql/codec"
)

func TestBridgeLogSource(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)`, id[:], "ann", 3); err != nil {
		t.Fatal(err)
	}
	src := db.BridgeLogSource()
	origins, err := src.KnownOrigins(ctx)
	if err != nil || len(origins) != 1 || origins[0] != db.NodeID() {
		t.Fatalf("origins = %v, %v", origins, err)
	}
	var got []*codec.MutationBatch
	last, err := src.ScanLog(ctx, db.NodeID(), 1, 64, 1<<20, func(mb *codec.MutationBatch) error {
		got = append(got, mb)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if last != 1 || len(got) != 1 {
		t.Fatalf("scan = %d batches ending %d", len(got), last)
	}
	if got[0].TxID.IsZero() || got[0].OriginNode != db.NodeID() || len(got[0].Mutations) == 0 {
		t.Fatalf("batch = %+v", got[0])
	}
}

func TestBridgeSchema(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "ann"); err != nil {
		t.Fatal(err)
	}
	resolver, err := db.BridgeSchema()
	if err != nil {
		t.Fatal(err)
	}
	// Resolve the IDs carried by the real commit (IDs are hash-derived).
	src := db.BridgeLogSource()
	_, err = src.ScanLog(ctx, db.NodeID(), 1, 8, 1<<20, func(mb *codec.MutationBatch) error {
		for _, m := range mb.Mutations {
			table, err := resolver.TableName(m.TableID)
			if err != nil || table != "contacts" {
				t.Fatalf("table %d = %q, %v", m.TableID, table, err)
			}
			if _, err := resolver.ColumnName(m.TableID, m.ColumnID); err != nil {
				t.Fatalf("column (%d,%d): %v", m.TableID, m.ColumnID, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.TableName(0xFFFFFFFE); err == nil {
		t.Fatal("unknown table resolved")
	}
}

func TestBridgeLogSourceProtectResume(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		id := NewRowID()
		if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)`, id[:], fmt.Sprintf("u%d", i), i); err != nil {
			t.Fatal(err)
		}
	}

	src := db.BridgeLogSource()
	// Protect at resume sequence 2.
	if err := src.ProtectResume(db.NodeID(), 2); err != nil {
		t.Fatal(err)
	}

	// Attempt aggressive GC for all sequences.
	n, err := db.store.CollectLog(db.NodeID(), ^uint64(0), 1<<62, 0)
	if err != nil {
		t.Fatalf("CollectLog: %v", err)
	}
	// Entries 1..2 can be collected, but entries 3..5 (> 2) must be preserved.
	if n != 2 {
		t.Fatalf("CollectLog deleted %d entries, want 2", n)
	}

	// Release protection and verify remaining entries can now be collected.
	if err := src.ReleaseProtection(db.NodeID()); err != nil {
		t.Fatal(err)
	}
	n2, err := db.store.CollectLog(db.NodeID(), ^uint64(0), 1<<62, 0)
	if err != nil {
		t.Fatalf("CollectLog after release: %v", err)
	}
	if n2 != 3 {
		t.Fatalf("CollectLog after release deleted %d entries, want 3", n2)
	}
}
