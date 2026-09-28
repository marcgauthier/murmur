package replicateddb

import (
	"context"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

func TestBridgeOwnershipPolicyReplicatesWithHighMutations(t *testing.T) {
	ctx := context.Background()
	cluster := ids.NewDBID()
	cfgA, cfgB := testConfig(t.TempDir()), testConfig(t.TempDir())
	cfgA.DBID, cfgB.DBID = cluster, cluster
	a, err := Open(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	row := ids.NewRowID()
	lowSource, stream := ids.NewDBID(), "mesh-policy"
	tx, err := a.BeginBridgeImportTx(ctx, ids.NewTxID(), lowSource, stream, ids.NewTxID(), 1, 1, false, &TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)`, row[:], "low", 1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ExecContext(ctx, `UPDATE contacts SET name=? WHERE id=?`, "high", row[:]); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		var batch *codec.MutationBatch
		_, err := a.store.LogScan(a.cfg.NodeID, seq, 1, 4<<20, func(b *codec.MutationBatch) error { batch = b; return nil })
		if err != nil || batch == nil {
			t.Fatalf("read source sequence %d: batch=%v err=%v", seq, batch, err)
		}
		if err := b.ApplyRemote(ctx, batch); err != nil {
			t.Fatalf("apply sequence %d: %v", seq, err)
		}
	}
	rowPolicy, ok, err := b.BridgeRowProvenance("contacts", row)
	if err != nil || !ok || rowPolicy.SourceDomain != lowSource || rowPolicy.Stream != stream {
		t.Fatalf("replicated row policy=%+v present=%v err=%v", rowPolicy, ok, err)
	}
	fieldPolicy, ok, err := b.BridgeFieldProvenance("contacts", row, "name")
	if err != nil || !ok || fieldPolicy.Owner != BridgeOwnerHigh {
		t.Fatalf("replicated field policy=%+v present=%v err=%v", fieldPolicy, ok, err)
	}
	var name string
	if err := b.QueryRowContext(ctx, `SELECT name FROM contacts WHERE id=?`, row[:]).Scan(&name); err != nil || name != "high" {
		t.Fatalf("replicated materialized value=%q err=%v", name, err)
	}
}
