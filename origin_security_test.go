package murmur

import (
	"context"
	"errors"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
)

func TestOriginPublicApplyAPIsCannotBypassVerification(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	writer := ids.NewNodeID()
	b := &codec.MutationBatch{ProtocolVersion: 4, TxID: ids.NewTxID(), OriginNode: writer, Sequence: 1, HLC: db.store.ClockNow(), SchemaEpoch: db.schemaIdentity().Epoch, SchemaHash: db.schemaIdentity().Hash, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("forgery")}}}
	testidentity.Sign(b, db.store.DBID())
	b.OriginSignature[0]++
	before := db.Status()
	if err := db.ApplyRemote(ctx, b); !errors.Is(err, codec.ErrOriginSignature) {
		t.Fatalf("public apply: %v", err)
	}
	second := *b
	second.TxID = ids.NewTxID()
	second.Sequence = 2
	if err := db.ApplyRemoteGroup(ctx, []*codec.MutationBatch{b, &second}); !errors.Is(err, codec.ErrOriginSignature) {
		t.Fatalf("public group: %v", err)
	}
	after := db.Status()
	if after.HLC != before.HLC || after.StateGeneration != before.StateGeneration {
		t.Fatal("invalid public apply changed state")
	}
	if yes, err := db.HasTransactionReceipt(b.TxID); err != nil || yes {
		t.Fatalf("invalid public apply receipt: %v %v", yes, err)
	}
}

func TestOriginCredentialsRequiredForOfflineWriters(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.OriginSigning = OriginSigningConfig{}
	if db, err := Open(context.Background(), cfg); err == nil {
		_ = db.Close()
		t.Fatal("offline writer accepted without identity")
	}
}

func TestOriginLogInspectionPreservesProofAcrossBridgeProjection(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id,name) VALUES (?,?)", row[:], "signed"); err != nil {
		t.Fatal(err)
	}
	var complete, projected *codec.MutationBatch
	_, err = db.ScanReplicationLog(ctx, cfg.NodeID, 1, 1, 1<<20, func(b *codec.MutationBatch) error { complete = b; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if complete == nil || db.store.VerifyOrigin(complete) != nil {
		t.Fatal("complete log lost proof")
	}
	_, err = db.BridgeLogSource().ScanLog(ctx, cfg.NodeID, 1, 1, 1<<20, func(b *codec.MutationBatch) error { projected = b; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if projected == nil {
		t.Fatal("projection missing")
	}
	if len(projected.Mutations) < len(complete.Mutations) && projected.SignatureVersion != 0 {
		t.Fatal("projection retained invalid origin proof")
	}
}
