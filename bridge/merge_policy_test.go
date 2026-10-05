package bridge

import (
	"context"
	"math/big"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/internal/testdb"
	"github.com/marcgauthier/murmur/schema"
)

// Exercise the actual capture, signed envelope, import and provenance paths.
func TestPolicyBundleReplayAcrossStreamsAndHighOwnership(t *testing.T) {
	ctx := context.Background()
	tables := []schema.TableSchema{{Name: "items", Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}, {Name: "count", Type: schema.ColText, MergePolicy: schema.PN_COUNTER}, {Name: "tags", Type: schema.ColText, MergePolicy: schema.OR_SET}, {Name: "maxv", Type: schema.ColInteger, MergePolicy: schema.MAX}}}}
	open := func() (*db.DB, db.DBID) {
		cfg := testdb.Configure(db.Config{Path: t.TempDir(), DBID: db.NewDBID(), NodeID: db.NewNodeID(), Encryption: db.EncryptionConfig{Key: make([]byte, 32), KeyID: "test"}, Schema: db.SchemaConfig{Version: 1, Tables: tables}})
		d, err := db.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { d.Close() })
		return d, cfg.DBID
	}
	low, domain := open()
	high, _ := open()
	row := db.NewRowID()
	if _, err := low.ExecContext(ctx, "INSERT INTO items VALUES (?, '0','[]',10)", row[:]); err != nil {
		t.Fatal(err)
	}
	tx, err := low.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(10)); err != nil {
		t.Fatal(err)
	}
	if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(2)); err != nil {
		t.Fatal(err)
	}
	if err = tx.SetAdd(ctx, "items", "tags", row, db.SetString("red")); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	outbox, err := OpenOutbox(t.TempDir(), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := low.BridgeSchema()
	if err != nil {
		t.Fatal(err)
	}
	capture, err := NewCapturer(low.BridgeLogSource(), resolver, outbox, 64, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	signer, recipient, trust := inboxKeys(t, "s")
	if err = trust.AddSigner(signer.ID, "s", "other"); err != nil {
		t.Fatal(err)
	}
	importer, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	deliver := func(stream string) {
		t.Helper()
		if _, err := capture.CaptureOnce(ctx); err != nil {
			t.Fatal(err)
		}
		pending := outbox.Pending(0)
		batches := make([]Batch, len(pending))
		ids := make([]db.TxID, len(pending))
		for i, event := range pending {
			batches[i] = event.Batch
			ids[i] = event.Batch.TxID
		}
		manifest := Manifest{SourceDomain: domain, Stream: stream, SeqFirst: 1, SeqLast: uint64(len(batches)), TxIDs: ids}
		raw, err := SealBatches(signer, recipient.ID, manifest, batches, Limits{}.withDefaults())
		if err != nil {
			t.Fatal(err)
		}
		bundle, err := OpenBundle(raw, trust, Limits{}.withDefaults())
		if err != nil {
			t.Fatal(err)
		}
		if err = importer.ApplyBundle(ctx, bundle); err != nil {
			t.Fatal(err)
		}
		if err = importer.ApplyBundle(ctx, bundle); err != nil {
			t.Fatal(err)
		}
	}
	check := func(count, tags string, max int64) {
		t.Helper()
		var c, s string
		var m int64
		if err := high.QueryRowContext(ctx, "SELECT count,tags,maxv FROM items WHERE id=?", row[:]).Scan(&c, &s, &m); err != nil {
			t.Fatal(err)
		}
		if c != count || s != tags || m != max {
			t.Fatalf("got %s %s %d, want %s %s %d", c, s, m, count, tags, max)
		}
	}
	deliver("s")
	deliver("other")
	check("12", `[{"type":"string","value":"red"}]`, 10)
	tx, err = high.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(3)); err != nil {
		t.Fatal(err)
	}
	if err = tx.SetAdd(ctx, "items", "tags", row, db.SetString("high")); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "UPDATE items SET maxv=5 WHERE id=?", row[:]); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	check("15", `[{"type":"string","value":"red"},{"type":"string","value":"high"}]`, 10)
	tx, err = low.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(8)); err != nil {
		t.Fatal(err)
	}
	if err = tx.SetRemove(ctx, "items", "tags", row, db.SetString("red")); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "UPDATE items SET maxv=20 WHERE id=?", row[:]); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	deliver("s")
	check("15", `[{"type":"string","value":"red"},{"type":"string","value":"high"}]`, 10)
	for _, column := range []string{"count", "tags", "maxv"} {
		if err = high.ReleaseBridgeOwnership(ctx, "items", row, column); err != nil {
			t.Fatal(err)
		}
	}
	check("20", "[]", 20)
}
