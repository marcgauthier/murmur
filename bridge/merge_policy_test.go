package bridge

import (
	"context"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/internal/testidentity"
)

// Exercise the actual capture, signed envelope, import and provenance paths.
func TestPolicyBundleReplayAcrossStreamsAndHighOwnership(t *testing.T) {
	ctx := context.Background()
	options := db.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Count": 2, "Tags": 3, "Peak": 4},
		MergePolicies: map[string]db.RecordMergePolicy{
			"Count": db.RecordMergeCounter, "Tags": db.RecordMergeORSet, "Peak": db.RecordMergeMax,
		},
	}
	definition, err := db.Define[typedBridgeCRDTRecord]("items", 93, options)
	if err != nil {
		t.Fatal(err)
	}
	open := func(databaseID db.DBID) (*db.DB, *db.RecordTable[typedBridgeCRDTRecord]) {
		node := db.NewNodeID()
		cfg := db.Config{Path: t.TempDir(), NodeID: node, DBID: databaseID, OriginSigning: testidentity.Config(node),
			Encryption: db.EncryptionConfig{Key: make([]byte, 32), KeyID: "test"}, Tables: []db.TableDefinition{definition}}
		d, err := db.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { d.Close() })
		table, err := db.TableOf[typedBridgeCRDTRecord](d, "items")
		if err != nil {
			t.Fatal(err)
		}
		return d, table
	}
	low, lowTable := open(db.NewDBID())
	high, highTable := open(db.NewDBID())
	domain := low.DBID()
	row := db.NewRowID()
	if err := low.WriteTxContext(ctx, func(tx *db.Tx) error {
		return lowTable.Insert(tx, &typedBridgeCRDTRecord{ID: row, Peak: 10})
	}); err != nil {
		t.Fatal(err)
	}
	if err := low.WriteTxContext(ctx, func(tx *db.Tx) error {
		if err := db.RecordCounterAdd(tx, lowTable, row, "Count", 10); err != nil {
			return err
		}
		if err := db.RecordCounterAdd(tx, lowTable, row, "Count", 2); err != nil {
			return err
		}
		return db.RecordSetAdd(tx, lowTable, row, "Tags", "red")
	}); err != nil {
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
	check := func(count int64, tags []string, max int64) {
		t.Helper()
		got, err := highTable.Get(row)
		if err != nil {
			t.Fatal(err)
		}
		if got.Count != count || len(got.Tags) != len(tags) || got.Peak != max {
			t.Fatalf("got %+v, want count=%d tags=%v peak=%d", got, count, tags, max)
		}
		for i := range tags {
			if got.Tags[i] != tags[i] {
				t.Fatalf("got tags %v, want %v", got.Tags, tags)
			}
		}
	}
	deliver("s")
	deliver("other")
	check(12, []string{"red"}, 10)
	if err := high.WriteTxContext(ctx, func(tx *db.Tx) error {
		if err := db.RecordCounterAdd(tx, highTable, row, "Count", 3); err != nil {
			return err
		}
		if err := db.RecordSetAdd(tx, highTable, row, "Tags", "high"); err != nil {
			return err
		}
		return db.RecordMax(tx, highTable, row, "Peak", int64(5))
	}); err != nil {
		t.Fatal(err)
	}
	check(15, []string{"high", "red"}, 10)
	peer, peerTable := open(high.DBID())
	defer peer.Close()
	last, err := high.ScanReplicationLog(ctx, high.NodeID(), 1, 100, 4<<20, func(batch *codec.MutationBatch) error {
		return peer.ApplyRemote(ctx, batch)
	})
	if err != nil || last == 0 {
		t.Fatalf("replicate typed bridge ownership: last sequence=%d err=%v", last, err)
	}
	peerValue, err := peerTable.Get(row)
	if err != nil || peerValue.Count != 15 || len(peerValue.Tags) != 2 || peerValue.Peak != 10 {
		t.Fatalf("replicated typed bridge row=%+v err=%v", peerValue, err)
	}
	rowPolicy, ok, err := peer.BridgeRowProvenance("items", row)
	if err != nil || !ok || rowPolicy.SourceDomain != domain || rowPolicy.Stream != "other" {
		t.Fatalf("replicated typed row provenance=%+v present=%v err=%v", rowPolicy, ok, err)
	}
	fieldPolicy, ok, err := peer.BridgeFieldProvenance("items", row, "Count")
	if err != nil || !ok || fieldPolicy.Owner != db.BridgeOwnerHigh {
		t.Fatalf("replicated typed field provenance=%+v present=%v err=%v", fieldPolicy, ok, err)
	}
	if err := low.WriteTxContext(ctx, func(tx *db.Tx) error {
		if err := db.RecordCounterAdd(tx, lowTable, row, "Count", 8); err != nil {
			return err
		}
		if err := db.RecordSetRemove(tx, lowTable, row, "Tags", "red"); err != nil {
			return err
		}
		return db.RecordMax(tx, lowTable, row, "Peak", int64(20))
	}); err != nil {
		t.Fatal(err)
	}
	deliver("s")
	check(15, []string{"high", "red"}, 10)
	for _, column := range []string{"Count", "Tags", "Peak"} {
		if err = high.ReleaseBridgeOwnership(ctx, "items", row, column); err != nil {
			t.Fatal(err)
		}
	}
	check(20, nil, 20)
}
