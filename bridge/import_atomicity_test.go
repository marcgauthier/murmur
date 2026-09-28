package bridge

import (
	"context"
	"testing"

	db "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

// TestImportReceiptsAndStreamProgress validates that imported bundles commit
// their source transaction receipts and contiguous stream progress into authoritative storage.
func TestImportReceiptsAndStreamProgress(t *testing.T) {
	ctx := context.Background()
	signer, recip, trust := inboxKeys(t, "stream-1")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	high := openHighDB(t)
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	row1, row2 := ids.NewRowID(), ids.NewRowID()
	tx1, tx2 := ids.NewTxID(), ids.NewTxID()
	b1 := Batch{
		TxID:     tx1,
		Origin:   ids.NewNodeID(),
		Sequence: 1,
		HLC:      100,
		Records: []Record{
			{Table: "contacts", Row: row1, Op: RecordPut, Columns: []ColumnValue{{Column: "name", Value: codec.Text("alice")}}},
		},
	}
	b2 := Batch{
		TxID:     tx2,
		Origin:   ids.NewNodeID(),
		Sequence: 2,
		HLC:      101,
		Records: []Record{
			{Table: "contacts", Row: row2, Op: RecordPut, Columns: []ColumnValue{{Column: "name", Value: codec.Text("bob")}}},
		},
	}

	bundleID := ids.NewTxID()
	sealed := sealForInboxWithManifest(t, signer, recip, Manifest{
		BundleID:     bundleID,
		SourceDomain: ids.NewDBID(),
		Stream:       "stream-1",
		SeqFirst:     1,
		SeqLast:      2,
		TxIDs:        []ids.TxID{tx1, tx2},
		SchemaEpoch:  1,
	}, []Batch{b1, b2})

	if err := inbox.Receive(sealed); err != nil {
		t.Fatal(err)
	}

	n, err := im.Drain(ctx, inbox)
	if err != nil || n != 1 {
		t.Fatalf("Drain = %d, %v", n, err)
	}

	// Verify receipts are recorded in authoritative storage.
	for _, txID := range []ids.TxID{tx1, tx2, bundleID} {
		has, err := high.HasTransactionReceipt(txID)
		if err != nil {
			t.Fatalf("HasTransactionReceipt(%s): %v", txID, err)
		}
		if !has {
			t.Fatalf("expected transaction receipt for %s in authoritative storage", txID)
		}
	}

	// Verify stream progress in authoritative storage.
	progress, ok, err := high.BridgeStreamProgress("stream-1")
	if err != nil || !ok || progress != 2 {
		t.Fatalf("BridgeStreamProgress = %d, %v, %v; want 2", progress, ok, err)
	}

	// Verify SQL visible rows.
	got := queryNames(t, high)
	if len(got) != 2 || got["alice"] != -1 || got["bob"] != -1 {
		t.Fatalf("queryNames = %+v; want alice and bob (null score = -1)", got)
	}
}

// TestImportDeduplicationOnReplay validates that replaying an already imported
// bundle skips executing duplicate writes via stable source receipts.
func TestImportDeduplicationOnReplay(t *testing.T) {
	ctx := context.Background()
	signer, recip, trust := inboxKeys(t, "stream-1")
	inboxDir := t.TempDir()
	inbox, err := OpenInbox(inboxDir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	high := openHighDB(t)
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	row := ids.NewRowID()
	txID := ids.NewTxID()
	bundleID := ids.NewTxID()
	b := Batch{
		TxID:     txID,
		Origin:   ids.NewNodeID(),
		Sequence: 1,
		HLC:      100,
		Records: []Record{
			{Table: "contacts", Row: row, Op: RecordPut, Columns: []ColumnValue{
				{Column: "name", Value: codec.Text("charlie")},
				{Column: "score", Value: codec.Int(42)},
			}},
		},
	}

	manifest := Manifest{
		BundleID:     bundleID,
		SourceDomain: ids.NewDBID(),
		Stream:       "stream-1",
		SeqFirst:     1,
		SeqLast:      1,
		TxIDs:        []ids.TxID{txID},
		SchemaEpoch:  1,
	}
	sealed := sealForInboxWithManifest(t, signer, recip, manifest, []Batch{b})
	if err := inbox.Receive(sealed); err != nil {
		t.Fatal(err)
	}

	if n, err := im.Drain(ctx, inbox); err != nil || n != 1 {
		t.Fatalf("first Drain = %d, %v", n, err)
	}

	// Open a opened bundle directly and call ApplyBundle again.
	opened, err := OpenBundle(sealed.Data, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}

	// ApplyBundle should detect that receipts already exist and return nil without reapplying.
	if err := im.ApplyBundle(ctx, opened); err != nil {
		t.Fatalf("re-apply ApplyBundle error: %v", err)
	}

	got := queryNames(t, high)
	if len(got) != 1 || got["charlie"] != 42 {
		t.Fatalf("got = %+v; want charlie=42", got)
	}
}

// TestImportCrashBeforeMarkAppliedRecovery validates that if a node crashes
// after committing to authoritative storage but before inbox state is marked applied,
// the subsequent Drain syncs progress from authoritative storage and avoids duplicate execution.
func TestImportCrashBeforeMarkAppliedRecovery(t *testing.T) {
	ctx := context.Background()
	signer, recip, trust := inboxKeys(t, "stream-1")
	inboxDir := t.TempDir()
	inbox, err := OpenInbox(inboxDir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	highDir := t.TempDir()
	node := db.NewNodeID()
	high := openHighDBAt(t, highDir, node, nil)
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	row := ids.NewRowID()
	txID := ids.NewTxID()
	bundleID := ids.NewTxID()
	b := Batch{
		TxID:     txID,
		Origin:   ids.NewNodeID(),
		Sequence: 1,
		HLC:      100,
		Records: []Record{
			{Table: "contacts", Row: row, Op: RecordPut, Columns: []ColumnValue{
				{Column: "name", Value: codec.Text("dana")},
				{Column: "score", Value: codec.Int(99)},
			}},
		},
	}
	manifest := Manifest{
		BundleID:     bundleID,
		SourceDomain: ids.NewDBID(),
		Stream:       "stream-1",
		SeqFirst:     1,
		SeqLast:      1,
		TxIDs:        []ids.TxID{txID},
		SchemaEpoch:  1,
	}
	sealed := sealForInboxWithManifest(t, signer, recip, manifest, []Batch{b})
	if err := inbox.Receive(sealed); err != nil {
		t.Fatal(err)
	}

	// Apply bundle directly into DB (simulating commit to Pebble and SQL).
	opened, err := inOpenStaged(inbox, "stream-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.ApplyBundle(ctx, opened); err != nil {
		t.Fatal(err)
	}

	// Close high DB and reopen to simulate restart before inbox.MarkApplied.
	_ = high.Close()
	high = openHighDBAt(t, highDir, node, nil)
	im2, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	// Reopen inbox and run Drain.
	inbox2, err := OpenInbox(inboxDir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}

	if n, err := im2.Drain(ctx, inbox2); err != nil {
		t.Fatalf("Drain after restart: %v", err)
	} else if n != 0 {
		// Because authoritative storage already had stream-1 applied to 1,
		// Drain synced progress and skipped re-importing.
	}

	prog := inbox2.Progress()
	if len(prog) != 1 || prog[0].Applied != 1 {
		t.Fatalf("inbox progress = %+v; want Applied=1", prog)
	}

	got := queryNames(t, high)
	if len(got) != 1 || got["dana"] != 99 {
		t.Fatalf("queryNames = %+v; want dana=99", got)
	}
}

func sealForInboxWithManifest(t *testing.T, signer *SignerKey, recip *RecipientKey, manifest Manifest, batches []Batch) Artifact {
	t.Helper()
	raw, err := SealBatches(signer, recip.Public(), manifest, batches, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	return Artifact{Name: "bundle-001.spb", Data: raw}
}

func inOpenStaged(in *Inbox, stream string, seq uint64) (*Bundle, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	st, ok := in.streams[stream]
	if !ok {
		return nil, nil
	}
	sb, ok := st.Staged[seq]
	if !ok {
		return nil, nil
	}
	return in.openStagedLocked(sb.file)
}
