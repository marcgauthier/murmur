package bridge

import (
	"bytes"
	"context"
	"strings"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

// TestMixedBundleCrashBetweenRowCommitAndFileCommit tests the multi-commit
// failure boundary where row mutations have committed to Spool
// but the node crashes before file metadata is committed. Replay must
// converge both row and file state without duplicate rows or partial progress.
func TestMixedBundleCrashBetweenRowCommitAndFileCommit(t *testing.T) {
	ctx := context.Background()
	signer, recip, trust := inboxKeys(t, "mixed-stream")
	inboxDir := t.TempDir()
	inbox, err := OpenInbox(inboxDir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}

	highDir := t.TempDir()
	highKey := bytes.Repeat([]byte{0x55}, 32)
	nodeID := db.NewNodeID()
	high := openFileDBAtNode(t, highDir, nodeID, highKey)
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	rowAlice := ids.NewRowID()
	rowBob := ids.NewRowID()
	fileRow := db.BridgeFileRowID("reports/summary.pdf")
	fileDigest := mustDigest([]byte("summary pdf contents"))
	fileSize := int64(len("summary pdf contents"))

	txRow1 := ids.NewTxID()
	txRow2 := ids.NewTxID()
	txFile := ids.NewTxID()
	bundleID := ids.NewTxID()
	sourceDomain := ids.NewDBID()

	b1 := Batch{
		TxID:     txRow1,
		Origin:   ids.NewNodeID(),
		Sequence: 1,
		HLC:      100,
		Records: []Record{
			{Table: "contacts", Row: rowAlice, Op: RecordPut, Columns: []ColumnValue{
				{Column: "name", Value: codec.Text("alice")},
				{Column: "score", Value: codec.Int(10)},
			}},
		},
	}
	b2 := Batch{
		TxID:     txRow2,
		Origin:   ids.NewNodeID(),
		Sequence: 2,
		HLC:      101,
		Records: []Record{
			{Table: "contacts", Row: rowBob, Op: RecordPut, Columns: []ColumnValue{
				{Column: "name", Value: codec.Text("bob")},
				{Column: "score", Value: codec.Int(20)},
			}},
		},
	}
	b3 := Batch{
		TxID:     txFile,
		Origin:   ids.NewNodeID(),
		Sequence: 3,
		HLC:      102,
		Records: []Record{
			{Table: db.BridgeFileTableName, Row: fileRow, Op: RecordPut, Columns: []ColumnValue{
				{Column: "id", Value: codec.Blob(fileRow[:])},
				{Column: "name", Value: codec.Text("reports/summary.pdf")},
				{Column: "digest", Value: codec.Blob(fileDigest[:])},
				{Column: "size", Value: codec.Int(fileSize)},
			}},
		},
	}

	manifest := Manifest{
		BundleID:     bundleID,
		SourceDomain: sourceDomain,
		Stream:       "mixed-stream",
		SeqFirst:     1,
		SeqLast:      3,
		TxIDs:        []ids.TxID{txRow1, txRow2, txFile},
		SchemaEpoch:  1,
	}

	rows := encodeTypedContactsBatches(t, []Batch{b1, b2, b3})
	sealed := sealForInboxWithManifest(t, signer, recip, manifest, rows)
	if err := inbox.Receive(sealed); err != nil {
		t.Fatal(err)
	}

	opened, err := inOpenStaged(inbox, "mixed-stream", 1)
	if err != nil || opened == nil {
		t.Fatalf("open staged: %v", err)
	}

	// 1. Inject fault: crash after row transaction commits, before file metadata commits.
	crashTriggered := false
	im.beforeFileApply = func() {
		crashTriggered = true
		panic("simulated crash before file metadata apply")
	}

	var applyErr error
	func() {
		defer func() { _ = recover() }()
		applyErr = im.ApplyBundle(ctx, opened)
	}()

	if !crashTriggered {
		t.Fatalf("simulated crash was not triggered, applyErr: %v", applyErr)
	}

	// 2. Verify intermediate crash state:
	// - Rows alice and bob are committed to typed storage
	// - File metadata is not yet committed
	// - Bundle receipt is absent
	// - Stream progress is not yet 3
	names := queryNames(t, high)
	if len(names) != 2 || names["alice"] != 10 || names["bob"] != 20 {
		t.Fatalf("intermediate row state = %+v, want alice=10, bob=20", names)
	}

	if st, err := high.FileStatus(ctx, "reports/summary.pdf"); err != nil || st.Exists {
		t.Fatalf("file metadata should not exist before file commit: %+v, err=%v", st, err)
	}

	hasReceipt, err := high.HasTransactionReceipt(bundleID)
	if err != nil || hasReceipt {
		t.Fatalf("bundle receipt should be absent: has=%v, err=%v", hasReceipt, err)
	}

	progress, ok, err := high.BridgeStreamProgress("mixed-stream")
	if err != nil || ok || progress != 0 {
		t.Fatalf("stream progress should be 0 (ok=%v), got %d, err=%v", ok, progress, err)
	}

	// 3. Simulate process restart.
	_ = high.Close()
	high = openFileDBAtNode(t, highDir, nodeID, highKey)
	im2, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	// 4. Replay the exact same mixed bundle.
	if err := im2.ApplyBundle(ctx, opened); err != nil {
		t.Fatalf("replay ApplyBundle failed: %v", err)
	}

	// 5. Verify post-replay state:
	// - Rows alice and bob are still intact (no duplicates or error)
	names = queryNames(t, high)
	if len(names) != 2 || names["alice"] != 10 || names["bob"] != 20 {
		t.Fatalf("post-replay row state = %+v, want alice=10, bob=20", names)
	}

	// - File metadata is now committed
	status, err := high.FileStatus(ctx, "reports/summary.pdf")
	if err != nil {
		t.Fatalf("FileStatus failed after replay: %v", err)
	}
	if status.Name != "reports/summary.pdf" || status.Size != fileSize || status.Digest != fileDigest {
		t.Fatalf("FileStatus = %+v, want size=%d digest=%x", status, fileSize, fileDigest)
	}

	// - Bundle receipt and stream progress are recorded
	hasReceipt, err = high.HasTransactionReceipt(bundleID)
	if err != nil || !hasReceipt {
		t.Fatalf("bundle receipt missing after replay: %v, %v", hasReceipt, err)
	}

	progress, ok, err = high.BridgeStreamProgress("mixed-stream")
	if err != nil || !ok || progress != 3 {
		t.Fatalf("stream progress = %d (ok=%v), want 3", progress, ok)
	}

	// 6. Deduplicated fast-path: third apply is a complete no-op
	if err := im2.ApplyBundle(ctx, opened); err != nil {
		t.Fatalf("subsequent ApplyBundle failed: %v", err)
	}
}

// TestMixedBundleCrashBetweenFileCommitAndCompletion tests the boundary
// where both rows and file metadata have committed, but the node crashes
// before CompleteBridgeImport writes the terminal bundle receipt and progress.
func TestMixedBundleCrashBetweenFileCommitAndCompletion(t *testing.T) {
	ctx := context.Background()
	signer, recip, trust := inboxKeys(t, "mixed-complete-fault")
	inboxDir := t.TempDir()
	inbox, err := OpenInbox(inboxDir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}

	highDir := t.TempDir()
	highKey := bytes.Repeat([]byte{0x66}, 32)
	nodeID := db.NewNodeID()
	high := openFileDBAtNode(t, highDir, nodeID, highKey)
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	rowID := ids.NewRowID()
	fileRow := db.BridgeFileRowID("logs/audit.log")
	fileDigest := mustDigest([]byte("audit log contents"))
	fileSize := int64(len("audit log contents"))

	txRow := ids.NewTxID()
	txFile := ids.NewTxID()
	bundleID := ids.NewTxID()
	sourceDomain := ids.NewDBID()

	b1 := Batch{
		TxID:     txRow,
		Origin:   ids.NewNodeID(),
		Sequence: 1,
		HLC:      100,
		Records: []Record{
			{Table: "contacts", Row: rowID, Op: RecordPut, Columns: []ColumnValue{
				{Column: "name", Value: codec.Text("charlie")},
				{Column: "score", Value: codec.Int(50)},
			}},
		},
	}
	b2 := Batch{
		TxID:     txFile,
		Origin:   ids.NewNodeID(),
		Sequence: 2,
		HLC:      101,
		Records: []Record{
			{Table: db.BridgeFileTableName, Row: fileRow, Op: RecordPut, Columns: []ColumnValue{
				{Column: "id", Value: codec.Blob(fileRow[:])},
				{Column: "name", Value: codec.Text("logs/audit.log")},
				{Column: "digest", Value: codec.Blob(fileDigest[:])},
				{Column: "size", Value: codec.Int(fileSize)},
			}},
		},
	}

	manifest := Manifest{
		BundleID:     bundleID,
		SourceDomain: sourceDomain,
		Stream:       "mixed-complete-fault",
		SeqFirst:     1,
		SeqLast:      2,
		TxIDs:        []ids.TxID{txRow, txFile},
		SchemaEpoch:  1,
	}

	rows := encodeTypedContactsBatches(t, []Batch{b1, b2})
	sealed := sealForInboxWithManifest(t, signer, recip, manifest, rows)
	if err := inbox.Receive(sealed); err != nil {
		t.Fatal(err)
	}

	opened, err := inOpenStaged(inbox, "mixed-complete-fault", 1)
	if err != nil || opened == nil {
		t.Fatalf("open staged: %v", err)
	}

	// 1. Crash after file metadata commits, before completeBundle.
	crashTriggered := false
	im.beforeCompletion = func() {
		crashTriggered = true
		panic("simulated crash before completeBundle")
	}

	var applyErr error
	func() {
		defer func() { _ = recover() }()
		applyErr = im.ApplyBundle(ctx, opened)
	}()

	if !crashTriggered {
		t.Fatalf("simulated crash was not triggered, applyErr: %v", applyErr)
	}

	// 2. Restart and replay.
	_ = high.Close()
	high = openFileDBAtNode(t, highDir, nodeID, highKey)
	im2, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	if err := im2.ApplyBundle(ctx, opened); err != nil {
		t.Fatalf("replay ApplyBundle failed: %v", err)
	}

	// 3. Verify row, file metadata, receipts, and progress.
	names := queryNames(t, high)
	if len(names) != 1 || names["charlie"] != 50 {
		t.Fatalf("post-replay row state = %+v, want charlie=50", names)
	}

	status, err := high.FileStatus(ctx, "logs/audit.log")
	if err != nil || status.Name != "logs/audit.log" || status.Size != fileSize || status.Digest != fileDigest {
		t.Fatalf("FileStatus = %+v, err=%v", status, err)
	}

	hasReceipt, err := high.HasTransactionReceipt(bundleID)
	if err != nil || !hasReceipt {
		t.Fatalf("bundle receipt missing: %v, %v", hasReceipt, err)
	}

	progress, ok, err := high.BridgeStreamProgress("mixed-complete-fault")
	if err != nil || !ok || progress != 2 {
		t.Fatalf("stream progress = %d (ok=%v), want 2", progress, ok)
	}
}

// TestMixedBundleFileObjectArrivalOrder tests both object delivery orderings:
// Case A: File object bytes are installed in object store first (out of order), then mixed bundle applies.
// Case B: Mixed bundle applies metadata first (reporting Pending), then file object bytes are installed.
func TestMixedBundleFileObjectArrivalOrder(t *testing.T) {
	ctx := context.Background()
	highKey := bytes.Repeat([]byte{0x99}, 32)
	nodeID := db.NewNodeID()
	high := openFileDBAtNode(t, t.TempDir(), nodeID, highKey)

	// Case A: Bytes installed first into objectstore
	contentA := "pre-delivered file content payload"
	digestA := mustDigest([]byte(contentA))
	sizeA := int64(len(contentA))
	nameA := "async/pre_delivered.dat"
	rowA := db.BridgeFileRowID(nameA)

	putDigestA, putSizeA, err := high.BridgePutFileObject(ctx, strings.NewReader(contentA))
	if err != nil || putDigestA != digestA || putSizeA != sizeA {
		t.Fatalf("BridgePutFileObject failed: digest=%x size=%d err=%v", putDigestA, putSizeA, err)
	}

	// Now apply metadata for nameA
	txA := ids.NewTxID()
	sourceA := ids.NewDBID()
	if err := high.BridgeApplyFileMetadata(ctx, sourceA, "stream-order", txA, 1, 1, false, []db.BridgeFilePut{
		{Row: rowA, Name: nameA, Digest: digestA, Size: sizeA},
	}, nil); err != nil {
		t.Fatalf("BridgeApplyFileMetadata failed: %v", err)
	}

	statusA, err := high.FileStatus(ctx, nameA)
	if err != nil {
		t.Fatalf("FileStatus A failed: %v", err)
	}
	if !statusA.Exists || !statusA.Available {
		t.Fatalf("statusA should be Exists && Available, got exists=%v, available=%v", statusA.Exists, statusA.Available)
	}

	// Case B: Metadata applied first (Exists, !Available), then bytes installed
	contentB := "post-delivered file content payload"
	digestB := mustDigest([]byte(contentB))
	sizeB := int64(len(contentB))
	nameB := "async/post_delivered.dat"
	rowB := db.BridgeFileRowID(nameB)

	txB := ids.NewTxID()
	if err := high.BridgeApplyFileMetadata(ctx, sourceA, "stream-order", txB, 2, 2, false, []db.BridgeFilePut{
		{Row: rowB, Name: nameB, Digest: digestB, Size: sizeB},
	}, nil); err != nil {
		t.Fatalf("BridgeApplyFileMetadata B failed: %v", err)
	}

	statusB, err := high.FileStatus(ctx, nameB)
	if err != nil {
		t.Fatalf("FileStatus B before bytes failed: %v", err)
	}
	if !statusB.Exists || statusB.Available {
		t.Fatalf("statusB before bytes should be Exists && !Available, got exists=%v, available=%v", statusB.Exists, statusB.Available)
	}

	// Now deliver bytes
	putDigestB, putSizeB, err := high.BridgePutFileObject(ctx, strings.NewReader(contentB))
	if err != nil || putDigestB != digestB || putSizeB != sizeB {
		t.Fatalf("BridgePutFileObject B failed: %v", err)
	}

	statusBAfter, err := high.FileStatus(ctx, nameB)
	if err != nil {
		t.Fatalf("FileStatus B after bytes failed: %v", err)
	}
	if !statusBAfter.Exists || !statusBAfter.Available {
		t.Fatalf("statusB after bytes should be Exists && Available, got exists=%v, available=%v", statusBAfter.Exists, statusBAfter.Available)
	}
}
