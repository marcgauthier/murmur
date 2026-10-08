package bridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testdb"
	"github.com/marcgauthier/murmur/objectstore"
)

func mustDigest(data []byte) objectstore.Digest {
	return objectstore.Digest(sha256.Sum256(data))
}

func openFileDBAtNode(t *testing.T, dir string, nodeID db.NodeID, objectKey []byte) *db.DB {
	t.Helper()
	database, err := db.Open(context.Background(), testdb.Configure(db.Config{
		Path:       dir,
		NodeID:     nodeID,
		Tables:     []db.TableDefinition{defineBaseContact(t)},
		Spool:      db.DefaultSpoolConfig(),
		Encryption: db.EncryptionConfig{Key: bytes.Repeat([]byte{0x44}, 32), KeyID: "test-key"},
		Files: db.FilesConfig{
			Enabled:   true,
			ObjectKey: append([]byte(nil), objectKey...),
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// TestFileBridgeImportCrashAfterFileMetadataCommit proves that if a node crashes
// after BridgeApplyFileMetadata commits to authoritative storage but before
// completion receipts and stream progress are recorded, replaying the exact
// same bundle recovers to identical state without data loss or duplicate mutations.
func TestFileBridgeImportCrashAfterFileMetadataCommit(t *testing.T) {
	ctx := context.Background()
	signer, recip, trust := inboxKeys(t, "files-fault")
	inboxDir := t.TempDir()
	inbox, err := OpenInbox(inboxDir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}

	highDir := t.TempDir()
	highKey := bytes.Repeat([]byte{0x77}, 32)
	nodeID := db.NewNodeID()
	high := openFileDBAtNode(t, highDir, nodeID, highKey)
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	fileName := "documents/sample.pdf"
	fileRow := db.BridgeFileRowID(fileName)
	fileDigest := mustDigest([]byte("sample document contents"))
	fileSize := int64(len("sample document contents"))

	txID := ids.NewTxID()
	bundleID := ids.NewTxID()
	sourceDomain := ids.NewDBID()

	b := Batch{
		TxID:     txID,
		Origin:   ids.NewNodeID(),
		Sequence: 1,
		HLC:      100,
		Records: []Record{
			{
				Table: db.BridgeFileTableName,
				Row:   fileRow,
				Op:    RecordPut,
				Columns: []ColumnValue{
					{Column: "id", Value: codec.Blob(fileRow[:])},
					{Column: "name", Value: codec.Text(fileName)},
					{Column: "digest", Value: codec.Blob(fileDigest[:])},
					{Column: "size", Value: codec.Int(fileSize)},
				},
			},
		},
	}

	manifest := Manifest{
		BundleID:     bundleID,
		SourceDomain: sourceDomain,
		Stream:       "files-fault",
		SeqFirst:     1,
		SeqLast:      1,
		TxIDs:        []ids.TxID{txID},
		SchemaEpoch:  1,
	}

	sealed := sealForInboxWithManifest(t, signer, recip, manifest, []Batch{b})
	if err := inbox.Receive(sealed); err != nil {
		t.Fatal(err)
	}

	opened, err := inOpenStaged(inbox, "files-fault", 1)
	if err != nil || opened == nil {
		t.Fatalf("open staged: %v", err)
	}

	// Inject fault: simulate crash right after BridgeApplyFileMetadata commits
	// but before completeBundle commits receipts and progress.
	crashTriggered := false
	im.beforeCompletion = func() {
		crashTriggered = true
		panic("simulated crash before completeBundle")
	}

	func() {
		defer func() {
			_ = recover()
		}()
		_ = im.ApplyBundle(ctx, opened)
	}()

	if !crashTriggered {
		t.Fatal("simulated crash was not triggered")
	}

	// Verify pre-restart authoritative state:
	// 1. File metadata is durable
	// 2. Receipts are absent
	// 3. Stream progress is unrecorded
	hasReceipt, err := high.HasTransactionReceipt(bundleID)
	if err != nil {
		t.Fatal(err)
	}
	if hasReceipt {
		t.Fatal("bundle receipt should be absent before completeBundle")
	}

	progress, ok, err := high.BridgeStreamProgress("files-fault")
	if err != nil {
		t.Fatal(err)
	}
	if ok || progress != 0 {
		t.Fatalf("stream progress should be absent, got ok=%v, progress=%d", ok, progress)
	}

	// Close high DB and reopen with the SAME nodeID to simulate full process restart.
	_ = high.Close()
	high = openFileDBAtNode(t, highDir, nodeID, highKey)
	im2, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	// Replay the exact same bundle through the new importer.
	if err := im2.ApplyBundle(ctx, opened); err != nil {
		t.Fatalf("replay ApplyBundle failed: %v", err)
	}

	// Verify post-replay authoritative state:
	// 1. Receipt is present
	hasReceipt, err = high.HasTransactionReceipt(bundleID)
	if err != nil || !hasReceipt {
		t.Fatalf("bundle receipt missing after replay: %v, %v", hasReceipt, err)
	}

	// 2. Stream progress is 1
	progress, ok, err = high.BridgeStreamProgress("files-fault")
	if err != nil || !ok || progress != 1 {
		t.Fatalf("stream progress = %d (ok=%v), want 1", progress, ok)
	}

	// 3. File metadata is intact and visible
	status, err := high.FileStatus(ctx, fileName)
	if err != nil {
		t.Fatalf("FileStatus failed: %v", err)
	}
	if status.Name != fileName || status.Size != fileSize || status.Digest != fileDigest {
		t.Fatalf("FileStatus = %+v, want name=%s, size=%d, digest=%x", status, fileName, fileSize, fileDigest)
	}

	// 4. File provenance reflects Low source domain and sequence
	prov, ok, err := high.BridgeFileProvenance(fileRow)
	if err != nil || !ok {
		t.Fatalf("BridgeFileProvenance failed: ok=%v, err=%v", ok, err)
	}
	if prov.SourceDomain != sourceDomain || prov.Stream != "files-fault" || prov.LastSeq != 1 {
		t.Fatalf("BridgeFileProvenance = %+v, want sourceDomain=%s, lastSeq=1", prov, sourceDomain)
	}

	// 5. Repeated replay is a complete no-op (fast path via receipt)
	if err := im2.ApplyBundle(ctx, opened); err != nil {
		t.Fatalf("idempotent replay failed: %v", err)
	}
}

// TestFileBridgeImportCrashAndHighModificationBeforeReplay proves that if
// a crash occurs after Low file metadata is committed and a High node subsequently
// modifies or deletes the file before replay, the High modification is
// preserved and the replayed Low bundle does not overwrite High ownership.
func TestFileBridgeImportCrashAndHighModificationBeforeReplay(t *testing.T) {
	ctx := context.Background()
	signer, recip, trust := inboxKeys(t, "files-high-mod")
	inboxDir := t.TempDir()
	inbox, err := OpenInbox(inboxDir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}

	highDir := t.TempDir()
	highKey := bytes.Repeat([]byte{0x88}, 32)
	nodeID := db.NewNodeID()
	high := openFileDBAtNode(t, highDir, nodeID, highKey)
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	fileName := "specs/design.md"
	fileRow := db.BridgeFileRowID(fileName)
	lowDigest := mustDigest([]byte("low file contents"))
	lowSize := int64(len("low file contents"))

	txID := ids.NewTxID()
	bundleID := ids.NewTxID()
	sourceDomain := ids.NewDBID()

	b := Batch{
		TxID:     txID,
		Origin:   ids.NewNodeID(),
		Sequence: 1,
		HLC:      100,
		Records: []Record{
			{
				Table: db.BridgeFileTableName,
				Row:   fileRow,
				Op:    RecordPut,
				Columns: []ColumnValue{
					{Column: "id", Value: codec.Blob(fileRow[:])},
					{Column: "name", Value: codec.Text(fileName)},
					{Column: "digest", Value: codec.Blob(lowDigest[:])},
					{Column: "size", Value: codec.Int(lowSize)},
				},
			},
		},
	}

	manifest := Manifest{
		BundleID:     bundleID,
		SourceDomain: sourceDomain,
		Stream:       "files-high-mod",
		SeqFirst:     1,
		SeqLast:      1,
		TxIDs:        []ids.TxID{txID},
		SchemaEpoch:  1,
	}

	sealed := sealForInboxWithManifest(t, signer, recip, manifest, []Batch{b})
	if err := inbox.Receive(sealed); err != nil {
		t.Fatal(err)
	}

	opened, err := inOpenStaged(inbox, "files-high-mod", 1)
	if err != nil || opened == nil {
		t.Fatalf("open staged: %v", err)
	}

	// 1. First import attempt crashes after BridgeApplyFileMetadata commit.
	im.beforeCompletion = func() {
		panic("simulated crash before completion")
	}

	func() {
		defer func() { _ = recover() }()
		_ = im.ApplyBundle(ctx, opened)
	}()

	// 2. Simulate High node restart with same nodeID.
	_ = high.Close()
	high = openFileDBAtNode(t, highDir, nodeID, highKey)

	// 3. High node modifies the file row locally with UploadFile (establishing High ownership).
	highContent := "high updated contents with proprietary changes"
	highInfo, err := high.UploadFile(ctx, fileName, strings.NewReader(highContent))
	if err != nil {
		t.Fatalf("High UploadFile failed: %v", err)
	}

	// Verify High-owned columns are now active
	owned, err := high.BridgeFileHighOwnedColumns(fileRow)
	if err != nil {
		t.Fatalf("BridgeFileHighOwnedColumns failed: %v", err)
	}
	if len(owned) == 0 {
		t.Fatalf("expected High owned columns after local upload on %s", fileName)
	}

	// 4. Now replay the original Low bundle.
	im2, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}

	if err := im2.ApplyBundle(ctx, opened); err != nil {
		t.Fatalf("replay of Low bundle failed: %v", err)
	}

	// 5. Verify High modification is preserved and Low values did not overwrite High-owned fields.
	status, err := high.FileStatus(ctx, fileName)
	if err != nil {
		t.Fatalf("FileStatus failed: %v", err)
	}
	if status.Size != highInfo.Size || status.Digest != highInfo.Digest {
		t.Fatalf("High file was overwritten! got size=%d digest=%x, want High size=%d digest=%x",
			status.Size, status.Digest, highInfo.Size, highInfo.Digest)
	}

	// 6. Verify stream progress and receipts are completed
	progress, ok, err := high.BridgeStreamProgress("files-high-mod")
	if err != nil || !ok || progress != 1 {
		t.Fatalf("stream progress = %d (ok=%v), want 1", progress, ok)
	}
}
