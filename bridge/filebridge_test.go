package bridge

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"github.com/marcgauthier/murmur/internal/testdb"
	"io"
	"os"
	"strings"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

// fileBridgeHarness wires a real Low/High pair with the full bridge pipeline.
type fileBridgeHarness struct {
	low  *db.DB
	high *db.DB

	outbox  *Outbox
	inbox   *Inbox
	exp     *Exporter
	signer  *SignerKey
	recip   *RecipientKey
	pub     *collectingPublisher
	lowDir  string
	highDir string
}

type collectingPublisher struct {
	items []Artifact
}

func (p *collectingPublisher) Publish(_ context.Context, a Artifact) error {
	p.items = append(p.items, a)
	return nil
}

func openFileDB(t *testing.T, objectKey []byte) *db.DB {
	t.Helper()
	return openFileDBAt(t, t.TempDir(), objectKey)
}

func openFileDBAt(t *testing.T, dir string, objectKey []byte) *db.DB {
	t.Helper()
	database, err := db.Open(context.Background(), testdb.Configure(db.Config{
		Path:   dir,
		NodeID: db.NewNodeID(),
		Schema: db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name:    "contacts",
			Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}},
		}}},
		Pebble:     db.DefaultPebbleConfig(),
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

func newFileBridge(t *testing.T, lowKey, highKey []byte) *fileBridgeHarness {
	t.Helper()
	return newFileBridgeAt(t, t.TempDir(), t.TempDir(), lowKey, highKey)
}

func newFileBridgeAt(t *testing.T, lowDir, highDir string, lowKey, highKey []byte) *fileBridgeHarness {
	t.Helper()
	h := &fileBridgeHarness{
		low:     openFileDBAt(t, lowDir, lowKey),
		high:    openFileDBAt(t, highDir, highKey),
		pub:     &collectingPublisher{},
		lowDir:  lowDir,
		highDir: highDir,
	}
	var err error
	h.outbox, err = OpenOutbox(t.TempDir(), Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	var trust *TrustStore
	h.signer, h.recip, trust = inboxKeys(t, "files")
	h.inbox, err = OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	h.exp, err = NewExporter(h.low, Config{Role: RoleLowExporter, Domain: h.low.DBID(), Stream: "files"}, h.pub)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *fileBridgeHarness) filePublisher() *FilePublisher {
	return &FilePublisher{
		Objects:   h.low.BridgeFileObjects(),
		Signer:    h.signer,
		Recipient: h.recip.Public(),
		Outbox:    h.outbox,
		Limits:    Limits{}.withDefaults(),
	}
}

// roundTrip captures Low, publishes (bundles + chunks), receives everything,
// and drains. It returns published bundle and artifact counts.
func (h *fileBridgeHarness) roundTrip(t *testing.T) (bundles, artifacts int) {
	t.Helper()
	return h.roundTripWith(t, h.filePublisher())
}

func (h *fileBridgeHarness) roundTripWith(t *testing.T, fp *FilePublisher) (bundles, artifacts int) {
	t.Helper()
	ctx := context.Background()
	schema, err := h.low.BridgeSchema()
	if err != nil {
		t.Fatal(err)
	}
	capturer, err := NewCapturer(h.low.BridgeLogSource(), schema, h.outbox, 64, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capturer.CaptureOnce(ctx); err != nil {
		t.Fatal(err)
	}
	seal := NewBundleSealer(h.exp, h.signer, h.recip.Public())
	n, err := PublishPendingWithFiles(ctx, h.exp, h.outbox, seal, h.pub, RetryPolicy{MaxAttempts: 1}, fp)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range h.pub.items {
		if err := h.inbox.Receive(a); err != nil {
			// Re-delivery to quarantine refuses loudly; production
			// senders treat that as "already known" and move on.
			if strings.Contains(err.Error(), "is quarantined") {
				continue
			}
			t.Fatalf("receive %s: %v", a.Name, err)
		}
	}
	artifacts = len(h.pub.items)
	h.pub.items = nil
	importer, err := NewImporter(h.high)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(ctx, h.inbox); err != nil {
		t.Fatalf("drain: %v", err)
	}
	return n, artifacts
}

func uploadFile(t *testing.T, database *db.DB, name string, data []byte) {
	t.Helper()
	if _, err := database.UploadFile(context.Background(), name, bytes.NewReader(data)); err != nil {
		t.Fatalf("upload %s: %v", name, err)
	}
}

func readFile(t *testing.T, database *db.DB, name string) []byte {
	t.Helper()
	r, err := database.OpenFile(context.Background(), name)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return got
}

func TestFileBridgeEndToEnd(t *testing.T) {
	lowKey := bytes.Repeat([]byte{0x10}, 32)
	highKey := bytes.Repeat([]byte{0x20}, 32)
	h := newFileBridge(t, lowKey, highKey)

	data := make([]byte, 2_500_000) // multi-chunk at 1 MiB
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	uploadFile(t, h.low, "shared/blob", data)
	bundles, artifacts := h.roundTrip(t)
	if bundles != 1 {
		t.Fatalf("published %d bundles, want 1", bundles)
	}
	if artifacts != 4 {
		t.Fatalf("published %d artifacts, want 1 bundle + 3 chunks", artifacts)
	}
	st, err := h.high.FileStatus(context.Background(), "shared/blob")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || st.Deleted || !st.Available {
		t.Fatalf("high status %+v, want live+available", st)
	}
	if got := readFile(t, h.high, "shared/blob"); !bytes.Equal(got, data) {
		t.Fatal("high bytes differ from low upload")
	}
	// Provenance matches the row contract: Low-owned, this stream.
	prov, ok, err := h.high.BridgeFileProvenance(db.BridgeFileRowID("shared/blob"))
	if err != nil || !ok || prov.Owner != db.BridgeOwnerLow || prov.Stream != "files" || prov.SourceDomain != h.low.DBID() {
		t.Fatalf("provenance %+v present=%v err=%v", prov, ok, err)
	}
	if n := len(h.outbox.PendingFiles()); n != 0 {
		t.Fatalf("%d pending files remain", n)
	}
}

func TestFileBridgeMetadataFirstPending(t *testing.T) {
	h := newFileBridge(t, bytes.Repeat([]byte{0x10}, 32), bytes.Repeat([]byte{0x20}, 32))
	data := []byte("metadata crosses first")
	uploadFile(t, h.low, "shared/early", data)

	// Metadata-only export: no chunk publisher, no pending journal.
	bundles, _ := h.roundTripWith(t, nil)
	if bundles != 1 {
		t.Fatalf("published %d bundles, want 1", bundles)
	}
	st, err := h.high.FileStatus(context.Background(), "shared/early")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || st.Deleted || st.Available {
		t.Fatalf("high status %+v, want exists+pending", st)
	}
	// Late chunks (as if bytes arrived on Low after metadata published).
	lowSt, err := h.low.FileStatus(context.Background(), "shared/early")
	if err != nil {
		t.Fatal(err)
	}
	batch := Batch{TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: 1, HLC: 1, Records: []Record{{
		Table: db.BridgeFileTableName, Row: db.BridgeFileRowID("shared/early"), Op: RecordPut,
		Columns: []ColumnValue{
			{Column: db.BridgeFileColName, Value: codec.Text("shared/early")},
			{Column: db.BridgeFileColDigest, Value: codec.Blob(lowSt.Digest[:])},
			{Column: db.BridgeFileColSize, Value: codec.Int(lowSt.Size)},
		},
	}}}
	fp := h.filePublisher()
	if err := fp.ExportRun(context.Background(), []Batch{batch}, "files", h.pub, RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	for _, a := range h.pub.items {
		if err := h.inbox.Receive(a); err != nil {
			t.Fatalf("receive %s: %v", a.Name, err)
		}
	}
	h.pub.items = nil
	importer, err := NewImporter(h.high)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(context.Background(), h.inbox); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := readFile(t, h.high, "shared/early"); !bytes.Equal(got, data) {
		t.Fatal("late-chunk bytes differ from upload")
	}
}

func TestFileBridgeObjectFirst(t *testing.T) {
	h := newFileBridge(t, bytes.Repeat([]byte{0x10}, 32), bytes.Repeat([]byte{0x20}, 32))
	data := []byte("bytes cross first")
	uploadFile(t, h.low, "shared/object-first", data)
	lowSt, err := h.low.FileStatus(context.Background(), "shared/object-first")
	if err != nil {
		t.Fatal(err)
	}
	// Chunks before metadata: stage only, nothing installs.
	batch := Batch{TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: 1, HLC: 1, Records: []Record{{
		Table: db.BridgeFileTableName, Row: db.BridgeFileRowID("shared/object-first"), Op: RecordPut,
		Columns: []ColumnValue{
			{Column: db.BridgeFileColName, Value: codec.Text("shared/object-first")},
			{Column: db.BridgeFileColDigest, Value: codec.Blob(lowSt.Digest[:])},
			{Column: db.BridgeFileColSize, Value: codec.Int(lowSt.Size)},
		},
	}}}
	fp := h.filePublisher()
	if err := fp.ExportRun(context.Background(), []Batch{batch}, "files", h.pub, RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	for _, a := range h.pub.items {
		if err := h.inbox.Receive(a); err != nil {
			t.Fatalf("receive %s: %v", a.Name, err)
		}
	}
	h.pub.items = nil
	importer, err := NewImporter(h.high)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(context.Background(), h.inbox); err != nil {
		t.Fatalf("drain with objects only: %v", err)
	}
	prog := h.inbox.FileProgress()
	if len(prog.Ready) != 1 {
		t.Fatalf("file progress %+v, want 1 ready object", prog)
	}
	// Metadata arrives later through the normal pipeline.
	h.roundTrip(t)
	if got := readFile(t, h.high, "shared/object-first"); !bytes.Equal(got, data) {
		t.Fatal("object-first bytes differ from upload")
	}
	if n := len(h.inbox.FileProgress().Ready); n != 0 {
		t.Fatalf("%d staged objects remain after install", n)
	}
}

func TestFileBridgeMissingBytesRetry(t *testing.T) {
	ctx := context.Background()
	h := newFileBridgeAt(t, t.TempDir(), t.TempDir(), bytes.Repeat([]byte{0x10}, 32), bytes.Repeat([]byte{0x20}, 32))
	data := []byte("bytes go missing, then return")
	uploadFile(t, h.low, "shared/flaky", data)
	lowSt, err := h.low.FileStatus(ctx, "shared/flaky")
	if err != nil {
		t.Fatal(err)
	}
	// Remove Low's bytes out of band (Low itself is now pending).
	objPath := h.lowDir + "/files/objects/" + lowSt.Digest.String() + ".spfo"
	saved, err := os.ReadFile(objPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(objPath); err != nil {
		t.Fatal(err)
	}
	bundles, _ := h.roundTrip(t)
	if bundles != 1 {
		t.Fatalf("published %d bundles, want 1", bundles)
	}
	if n := len(h.outbox.PendingFiles()); n != 1 {
		t.Fatalf("%d pending files, want 1", n)
	}
	st, err := h.high.FileStatus(ctx, "shared/flaky")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || st.Available {
		t.Fatalf("high status %+v, want exists+pending", st)
	}
	// Bytes return; the next drain retries the journaled object.
	if err := os.WriteFile(objPath, saved, 0600); err != nil {
		t.Fatal(err)
	}
	seal := NewBundleSealer(h.exp, h.signer, h.recip.Public())
	if _, err := PublishPendingWithFiles(ctx, h.exp, h.outbox, seal, h.pub, RetryPolicy{MaxAttempts: 1}, h.filePublisher()); err != nil {
		t.Fatal(err)
	}
	if len(h.pub.items) != 1 {
		t.Fatalf("retry published %d artifacts, want 1 chunk", len(h.pub.items))
	}
	for _, a := range h.pub.items {
		if err := h.inbox.Receive(a); err != nil {
			t.Fatalf("receive %s: %v", a.Name, err)
		}
	}
	h.pub.items = nil
	importer, err := NewImporter(h.high)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(ctx, h.inbox); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := readFile(t, h.high, "shared/flaky"); !bytes.Equal(got, data) {
		t.Fatal("retried bytes differ from upload")
	}
	if n := len(h.outbox.PendingFiles()); n != 0 {
		t.Fatalf("%d pending files remain", n)
	}
}

func TestFileBridgeConflictingChunk(t *testing.T) {
	ctx := context.Background()
	h := newFileBridge(t, bytes.Repeat([]byte{0x10}, 32), bytes.Repeat([]byte{0x20}, 32))
	data := []byte("contested content")
	uploadFile(t, h.low, "shared/contested", data)
	lowSt, err := h.low.FileStatus(ctx, "shared/contested")
	if err != nil {
		t.Fatal(err)
	}
	// Emit the genuine chunk directly, then a conflicting same-index seal.
	var digest [32]byte
	copy(digest[:], lowSt.Digest[:])
	limits := Limits{}.withDefaults()
	genuine, err := SealFileChunk(h.signer, h.recip.Public(), "files", digest, 0, 1, uint64(len(data)), data, limits)
	if err != nil {
		t.Fatal(err)
	}
	tampered := bytes.Clone(data)
	tampered[0] ^= 0xff
	conflict, err := SealFileChunk(h.signer, h.recip.Public(), "files", digest, 0, 1, uint64(len(data)), tampered, limits)
	if err != nil {
		t.Fatal(err)
	}
	name := FileChunkArtifactName("files", digest, 0, 1)
	if err := h.inbox.Receive(Artifact{Name: name, Data: genuine}); err != nil {
		t.Fatal(err)
	}
	if err := h.inbox.Receive(Artifact{Name: name, Data: conflict}); err == nil {
		t.Fatal("conflicting chunk accepted")
	}
	prog := h.inbox.FileProgress()
	if len(prog.Quarantined) != 1 {
		t.Fatalf("file progress %+v, want 1 quarantined", prog)
	}
	// Metadata still imports (pending); the quarantined object never installs.
	h.roundTrip(t)
	st, err := h.high.FileStatus(ctx, "shared/contested")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || st.Available {
		t.Fatalf("high status %+v, want exists+pending", st)
	}
	// Operator retry restores the genuine staged chunk; the next drain installs.
	if err := h.inbox.RetryQuarantinedFile(digest); err != nil {
		t.Fatal(err)
	}
	importer, err := NewImporter(h.high)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(ctx, h.inbox); err != nil {
		t.Fatalf("drain after retry: %v", err)
	}
	if got := readFile(t, h.high, "shared/contested"); !bytes.Equal(got, data) {
		t.Fatal("post-retry bytes differ from upload")
	}
}

func TestFileBridgeHighOwnedDelete(t *testing.T) {
	ctx := context.Background()
	h := newFileBridge(t, bytes.Repeat([]byte{0x10}, 32), bytes.Repeat([]byte{0x20}, 32))
	uploadFile(t, h.low, "shared/owned", []byte("low original"))
	h.roundTrip(t)
	if got := readFile(t, h.high, "shared/owned"); string(got) != "low original" {
		t.Fatalf("high has %q", got)
	}
	// High overrides the file: its fields become High-owned.
	uploadFile(t, h.high, "shared/owned", []byte("high override"))
	owned, err := h.high.BridgeFileHighOwnedColumns(db.BridgeFileRowID("shared/owned"))
	if err != nil || len(owned) == 0 {
		t.Fatalf("high-owned columns %v err=%v, want non-empty", owned, err)
	}
	// Low deletes; the import must hold for an explicit decision.
	if err := h.low.DeleteFile(ctx, "shared/owned"); err != nil {
		t.Fatal(err)
	}
	schema, err := h.low.BridgeSchema()
	if err != nil {
		t.Fatal(err)
	}
	capturer, err := NewCapturer(h.low.BridgeLogSource(), schema, h.outbox, 64, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capturer.CaptureOnce(ctx); err != nil {
		t.Fatal(err)
	}
	seal := NewBundleSealer(h.exp, h.signer, h.recip.Public())
	if _, err := PublishPendingWithFiles(ctx, h.exp, h.outbox, seal, h.pub, RetryPolicy{MaxAttempts: 1}, h.filePublisher()); err != nil {
		t.Fatal(err)
	}
	for _, a := range h.pub.items {
		if err := h.inbox.Receive(a); err != nil {
			t.Fatalf("receive %s: %v", a.Name, err)
		}
	}
	h.pub.items = nil
	importer, err := NewImporter(h.high)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(ctx, h.inbox); err == nil {
		t.Fatal("low delete of high-owned file applied without a decision")
	} else if !errors.Is(err, ErrProtectedDelete) {
		t.Fatalf("drain error %v, want protected delete", err)
	}
	// Keep-high consumes the delete; the file survives.
	if err := importer.ResolvePolicyHold(ctx, h.inbox, "files", 2, "keep-high"); err != nil {
		t.Fatalf("resolve keep-high: %v", err)
	}
	if got := readFile(t, h.high, "shared/owned"); string(got) != "high override" {
		t.Fatalf("after keep-high, high has %q", got)
	}
	// A fresh Low delete with accept-low-delete removes it and releases ownership.
	if err := h.low.DeleteFile(ctx, "shared/owned"); err != nil {
		t.Fatal(err)
	}
	if _, err := capturer.CaptureOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := PublishPendingWithFiles(ctx, h.exp, h.outbox, seal, h.pub, RetryPolicy{MaxAttempts: 1}, h.filePublisher()); err != nil {
		t.Fatal(err)
	}
	for _, a := range h.pub.items {
		if err := h.inbox.Receive(a); err != nil {
			t.Fatalf("receive %s: %v", a.Name, err)
		}
	}
	h.pub.items = nil
	if _, err := importer.Drain(ctx, h.inbox); err == nil {
		t.Fatal("second low delete applied without a decision")
	}
	if err := importer.ResolvePolicyHold(ctx, h.inbox, "files", 3, "accept-low-delete"); err != nil {
		t.Fatalf("resolve accept-low-delete: %v", err)
	}
	st, err := h.high.FileStatus(ctx, "shared/owned")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Deleted {
		t.Fatalf("high status %+v, want deleted", st)
	}
}

func TestFileBridgeCollision(t *testing.T) {
	h := newFileBridge(t, bytes.Repeat([]byte{0x10}, 32), bytes.Repeat([]byte{0x20}, 32))
	uploadFile(t, h.high, "shared/clash", []byte("high bytes"))
	uploadFile(t, h.low, "shared/clash", []byte("low bytes"))
	// Capture + publish metadata only; the import must refuse the collision.
	ctx := context.Background()
	schema, err := h.low.BridgeSchema()
	if err != nil {
		t.Fatal(err)
	}
	capturer, err := NewCapturer(h.low.BridgeLogSource(), schema, h.outbox, 64, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capturer.CaptureOnce(ctx); err != nil {
		t.Fatal(err)
	}
	seal := NewBundleSealer(h.exp, h.signer, h.recip.Public())
	if _, err := PublishPending(ctx, h.exp, h.outbox, seal, h.pub, RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	for _, a := range h.pub.items {
		if err := h.inbox.Receive(a); err != nil {
			t.Fatalf("receive %s: %v", a.Name, err)
		}
	}
	h.pub.items = nil
	importer, err := NewImporter(h.high)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(ctx, h.inbox); err == nil {
		t.Fatal("colliding low file imported over high-local file")
	} else if !errors.Is(err, ErrIdentityCollision) {
		t.Fatalf("drain error %v, want identity collision", err)
	}
	if got := readFile(t, h.high, "shared/clash"); string(got) != "high bytes" {
		t.Fatalf("high bytes now %q", got)
	}
}

func TestFileBridgeDisabledFilesHold(t *testing.T) {
	ctx := context.Background()
	lowDir := t.TempDir()
	highDir := t.TempDir()
	highNode := db.NewNodeID()
	low := openFileDBAt(t, lowDir, bytes.Repeat([]byte{0x10}, 32))
	// High starts without files: build the harness around it manually.
	pebbleCfg := db.DefaultPebbleConfig()
	high, err := db.Open(ctx, testdb.Configure(db.Config{
		Path:   highDir,
		NodeID: highNode,
		Schema: db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name:    "contacts",
			Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}},
		}}},
		Pebble:     pebbleCfg,
		Encryption: db.EncryptionConfig{Key: bytes.Repeat([]byte{0x44}, 32), KeyID: "test-key"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = high.Close() })
	outbox, err := OpenOutbox(t.TempDir(), Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	signer, recip, trust := inboxKeys(t, "files")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	pub := &collectingPublisher{}
	exp, err := NewExporter(low, Config{Role: RoleLowExporter, Domain: low.DBID(), Stream: "files"}, pub)
	if err != nil {
		t.Fatal(err)
	}
	uploadFile(t, low, "shared/held", []byte("waiting for files"))
	lowSchema, err := low.BridgeSchema()
	if err != nil {
		t.Fatal(err)
	}
	capturer, err := NewCapturer(low.BridgeLogSource(), lowSchema, outbox, 64, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capturer.CaptureOnce(ctx); err != nil {
		t.Fatal(err)
	}
	fp := &FilePublisher{Objects: low.BridgeFileObjects(), Signer: signer, Recipient: recip.Public(), Outbox: outbox, Limits: Limits{}.withDefaults()}
	seal := NewBundleSealer(exp, signer, recip.Public())
	if _, err := PublishPendingWithFiles(ctx, exp, outbox, seal, pub, RetryPolicy{MaxAttempts: 1}, fp); err != nil {
		t.Fatal(err)
	}
	for _, a := range pub.items {
		if err := inbox.Receive(a); err != nil {
			t.Fatalf("receive %s: %v", a.Name, err)
		}
	}
	importer, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(ctx, inbox); err == nil {
		t.Fatal("file bundle imported with files disabled")
	}
	// Enable files and reopen: the held bundle releases and installs.
	if err := high.Close(); err != nil {
		t.Fatal(err)
	}
	high2, err := db.Open(ctx, testdb.Configure(db.Config{
		Path:   highDir,
		NodeID: highNode,
		Schema: db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name:    "contacts",
			Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}},
		}}},
		Pebble:     pebbleCfg,
		Encryption: db.EncryptionConfig{Key: bytes.Repeat([]byte{0x44}, 32), KeyID: "test-key"},
		Files:      db.FilesConfig{Enabled: true, ObjectKey: bytes.Repeat([]byte{0x20}, 32)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = high2.Close() })
	importer2, err := NewImporter(high2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer2.Drain(ctx, inbox); err != nil {
		t.Fatalf("drain after enabling files: %v", err)
	}
	r, err := high2.OpenFile(ctx, "shared/held")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if string(got) != "waiting for files" {
		t.Fatalf("high has %q", got)
	}
}

func TestFileChunkCodec(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "files")
	limits := Limits{}.withDefaults()
	var digest [32]byte
	if _, err := rand.Read(digest[:]); err != nil {
		t.Fatal(err)
	}
	sealed, err := SealFileChunk(signer, recip.Public(), "files", digest, 2, 5, 4500, []byte("chunk-bytes"), limits)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenFileChunk(sealed, trust, limits)
	if err != nil {
		t.Fatal(err)
	}
	if opened.Stream != "files" || opened.Digest != digest || opened.Index != 2 || opened.Count != 5 || opened.TotalLen != 4500 || string(opened.Bytes) != "chunk-bytes" {
		t.Fatalf("opened %+v", opened)
	}
	// Tamper fails authentication.
	bad := bytes.Clone(sealed)
	bad[len(bad)-1] ^= 0xff
	if _, err := OpenFileChunk(bad, trust, limits); err == nil {
		t.Fatal("tampered chunk opened")
	}
	// Unauthorized stream fails even with a valid seal.
	otherTrust := NewTrustStore()
	if err := otherTrust.AddSigner(signer.ID, "other"); err != nil {
		t.Fatal(err)
	}
	if err := otherTrust.AddRecipient(recip); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFileChunk(sealed, otherTrust, limits); err == nil {
		t.Fatal("chunk opened under unauthorized stream")
	}
	// Artifact names round-trip, including dashed streams.
	name := FileChunkArtifactName("my-stream", digest, 7, 12)
	stream, back, index, count, err := ParseFileChunkArtifactName(name)
	if err != nil {
		t.Fatal(err)
	}
	if stream != "my-stream" || back != digest || index != 7 || count != 12 {
		t.Fatalf("parsed %q %x %d %d", stream, back[:4], index, count)
	}
	for _, badName := range []string{
		"bundle-x.spb",
		"fobj-nope.fobj",
		"fobj-s-zzzz-000000-of-000001.fobj",
		"fobj-s-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef-000001-of-000001.fobj",
		"fobj-s-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef-000000-of-000000.fobj",
		"fobj--0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef-000000-of-000001.fobj",
	} {
		if _, _, _, _, err := ParseFileChunkArtifactName(badName); err == nil {
			t.Fatalf("%q parsed", badName)
		}
	}
}
