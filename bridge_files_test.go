package replicateddb

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/objectstore"
)

func TestBridgeFileResolver(t *testing.T) {
	db, err := Open(context.Background(), fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	resolver, err := db.BridgeSchema()
	if err != nil {
		t.Fatal(err)
	}
	fids, err := resolveFileIDs()
	if err != nil {
		t.Fatal(err)
	}
	name, err := resolver.TableName(fids.table)
	if err != nil || name != BridgeFileTableName {
		t.Fatalf("table name %q err=%v", name, err)
	}
	for colID, want := range map[uint32]string{
		fids.id: BridgeFileColID, fids.name: BridgeFileColName,
		fids.digest: BridgeFileColDigest, fids.size: BridgeFileColSize,
	} {
		got, err := resolver.ColumnName(fids.table, colID)
		if err != nil || got != want {
			t.Fatalf("column %d = %q err=%v, want %q", colID, got, err, want)
		}
	}
}

func TestBridgeFileUploadOwnership(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	uploadBytes(t, db, "owned/doc", []byte("high bytes"))
	row := BridgeFileRowID("owned/doc")
	prov, ok, err := db.BridgeFileProvenance(row)
	if err != nil || !ok || prov.Owner != BridgeOwnerHigh {
		t.Fatalf("upload provenance %+v present=%v err=%v, want High-owned", prov, ok, err)
	}
	exists, err := db.BridgeFileRowExists(row)
	if err != nil || !exists {
		t.Fatalf("row exists=%v err=%v, want true", exists, err)
	}
	if err := db.DeleteFile(ctx, "owned/doc"); err != nil {
		t.Fatal(err)
	}
	exists, err = db.BridgeFileRowExists(row)
	if err != nil || exists {
		t.Fatalf("row exists=%v err=%v, want false after delete", exists, err)
	}
	// Deleting a High-owned row writes no new policy (row semantics are
	// shared with SQL): the tombstone hides the row; Deleted marks only
	// High deletes of Low-owned rows.
	prov, ok, err = db.BridgeFileProvenance(row)
	if err != nil || !ok || prov.Owner != BridgeOwnerHigh || prov.Deleted {
		t.Fatalf("delete provenance %+v present=%v err=%v", prov, ok, err)
	}
}

func TestBridgeApplyFileMetadata(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	source := NewDBID()
	bundle := NewTxID()
	row := BridgeFileRowID("imported/doc")
	digest := objectstore.Digest{1, 2, 3}
	if err := db.BridgeApplyFileMetadata(ctx, source, "files", bundle, 1, 1, false,
		[]BridgeFilePut{{Row: row, Name: "imported/doc", Digest: digest, Size: 42}}, nil); err != nil {
		t.Fatal(err)
	}
	st, err := db.FileStatus(ctx, "imported/doc")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || st.Deleted || st.Size != 42 || st.Digest != digest {
		t.Fatalf("imported status %+v", st)
	}
	prov, ok, err := db.BridgeFileProvenance(row)
	if err != nil || !ok || prov.Owner != BridgeOwnerLow || prov.Stream != "files" || prov.SourceDomain != source {
		t.Fatalf("import provenance %+v present=%v err=%v", prov, ok, err)
	}
	// High override flips field ownership; an accepted Low delete then
	// removes the row and releases the fields.
	uploadBytes(t, db, "imported/doc", []byte("high override"))
	owned, err := db.BridgeFileHighOwnedColumns(row)
	if err != nil || len(owned) == 0 {
		t.Fatalf("high-owned %v err=%v, want non-empty", owned, err)
	}
	if err := db.BridgeApplyFileMetadata(ctx, source, "files", bundle, 2, 2, true, nil, []ids.RowID{row}); err != nil {
		t.Fatal(err)
	}
	st, err = db.FileStatus(ctx, "imported/doc")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Deleted {
		t.Fatalf("status %+v, want deleted", st)
	}
	owned, err = db.BridgeFileHighOwnedColumns(row)
	if err != nil || len(owned) != 0 {
		t.Fatalf("high-owned after accept %v err=%v, want empty", owned, err)
	}
}

func TestBridgePutFileObject(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data := []byte("bridge plaintext")
	digest, size, err := db.BridgePutFileObject(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(data)) {
		t.Fatalf("size %d", size)
	}
	r, err := db.files.objects.Read(ctx, digest, io.Discard)
	_ = r
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	// Disabled files refuse.
	db2, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, _, err := db2.BridgePutFileObject(ctx, bytes.NewReader(data)); err == nil {
		t.Fatal("put with files disabled succeeded")
	}
	// Provenance reads need only the policy store, not files.
	if _, ok, err := db2.BridgeFileProvenance(BridgeFileRowID("x")); err != nil || ok {
		t.Fatalf("provenance on empty db: ok=%v err=%v", ok, err)
	}
}

func TestBridgeFileRowIDDeterminism(t *testing.T) {
	a := BridgeFileRowID("same/name")
	b := BridgeFileRowID("same/name")
	c := BridgeFileRowID("other/name")
	if a != b || a == c || a.IsZero() {
		t.Fatal("row ID derivation is not deterministic")
	}
}
