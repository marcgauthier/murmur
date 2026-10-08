package murmur

import (
	"context"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/objectstore"
)

func openCoverageDB(t *testing.T) *DB {
	t.Helper()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestDBStateStrings pins every lifecycle state name and the write gate.
func TestDBStateStrings(t *testing.T) {
	for _, tc := range []struct {
		state DBState
		name  string
		write bool
	}{
		{StateOpening, "opening", false},
		{StateRebuilding, "rebuilding", false},
		{StateReady, "ready", true},
		{StateMaterializerDirty, "materializer-dirty", false},
		{StateRotatingKey, "rotating-key", false},
		{StateMaintenance, "maintenance", false},
		{StateClosing, "closing", false},
		{StateClosed, "closed", false},
		{StateFailed, "failed", false},
		{DBState(99), "unknown", false},
	} {
		if got := tc.state.String(); got != tc.name {
			t.Fatalf("state %d = %q, want %q", uint8(tc.state), got, tc.name)
		}
		if got := tc.state.WritesAllowed(); got != tc.write {
			t.Fatalf("WritesAllowed(%s) = %v", tc.name, got)
		}
	}
	var l DiscardLogger
	l.Debug("d")
	l.Info("i")
	l.Warn("w")
	l.Error("e")
}

// TestTypedTxAccessors covers read-your-writes, staged query overlay, and
// post-commit rejection.
func TestTypedTxAccessors(t *testing.T) {
	ctx := context.Background()
	db := openCoverageDB(t)
	table, err := TableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	id := ids.NewRowID()
	tx, err := db.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(tx, &facadeRecord{ID: id, Name: "ann"}); err != nil {
		t.Fatal(err)
	}
	got, err := table.GetTx(tx, id)
	if err != nil || got.Name != "ann" {
		t.Fatalf("transaction read-your-writes = %+v/%v", got, err)
	}
	query, err := table.WhereTx(tx)
	if err != nil {
		t.Fatal(err)
	}
	count, err := query.Count()
	if err != nil || count != 1 {
		t.Fatalf("staged query count = %d/%v", count, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := table.GetTx(tx, id); err == nil {
		t.Fatal("post-commit transaction read succeeded")
	}
	rows, err := table.Where().Find()
	if err != nil || len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("committed typed rows = %+v/%v", rows, err)
	}
}

// TestDBReceiptsAndBridgeProgress covers the durable receipt and bridge
// progress wrappers.
func TestDBReceiptsAndBridgeProgress(t *testing.T) {
	db := openCoverageDB(t)
	txID := ids.NewTxID()
	if has, err := db.HasTransactionReceipt(txID); err != nil || has {
		t.Fatalf("fresh receipt = %v/%v", has, err)
	}
	if err := db.RecordTransactionReceipt(txID); err != nil {
		t.Fatal(err)
	}
	if has, err := db.HasTransactionReceipt(txID); err != nil || !has {
		t.Fatalf("recorded receipt = %v/%v", has, err)
	}
	if _, ok, err := db.BridgeStreamProgress("s"); err != nil || ok {
		t.Fatalf("fresh progress = %v/%v", ok, err)
	}
	if err := db.SetBridgeStreamProgress("s", 9); err != nil {
		t.Fatal(err)
	}
	if seq, ok, err := db.BridgeStreamProgress("s"); err != nil || !ok || seq != 9 {
		t.Fatalf("progress = %d/%v/%v", seq, ok, err)
	}
}

// TestCurrentSchemaMatchesStore proves the syncer identity tracks the store.
func TestCurrentSchemaMatchesStore(t *testing.T) {
	db := openCoverageDB(t)
	epoch, hash, err := db.store.SchemaEpoch()
	if err != nil {
		t.Fatal(err)
	}
	cur := db.CurrentSchema()
	if cur.Epoch != epoch || cur.Hash != hash {
		t.Fatalf("CurrentSchema = %+v, store = %d/%x", cur, epoch, hash)
	}
}

func TestWriterClassString(t *testing.T) {
	for _, tc := range []struct {
		class WriterClass
		name  string
	}{
		{WriterLocal, "local"},
		{WriterRemote, "remote"},
		{WriterMaintenance, "maintenance"},
		{WriterClass(99), "unknown"},
	} {
		if got := tc.class.String(); got != tc.name {
			t.Fatalf("class %d = %q", int(tc.class), got)
		}
	}
	tk := &Ticket{class: WriterRemote}
	if tk.Class() != WriterRemote {
		t.Fatalf("ticket class = %v", tk.Class())
	}
}

// TestFileReaderAccessors pins the open-file descriptors.
func TestFileReaderAccessors(t *testing.T) {
	var zero objectstore.Digest
	r := &FileReader{name: "n", digest: zero, size: 42}
	if r.Name() != "n" || r.Digest() != zero || r.Size() != 42 {
		t.Fatal("accessor mismatch")
	}
}

// TestBridgeThinWrappers covers files-enabled probing and the fail-closed
// ownership/provenance errors on a fresh database.
func TestBridgeThinWrappers(t *testing.T) {
	ctx := context.Background()
	plain := openCoverageDB(t)
	if plain.BridgeFilesEnabled() {
		t.Fatal("files reported enabled without config")
	}
	if plain.BridgeFileObjects() != nil {
		t.Fatal("nil files returned an object store")
	}
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	cfg.Files.Enabled = true
	cfg.Files.ObjectKey = append([]byte(nil), testObjectKey...)
	fdb, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fdb.Close()
	if !fdb.BridgeFilesEnabled() {
		t.Fatal("files not reported enabled")
	}
	if fdb.BridgeFileObjects() == nil {
		t.Fatal("enabled files returned nil object store")
	}

	row := NewRowID()
	if _, err := plain.BridgeHighOwnedColumns("nope", row); err == nil {
		t.Fatal("unknown table accepted")
	}
	if cols, err := plain.BridgeHighOwnedColumns("records", row); err != nil || len(cols) != 0 {
		t.Fatalf("fresh row owned columns = %v/%v", cols, err)
	}
	if err := plain.ReleaseBridgeRowOwnership(ctx, "records", row); err == nil {
		t.Fatal("release without provenance succeeded")
	}
	if err := fdb.ReleaseBridgeFileRowOwnership(ctx, row); err == nil {
		t.Fatal("file release without provenance succeeded")
	}
	if _, err := bridgeShadowClearsForRow(plain, 0xBEEF, row); err == nil {
		t.Fatal("shadow clear for unknown table succeeded")
	}
	clears, err := bridgeShadowClearsForRow(plain, bridgeRecordsID(t, plain), row)
	if err != nil || len(clears) == 0 {
		t.Fatalf("shadow clears = %d/%v", len(clears), err)
	}
}

func bridgeRecordsID(t *testing.T, db *DB) uint32 {
	t.Helper()
	tt, err := db.bridgeTable("records")
	if err != nil {
		t.Fatal(err)
	}
	return tt.ID
}

// TestSpoolCompressionValidation tests supported Spool compression options.
func TestSpoolCompressionValidation(t *testing.T) {
	for _, tc := range []struct {
		algo CompressionAlgorithm
		want bool
	}{
		{CompressionNone, true},
		{CompressionDeflate, true},
		{"", true},
		{"zstd", false},
		{"zstd-fast", false},
		{"snappy", false},
		{"lz4", false},
	} {
		t.Run(string(tc.algo), func(t *testing.T) {
			cfg := testConfig(t.TempDir())
			cfg.Spool.Compression = tc.algo
			cfg.withDefaults()
			err := cfg.validate()
			if tc.want && err != nil {
				t.Fatalf("algorithm %q rejected: %v", tc.algo, err)
			}
			if !tc.want && err == nil {
				t.Fatalf("algorithm %q accepted", tc.algo)
			}
		})
	}
}

// TestOpenWithSpoolCompression proves databases open, write, and close on every
// supported Spool compression mode.
func TestOpenWithSpoolCompression(t *testing.T) {
	ctx := context.Background()
	modes := []CompressionAlgorithm{
		CompressionNone,
		CompressionDeflate,
	}
	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			cfg := testConfig(t.TempDir())
			cfg.Schema.Tables = nil
			cfg.Tables = []TableDefinition{recordDefinition(t)}
			cfg.Spool.Compression = mode
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			table, err := TableOf[facadeRecord](db, "records")
			if err != nil {
				t.Fatal(err)
			}
			id := ids.NewRowID()
			if err := db.WriteTxContext(ctx, func(tx *Tx) error {
				return table.Insert(tx, &facadeRecord{ID: id, Name: "z"})
			}); err != nil {
				t.Fatal(err)
			}
			if got, err := table.Get(id); err != nil || got.Name != "z" {
				t.Fatalf("compressed typed row = %+v/%v", got, err)
			}
		})
	}
}
