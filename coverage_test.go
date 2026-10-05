package murmur

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/sstable/block"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/objectstore"
)

func openCoverageDB(t *testing.T) *DB {
	t.Helper()
	db, err := openSignedFixture(context.Background(), testConfig(t.TempDir()))
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

// TestIsReadOnlyStatementTable pins the read/write routing classifier,
// including comment-prefix handling.
func TestIsReadOnlyStatementTable(t *testing.T) {
	for _, tc := range []struct {
		q    string
		want bool
	}{
		{"SELECT 1", true},
		{"  select a from t", true},
		{"EXPLAIN SELECT 1", true},
		{"PRAGMA table_info(t)", true},
		{"VALUES (1), (2)", true},
		{"TABLE t", true},
		{"-- just a comment", true},
		{"-- lead comment\nSELECT 1", true},
		{"/* block */ SELECT 1", true},
		{"INSERT INTO t VALUES (1)", false},
		{"UPDATE t SET a = 1", false},
		{"DELETE FROM t", false},
		{"CREATE TABLE t (a)", false},
		{"WITH x AS (SELECT 1) SELECT * FROM x", false},
		{"", false},
		{"/* unclosed", false},
	} {
		if got := IsReadOnlyStatement(tc.q); got != tc.want {
			t.Fatalf("IsReadOnlyStatement(%q) = %v, want %v", tc.q, got, tc.want)
		}
	}
}

// TestTxAccessors covers the explicit-transaction surface: idempotency key,
// single-row reads, in-tx reads, prepared-statement shims, and post-commit
// rejection.
func TestTxAccessors(t *testing.T) {
	ctx := context.Background()
	db := openCoverageDB(t)
	id := NewRowID()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tx.TxID() == (TxID{}) {
		t.Fatal("zero TxID")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "ann"); err != nil {
		t.Fatal(err)
	}
	trow := tx.QueryRowContext(ctx, `SELECT name FROM contacts WHERE id = ?`, id[:])
	var name string
	if err := trow.Scan(&name); err != nil || name != "ann" {
		t.Fatalf("tx row = %q/%v", name, err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT COUNT(*) FROM contacts`)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	for rows.Next() {
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
	}
	_ = rows.Close()
	if n != 1 {
		t.Fatalf("count = %d", n)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `SELECT 1`); !errors.Is(err, ErrTxDone) {
		t.Fatalf("post-commit exec = %v", err)
	}
	if _, err := tx.QueryContext(ctx, `SELECT 1`); !errors.Is(err, ErrTxDone) {
		t.Fatalf("post-commit query = %v", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT 1`).Scan(&n); !errors.Is(err, ErrTxDone) {
		t.Fatalf("post-commit row scan = %v", err)
	}

	stmt, err := db.PrepareContext(ctx, `SELECT name FROM contacts WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	srows, err := stmt.QueryContext(ctx, id[:])
	if err != nil {
		t.Fatal(err)
	}
	_ = srows.Close()
	if err := stmt.QueryRowContext(ctx, id[:]).Scan(&name); err != nil || name != "ann" {
		t.Fatalf("stmt row = %q/%v", name, err)
	}
	if _, err := stmt.ExecContext(ctx, id[:]); err == nil {
		// A SELECT through the exec shim fails: the statement is read-only
		// but exec requires the write path; either way it must not panic.
		t.Log("select-via-exec unexpectedly succeeded")
	}
	dbRow := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM contacts`)
	if err := dbRow.Err(); err != nil {
		t.Fatalf("Row.Err = %v", err)
	}
	if err := dbRow.Scan(&n); err != nil || n != 1 {
		t.Fatalf("db row = %d/%v", n, err)
	}
}

// TestSubscriptionAccessors pins the subscription metadata surface.
func TestSubscriptionAccessors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db := openCoverageDB(t)
	const q = "SELECT name FROM contacts ORDER BY name"
	sub, err := db.Subscribe(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	select {
	case <-sub.Events():
	case <-time.After(10 * time.Second):
		t.Fatal("no initial event")
	}
	if sub.Query() != q {
		t.Fatalf("Query = %q", sub.Query())
	}
	if cols := sub.Columns(); len(cols) != 1 || cols[0] != "name" {
		t.Fatalf("Columns = %v", cols)
	}
	_ = sub.Cursor()
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

// TestDriverLegacyMethods drives the non-context database/sql surface:
// connector driver, handle open, legacy begin, and legacy exec/query.
func TestDriverLegacyMethods(t *testing.T) {
	ctx := context.Background()
	db := openCoverageDB(t)
	conn, err := NewConnector(db).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := conn.(*driverConn)
	if NewConnector(db).Driver() == nil {
		t.Fatal("Connector.Driver is nil")
	}
	dtx, err := c.Begin()
	if err != nil {
		t.Fatal(err)
	}
	id := NewRowID()
	stmt, err := c.Prepare(`INSERT INTO contacts (id, name) VALUES (?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec([]driver.Value{id[:], "zed"}); err != nil {
		t.Fatal(err)
	}
	_ = stmt.Close()
	if err := dtx.Commit(); err != nil {
		t.Fatal(err)
	}
	qstmt, err := c.Prepare(`SELECT name FROM contacts WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	drows, err := qstmt.Query([]driver.Value{id[:]})
	if err != nil {
		t.Fatal(err)
	}
	cols := drows.Columns()
	vals := make([]driver.Value, len(cols))
	if err := drows.Next(vals); err != nil {
		t.Fatal(err)
	}
	_ = drows.Close()
	_ = qstmt.Close()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	var drv sqlDriver
	if _, err := drv.Open("coverage-missing-handle"); err == nil {
		t.Fatal("unknown handle accepted")
	}
	handle := fmt.Sprintf("coverage-%d", time.Now().UnixNano())
	RegisterDriverDB(handle, db)
	rc, err := drv.Open(handle)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
}

// TestToDriverValueTable pins integer-width normalization and overflow.
func TestToDriverValueTable(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		in   any
		want any
	}{
		{"nil", nil, nil},
		{"int64", int64(-1), int64(-1)},
		{"float64", 1.5, 1.5},
		{"bool", true, true},
		{"string", "s", "s"},
		{"time", now, now},
		{"int", int(2), int64(2)},
		{"int8", int8(3), int64(3)},
		{"int16", int16(4), int64(4)},
		{"int32", int32(5), int64(5)},
		{"uint", uint(6), int64(6)},
		{"uint8", uint8(7), int64(7)},
		{"uint16", uint16(8), int64(8)},
		{"uint32", uint32(9), int64(9)},
		{"uint64", uint64(10), int64(10)},
		{"float32", float32(1.5), float64(float32(1.5))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := toDriverValue(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprintf("%v|%T", got, got) != fmt.Sprintf("%v|%T", tc.want, tc.want) {
				t.Fatalf("= %#v, want %#v", got, tc.want)
			}
		})
	}
	if _, err := toDriverValue(uint64(1) << 63); err == nil {
		t.Fatal("uint64 overflow accepted")
	}
	if _, err := toDriverValue(uint(1) << 63); err == nil {
		t.Fatal("uint overflow accepted")
	}
	if _, err := toDriverValue(struct{}{}); err == nil {
		t.Fatal("unsupported type accepted")
	}
	var nilBytes []byte
	if got, err := toDriverValue(nilBytes); err != nil || got != nil {
		t.Fatalf("nil bytes = %#v/%v", got, err)
	}
}

// TestWriterClassString pins scheduler class names and ticket class.
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
	if cols, err := plain.BridgeHighOwnedColumns("contacts", row); err != nil || len(cols) != 0 {
		t.Fatalf("fresh row owned columns = %v/%v", cols, err)
	}
	if err := plain.ReleaseBridgeRowOwnership(ctx, "contacts", row); err == nil {
		t.Fatal("release without provenance succeeded")
	}
	if err := fdb.ReleaseBridgeFileRowOwnership(ctx, row); err == nil {
		t.Fatal("file release without provenance succeeded")
	}
	if _, err := bridgeShadowClearsForRow(plain, 0xBEEF, row); err == nil {
		t.Fatal("shadow clear for unknown table succeeded")
	}
	clears, err := bridgeShadowClearsForRow(plain, bridgeContactsID(t, plain), row)
	if err != nil || len(clears) == 0 {
		t.Fatalf("shadow clears = %d/%v", len(clears), err)
	}
}

func bridgeContactsID(t *testing.T, db *DB) uint32 {
	t.Helper()
	tt, err := db.bridgeTable("contacts")
	if err != nil {
		t.Fatal(err)
	}
	return tt.ID
}

// TestPebbleZstdLevelValidation pins the supported Zstd levels: 3 (default),
// 9, and 12. Level 0 defaults to 3; anything else fails closed.
func TestPebbleZstdLevelValidation(t *testing.T) {
	for _, tc := range []struct {
		level int
		want  bool
	}{
		{0, true}, {3, true}, {9, true}, {12, true},
		{1, false}, {2, false}, {5, false}, {7, false}, {13, false}, {22, false}, {-1, false},
	} {
		t.Run(fmt.Sprintf("level-%d", tc.level), func(t *testing.T) {
			cfg := testConfig(t.TempDir())
			cfg.Pebble.Compression = CompressionConfig{Algorithm: CompressionZstd, ZstdLevel: tc.level}
			cfg.withDefaults()
			err := cfg.validate()
			if tc.want && err != nil {
				t.Fatalf("level %d rejected: %v", tc.level, err)
			}
			if !tc.want && err == nil {
				t.Fatalf("level %d accepted", tc.level)
			}
		})
	}
}

// TestZstdProfileForLevel proves level 3 reuses Pebble's shared profile
// while 9/12 get copies with the level overridden, never mutating the
// shared profile.
func TestZstdProfileForLevel(t *testing.T) {
	builtin := block.CompressionProfileByName("zstd")
	if zstdProfileForLevel(3) != builtin {
		t.Fatal("level 3 does not reuse the built-in profile")
	}
	for _, level := range []int{9, 12} {
		prof := zstdProfileForLevel(level)
		if prof == builtin {
			t.Fatalf("level %d aliases the shared profile", level)
		}
		if prof.Name != fmt.Sprintf("zstd-%d", level) {
			t.Fatalf("name = %q", prof.Name)
		}
		if prof.DataBlocks.Level != uint8(level) || prof.ValueBlocks.Level != uint8(level) || prof.OtherBlocks.Level != uint8(level) {
			t.Fatalf("level %d not applied to all block kinds", level)
		}
		if prof.MinReductionPercent != builtin.MinReductionPercent {
			t.Fatal("reduction threshold differs from built-in")
		}
	}
	if builtin.DataBlocks.Level != 3 {
		t.Fatalf("shared profile mutated: level %d", builtin.DataBlocks.Level)
	}
}

// TestOpenWithZstdLevels proves databases open, write, and close on every
// supported compression mode.
func TestOpenWithZstdLevels(t *testing.T) {
	ctx := context.Background()
	modes := []CompressionConfig{
		{Algorithm: CompressionNone},
		{Algorithm: CompressionZstd, ZstdLevel: 3},
		{Algorithm: CompressionZstd, ZstdLevel: 9},
		{Algorithm: CompressionZstd, ZstdLevel: 12},
		{Algorithm: CompressionSnappy},
	}
	for _, mode := range modes {
		t.Run(fmt.Sprintf("%s-%d", mode.Algorithm, mode.ZstdLevel), func(t *testing.T) {
			cfg := testConfig(t.TempDir())
			cfg.Pebble.Compression = mode
			db, err := openSignedFixture(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			id := NewRowID()
			if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "z"); err != nil {
				t.Fatal(err)
			}
			var n int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM contacts`).Scan(&n); err != nil || n != 1 {
				t.Fatalf("count = %d/%v", n, err)
			}
		})
	}
}
