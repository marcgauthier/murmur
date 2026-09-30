package replicateddb

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/crypto"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

func testSchema() []schema.TableSchema {
	return []schema.TableSchema{
		{
			Name: "contacts",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
				{Name: "phone", Type: schema.ColText, Nullable: true},
				{Name: "score", Type: schema.ColInteger, Nullable: true},
			},
		},
	}
}

// testKeyID and testKey give every test an encrypted store in direct-key
// mode. Tests exercising provider mode overwrite cfg.Encryption wholesale.
var testKey = bytes.Repeat([]byte{0x3a}, 32)

const testKeyID = "test-key"

func testConfig(path string) Config {
	return Config{
		Path:   path,
		NodeID: NewNodeID(),
		Schema: SchemaConfig{Version: 1, Tables: testSchema()},
		Pebble: DefaultPebbleConfig(),
		Encryption: EncryptionConfig{
			Key:   append([]byte(nil), testKey...),
			KeyID: testKeyID,
		},
	}
}

func TestReplicationDisseminationConfigValidation(t *testing.T) {
	base := testConfig(t.TempDir())
	base.withDefaults()
	base.Replication.Dissemination = DisseminationPlumtree
	base.Replication.Fanout = 1
	if err := base.validate(); err == nil {
		t.Fatal("Plumtree accepted fanout below two")
	}

	base.Replication.Fanout = 2
	if err := base.validate(); err != nil {
		t.Fatalf("valid Plumtree config rejected: %v", err)
	}
	base.Replication.Dissemination = "unknown"
	if err := base.validate(); err == nil {
		t.Fatal("unknown dissemination mode accepted")
	}
}

// providerConfig returns cfg with provider-mode encryption under id.
func providerConfig(cfg Config, id string, key []byte) Config {
	cfg.Encryption = EncryptionConfig{
		KeyID:    id,
		Provider: &crypto.StaticProvider{ID: id, Key: key},
	}
	return cfg
}

func queryAll(t *testing.T, db *DB, q string, args ...any) [][]any {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), q, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out [][]any
	cols := rows.Columns()
	for rows.Next() {
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, dest)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestLocalWriteReopenRebuild is the PLAN section 91 prototype path: SQL
// write -> capture -> Badger -> close -> reopen -> rebuild -> identical query.
func TestLocalWriteReopenRebuild(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	cfg := testConfig(path)

	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	id1, id2 := NewRowID(), NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name, phone, score) VALUES (?, ?, ?, ?)`,
		id1[:], "ann", "111", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id2[:], "bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE contacts SET phone = ?, score = ? WHERE id = ?`, "222", 20, id1[:]); err != nil {
		t.Fatal(err)
	}
	before := queryAll(t, db, `SELECT id, name, phone, score FROM contacts ORDER BY name`)
	if len(before) != 2 {
		t.Fatalf("want 2 rows, got %d", len(before))
	}
	st := db.Status()
	if st.State != StateReady || st.StateGeneration == 0 || st.MaterializedGeneration != st.StateGeneration {
		t.Fatalf("bad status: %+v", st)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with the same NodeID: the in-memory database is rebuilt purely
	// from Badger and must be identical.
	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	after := queryAll(t, db2, `SELECT id, name, phone, score FROM contacts ORDER BY name`)
	if fmt.Sprintf("%v", before) != fmt.Sprintf("%v", after) {
		t.Fatalf("rebuilt state differs:\nbefore=%v\nafter=%v", before, after)
	}
	// The DBID persisted across the restart.
	if db2.DBID().IsZero() {
		t.Fatal("dbid not persisted")
	}
}

func TestExplicitTxCoalescing(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, phone) VALUES (?, ?)`, id[:], "0"); err != nil {
		t.Fatal(err)
	}
	genBefore := db.Status().StateGeneration
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, ph := range []string{"1", "2", "3"} {
		if _, err := tx.ExecContext(ctx, `UPDATE contacts SET phone = ? WHERE id = ?`, ph, id[:]); err != nil {
			t.Fatal(err)
		}
	}
	// Read-your-write inside the tx.
	txrows, err := tx.QueryContext(ctx, `SELECT phone FROM contacts WHERE id = ?`, id[:])
	if err != nil {
		t.Fatal(err)
	}
	var ph string
	for txrows.Next() {
		_ = txrows.Scan(&ph)
	}
	txrows.Close()
	if ph != "3" {
		t.Fatalf("read-your-write = %q", ph)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// One Badger generation bump for the whole multi-statement transaction.
	if got := db.Status().StateGeneration; got != genBefore+1 {
		t.Fatalf("generation %d -> %d, want +1", genBefore, got)
	}
	got := queryAll(t, db, `SELECT phone FROM contacts WHERE id = ?`, id[:])
	if len(got) != 1 || got[0][0] != "3" {
		t.Fatalf("got %v", got)
	}
}

func TestTxRollback(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := NewRowID()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO contacts (id) VALUES (?)`, id[:]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxDone) {
		t.Fatalf("expected ErrTxDone, got %v", err)
	}
	if n := len(queryAll(t, db, `SELECT id FROM contacts`)); n != 0 {
		t.Fatalf("rolled-back row visible: %d rows", n)
	}
	if gen := db.Status().StateGeneration; gen != 0 {
		t.Fatalf("generation = %d after rollback-only", gen)
	}
}

func TestDeleteAndResurrectSQL(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM contacts WHERE id = ?`, id[:]); err != nil {
		t.Fatal(err)
	}
	if n := len(queryAll(t, db, `SELECT id FROM contacts`)); n != 0 {
		t.Fatalf("deleted row visible")
	}
	// Resurrect with the same UUID.
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "y"); err != nil {
		t.Fatal(err)
	}
	got := queryAll(t, db, `SELECT name FROM contacts`)
	if len(got) != 1 || got[0][0] != "y" {
		t.Fatalf("got %v", got)
	}
}

func TestQueryRowAndPrepare(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "zed"); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.QueryRowContext(ctx, `SELECT name FROM contacts WHERE id = ?`, id[:]).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "zed" {
		t.Fatalf("got %q", name)
	}
	stmt, err := db.PrepareContext(ctx, `SELECT name FROM contacts WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var name2 string
	if err := stmt.QueryRowContext(ctx, id[:]).Scan(&name2); err != nil {
		t.Fatal(err)
	}
	if name2 != "zed" {
		t.Fatalf("got %q", name2)
	}
}

func TestOversizeValueRejected(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.MaxReplicatedValueBytes = 16
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := NewRowID()
	_, err = db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "way too long value")
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got %v", err)
	}
	// The failed write rolled back cleanly: nothing visible, still ready.
	if n := len(queryAll(t, db, `SELECT id FROM contacts`)); n != 0 {
		t.Fatalf("oversize row visible")
	}
	if st := db.Status().State; st != StateReady {
		t.Fatalf("state = %s", st)
	}
}

func TestSchemaMismatchFailsClosed(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	cfg := testConfig(path)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	// Reopen with a different schema epoch: must refuse.
	cfg.Schema.Version = 2
	if _, err := Open(ctx, cfg); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("expected ErrSchemaMismatch, got %v", err)
	}
}

func randomKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

func TestEncryptedOpenWrongKeyFails(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	key := randomKey(t)
	cfg := providerConfig(testConfig(path), "k1", key)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "secret"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// Correct key reopens.
	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(queryAll(t, db2, `SELECT id FROM contacts`)); n != 1 {
		t.Fatalf("want 1 row, got %d", n)
	}
	_ = db2.Close()

	// Wrong key fails to open.
	cfg = providerConfig(cfg, "k1", randomKey(t))
	if _, err := Open(ctx, cfg); err == nil {
		t.Fatal("expected open with wrong key to fail")
	}
}

func TestRotateStorageKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	path := t.TempDir()
	key1 := randomKey(t)
	key2 := randomKey(t)
	cfg := providerConfig(testConfig(path), "k1", key1)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	id1 := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id1[:], "pre-rotation"); err != nil {
		t.Fatal(err)
	}
	// Invalid rotation inputs are rejected without touching the registry.
	bad := []KeyMaterial{
		{ID: "", Algorithm: string(AES256GCM), Key: key2},        // empty ID
		{ID: "k1", Algorithm: string(AES256GCM), Key: key2},      // current ID
		{ID: "k2", Algorithm: "", Key: key2},                     // missing algorithm
		{ID: "k2", Algorithm: string(AES256GCM), Key: key2[:16]}, // wrong length
		{ID: "k2", Algorithm: "nope", Key: key2},                 // unknown algorithm
	}
	for i, m := range bad {
		if err := db.RotateStorageKey(ctx, m); err == nil {
			t.Fatalf("bad material %d accepted", i)
		}
	}
	if st := db.EncryptionStatus(); st.ApplicationKeyID != "k1" {
		t.Fatalf("appid after rejected rotations = %q", st.ApplicationKeyID)
	}
	if err := db.RotateStorageKey(ctx, KeyMaterial{ID: "k2", Algorithm: string(AES256GCM), Key: key2}); err != nil {
		t.Fatal(err)
	}
	if st := db.EncryptionStatus(); st.ApplicationKeyID != "k2" {
		t.Fatalf("appid after rotation = %q", st.ApplicationKeyID)
	}
	// A data-key rotation after storage rotation must persist under the new
	// wrapping key (not silently re-wrap under the stale provider key).
	if err := db.RotateDataKey(ctx); err != nil {
		t.Fatal(err)
	}
	id2 := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id2[:], "post-rotation"); err != nil {
		t.Fatal(err)
	}
	before := queryAll(t, db, `SELECT name FROM contacts ORDER BY name`)
	_ = db.Close()

	// New key opens with all data.
	cfg = providerConfig(cfg, "k2", key2)
	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	after := queryAll(t, db2, `SELECT name FROM contacts ORDER BY name`)
	_ = db2.Close()
	if fmt.Sprintf("%v", before) != fmt.Sprintf("%v", after) {
		t.Fatalf("after rotation: %v != %v", before, after)
	}

	// Old key is rejected.
	cfg = providerConfig(cfg, "k1", key1)
	if _, err := Open(ctx, cfg); err == nil {
		t.Fatal("expected old key to be rejected after rotation")
	}
}

func TestStatusBasics(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st := db.Status()
	if st.State != StateReady || st.NodeID.IsZero() || st.DBID.IsZero() {
		t.Fatalf("bad status: %+v", st)
	}
	if st.SchemaEpoch != 1 {
		t.Fatalf("schema epoch = %d", st.SchemaEpoch)
	}
}

func TestIsReadOnlyStatement(t *testing.T) {
	for _, q := range []string{
		"SELECT 1", "  select a from t", "-- comment\nSELECT 1",
		"/* x */ SELECT 1", "EXPLAIN SELECT 1", "PRAGMA table_info(t)",
	} {
		if !isReadOnlyStatement(q) {
			t.Fatalf("%q should be read-only", q)
		}
	}
	for _, q := range []string{
		"INSERT INTO t VALUES (1)", "UPDATE t SET a=1", "DELETE FROM t",
		"WITH x AS (SELECT 1) UPDATE t SET a=1", "BEGIN", "",
	} {
		if isReadOnlyStatement(q) {
			t.Fatalf("%q should be a write", q)
		}
	}
}

func TestNodeIDHelpers(t *testing.T) {
	n := NewNodeID()
	if n.IsZero() {
		t.Fatal("zero node id")
	}
	parsed, err := ParseNodeID(n.String())
	if err != nil || parsed != n {
		t.Fatalf("roundtrip: %v", err)
	}
	if MustNodeID(n.String()) != n {
		t.Fatal("MustNodeID mismatch")
	}
	_ = ids.NewDBID
}

// Reopening a database must preserve declaration column order. The stored
// schema manifest sorts columns by hash-derived ID; rebuilding the live
// registry from it (instead of the config declaration) used to permute
// physical column order on every reopen whenever ID order differed from
// declaration order (contacts.name sorts before contacts.id by ID).
func TestRestartPreservesDeclarationColumnOrder(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := testConfig(dir)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cols := queryColumns(t, db, `SELECT * FROM contacts`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "name", "phone", "score"}
	if fmt.Sprint(cols) != fmt.Sprint(want) {
		t.Fatalf("fresh open columns=%v, want %v", cols, want)
	}

	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	cols2 := queryColumns(t, db2, `SELECT * FROM contacts`)
	if fmt.Sprint(cols2) != fmt.Sprint(want) {
		t.Fatalf("reopen columns=%v, want %v (declaration order)", cols2, want)
	}
}

func queryColumns(t *testing.T, db *DB, q string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	return rows.Columns()
}

func TestMetricsStmtCacheCounters(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before := db.Metrics()
	queryAll(t, db, `SELECT id, name FROM contacts`)
	queryAll(t, db, `SELECT id, name FROM contacts`)
	after := db.Metrics()
	if after.StmtCacheMisses-before.StmtCacheMisses != 1 ||
		after.StmtCacheHits-before.StmtCacheHits != 1 {
		t.Fatalf("stmt cache delta hits=%d misses=%d, want 1/1",
			after.StmtCacheHits-before.StmtCacheHits,
			after.StmtCacheMisses-before.StmtCacheMisses)
	}
}

func TestPebbleCacheSettings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// 1. Automatic default when zero: 256 MiB
	cfgDefault := testConfig(filepath.Join(dir, "default"))
	cfgDefault.withDefaults()
	if cfgDefault.Pebble.CacheBytes != 256<<20 {
		t.Fatalf("expected default 256MiB, got %d", cfgDefault.Pebble.CacheBytes)
	}

	// 2. Setting via cfg.Cache.BlockCacheBytes alias
	cfgAlias := testConfig(filepath.Join(dir, "alias"))
	cfgAlias.Cache.BlockCacheBytes = 64 << 20
	cfgAlias.withDefaults()
	if cfgAlias.Pebble.CacheBytes != 64<<20 {
		t.Fatalf("expected 64MiB via alias, got %d", cfgAlias.Pebble.CacheBytes)
	}

	// 3. Setting via cfg.Pebble.CacheBytes directly
	cfgDirect := testConfig(filepath.Join(dir, "direct"))
	cfgDirect.Pebble.CacheBytes = 128 << 20
	cfgDirect.withDefaults()
	if cfgDirect.Pebble.CacheBytes != 128<<20 {
		t.Fatalf("expected 128MiB direct, got %d", cfgDirect.Pebble.CacheBytes)
	}

	// 4. Open real DB with custom cache size
	db, err := Open(ctx, cfgDirect)
	if err != nil {
		t.Fatalf("Open with custom cache failed: %v", err)
	}
	metrics := db.store.Metrics()
	t.Logf("Pebble metrics with 128MiB cache setting: %+v", metrics)
	_ = db.Close()
}

