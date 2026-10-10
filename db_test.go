package murmur

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/crypto"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/rime"
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

type testContactRecord struct {
	ID    ids.RowID `rime:"primary"`
	Name  string
	Phone string
	Score int64
}

// testKeyID and testKey give every test an encrypted store in direct-key
// mode. Tests exercising provider mode overwrite cfg.Encryption wholesale.
var testKey = bytes.Repeat([]byte{0x3a}, 32)

const testKeyID = "test-key"

func testConfig(path string) Config {
	node := NewNodeID()
	contacts, err := define[testContactRecord]("contacts", 1, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2, "Phone": 3, "Score": 4},
	})
	if err != nil {
		panic(fmt.Sprintf("compile test record schema: %v", err))
	}
	return Config{
		OriginSigning: testidentity.Config(node),
		Path:          path,
		NodeID:        node,
		Schema:        SchemaConfig{Version: 1},
		Tables:        []TableDefinition{contacts},
		Spool:         DefaultSpoolConfig(),
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

func TestOpenRejectsLegacySQLSchema(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Tables = nil
	cfg.Schema = SchemaConfig{Version: 1, Tables: testSchema()}
	_, err := Open(context.Background(), cfg)
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("Open with Schema.Tables only: got %v, want ErrUnsupportedSchema", err)
	}
	if !strings.Contains(err.Error(), "SQL schemas are no longer supported") {
		t.Fatalf("Open error lacks migration guidance: %v", err)
	}
}

func assertMaterializerCurrent(t *testing.T, db *DB) {
	t.Helper()
	status := db.Status()
	if status.MaterializedGeneration != status.StateGeneration {
		t.Fatalf("RIME materializer generation %d != durable generation %d", status.MaterializedGeneration, status.StateGeneration)
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

func TestTypedLocalWriteReopenRebuild(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	id1, id2 := NewRowID(), NewRowID()
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Insert(tx, &facadeRecord{ID: id1, Name: "ann"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Insert(tx, &facadeRecord{ID: id2, Name: "bob"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Update(tx, id1, func(row *facadeRecord) error { row.Name = "ann-updated"; return nil })
	}); err != nil {
		t.Fatal(err)
	}
	before1, err := table.Get(id1)
	if err != nil {
		t.Fatal(err)
	}
	before2, err := table.Get(id2)
	if err != nil {
		t.Fatal(err)
	}
	if before1.Name != "ann-updated" || before2.Name != "bob" {
		t.Fatalf("typed rows before reopen = %+v, %+v", before1, before2)
	}
	st := db.Status()
	if st.State != StateReady || st.StateGeneration == 0 || st.MaterializedGeneration != st.StateGeneration {
		t.Fatalf("bad status: %+v", st)
	}
	dbid := db.DBID()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with the same identity: RIME is rebuilt from durable Spool state.
	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	table2, err := tableOf[facadeRecord](db2, "records")
	if err != nil {
		t.Fatal(err)
	}
	after1, err := table2.Get(id1)
	if err != nil || *after1 != *before1 {
		t.Fatalf("rebuilt first row = %+v, %v; before %+v", after1, err, before1)
	}
	after2, err := table2.Get(id2)
	if err != nil || *after2 != *before2 {
		t.Fatalf("rebuilt second row = %+v, %v; before %+v", after2, err, before2)
	}
	if db2.DBID() != dbid {
		t.Fatalf("dbid after restart = %v, want %v", db2.DBID(), dbid)
	}
}

func TestTypedExplicitTxCoalescing(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	id := NewRowID()
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Insert(tx, &facadeRecord{ID: id, Name: "0"})
	}); err != nil {
		t.Fatal(err)
	}
	genBefore := db.Status().StateGeneration
	tx, err := db.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, ph := range []string{"1", "2", "3"} {
		if err := table.Update(tx, id, func(row *facadeRecord) error { row.Name = ph; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	// Read-your-write inside the typed transaction overlay.
	got, err := table.GetTx(tx, id)
	if err != nil || got.Name != "3" {
		t.Fatalf("typed read-your-write = %+v, %v", got, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// One durable generation bump for the whole multi-update transaction.
	if got := db.Status().StateGeneration; got != genBefore+1 {
		t.Fatalf("generation %d -> %d, want +1", genBefore, got)
	}
	committed, err := table.Get(id)
	if err != nil || committed.Name != "3" {
		t.Fatalf("committed typed value = %+v, %v", committed, err)
	}
}

func TestTypedDeleteAndResurrect(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	id := NewRowID()
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Insert(tx, &facadeRecord{ID: id, Name: "x"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error { return table.Delete(tx, id) }); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Get(id); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("deleted typed row lookup error = %v", err)
	}
	// Resurrect with the same UUID.
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Insert(tx, &facadeRecord{ID: id, Name: "y"})
	}); err != nil {
		t.Fatal(err)
	}
	got, err := table.Get(id)
	if err != nil || got.Name != "y" {
		t.Fatalf("resurrected typed row = %+v, %v", got, err)
	}
}

func TestTypedOversizeValueRejected(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	cfg.MaxReplicatedValueBytes = 16
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	id := ids.NewRowID()
	err = db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Insert(tx, &facadeRecord{ID: id, Name: "way too long value"})
	})
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got %v", err)
	}
	// The failed write rolled back cleanly: nothing visible, still ready.
	if _, err := table.Get(id); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("oversize typed row lookup error=%v", err)
	}
	if st := db.Status().State; st != StateReady {
		t.Fatalf("state = %s", st)
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
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	id := ids.NewRowID()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error { return table.Insert(tx, &facadeRecord{ID: id, Name: "secret"}) }); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// Correct key reopens.
	db2, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err = tableOf[facadeRecord](db2, "records")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := table.Get(id); err != nil || got.Name != "secret" {
		t.Fatalf("correct-key typed reopen row = %+v, %v", got, err)
	}
	_ = db2.Close()

	// Wrong key fails to open.
	cfg = providerConfig(cfg, "k1", randomKey(t))
	if _, err := openSignedFixture(ctx, cfg); err == nil {
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
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	id1 := ids.NewRowID()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error { return table.Insert(tx, &facadeRecord{ID: id1, Name: "pre-rotation"}) }); err != nil {
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
	id2 := ids.NewRowID()
	if err := db.WriteTxContext(ctx, func(tx *Tx) error { return table.Insert(tx, &facadeRecord{ID: id2, Name: "post-rotation"}) }); err != nil {
		t.Fatal(err)
	}
	before1, err := table.Get(id1)
	if err != nil {
		t.Fatal(err)
	}
	before2, err := table.Get(id2)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// New key opens with all data.
	cfg = providerConfig(cfg, "k2", key2)
	db2, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table2, err := tableOf[facadeRecord](db2, "records")
	if err != nil {
		t.Fatal(err)
	}
	after1, err := table2.Get(id1)
	if err != nil {
		t.Fatal(err)
	}
	after2, err := table2.Get(id2)
	if err != nil {
		t.Fatal(err)
	}
	_ = db2.Close()
	if *before1 != *after1 || *before2 != *after2 {
		t.Fatalf("rows after rotation: before=(%+v,%+v), after=(%+v,%+v)", before1, before2, after1, after2)
	}

	// Old key is rejected.
	cfg = providerConfig(cfg, "k1", key1)
	if _, err := openSignedFixture(ctx, cfg); err == nil {
		t.Fatal("expected old key to be rejected after rotation")
	}
}

func TestStatusBasics(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(ctx, cfg)
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
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	schemaBefore, err := db.SchemaTables()
	if err != nil || len(schemaBefore) != 1 {
		t.Fatalf("typed schema before reopen: tables=%v err=%v", schemaBefore, err)
	}
	cols := schemaBefore[0].Columns
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	schemaAfter, err := db2.SchemaTables()
	if err != nil || len(schemaAfter) != 1 {
		t.Fatalf("typed schema after reopen: tables=%v err=%v", schemaAfter, err)
	}
	if fmt.Sprint(schemaAfter[0].Columns) != fmt.Sprint(cols) {
		t.Fatalf("reopen field order=%v, want %v", schemaAfter[0].Columns, cols)
	}
}

func TestSpoolConfigSettings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// 1. Automatic defaults
	cfgDefault := testConfig(filepath.Join(dir, "default"))
	cfgDefault.withDefaults()
	if cfgDefault.Spool.TargetBlockBytes != 4<<20 {
		t.Fatalf("expected default TargetBlockBytes 4MiB, got %d", cfgDefault.Spool.TargetBlockBytes)
	}

	// 2. Custom Spool settings
	cfgDirect := testConfig(filepath.Join(dir, "direct"))
	cfgDirect.Spool.TargetBlockBytes = 2 << 20
	cfgDirect.Spool.MaxBlockBytes = 32 << 20
	cfgDirect.withDefaults()
	if cfgDirect.Spool.TargetBlockBytes != 2<<20 {
		t.Fatalf("expected 2MiB direct, got %d", cfgDirect.Spool.TargetBlockBytes)
	}

	// 3. Open real DB with custom Spool configuration
	cfgDirect.Schema.Tables = nil
	cfgDirect.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(ctx, cfgDirect)
	if err != nil {
		t.Fatalf("Open with custom Spool config failed: %v", err)
	}
	metrics := db.store.Metrics()
	t.Logf("Spool metrics: %+v", metrics)
	_ = db.Close()
}
