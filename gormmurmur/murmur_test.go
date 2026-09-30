package murmur

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	replicateddb "github.com/marcgauthier/murmur"
	murmurSchema "github.com/marcgauthier/murmur/schema"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

var testKey = []byte("0123456789abcdef0123456789abcdef")

// Author exercises the full column spread: text, integer, real,
// boolean-as-integer, nullable pointer, blob, integer auto-time, and
// a has-many association.
type Author struct {
	Model
	Name   string
	Age    int
	Score  float64
	Active bool
	Nick   *string
	Avatar []byte
	Stamp  int64  `gorm:"autoCreateTime"`
	Books  []Book `gorm:"foreignKey:AuthorID"`
}

type Book struct {
	Model
	Title    string
	AuthorID ID
	Author   Author `gorm:"foreignKey:AuthorID"`
}

type AuthorWithExtra struct {
	Model
	Name  string
	Extra *string
}

func (AuthorWithExtra) TableName() string { return "authors" }

type rxWrongPKAuthor struct {
	Key  ID `gorm:"primaryKey;column:key"`
	Name string
}

func (rxWrongPKAuthor) TableName() string { return "authors" }

type preflightFirst struct {
	Model
	Name string
}

func (preflightFirst) TableName() string { return "preflight_first" }

type preflightInvalid struct {
	Model
	Name string `gorm:"default:'missing'"`
}

func (preflightInvalid) TableName() string { return "preflight_invalid" }

func openEngine(t *testing.T, models ...interface{}) *replicateddb.DB {
	t.Helper()
	genesis, err := GenesisTables(models...)
	if err != nil {
		t.Fatalf("genesis: %v", err)
	}
	return openEngineWithTables(t, genesis)
}

func openEngineWithTables(t *testing.T, tables []murmurSchema.TableSchema) *replicateddb.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := replicateddb.Open(context.Background(), replicateddb.Config{
		Path:       filepath.Join(dir, "data"),
		NodeID:     replicateddb.NewNodeID(),
		DBID:       replicateddb.NewDBID(),
		Encryption: replicateddb.EncryptionConfig{Key: append([]byte(nil), testKey...), KeyID: "test"},
		Schema:     replicateddb.SchemaConfig{Version: 1, Tables: tables},
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func openGorm(t *testing.T, db *replicateddb.DB, models ...interface{}) *gorm.DB {
	t.Helper()
	gdb, err := gorm.Open(Open(db), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm open: %v", err)
	}
	// Migrate twice: genesis tables must be idempotent no-ops.
	for i := 0; i < 2; i++ {
		if err := gdb.AutoMigrate(models...); err != nil {
			t.Fatalf("automigrate (pass %d): %v", i, err)
		}
	}
	return gdb
}

func TestGormMurmurCRUD(t *testing.T) {
	db := openEngine(t, &Author{}, &Book{})
	gdb := openGorm(t, db, &Author{}, &Book{})

	author := Author{Name: "ada", Age: 36, Score: 9.5, Active: true, Avatar: []byte{1, 2, 3}}
	if err := gdb.Create(&author).Error; err != nil {
		t.Fatalf("create author: %v", err)
	}
	if author.ID.IsZero() {
		t.Fatal("author ID not filled")
	}
	if author.CreatedAt.Time.IsZero() || author.UpdatedAt.Time.IsZero() {
		t.Fatalf("timestamps not set: %+v", author.Model)
	}
	if author.Stamp == 0 {
		t.Fatal("integer autoCreateTime not set")
	}
	if author.DeletedAt.Valid {
		t.Fatal("fresh DeletedAt valid")
	}

	explicit := NewID()
	books := []Book{
		{Model: Model{ID: explicit}, Title: "notes", AuthorID: author.ID},
		{Title: "letters", AuthorID: author.ID}, // zero ID fills in
	}
	if err := gdb.Create(&books).Error; err != nil {
		t.Fatalf("create books: %v", err)
	}
	if books[0].ID != explicit {
		t.Fatal("explicit ID not preserved")
	}
	if books[1].ID.IsZero() {
		t.Fatal("batch ID not filled")
	}

	var count int64
	if err := gdb.Model(&Book{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("count = %d, err = %v, want 2", count, err)
	}
	var found Author
	if err := gdb.Preload("Books").First(&found, "name = ?", "ada").Error; err != nil {
		t.Fatalf("preload: %v", err)
	}
	if len(found.Books) != 2 || !found.Active || found.Age != 36 || found.Score != 9.5 {
		t.Fatalf("preloaded author mismatch: %+v", found)
	}
	var one Book
	if err := gdb.Preload("Author").First(&one, "title = ?", "notes").Error; err != nil {
		t.Fatalf("belongs-to preload: %v", err)
	}
	if one.Author.Name != "ada" {
		t.Fatalf("preloaded book author = %q", one.Author.Name)
	}

	before := author.UpdatedAt.Time
	time.Sleep(5 * time.Millisecond)
	if err := gdb.Model(&author).Update("Name", "lovelace").Error; err != nil {
		t.Fatalf("update: %v", err)
	}
	var updated Author
	if err := gdb.First(&updated, "id = ?", author.ID).Error; err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if updated.Name != "lovelace" || !updated.UpdatedAt.Time.After(before) {
		t.Fatalf("after update: %+v", updated)
	}

	if err := gdb.Delete(&author).Error; err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	if err := gdb.First(&Author{}, "id = ?", author.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("soft-deleted row visible: %v", err)
	}
	var deleted Author
	if err := gdb.Unscoped().First(&deleted, "id = ?", author.ID).Error; err != nil {
		t.Fatalf("unscoped read: %v", err)
	}
	if !deleted.DeletedAt.Valid {
		t.Fatal("DeletedAt not set by soft delete")
	}
	if err := gdb.Unscoped().Delete(&author).Error; err != nil {
		t.Fatalf("hard delete: %v", err)
	}
	if err := gdb.Unscoped().First(&Author{}, "id = ?", author.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("hard-deleted row visible: %v", err)
	}

	// The stored table carries an explicit NOT NULL primary key.
	var ddl string
	rows, err := db.QueryContext(context.Background(), "SELECT sql FROM sqlite_master WHERE name = 'authors'")
	if err != nil {
		t.Fatalf("sqlite_master: %v", err)
	}
	defer rows.Close()
	if !rows.Next() || rows.Scan(&ddl) != nil {
		t.Fatal("authors missing from sqlite_master")
	}
	if !strings.Contains(ddl, "PRIMARY KEY NOT NULL") {
		t.Fatalf("stored DDL lacks explicit NOT NULL PK: %s", ddl)
	}
}

func TestGormMurmurUpsert(t *testing.T) {
	db := openEngine(t, &Author{})
	gdb := openGorm(t, db, &Author{})

	id := NewID()
	if err := gdb.Create(&Author{Model: Model{ID: id}, Name: "v1"}).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := gdb.Clauses(clause.OnConflict{UpdateAll: true}).
		Create(&Author{Model: Model{ID: id}, Name: "v2"}).Error; err != nil {
		t.Fatalf("upsert: %v", err)
	}
	var got Author
	if err := gdb.First(&got, "id = ?", id).Error; err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Name != "v2" {
		t.Fatalf("after upsert name = %q, want v2", got.Name)
	}
	var count int64
	if err := gdb.Model(&Author{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("count = %d, err = %v, want 1", count, err)
	}
}

// GadgetV1/V2 share one table to prove additive migration.
type GadgetV1 struct {
	Model
	Name string
}

func (GadgetV1) TableName() string { return "gadgets" }

type GadgetV2 struct {
	Model
	Name  string
	Color *string
}

func (GadgetV2) TableName() string { return "gadgets" }

type GadgetV3 struct {
	Model
	Name int // incompatible drift: string -> integer
}

func (GadgetV3) TableName() string { return "gadgets" }

func TestGormMurmurAddColumn(t *testing.T) {
	db := openEngine(t, &GadgetV1{})
	gdb, err := gorm.Open(Open(db), &gorm.Config{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := gdb.AutoMigrate(&GadgetV1{}); err != nil {
		t.Fatalf("migrate v1: %v", err)
	}
	g := GadgetV1{Name: "widget"}
	if err := gdb.Create(&g).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := gdb.AutoMigrate(&GadgetV2{}); err != nil {
		t.Fatalf("migrate v2: %v", err)
	}
	var got GadgetV2
	if err := gdb.First(&got, "id = ?", g.ID).Error; err != nil {
		t.Fatalf("read after add-column: %v", err)
	}
	if got.Name != "widget" || got.Color != nil {
		t.Fatalf("row damaged by migration: %+v", got)
	}
	color := "red"
	if err := gdb.Model(&got).Update("Color", color).Error; err != nil {
		t.Fatalf("update new column: %v", err)
	}
	// Type drift is a loud error, never an ALTER.
	if err := gdb.AutoMigrate(&GadgetV3{}); err == nil {
		t.Fatal("type drift accepted, want additive-only rejection")
	} else if !strings.Contains(strings.ToLower(err.Error()), "additive-only") {
		t.Fatalf("drift error = %v, want additive-only reason", err)
	}
}

func TestGormMurmurMigrationPreflightAndTransactionGuard(t *testing.T) {
	db := openEngine(t, &Author{})
	gdb := openGorm(t, db, &Author{})

	// A later invalid model must not leave an earlier model published.
	err := gdb.AutoMigrate(&preflightFirst{}, &preflightInvalid{})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "default") {
		t.Fatalf("multi-model migration error = %v, want unsupported default tag", err)
	}
	epoch, live, err := db.LiveSchema()
	if err != nil {
		t.Fatalf("live schema: %v", err)
	}
	if epoch != 1 || len(live) != 1 || live[0].Name != "authors" {
		t.Fatalf("schema changed after rejected preflight: epoch=%d tables=%v", epoch, live)
	}

	// Existing tables must still satisfy the id primary-key contract.
	if err := gdb.AutoMigrate(&rxWrongPKAuthor{}); err == nil || !strings.Contains(strings.ToLower(err.Error()), "primary key") {
		t.Fatalf("model without id primary key migration error = %v, want primary-key rejection", err)
	}

	// Publishing a schema while a transaction owns the write lock must
	// fail immediately instead of waiting on that same lock.
	err = gdb.Transaction(func(tx *gorm.DB) error {
		return tx.AutoMigrate(&AuthorWithExtra{})
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "inside a transaction") {
		t.Fatalf("transactional migration error = %v, want explicit rejection", err)
	}
	epoch, live, err = db.LiveSchema()
	if err != nil || epoch != 1 || len(live) != 1 {
		t.Fatalf("schema changed after transactional migration: epoch=%d tables=%v err=%v", epoch, live, err)
	}
}

func TestGormMurmurTransactions(t *testing.T) {
	db := openEngine(t, &Author{})
	gdb := openGorm(t, db, &Author{})

	boom := errors.New("boom")
	err := gdb.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Author{Name: "doomed"}).Error; err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("transaction err = %v, want boom", err)
	}
	var count int64
	if err := gdb.Model(&Author{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("count = %d after rollback, want 0", count)
	}

	err = gdb.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Author{Name: "kept"}).Error; err != nil {
			return err
		}
		return tx.Create(&Author{Name: "kept2"}).Error
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := gdb.Model(&Author{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("count = %d after commit, want 2", count)
	}

	// Manual begin/rollback and nested-transaction rejection.
	tx := gdb.Begin()
	if err := tx.Create(&Author{Name: "manual"}).Error; err != nil {
		t.Fatalf("manual create: %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("manual rollback: %v", err)
	}
	if err := gdb.Model(&Author{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("count = %d after manual rollback, want 2", count)
	}
	err = gdb.Transaction(func(tx *gorm.DB) error {
		return tx.Transaction(func(tx2 *gorm.DB) error { return nil })
	})
	if !errors.Is(err, gorm.ErrUnsupportedDriver) {
		t.Fatalf("nested transaction err = %v, want ErrUnsupportedDriver", err)
	}
}

func TestGormMurmurReopen(t *testing.T) {
	dir := t.TempDir()
	genesis, err := GenesisTables(&Author{})
	if err != nil {
		t.Fatalf("genesis: %v", err)
	}
	nodeID, dbID := replicateddb.NewNodeID(), replicateddb.NewDBID()
	cfg := replicateddb.Config{
		Path:       filepath.Join(dir, "data"),
		NodeID:     nodeID,
		DBID:       dbID,
		Encryption: replicateddb.EncryptionConfig{Key: append([]byte(nil), testKey...), KeyID: "test"},
		Schema:     replicateddb.SchemaConfig{Version: 1, Tables: genesis},
	}
	db, err := replicateddb.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	gdb, err := gorm.Open(Open(db), &gorm.Config{})
	if err != nil {
		db.Close()
		t.Fatalf("gorm open: %v", err)
	}
	if err := gdb.AutoMigrate(&Author{}); err != nil {
		db.Close()
		t.Fatalf("migrate: %v", err)
	}
	a := Author{Name: "persist"}
	if err := gdb.Create(&a).Error; err != nil {
		db.Close()
		t.Fatalf("create: %v", err)
	}
	epoch, live, err := db.LiveSchema()
	if err != nil {
		db.Close()
		t.Fatalf("liveschema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen from the live export and prove data + idempotent migrate.
	cfg.Schema = replicateddb.SchemaConfig{Version: epoch, Tables: live}
	db2, err := replicateddb.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	gdb2, err := gorm.Open(Open(db2), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm reopen: %v", err)
	}
	if err := gdb2.AutoMigrate(&Author{}); err != nil {
		t.Fatalf("migrate after reopen: %v", err)
	}
	var got Author
	if err := gdb2.First(&got, "id = ?", a.ID).Error; err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if got.Name != "persist" {
		t.Fatalf("name = %q after reopen", got.Name)
	}
}

// --- rejection matrix: everything outside the contract fails loudly ---

type rxUniqueIndex struct {
	Model
	Code string `gorm:"uniqueIndex"`
}

type rxIndex struct {
	Model
	Code string `gorm:"index"`
}

type rxUnique struct {
	Model
	Code string `gorm:"unique"`
}

type rxCheck struct {
	Model
	Code string `gorm:"check:code <> ''"`
}

type rxDefault struct {
	Model
	Code string `gorm:"default:'x'"`
}

type rxAutoIncr struct {
	ID   uint `gorm:"primaryKey;autoIncrement"`
	Name string
}

type rxUintPK struct {
	ID   uint `gorm:"primaryKey;column:id"`
	Name string
}

type rxIntPKNoIncr struct {
	ID   uint `gorm:"primaryKey;column:id;autoIncrement:false"`
	Name string
}

type rxPlainTime struct {
	Model
	At time.Time
}

type rxGormDeletedAt struct {
	ID        ID `gorm:"primaryKey;column:id"`
	DeletedAt gorm.DeletedAt
}

type rxNullTime struct {
	Model
	At sql.NullTime
}

type rxNoPK struct {
	Name string
}

type rxCompositePK struct {
	A ID `gorm:"primaryKey"`
	B ID `gorm:"primaryKey"`
}

type rxNonIDPK struct {
	Key  ID `gorm:"primaryKey"`
	Name string
}

type rxRawArrayPK struct {
	ID   [16]byte `gorm:"primaryKey;column:id"`
	Name string
}

type rxConstraint struct {
	Model
	AuthorID ID
	Author   Author `gorm:"foreignKey:AuthorID;constraint:OnDelete:CASCADE"`
}

type rxMany2Many struct {
	Model
	Tags []rxTag `gorm:"many2many:rx_taggings"`
}

type rxTag struct {
	Model
	Name string
}

type rxBadType struct {
	Model
	Code string `gorm:"type:jsonb"`
}

func TestGormMurmurRejections(t *testing.T) {
	db := openEngine(t, &Author{})
	gdb, err := gorm.Open(Open(db), &gorm.Config{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := gdb.AutoMigrate(&Author{}); err != nil {
		t.Fatalf("genesis migrate: %v", err)
	}

	cases := []struct {
		name  string
		model interface{}
		want  string
	}{
		{"uniqueIndex tag", &rxUniqueIndex{}, "unique"},
		{"index tag", &rxIndex{}, "index"},
		{"unique tag", &rxUnique{}, "unique"},
		{"check tag", &rxCheck{}, "check"},
		{"default tag", &rxDefault{}, "default"},
		{"autoincrement pk", &rxAutoIncr{}, "auto_increment"},
		{"uint pk", &rxUintPK{}, "auto_increment"},
		{"uint pk no incr", &rxIntPKNoIncr{}, "blob"},
		{"plain time.Time", &rxPlainTime{}, "murmur.time"},
		{"gorm.DeletedAt", &rxGormDeletedAt{}, "murmur.deletedat"},
		{"sql.NullTime", &rxNullTime{}, "murmur.time"},
		{"no pk", &rxNoPK{}, "exactly one primary key"},
		{"composite pk", &rxCompositePK{}, "composite"},
		{"non-id pk", &rxNonIDPK{}, "must be the `id` column"},
		{"raw array pk", &rxRawArrayPK{}, "cannot bind"},
		{"constraint tag", &rxConstraint{}, "constraint"},
		{"many2many", &rxMany2Many{}, "many-to-many"},
		{"bad type tag", &rxBadType{}, "no murmur equivalent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := gdb.AutoMigrate(tc.model); err == nil {
				t.Fatalf("AutoMigrate(%T) succeeded, want rejection containing %q", tc.model, tc.want)
			} else if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("AutoMigrate(%T) = %v, want containing %q", tc.model, err, tc.want)
			}
			if err := gdb.Migrator().CreateTable(tc.model); err == nil {
				t.Fatalf("CreateTable(%T) succeeded, want rejection", tc.model)
			}
		})
	}

	// Destructive and unsupported migrator operations.
	m := gdb.Migrator()
	for name, err := range map[string]error{
		"DropTable":        m.DropTable(&Author{}),
		"DropColumn":       m.DropColumn(&Author{}, "Name"),
		"AlterColumn":      m.AlterColumn(&Author{}, "Name"),
		"RenameColumn":     m.RenameColumn(&Author{}, "Name", "Title"),
		"RenameTable":      m.RenameTable(&Author{}, "writers"),
		"CreateIndex":      m.CreateIndex(&Author{}, "Name"),
		"DropIndex":        m.DropIndex(&Author{}, "Name"),
		"CreateConstraint": m.CreateConstraint(&Author{}, "fk"),
	} {
		if err == nil {
			t.Errorf("%s succeeded, want rejection", name)
		} else if !strings.Contains(strings.ToLower(err.Error()), "not supported") {
			t.Errorf("%s = %v, want not-supported reason", name, err)
		}
	}

	// No residue from any rejection: only the genesis table exists.
	tables, err := m.GetTables()
	if err != nil {
		t.Fatalf("gettables: %v", err)
	}
	if len(tables) != 1 || tables[0] != "authors" {
		t.Fatalf("tables = %v after rejections, want [authors]", tables)
	}
	if epoch, _, err := db.LiveSchema(); err != nil || epoch != 1 {
		t.Fatalf("epoch = %d, err = %v, want 1", epoch, err)
	}
	if m.HasIndex(&Author{}, "Name") || m.HasConstraint(&Author{}, "fk") {
		t.Fatal("HasIndex/HasConstraint true, want false")
	}
}

func TestGenesisTables(t *testing.T) {
	tables, err := GenesisTables(&Author{}, &Book{})
	if err != nil {
		t.Fatalf("genesis: %v", err)
	}
	if len(tables) != 2 || tables[0].Name != "authors" || tables[1].Name != "books" {
		t.Fatalf("tables = %+v", tables)
	}
	for _, ts := range tables {
		if len(ts.Columns) == 0 || ts.Columns[0].Name != "id" {
			t.Fatalf("%s columns = %+v, want id first", ts.Name, ts.Columns)
		}
	}
	if _, err := GenesisTables(&rxUintPK{}); err == nil {
		t.Fatal("genesis accepted uint pk, want rejection")
	}
	if _, err := GenesisTables(); err == nil {
		t.Fatal("genesis accepted no models, want rejection")
	}
}

func TestMurmurIDJSON(t *testing.T) {
	id := NewID()
	if id.IsZero() {
		t.Fatal("fresh ID is zero")
	}
	raw, err := id.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back ID
	if err := back.UnmarshalJSON(raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back != id {
		t.Fatal("JSON roundtrip changed the ID")
	}
	if err := back.UnmarshalJSON([]byte(`"not-a-uuid"`)); err == nil {
		t.Fatal("garbage UUID accepted, want rejection")
	}
	// valuer/scanner roundtrip through the stored blob form.
	v, err := id.Value()
	if err != nil {
		t.Fatalf("value: %v", err)
	}
	var scanned ID
	if err := scanned.Scan(v); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if scanned != id {
		t.Fatal("valuer/scanner roundtrip changed the ID")
	}
	if err := scanned.Scan([]byte{1, 2, 3}); err == nil {
		t.Fatal("short blob accepted, want rejection")
	}
}

func TestMurmurTimeRoundtrip(t *testing.T) {
	now := time.Now().Round(0)
	var tm Time
	tm.Time = now
	// Values bind in one canonical UTC text format across backends.
	v, err := tm.Value()
	if err != nil {
		t.Fatalf("value: %v", err)
	}
	vs, ok := v.(string)
	if !ok {
		t.Fatalf("bound form = %T(%v), want canonical text", v, v)
	}
	if parsed, err := time.Parse(timeStoreFormat, vs); err != nil || !parsed.Equal(now) || parsed.Location() != time.UTC {
		t.Fatalf("bound form = %q, parsed=%v err=%v, want UTC instant", vs, parsed, err)
	}
	first := Time{Time: time.Date(2026, 1, 1, 22, 4, 5, 100_000_000, time.FixedZone("west", -5*60*60))}
	second := Time{Time: time.Date(2026, 1, 2, 3, 4, 5, 200_000_000, time.UTC)}
	firstValue, _ := first.Value()
	secondValue, _ := second.Value()
	if firstValue.(string) >= secondValue.(string) {
		t.Fatalf("canonical timestamps do not preserve instant order: %q >= %q", firstValue, secondValue)
	}
	// Every spelling the bundled drivers store must parse back.
	for _, s := range []string{
		vs,
		now.Format("2006-01-02 15:04:05.999999999-07:00"),
		now.Format("2006-01-02 15:04:05.999999999 -0700 MST"),
		now.Format(time.RFC3339Nano),
	} {
		var back Time
		if err := back.Scan(s); err != nil {
			t.Fatalf("scan %q: %v", s, err)
		}
		if !back.Time.Equal(now) {
			t.Fatalf("scan %q = %v, want %v", s, back.Time, now)
		}
	}
	var num Time
	if err := num.Scan(int64(123)); err == nil {
		t.Fatal("integer timestamp accepted, want rejection")
	}
}

func TestGormMurmurCreateTableDirect(t *testing.T) {
	db := openEngine(t, &Author{})
	gdb, err := gorm.Open(Open(db), &gorm.Config{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	m := gdb.Migrator()

	if err := m.CreateTable(&Book{}); err != nil {
		t.Fatalf("create books: %v", err)
	}
	if !m.HasTable(&Book{}) {
		t.Fatal("books missing after CreateTable")
	}
	b := Book{Title: "fresh"}
	if err := gdb.Create(&b).Error; err != nil {
		t.Fatalf("create row: %v", err)
	}
	var count int64
	if err := gdb.Model(&Book{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("count = %d, err = %v, want 1", count, err)
	}

	if err := m.CreateTable(&Author{}); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "already exists") {
		t.Fatalf("CreateTable existing = %v, want already-exists rejection", err)
	}
	if err := m.CreateTable(&rxUnique{}); err == nil {
		t.Fatal("CreateTable invalid model succeeded, want rejection")
	}
	err = gdb.Transaction(func(tx *gorm.DB) error {
		return tx.Migrator().CreateTable(&GadgetV1{})
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "inside a transaction") {
		t.Fatalf("transactional CreateTable = %v, want explicit rejection", err)
	}

	tables, err := m.GetTables()
	if err != nil {
		t.Fatalf("gettables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("tables = %v after CreateTable tests, want [authors books]", tables)
	}
}

func TestGormMurmurAddColumnDirect(t *testing.T) {
	db := openEngine(t, &GadgetV1{})
	gdb, err := gorm.Open(Open(db), &gorm.Config{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := gdb.AutoMigrate(&GadgetV1{}); err != nil {
		t.Fatalf("migrate v1: %v", err)
	}
	m := gdb.Migrator()

	if err := m.AddColumn(&GadgetV2{}, "Color"); err != nil {
		t.Fatalf("add color: %v", err)
	}
	if !m.HasColumn(&GadgetV2{}, "Color") {
		t.Fatal("color missing after AddColumn")
	}
	color := "red"
	g := GadgetV2{Name: "widget", Color: &color}
	if err := gdb.Create(&g).Error; err != nil {
		t.Fatalf("create with new column: %v", err)
	}
	var got GadgetV2
	if err := gdb.First(&got, "id = ?", g.ID).Error; err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Color == nil || *got.Color != "red" {
		t.Fatalf("color = %+v, want red", got.Color)
	}

	if err := m.AddColumn(&GadgetV2{}, "Color"); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "already has column") {
		t.Fatalf("duplicate AddColumn = %v, want already-has-column rejection", err)
	}
	if err := m.AddColumn(&GadgetV2{}, "Nope"); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "has no field") {
		t.Fatalf("unknown-field AddColumn = %v, want no-field rejection", err)
	}
	if err := m.AddColumn(&Author{}, "Name"); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "does not exist") {
		t.Fatalf("missing-table AddColumn = %v, want does-not-exist rejection", err)
	}
	err = gdb.Transaction(func(tx *gorm.DB) error {
		return tx.Migrator().AddColumn(&GadgetV2{}, "Color")
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "inside a transaction") {
		t.Fatalf("transactional AddColumn = %v, want explicit rejection", err)
	}
}

// keyedWidget maps a table whose blob primary key is not named `id`:
// legal for the engine, unmappable for GORM.
type keyedWidget struct {
	Key  ID `gorm:"primaryKey;column:key"`
	Name string
	Note *string
}

func (keyedWidget) TableName() string { return "widgets" }

func TestGormMurmurAddColumnRejectsNonIDKey(t *testing.T) {
	db := openEngineWithTables(t, []murmurSchema.TableSchema{{
		Name: "widgets", PK: 1,
		Columns: []murmurSchema.ColumnSchema{
			{ID: 1, Name: "key", Type: murmurSchema.ColBlob},
			{ID: 2, Name: "name", Type: murmurSchema.ColText, Nullable: true},
		},
	}})
	gdb, err := gorm.Open(Open(db), &gorm.Config{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := gdb.Migrator().AddColumn(&keyedWidget{}, "Note"); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "primary key") {
		t.Fatalf("AddColumn on non-id key = %v, want primary-key rejection", err)
	}
	if err := gdb.AutoMigrate(&keyedWidget{}); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "primary key") {
		t.Fatalf("AutoMigrate on non-id key = %v, want primary-key rejection", err)
	}
	if epoch, live, err := db.LiveSchema(); err != nil || epoch != 1 || len(live) != 1 {
		t.Fatalf("schema changed after rejections: epoch=%d tables=%v err=%v", epoch, live, err)
	}
}

func TestGormMurmurMigratorReads(t *testing.T) {
	db := openEngine(t, &Author{})
	gdb := openGorm(t, db, &Author{})
	m := gdb.Migrator()

	if !m.HasTable(&Author{}) || !m.HasTable("authors") {
		t.Fatal("HasTable(authors) false, want true")
	}
	if m.HasTable(&Book{}) {
		t.Fatal("HasTable(books) true, want false")
	}
	if !m.HasColumn(&Author{}, "Name") || !m.HasColumn(&Author{}, "name") {
		t.Fatal("HasColumn(Name) false, want true")
	}
	if m.HasColumn(&Author{}, "Missing") {
		t.Fatal("HasColumn(Missing) true, want false")
	}

	cols, err := m.ColumnTypes(&Author{})
	if err != nil {
		t.Fatalf("columntypes: %v", err)
	}
	if len(cols) != 11 {
		t.Fatalf("column count = %d, want 11", len(cols))
	}
	byName := map[string]gorm.ColumnType{}
	for _, c := range cols {
		byName[c.Name()] = c
	}
	expect := map[string]struct {
		typ      string
		nullable bool
		pk       bool
	}{
		"id":         {"BLOB", false, true},
		"name":       {"TEXT", true, false},
		"age":        {"INTEGER", true, false},
		"score":      {"REAL", true, false},
		"active":     {"INTEGER", true, false},
		"nick":       {"TEXT", true, false},
		"avatar":     {"BLOB", true, false},
		"stamp":      {"INTEGER", true, false},
		"created_at": {"TEXT", true, false},
		"updated_at": {"TEXT", true, false},
		"deleted_at": {"TEXT", true, false},
	}
	for name, want := range expect {
		c, ok := byName[name]
		if !ok {
			t.Fatalf("column %q missing from ColumnTypes", name)
		}
		if got := c.DatabaseTypeName(); got != want.typ {
			t.Fatalf("column %q type = %s, want %s", name, got, want.typ)
		}
		if nullable, _ := c.Nullable(); nullable != want.nullable {
			t.Fatalf("column %q nullable = %v, want %v", name, nullable, want.nullable)
		}
		if pk, _ := c.PrimaryKey(); pk != want.pk {
			t.Fatalf("column %q pk = %v, want %v", name, pk, want.pk)
		}
	}

	tt, err := m.TableType(&Author{})
	if err != nil || tt.Name() != "authors" {
		t.Fatalf("tabletype = %+v, err = %v, want authors", tt, err)
	}
	if _, err := m.TableType(&Book{}); err == nil {
		t.Fatal("TableType(books) succeeded, want missing-table error")
	}

	idxs, err := m.GetIndexes(&Author{})
	if err != nil {
		t.Fatalf("getindexes: %v", err)
	}
	if len(idxs) != 1 || idxs[0].Name() != "PRIMARY" {
		t.Fatalf("indexes = %+v, want [PRIMARY]", idxs)
	}
	if got := idxs[0].Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("primary columns = %v, want [id]", got)
	}
	if pk, _ := idxs[0].PrimaryKey(); !pk {
		t.Fatal("PRIMARY not reported as primary key")
	}
}

// BlobKeyDoc proves ID auto-fill works for raw []byte primary keys.
type BlobKeyDoc struct {
	ID    []byte `gorm:"primaryKey;column:id"`
	Title string
}

func TestGormMurmurByteSlicePKFill(t *testing.T) {
	db := openEngine(t, &BlobKeyDoc{})
	gdb := openGorm(t, db, &BlobKeyDoc{})

	d := BlobKeyDoc{Title: "auto"}
	if err := gdb.Create(&d).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(d.ID) != 16 {
		t.Fatalf("filled ID length = %d, want 16", len(d.ID))
	}
	explicit := NewID()
	d2 := BlobKeyDoc{ID: append([]byte(nil), explicit[:]...), Title: "explicit"}
	if err := gdb.Create(&d2).Error; err != nil {
		t.Fatalf("create explicit: %v", err)
	}
	var got BlobKeyDoc
	if err := gdb.First(&got, "id = ?", d2.ID).Error; err != nil {
		t.Fatalf("read explicit: %v", err)
	}
	if got.Title != "explicit" {
		t.Fatalf("title = %q, want explicit", got.Title)
	}
}

func TestGormMurmurCustomNamer(t *testing.T) {
	namer := schema.NamingStrategy{TablePrefix: "t_", SingularTable: true}
	tables, err := GenesisTablesWithNamer(namer, &Author{})
	if err != nil {
		t.Fatalf("genesis: %v", err)
	}
	if len(tables) != 1 || tables[0].Name != "t_author" {
		t.Fatalf("tables = %+v, want [t_author]", tables)
	}
	db := openEngineWithTables(t, tables)
	gdb, err := gorm.Open(Open(db), &gorm.Config{NamingStrategy: namer})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := gdb.AutoMigrate(&Author{}); err != nil {
			t.Fatalf("automigrate (pass %d): %v", i, err)
		}
	}
	a := Author{Name: "prefixed"}
	if err := gdb.Create(&a).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	var got Author
	if err := gdb.First(&got, "id = ?", a.ID).Error; err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Name != "prefixed" {
		t.Fatalf("name = %q, want prefixed", got.Name)
	}
}

// NullNote proves database/sql nullable scalars round-trip.
type NullNote struct {
	Model
	Note  sql.NullString
	Score sql.NullInt64
}

func TestGormMurmurNullTypes(t *testing.T) {
	db := openEngine(t, &NullNote{})
	gdb := openGorm(t, db, &NullNote{})

	n := NullNote{
		Note:  sql.NullString{String: "hi", Valid: true},
		Score: sql.NullInt64{Valid: false},
	}
	if err := gdb.Create(&n).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	var got NullNote
	if err := gdb.First(&got, "id = ?", n.ID).Error; err != nil {
		t.Fatalf("read: %v", err)
	}
	if !got.Note.Valid || got.Note.String != "hi" {
		t.Fatalf("note = %+v, want hi/valid", got.Note)
	}
	if got.Score.Valid {
		t.Fatalf("score = %+v, want invalid", got.Score)
	}
}

func TestGormMurmurReturningAndLocking(t *testing.T) {
	db := openEngine(t, &Author{})
	gdb := openGorm(t, db, &Author{})

	// Explicit RETURNING works on autocommit and inserts the row.
	r := Author{Name: "r"}
	if err := gdb.Clauses(clause.Returning{}).Create(&r).Error; err != nil {
		t.Fatalf("autocommit RETURNING: %v", err)
	}
	var count int64
	if err := gdb.Model(&Author{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("count = %d after autocommit RETURNING create, want 1", count)
	}
	var reread Author
	if err := gdb.First(&reread, "id = ?", r.ID).Error; err != nil || reread.Name != "r" {
		t.Fatalf("reread = %+v, err = %v, want name r", reread, err)
	}

	// Inside an explicit transaction the same create works.
	err := gdb.Transaction(func(tx *gorm.DB) error {
		return tx.Clauses(clause.Returning{}).Create(&Author{Name: "r2"}).Error
	})
	if err != nil {
		t.Fatalf("transactional RETURNING: %v", err)
	}
	if err := gdb.Model(&Author{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("count = %d after transactional create, want 2", count)
	}

	// Locking reads are rejected: Murmur has no SELECT FOR UPDATE.
	var a Author
	if err := gdb.Clauses(clause.Locking{Strength: "UPDATE"}).First(&a).Error; err == nil {
		t.Fatal("locking read succeeded, want rejection")
	}
}
