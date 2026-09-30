package murmur

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	replicateddb "github.com/marcgauthier/murmur"
	murmurSchema "github.com/marcgauthier/murmur/schema"
	"gorm.io/gorm"
	gormmigrator "gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// Migrator implements gorm.Migrator for Murmur. Schema changes go
// through the engine's additive Migrate API — never through SQL DDL,
// which would land on the ephemeral materialization and never
// replicate. Anything Murmur cannot replicate (secondary indexes,
// defaults, autoincrement, checks, constraints) fails loudly.
type Migrator struct {
	gormmigrator.Migrator
	murmur *replicateddb.DB
}

var _ gorm.Migrator = (*Migrator)(nil)

// ctx returns the statement context, defaulting to Background.
func (m *Migrator) ctx() context.Context {
	if m.DB != nil && m.DB.Statement != nil && m.DB.Statement.Context != nil {
		return m.DB.Statement.Context
	}
	return context.Background()
}

// liveSchema returns the engine's current table declarations.
func (m *Migrator) liveSchema() ([]murmurSchema.TableSchema, error) {
	_, live, err := m.murmur.LiveSchema()
	if err != nil {
		return nil, err
	}
	return live, nil
}

// rejectInTransaction prevents schema publication from waiting on the
// write lock held by the transaction that invoked the migrator.
func (m *Migrator) rejectInTransaction() error {
	if m.DB != nil && m.DB.Statement != nil {
		if _, ok := m.DB.Statement.ConnPool.(gorm.TxCommitter); ok {
			return fmt.Errorf("murmur: schema migrations cannot run inside a transaction")
		}
	}
	return nil
}

// findLiveTable returns the live declaration for name (case-insensitive).
func findLiveTable(live []murmurSchema.TableSchema, name string) *murmurSchema.TableSchema {
	for i := range live {
		if strings.EqualFold(live[i].Name, name) {
			return &live[i]
		}
	}
	return nil
}

// checkLivePK enforces the GORM key contract on an existing table: the
// primary key must be a non-null BLOB `id` column. Tables created
// outside GORM can carry a differently-named blob key; GORM models
// cannot map those, so every schema-writing path must refuse them.
func checkLivePK(table *murmurSchema.TableSchema) error {
	var pk *murmurSchema.ColumnSchema
	for i := range table.Columns {
		if strings.EqualFold(table.Columns[i].Name, "id") {
			pk = &table.Columns[i]
			break
		}
	}
	if pk == nil || table.PK != pk.ID || pk.Type != murmurSchema.ColBlob || pk.Nullable {
		return fmt.Errorf("murmur: live table %q does not have the required non-null BLOB id primary key", table.Name)
	}
	return nil
}

// AutoMigrate validates all requested models before publishing missing
// tables and columns in one additive engine migration. Index, check,
// and constraint tags are rejected loudly: silently skipping them
// would leave the application believing in guarantees Murmur never
// enforces. It cannot run inside a transaction because migrations take
// the engine's write lock independently of GORM's SQL transaction.
func (m *Migrator) AutoMigrate(values ...interface{}) error {
	if err := m.rejectInTransaction(); err != nil {
		return err
	}
	// Phase 1 validates the caller-passed models first, so
	// relationship-level rejections (many-to-many, constraint tags,
	// indexes) report against the user's model rather than a
	// synthesized join-table value from reordering.
	for _, value := range values {
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
			if stmt.Schema == nil {
				return fmt.Errorf("murmur: cannot migrate %T: pass a model struct, not a table name", value)
			}
			return validateModelForMurmur(stmt.Schema)
		}); err != nil {
			return err
		}
	}
	return m.migrateModels(m.ReorderModels(values, true), false)
}

// migrateModels validates all requested models against one schema snapshot,
// then publishes their additions in a single engine revision.
func (m *Migrator) migrateModels(values []interface{}, createOnly bool) error {
	live, err := m.liveSchema()
	if err != nil {
		return err
	}
	next := append([]murmurSchema.TableSchema(nil), live...)
	changed := false
	for _, value := range values {
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
			if stmt.Schema == nil {
				return fmt.Errorf("murmur: cannot migrate %T: pass a model struct, not a table name", value)
			}
			target, err := tableSchemaFor(stmt.Schema, stmt.Table)
			if err != nil {
				return err
			}
			current := findLiveTable(next, stmt.Table)
			if current == nil {
				next = append(next, target)
				changed = true
				return nil
			}
			if createOnly {
				return fmt.Errorf("murmur: table %q already exists", stmt.Table)
			}
			if err := checkLivePK(current); err != nil {
				return err
			}
			for _, wanted := range target.Columns {
				var existing *murmurSchema.ColumnSchema
				for i := range current.Columns {
					if strings.EqualFold(current.Columns[i].Name, wanted.Name) {
						existing = &current.Columns[i]
						break
					}
				}
				if existing == nil {
					current.Columns = append(current.Columns, wanted)
					changed = true
					continue
				}
				if existing.Type != wanted.Type {
					return fmt.Errorf("murmur: column %q is %s, model wants %s: murmur migrations are additive-only", wanted.Name, existing.Type, wanted.Type)
				}
				if existing.Nullable != wanted.Nullable {
					return fmt.Errorf("murmur: column %q nullability changed: murmur migrations are additive-only (no ALTER COLUMN)", wanted.Name)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if !changed {
		return nil
	}
	if err := m.murmur.Migrate(m.ctx(), next); err != nil {
		return fmt.Errorf("murmur: publish schema migration: %w", err)
	}
	return nil
}

// CreateTable publishes one model as a new replicated table.
func (m *Migrator) CreateTable(values ...interface{}) error {
	if err := m.rejectInTransaction(); err != nil {
		return err
	}
	return m.migrateModels(values, true)
}

// AddColumn adds one missing column through an additive migration.
func (m *Migrator) AddColumn(value interface{}, name string) error {
	if err := m.rejectInTransaction(); err != nil {
		return err
	}
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema == nil {
			return fmt.Errorf("murmur: cannot add column to %T: pass a model struct", value)
		}
		field := stmt.Schema.LookUpField(name)
		if field == nil {
			return fmt.Errorf("murmur: model %s has no field %q", stmt.Schema.Name, name)
		}
		ct, err := murmurTypeForField(field)
		if err != nil {
			return err
		}
		live, err := m.liveSchema()
		if err != nil {
			return err
		}
		liveTable := findLiveTable(live, stmt.Table)
		if liveTable == nil {
			return fmt.Errorf("murmur: table %q does not exist", stmt.Table)
		}
		if err := checkLivePK(liveTable); err != nil {
			return err
		}
		for _, c := range liveTable.Columns {
			if strings.EqualFold(c.Name, field.DBName) {
				return fmt.Errorf("murmur: table %s already has column %q", stmt.Table, field.DBName)
			}
		}
		next := make([]murmurSchema.TableSchema, len(live))
		for i, t := range live {
			next[i] = t
			if strings.EqualFold(t.Name, stmt.Table) {
				next[i].Columns = append(append([]murmurSchema.ColumnSchema(nil), t.Columns...),
					murmurSchema.ColumnSchema{Name: field.DBName, Type: ct, Nullable: !field.NotNull})
			}
		}
		if err := m.murmur.Migrate(m.ctx(), next); err != nil {
			return fmt.Errorf("murmur: add column %s.%s: %w", stmt.Table, field.DBName, err)
		}
		return nil
	})
}

// MigrateColumn verifies a live column already matches the model.
// Murmur migrations are additive-only: any type or nullability drift
// is a loud error, never an ALTER.
func (m *Migrator) MigrateColumn(_ interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	if field.IgnoreMigration {
		return nil
	}
	want, err := murmurTypeForField(field)
	if err != nil {
		return err
	}
	if got := strings.ToUpper(columnType.DatabaseTypeName()); got != want.String() {
		return fmt.Errorf("murmur: column %q is %s, model wants %s: murmur migrations are additive-only (drop the column from the model or rebuild the table through teardown)", field.DBName, got, want)
	}
	if nullable, ok := columnType.Nullable(); ok {
		if wantNotNull := field.PrimaryKey || field.NotNull; wantNotNull == nullable {
			return fmt.Errorf("murmur: column %q nullability changed: murmur migrations are additive-only (no ALTER COLUMN)", field.DBName)
		}
	}
	return nil
}

// MigrateColumnUnique reports whether a UNIQUE migration is needed.
// Murmur keeps no secondary unique constraints, so any unique flag
// outside the primary key itself is a loud error.
func (m *Migrator) MigrateColumnUnique(_ interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	if field.Unique {
		return fmt.Errorf("murmur: column %q: UNIQUE is not supported (murmur replicates only the primary-key index; remove the `unique` tag)", field.DBName)
	}
	if unique, ok := columnType.Unique(); ok && unique && !field.PrimaryKey {
		return fmt.Errorf("murmur: column %q carries a unique constraint murmur cannot manage", field.DBName)
	}
	return nil
}

// HasTable reports whether the table exists in the live schema.
func (m *Migrator) HasTable(value interface{}) bool {
	exists := false
	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		live, err := m.liveSchema()
		if err != nil {
			return err
		}
		exists = findLiveTable(live, stmt.Table) != nil
		return nil
	})
	return exists
}

// HasColumn reports whether the live table has the column.
func (m *Migrator) HasColumn(value interface{}, field string) bool {
	exists := false
	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if f := stmt.Schema.LookUpField(field); f != nil {
				field = f.DBName
			}
		}
		live, err := m.liveSchema()
		if err != nil {
			return err
		}
		if t := findLiveTable(live, stmt.Table); t != nil {
			for _, c := range t.Columns {
				if strings.EqualFold(c.Name, field) {
					exists = true
					break
				}
			}
		}
		return nil
	})
	return exists
}

// ColumnTypes describes the live columns from the schema registry.
func (m *Migrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
	var out []gorm.ColumnType
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		live, err := m.liveSchema()
		if err != nil {
			return err
		}
		t := findLiveTable(live, stmt.Table)
		if t == nil {
			return nil
		}
		for _, c := range t.Columns {
			out = append(out, columnType{name: c.Name, typ: c.Type, nullable: c.Nullable, pk: c.ID == t.PK})
		}
		return nil
	})
	return out, err
}

// GetTables lists the live replicated tables.
func (m *Migrator) GetTables() ([]string, error) {
	live, err := m.liveSchema()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(live))
	for _, t := range live {
		out = append(out, t.Name)
	}
	return out, nil
}

// TableType describes one live table.
func (m *Migrator) TableType(value interface{}) (gorm.TableType, error) {
	var out gorm.TableType
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		live, err := m.liveSchema()
		if err != nil {
			return err
		}
		if findLiveTable(live, stmt.Table) == nil {
			return fmt.Errorf("murmur: table %q does not exist", stmt.Table)
		}
		out = tableType{name: stmt.Table}
		return nil
	})
	return out, err
}

// GetIndexes returns the primary-key index: the only index Murmur
// maintains. Secondary indexes never exist.
func (m *Migrator) GetIndexes(value interface{}) ([]gorm.Index, error) {
	var out []gorm.Index
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		live, err := m.liveSchema()
		if err != nil {
			return err
		}
		if t := findLiveTable(live, stmt.Table); t != nil {
			if pk := t.ColumnByID(t.PK); pk != nil {
				out = append(out, murmurIndex{table: t.Name, name: "PRIMARY", columns: []string{pk.Name}})
			}
		}
		return nil
	})
	return out, err
}

// HasIndex always reports false: tag-defined indexes are never
// created (CreateIndex rejects them loudly).
func (m *Migrator) HasIndex(_ interface{}, _ string) bool {
	return false
}

// HasConstraint always reports false: Murmur stores no check or
// foreign-key constraints.
func (m *Migrator) HasConstraint(_ interface{}, _ string) bool {
	return false
}

// CurrentDatabase implements gorm.Migrator.
func (m *Migrator) CurrentDatabase() string {
	return "murmur"
}

// DropTable implements gorm.Migrator. Murmur schemas are
// additive-only; dropping a table needs coordinated teardown outside
// the migrator.
func (m *Migrator) DropTable(_ ...interface{}) error {
	return fmt.Errorf("murmur: DROP TABLE is not supported (murmur schemas are additive-only)")
}

// RenameTable implements gorm.Migrator.
func (m *Migrator) RenameTable(_, _ interface{}) error {
	return fmt.Errorf("murmur: RENAME TABLE is not supported (murmur schemas are additive-only)")
}

// DropColumn implements gorm.Migrator.
func (m *Migrator) DropColumn(_ interface{}, _ string) error {
	return fmt.Errorf("murmur: DROP COLUMN is not supported (murmur schemas are additive-only)")
}

// AlterColumn implements gorm.Migrator.
func (m *Migrator) AlterColumn(_ interface{}, _ string) error {
	return fmt.Errorf("murmur: ALTER COLUMN is not supported (murmur migrations are additive-only)")
}

// RenameColumn implements gorm.Migrator.
func (m *Migrator) RenameColumn(_ interface{}, _, _ string) error {
	return fmt.Errorf("murmur: RENAME COLUMN is not supported (murmur migrations are additive-only)")
}

// CreateIndex implements gorm.Migrator.
func (m *Migrator) CreateIndex(_ interface{}, name string) error {
	return fmt.Errorf("murmur: CREATE INDEX %s is not supported (murmur replicates only the primary-key index; remove the `index`/`uniqueIndex` tag)", name)
}

// DropIndex implements gorm.Migrator.
func (m *Migrator) DropIndex(_ interface{}, name string) error {
	return fmt.Errorf("murmur: DROP INDEX %s is not supported (murmur keeps no secondary indexes)", name)
}

// RenameIndex implements gorm.Migrator.
func (m *Migrator) RenameIndex(_ interface{}, _, _ string) error {
	return fmt.Errorf("murmur: RENAME INDEX is not supported (murmur keeps no secondary indexes)")
}

// CreateConstraint implements gorm.Migrator.
func (m *Migrator) CreateConstraint(_ interface{}, name string) error {
	return fmt.Errorf("murmur: constraint %s is not supported (murmur stores no check or foreign-key constraints)", name)
}

// DropConstraint implements gorm.Migrator.
func (m *Migrator) DropConstraint(_ interface{}, name string) error {
	return fmt.Errorf("murmur: constraint %s is not supported (murmur stores no check or foreign-key constraints)", name)
}

// CreateView implements gorm.Migrator.
func (m *Migrator) CreateView(_ string, _ gorm.ViewOption) error {
	return fmt.Errorf("murmur: CREATE VIEW is not supported over GORM (views are local-only objects; use LocalDDL)")
}

// DropView implements gorm.Migrator.
func (m *Migrator) DropView(_ string) error {
	return fmt.Errorf("murmur: DROP VIEW is not supported over GORM (views are local-only objects; use LocalDDL)")
}

// validateModelForMurmur rejects model features Murmur cannot
// replicate: secondary indexes, check constraints, explicit
// foreign-key constraint tags, and many-to-many join tables.
func validateModelForMurmur(sch *schema.Schema) error {
	if idxs := sch.ParseIndexes(); len(idxs) > 0 {
		names := make([]string, 0, len(idxs))
		for _, idx := range idxs {
			kind := "index"
			if strings.EqualFold(idx.Class, "UNIQUE") {
				kind = "UNIQUE"
			}
			names = append(names, idx.Name+" ("+kind+")")
		}
		sort.Strings(names)
		return fmt.Errorf("murmur: model %s declares indexes [%s]: murmur replicates only the primary-key index (remove the `index`/`uniqueIndex` tags)", sch.Name, strings.Join(names, ", "))
	}
	if chks := sch.ParseCheckConstraints(); len(chks) > 0 {
		names := make([]string, 0, len(chks))
		for name := range chks {
			names = append(names, name)
		}
		sort.Strings(names)
		return fmt.Errorf("murmur: model %s declares check constraints [%s]: murmur does not replicate check constraints (remove the `check:` tags)", sch.Name, strings.Join(names, ", "))
	}
	for _, rel := range sch.Relationships.Relations {
		if rel.Field.IgnoreMigration {
			continue
		}
		if rel.JoinTable != nil {
			return fmt.Errorf("murmur: model %s relation %s is many-to-many: murmur tables need a single `id` blob key, so join tables cannot migrate (model the join explicitly with murmur.Model and two one-to-many relations)", sch.Name, rel.Name)
		}
		if tag, ok := rel.Field.TagSettings["CONSTRAINT"]; ok {
			return fmt.Errorf("murmur: model %s relation %s declares constraint %q: murmur leaves foreign keys unenforced (remove the `constraint:` tag; associations still work, without database cascades)", sch.Name, rel.Name, tag)
		}
	}
	return nil
}

// tableSchemaFor converts a parsed GORM schema to a Murmur table
// declaration, enforcing the replicated-schema contract.
func tableSchemaFor(sch *schema.Schema, table string) (murmurSchema.TableSchema, error) {
	var ts murmurSchema.TableSchema
	if err := validateModelForMurmur(sch); err != nil {
		return ts, err
	}
	ts.Name = table
	pks := 0
	for _, dbName := range sch.DBNames {
		field := sch.FieldsByDBName[dbName]
		if field.IgnoreMigration {
			continue
		}
		ct, err := murmurTypeForField(field)
		if err != nil {
			return ts, err
		}
		if field.PrimaryKey {
			pks++
			if !strings.EqualFold(dbName, "id") {
				return ts, fmt.Errorf("murmur: model %s primary key must be the `id` column, got %q (murmur key convention)", sch.Name, dbName)
			}
			if ct != murmurSchema.ColBlob {
				return ts, fmt.Errorf("murmur: model %s primary key `id` must be a blob type (murmur.ID or []byte); integer keys cannot replicate", sch.Name)
			}
		}
		ts.Columns = append(ts.Columns, murmurSchema.ColumnSchema{
			Name:     dbName,
			Type:     ct,
			Nullable: !field.PrimaryKey && !field.NotNull,
		})
	}
	if pks == 0 {
		return ts, fmt.Errorf("murmur: model %s needs exactly one PRIMARY KEY: every murmur table needs a single `id` BLOB(16) key (embed murmur.Model or add ID murmur.ID `gorm:\"primaryKey;column:id\"`)", sch.Name)
	}
	if pks > 1 {
		return ts, fmt.Errorf("murmur: model %s has a composite PRIMARY KEY: murmur keys are single-column", sch.Name)
	}
	// Leave IDs zero: the registry derives stable IDs from names and
	// resolves the `id` column as the key.
	return ts, nil
}

var (
	murmurTimeType      = reflect.TypeOf(Time{})
	murmurDeletedAtType = reflect.TypeOf(DeletedAt{})
	murmurIDType        = reflect.TypeOf(ID{})
	gormDeletedAtType   = reflect.TypeOf(gorm.DeletedAt{})
	byteType            = reflect.TypeOf(byte(0))
)

// murmurTypeForField maps one GORM field to a Murmur column type,
// rejecting column options Murmur does not model.
func murmurTypeForField(field *schema.Field) (murmurSchema.ColumnType, error) {
	if field.AutoIncrement {
		return 0, fmt.Errorf("murmur: column %q: AUTO_INCREMENT is not supported (murmur forbids autoincrement; use a client-generated murmur.ID via murmur.NewID)", field.DBName)
	}
	if _, ok := field.TagSettings["DEFAULT"]; ok {
		return 0, fmt.Errorf("murmur: column %q: DEFAULT is not supported (murmur columns have no defaults; remove the `default:` tag)", field.DBName)
	}
	if field.Unique {
		return 0, fmt.Errorf("murmur: column %q: UNIQUE is not supported (murmur replicates only the primary-key index; remove the `unique` tag)", field.DBName)
	}
	if typ, ok := field.TagSettings["TYPE"]; ok && !isMurmurTypeSpelling(typ) {
		return 0, fmt.Errorf("murmur: column %q: type %q has no murmur equivalent (use integer, real, text, or blob)", field.DBName, typ)
	}
	// Byte slices ([]byte blobs, JSON payloads) map by kind before
	// consulting the data type. Byte arrays only bind when they
	// implement driver.Valuer: murmur.ID does, raw [16]byte cannot.
	ft := field.IndirectFieldType
	if ft.Kind() == reflect.Slice && ft.Elem() == byteType {
		return murmurSchema.ColBlob, nil
	}
	if ft.Kind() == reflect.Array && ft.Elem() == byteType {
		if ft != murmurIDType {
			return 0, fmt.Errorf("murmur: column %q: raw [%d]byte cannot bind through database/sql (use murmur.ID or []byte)", field.DBName, ft.Len())
		}
		return murmurSchema.ColBlob, nil
	}
	switch field.DataType {
	case schema.Bool:
		return murmurSchema.ColInteger, nil
	case schema.Int, schema.Uint:
		return murmurSchema.ColInteger, nil
	case schema.Float:
		return murmurSchema.ColReal, nil
	case schema.String:
		return murmurSchema.ColText, nil
	case schema.Bytes:
		return murmurSchema.ColBlob, nil
	case schema.Time:
		ft := field.IndirectFieldType
		if ft == murmurTimeType || ft == murmurDeletedAtType {
			return murmurSchema.ColText, nil
		}
		if ft == gormDeletedAtType {
			return 0, fmt.Errorf("murmur: column %q: gorm.DeletedAt cannot scan murmur TEXT storage (use murmur.DeletedAt for soft delete)", field.DBName)
		}
		return 0, fmt.Errorf("murmur: column %q: plain time.Time has no murmur mapping (use murmur.Time for timestamps or an integer with autoCreateTime/autoUpdateTime)", field.DBName)
	default:
		return 0, fmt.Errorf("murmur: column %q: GORM data type %q has no murmur mapping (custom types must be []byte/string/int/float-based, or murmur.Time)", field.DBName, field.DataType)
	}
}

// isMurmurTypeSpelling reports whether an explicit `type:` tag names a
// Murmur-storable type (sizes ignored, as in SQLite).
func isMurmurTypeSpelling(typ string) bool {
	t := strings.ToLower(strings.TrimSpace(typ))
	if i := strings.IndexByte(t, '('); i >= 0 {
		t = strings.TrimSpace(t[:i])
	}
	switch t {
	case "integer", "int", "bigint", "smallint", "tinyint", "mediumint",
		"boolean", "bool",
		"real", "float", "double",
		"text", "varchar", "char", "character", "character varying", "clob", "string",
		"blob", "binary", "varbinary", "bytea":
		return true
	default:
		return false
	}
}

// GenesisTables parses models into Murmur table declarations for
// engine genesis. The engine requires at least one table at Open, so
// GORM-first applications derive that genesis from their models:
//
//	tables, err := murmur.GenesisTables(&User{}, &Order{})
//	db, err := replicateddb.Open(ctx, cfgWithTables(tables))
//	gdb, err := gorm.Open(murmur.Open(db))
//
// Models use the default naming strategy; use
// GenesisTablesWithNamer when gorm.Config carries a custom one.
func GenesisTables(models ...interface{}) ([]murmurSchema.TableSchema, error) {
	return GenesisTablesWithNamer(schema.NamingStrategy{}, models...)
}

// GenesisTablesWithNamer is GenesisTables with an explicit naming
// strategy. It must match the strategy GORM parses with at runtime,
// or table/column names diverge and later migrations misread them.
func GenesisTablesWithNamer(namer schema.Namer, models ...interface{}) ([]murmurSchema.TableSchema, error) {
	var out []murmurSchema.TableSchema
	for _, model := range models {
		sch, err := schema.Parse(model, &sync.Map{}, namer)
		if err != nil {
			return nil, fmt.Errorf("murmur: parse %T: %w", model, err)
		}
		ts, err := tableSchemaFor(sch, sch.Table)
		if err != nil {
			return nil, err
		}
		out = append(out, ts)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("murmur: GenesisTables needs at least one model")
	}
	return out, nil
}

// columnType implements gorm.ColumnType from registry data.
type columnType struct {
	name     string
	typ      murmurSchema.ColumnType
	nullable bool
	pk       bool
}

func (c columnType) Name() string { return c.name }

func (c columnType) DatabaseTypeName() string { return c.typ.String() }

func (c columnType) ColumnType() (string, bool) { return c.typ.String(), true }

func (c columnType) PrimaryKey() (bool, bool) { return c.pk, true }

func (c columnType) AutoIncrement() (bool, bool) { return false, true }

func (c columnType) Length() (int64, bool) { return 0, false }

func (c columnType) DecimalSize() (int64, int64, bool) { return 0, 0, false }

func (c columnType) Nullable() (bool, bool) { return c.nullable, true }

func (c columnType) Unique() (bool, bool) { return c.pk, true }

func (c columnType) ScanType() reflect.Type {
	switch c.typ {
	case murmurSchema.ColInteger:
		return reflect.TypeOf(int64(0))
	case murmurSchema.ColReal:
		return reflect.TypeOf(float64(0))
	case murmurSchema.ColBlob:
		return reflect.TypeOf([]byte(nil))
	default:
		return reflect.TypeOf("")
	}
}

func (c columnType) Comment() (string, bool) { return "", false }

func (c columnType) DefaultValue() (string, bool) { return "", false }

// tableType implements gorm.TableType.
type tableType struct{ name string }

func (t tableType) Schema() string          { return "" }
func (t tableType) Name() string            { return t.name }
func (t tableType) Type() string            { return "BASE TABLE" }
func (t tableType) Comment() (string, bool) { return "", false }

// murmurIndex implements gorm.Index for the primary-key index.
type murmurIndex struct {
	table   string
	name    string
	columns []string
}

func (m murmurIndex) Table() string            { return m.table }
func (m murmurIndex) Name() string             { return m.name }
func (m murmurIndex) Columns() []string        { return m.columns }
func (m murmurIndex) PrimaryKey() (bool, bool) { return true, true }
func (m murmurIndex) Unique() (bool, bool)     { return true, true }
func (m murmurIndex) Option() string           { return "" }
