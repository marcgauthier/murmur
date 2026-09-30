package murmurd

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"

	db "github.com/marcgauthier/spedsql"
	murmurSchema "github.com/marcgauthier/spedsql/schema"
)

// This file adapts murmur to go-mysql-server. One murmur *DB backs one
// sql.Database; table schemas come from the live SQLite materialization
// (sqlite_master + PRAGMA table_info), so DDL through either frontend
// is visible immediately. Writes execute as murmur implicit
// transactions (autocommit): there are no multi-statement murmur
// transactions behind this provider.
//
// SQLite-subset filtering is structural: the provider implements
// reads, INSERT/UPDATE/DELETE, TRUNCATE, and CREATE TABLE only. DROP,
// ALTER, RENAME, indexes, views, procedures, and users have no murmur
// equivalent, so those interfaces are absent and the engine rejects
// them. Column types are murmur's four (integer, real, text, blob).

// Provider is a single-database sql.DatabaseProvider over murmur.
type Provider struct {
	mu     sync.Mutex
	db     *db.DB
	name   string
	tables []murmurSchema.TableSchema // last declaration this provider migrated (debug aid)

	// schemaDir, when set by the server, receives the live-schema
	// sidecar after every successful CREATE TABLE so the next boot
	// reopens the advanced schema exactly.
	schemaDir string
	logger    *slog.Logger
}

// NewProvider builds the MySQL provider for one open murmur database.
// declared carries the config-file tables (zero IDs are fine).
func NewProvider(database *db.DB, name string, declared []murmurSchema.TableSchema) *Provider {
	cp := append([]murmurSchema.TableSchema(nil), declared...)
	return &Provider{db: database, name: name, tables: cp}
}

// Engine builds the default go-mysql-server engine over the provider.
func (p *Provider) Engine() *sqle.Engine {
	return sqle.NewDefault(p)
}

func (p *Provider) Database(_ *sql.Context, name string) (sql.Database, error) {
	if !strings.EqualFold(name, p.name) {
		return nil, sql.ErrDatabaseNotFound.New(name)
	}
	return &murmurDB{provider: p, name: p.name}, nil
}

func (p *Provider) HasDatabase(_ *sql.Context, name string) bool {
	return strings.EqualFold(name, p.name)
}

func (p *Provider) AllDatabases(_ *sql.Context) []sql.Database {
	return []sql.Database{&murmurDB{provider: p, name: p.name}}
}

// murmurDB is the single sql.Database. It implements TableCreator
// (additive CREATE TABLE via Migrate) and deliberately nothing else:
// no DropTable, no RenameTable, no triggers/procedures.
type murmurDB struct {
	provider *Provider
	name     string
}

func (d *murmurDB) Name() string { return d.name }

func (d *murmurDB) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	names, err := d.tableNames(ctx.Context)
	if err != nil {
		return nil, false, err
	}
	for _, n := range names {
		if strings.EqualFold(n, tblName) {
			t, err := d.openTable(ctx.Context, n)
			if err != nil {
				return nil, false, err
			}
			return t, true, nil
		}
	}
	return nil, false, nil
}

func (d *murmurDB) GetTableNames(ctx *sql.Context) ([]string, error) {
	return d.tableNames(ctx.Context)
}

func (d *murmurDB) tableNames(ctx context.Context) ([]string, error) {
	rows, err := d.provider.db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("murmurd: list tables: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("murmurd: list tables: %w", err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func (d *murmurDB) openTable(ctx context.Context, name string) (sql.Table, error) {
	sch, err := d.tableSchema(ctx, name)
	if err != nil {
		return nil, err
	}
	return &murmurTable{db: d.provider.db, name: name, schema: sch}, nil
}

// tableSchema reads PRAGMA table_info and maps SQLite declarations
// to go-mysql-server types.
func (d *murmurDB) tableSchema(ctx context.Context, name string) (sql.PrimaryKeySchema, error) {
	rows, err := d.provider.db.QueryContext(ctx, "SELECT name, type, \"notnull\", pk FROM pragma_table_info(?)", name)
	if err != nil {
		return sql.PrimaryKeySchema{}, fmt.Errorf("murmurd: describe %s: %w", name, err)
	}
	defer rows.Close()
	var sch sql.Schema
	var pkOrds []int
	for rows.Next() {
		var colName, colType string
		var notNull, pk int
		if err := rows.Scan(&colName, &colType, &notNull, &pk); err != nil {
			return sql.PrimaryKeySchema{}, fmt.Errorf("murmurd: describe %s: %w", name, err)
		}
		gt := sqliteDeclToGMSType(colType)
		sch = append(sch, &sql.Column{
			Name:       colName,
			Type:       gt,
			Nullable:   notNull == 0 && pk == 0,
			PrimaryKey: pk > 0,
			Source:     name,
		})
		if pk > 0 {
			pkOrds = append(pkOrds, len(sch)-1)
		}
	}
	if err := rows.Err(); err != nil {
		return sql.PrimaryKeySchema{}, fmt.Errorf("murmurd: describe %s: %w", name, err)
	}
	if len(sch) == 0 {
		return sql.PrimaryKeySchema{}, sql.ErrTableNotFound.New(name)
	}
	sort.Ints(pkOrds)
	return sql.NewPrimaryKeySchema(sch, pkOrds...), nil
}

// sqliteDeclToGMSType maps a SQLite column decltype to a MySQL type.
// Unknown affinities fall back to Text (values stringify); murmur
// itself only creates the four mapped types.
func sqliteDeclToGMSType(decl string) sql.Type {
	u := strings.ToUpper(decl)
	switch {
	case strings.Contains(u, "INT"):
		return types.Int64
	case strings.Contains(u, "CHAR"), strings.Contains(u, "CLOB"), strings.Contains(u, "TEXT"):
		return types.Text
	case strings.Contains(u, "BLOB"):
		return types.Blob
	case strings.Contains(u, "REAL"), strings.Contains(u, "FLOA"), strings.Contains(u, "DOUB"):
		return types.Float64
	default:
		return types.Text
	}
}

// CreateTable maps the MySQL column list to a murmur table and
// publishes it with Migrate (additive). Murmur's key rule applies:
// exactly one PRIMARY KEY column on `id`, BLOB typed.
func (d *murmurDB) CreateTable(ctx *sql.Context, name string, sch sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	_ = collation
	_ = comment
	if len(sch.PkOrdinals) != 1 {
		return fmt.Errorf("murmurd: CREATE TABLE %s needs exactly one PRIMARY KEY column (murmur keys are single-column)", name)
	}
	pkCol := sch.Schema[sch.PkOrdinals[0]]
	if !strings.EqualFold(pkCol.Name, "id") {
		return fmt.Errorf("murmurd: CREATE TABLE %s: primary key must be the `id` column (murmur key convention)", name)
	}
	ts := murmurSchema.TableSchema{Name: name}
	for _, c := range sch.Schema {
		ct, err := gmsTypeToMurmur(c.Type)
		if err != nil {
			return fmt.Errorf("murmurd: CREATE TABLE %s column %s: %w", name, c.Name, err)
		}
		ts.Columns = append(ts.Columns, murmurSchema.ColumnSchema{
			Name:     c.Name,
			Type:     ct,
			Nullable: c.Nullable && !c.PrimaryKey,
		})
	}
	if err := d.provider.addTable(ctx.Context, ts); err != nil {
		if sql.ErrTableAlreadyExists.Is(err) {
			return err
		}
		return fmt.Errorf("murmurd: CREATE TABLE %s: %w", name, err)
	}
	return nil
}

// addTable publishes one additive table via Migrate. It is shared by
// the MySQL CreateTable path and the PostgreSQL CREATE TABLE parser
// so both frontends see the same declared set.
func (p *Provider) addTable(ctx context.Context, ts murmurSchema.TableSchema) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Rebase on the live engine declaration, not the provider's
	// boot-time copy: a replicated adoption may have advanced the
	// stored schema since boot, and Migrate rejects a base that
	// drops it.
	_, live, err := p.db.LiveSchema()
	if err != nil {
		return err
	}
	for _, t := range live {
		if strings.EqualFold(t.Name, ts.Name) {
			return sql.ErrTableAlreadyExists.New(ts.Name)
		}
	}
	next := append(append([]murmurSchema.TableSchema(nil), live...), ts)
	if err := p.db.Migrate(ctx, next); err != nil {
		return err
	}
	p.tables = next
	// The table is committed; the sidecar must follow or the next
	// boot cannot reopen. Fail the statement loudly on persist
	// errors so the operator fixes it now, not at 3am.
	if p.schemaDir != "" {
		if _, err := persistLiveSchema(p.db, p.schemaDir); err != nil {
			if p.logger != nil {
				p.logger.Error("murmurd: table committed but schema sidecar persist failed", "table", ts.Name, "err", err)
			}
			return fmt.Errorf("murmurd: CREATE TABLE %s committed but schema persist failed: %w", ts.Name, err)
		}
	}
	return nil
}

// gmsTypeToMurmur maps go-mysql-server column types to murmur's four.
// Everything else is rejected: the engine cannot store it.
func gmsTypeToMurmur(t sql.Type) (murmurSchema.ColumnType, error) {
	switch t.Type() {
	case query.Type_INT8, query.Type_INT16, query.Type_INT24, query.Type_INT32,
		query.Type_INT64, query.Type_UINT8, query.Type_UINT16, query.Type_UINT24,
		query.Type_UINT32, query.Type_UINT64, query.Type_YEAR, query.Type_BIT:
		return murmurSchema.ColInteger, nil
	case query.Type_FLOAT32, query.Type_FLOAT64:
		return murmurSchema.ColReal, nil
	case query.Type_TEXT, query.Type_VARCHAR, query.Type_CHAR:
		return murmurSchema.ColText, nil
	case query.Type_BLOB, query.Type_VARBINARY, query.Type_BINARY:
		return murmurSchema.ColBlob, nil
	default:
		return 0, fmt.Errorf("type %s has no murmur equivalent (use integer, float, text, or blob columns)", t.String())
	}
}
