package murmur

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"

	replicateddb "github.com/marcgauthier/murmur"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	gormmigrator "gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// Dialector is the GORM dialect for an embedded Murmur engine. The
// engine speaks SQLite, so SQL generation mirrors the sqlite dialect;
// schema management goes through the engine's Migrate API instead of
// SQL DDL (DDL through SQL would land on the ephemeral materialization
// and never replicate).
type Dialector struct {
	Config Config
}

// Config opens the dialect on an already-open engine.
type Config struct {
	// DB is the embedded engine. It must already be open (which
	// requires at least one table; see GenesisTables).
	DB *replicateddb.DB
}

// Open builds the dialect on an open engine handle.
func Open(db *replicateddb.DB) gorm.Dialector {
	return Dialector{Config: Config{DB: db}}
}

// New builds the dialect from a Config.
func New(config Config) gorm.Dialector {
	return Dialector{Config: config}
}

// Name implements gorm.Dialector.
func (d Dialector) Name() string {
	return "murmur"
}

// Initialize implements gorm.Dialector.
func (d Dialector) Initialize(db *gorm.DB) error {
	if d.Config.DB == nil {
		return fmt.Errorf("murmur: Config.DB is required (open the engine first)")
	}
	db.ConnPool = sql.OpenDB(replicateddb.NewConnector(d.Config.DB))

	var version string
	if err := db.ConnPool.QueryRowContext(context.Background(), "select sqlite_version()").Scan(&version); err != nil {
		return err
	}
	// https://www.sqlite.org/releaselog/3_35_0.html
	if compareVersion(version, "3.35.0") >= 0 {
		callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
			CreateClauses:        []string{"INSERT", "VALUES", "ON CONFLICT", "RETURNING"},
			UpdateClauses:        []string{"UPDATE", "SET", "FROM", "WHERE", "RETURNING"},
			DeleteClauses:        []string{"DELETE", "FROM", "WHERE", "RETURNING"},
			LastInsertIDReversed: true,
		})
	} else {
		callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
			LastInsertIDReversed: true,
		})
	}

	for k, v := range d.ClauseBuilders() {
		if _, ok := db.ClauseBuilders[k]; !ok {
			db.ClauseBuilders[k] = v
		}
	}

	// Murmur has no autoincrement: fill zero blob primary keys so
	// ordinary Create calls never fail on (or collide at) the zero key.
	db.Callback().Create().Before("gorm:create").Register("murmur:fill_id", fillMissingIDs)
	return nil
}

// fillMissingIDs assigns murmur.NewID() to zero ID-typed primary
// keys ahead of gorm:create. Explicit IDs pass through untouched.
func fillMissingIDs(db *gorm.DB) {
	stmt := db.Statement
	if db.Error != nil || stmt == nil || stmt.Schema == nil {
		return
	}
	pk := stmt.Schema.PrioritizedPrimaryField
	if pk == nil {
		return
	}
	var fresh func() interface{}
	switch pk.IndirectFieldType {
	case murmurIDType:
		fresh = func() interface{} { return NewID() }
	default:
		ft := pk.IndirectFieldType
		if ft.Kind() != reflect.Slice || ft.Elem().Kind() != reflect.Uint8 {
			return
		}
		fresh = func() interface{} {
			id := NewID()
			return append([]byte(nil), id[:]...)
		}
	}
	if stmt.Dest == nil {
		return
	}
	rv := reflect.ValueOf(stmt.Dest)
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}
	var rows []reflect.Value
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			rows = append(rows, rv.Index(i))
		}
	case reflect.Struct:
		rows = append(rows, rv)
	default:
		// Map and scalar destinations keep caller-supplied IDs.
		return
	}
	ctx := context.Background()
	if stmt.Context != nil {
		ctx = stmt.Context
	}
	for _, row := range rows {
		for row.Kind() == reflect.Ptr || row.Kind() == reflect.Interface {
			if row.IsNil() {
				break
			}
			row = row.Elem()
		}
		if row.Kind() != reflect.Struct || !row.CanAddr() {
			continue
		}
		if _, isZero := pk.ValueOf(ctx, row); !isZero {
			continue
		}
		if err := pk.Set(ctx, row, fresh()); err != nil {
			db.AddError(fmt.Errorf("murmur: fill primary key: %w", err))
			return
		}
	}
}

// ClauseBuilders customizes INSERT (SQLite modifiers), LIMIT (SQLite
// LIMIT -1/OFFSET), and deliberately nothing else: in particular
// there is no FOR override, so locking reads fall through to the
// engine and fail loudly instead of silently dropping the lock.
func (d Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"INSERT": func(c clause.Clause, builder clause.Builder) {
			if insert, ok := c.Expression.(clause.Insert); ok {
				if stmt, ok := builder.(*gorm.Statement); ok {
					stmt.WriteString("INSERT ")
					if insert.Modifier != "" {
						stmt.WriteString(insert.Modifier)
						stmt.WriteByte(' ')
					}

					stmt.WriteString("INTO ")
					if insert.Table.Name == "" {
						stmt.WriteQuoted(stmt.Table)
					} else {
						stmt.WriteQuoted(insert.Table)
					}
					return
				}
			}

			c.Build(builder)
		},
		"LIMIT": func(c clause.Clause, builder clause.Builder) {
			if limit, ok := c.Expression.(clause.Limit); ok {
				var lmt = -1
				if limit.Limit != nil && *limit.Limit >= 0 {
					lmt = *limit.Limit
				}
				if lmt >= 0 || limit.Offset > 0 {
					builder.WriteString("LIMIT ")
					builder.WriteString(strconv.Itoa(lmt))
				}
				if limit.Offset > 0 {
					builder.WriteString(" OFFSET ")
					builder.WriteString(strconv.Itoa(limit.Offset))
				}
			}
		},
	}
}

// DefaultValueOf implements gorm.Dialector. Murmur columns have no
// database defaults (the migrator rejects `default:` tags), so this
// only mirrors the sqlite spelling for shared GORM code paths.
func (d Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	if field.AutoIncrement {
		return clause.Expr{SQL: "NULL"}
	}

	// doesn't work, will raise error
	return clause.Expr{SQL: "DEFAULT"}
}

// Migrator implements gorm.Dialector.
func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return &Migrator{
		Migrator: gormmigrator.Migrator{Config: gormmigrator.Config{
			DB:        db,
			Dialector: d,
		}},
		murmur: d.Config.DB,
	}
}

// BindVarTo implements gorm.Dialector.
func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

// QuoteTo implements gorm.Dialector (backtick quoting, as SQLite).
func (d Dialector) QuoteTo(writer clause.Writer, str string) {
	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(str) {
		switch v {
		case '`':
			continuousBacktick++
			if continuousBacktick == 2 {
				writer.WriteString("``")
				continuousBacktick = 0
			}
		case '.':
			if continuousBacktick > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousBacktick = 0
				writer.WriteString("`")
			}
			writer.WriteByte(v)
			continue
		default:
			if shiftDelimiter-continuousBacktick <= 0 && !underQuoted {
				writer.WriteString("`")
				underQuoted = true
				if selfQuoted = continuousBacktick > 0; selfQuoted {
					continuousBacktick -= 1
				}
			}

			for ; continuousBacktick > 0; continuousBacktick -= 1 {
				writer.WriteString("``")
			}

			writer.WriteByte(v)
		}
		shiftDelimiter++
	}

	if continuousBacktick > 0 && !selfQuoted {
		writer.WriteString("``")
	}
	writer.WriteString("`")
}

// Explain implements gorm.Dialector.
func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `"`, vars...)
}

// DataTypeOf maps GORM field types to Murmur's four column types for
// display. DDL never goes through here: the migrator maps fields via
// murmurTypeForField, which rejects what Murmur cannot store.
func (d Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "integer"
	case schema.Int, schema.Uint:
		return "integer"
	case schema.Float:
		return "real"
	case schema.String:
		return "text"
	case schema.Time:
		return "text"
	case schema.Bytes:
		return "blob"
	}

	return string(field.DataType)
}

func compareVersion(version1, version2 string) int {
	n, m := len(version1), len(version2)
	i, j := 0, 0
	for i < n || j < m {
		x := 0
		for ; i < n && version1[i] != '.'; i++ {
			x = x*10 + int(version1[i]-'0')
		}
		i++
		y := 0
		for ; j < m && version2[j] != '.'; j++ {
			y = y*10 + int(version2[j]-'0')
		}
		j++
		if x > y {
			return 1
		}
		if x < y {
			return -1
		}
	}
	return 0
}
