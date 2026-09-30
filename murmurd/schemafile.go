package murmurd

import (
	"fmt"
	"os"
	"strings"

	murmurSchema "github.com/marcgauthier/spedsql/schema"
)

// parseSchemaFile reads a user-provided schema.sql and converts it to
// murmur table declarations. The file holds one or more CREATE TABLE
// statements in the strict subset (see ParseCreateTable) plus SQL
// comments; anything else fails the boot with the statement quoted.
// Tables need an `id` blob primary key (murmur key convention),
// enforced here exactly as for runtime CREATE TABLE.
func parseSchemaFile(path string) ([]murmurSchema.TableSchema, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("murmurd: read schema file %s: %w", path, err)
	}
	var out []murmurSchema.TableSchema
	seen := map[string]bool{}
	for i, stmt := range SplitStatements(stripSQLComments(string(raw))) {
		class, ferr := Classify(stmt)
		if class != ClassCreateTable {
			reason := "only CREATE TABLE statements are allowed in the schema file"
			if ferr != nil {
				reason = ferr.Reason
			}
			return nil, fmt.Errorf("murmurd: schema file %s statement %d: %s (in %q)",
				path, i+1, reason, truncStmt(stmt))
		}
		def, err := ParseCreateTable(stmt)
		if err != nil {
			return nil, fmt.Errorf("murmurd: schema file %s statement %d: %w", path, i+1, err)
		}
		if seen[strings.ToLower(def.Name)] {
			return nil, fmt.Errorf("murmurd: schema file %s: duplicate table %q", path, def.Name)
		}
		seen[strings.ToLower(def.Name)] = true
		ts, err := createDefToTableSchema(def)
		if err != nil {
			return nil, fmt.Errorf("murmurd: schema file %s table %q: %w", path, def.Name, err)
		}
		out = append(out, ts)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("murmurd: schema file %s declares no tables", path)
	}
	return out, nil
}

// createDefToTableSchema converts a parsed CREATE TABLE to murmur
// schema form. It is shared by the schema-file loader and the
// PostgreSQL runtime path so file and SQL DDL enforce identical rules:
// exactly one PRIMARY KEY, on the `id` blob column.
func createDefToTableSchema(def *CreateTableDef) (murmurSchema.TableSchema, error) {
	var ts murmurSchema.TableSchema
	pks := 0
	for _, c := range def.Columns {
		if c.PK {
			pks++
			if !strings.EqualFold(c.Name, "id") {
				return ts, fmt.Errorf("primary key must be the `id` column (murmur key convention)")
			}
			if c.Type != "blob" {
				return ts, fmt.Errorf("primary key `id` must be a blob type (BYTEA/BLOB)")
			}
		}
	}
	if pks != 1 {
		return ts, fmt.Errorf("CREATE TABLE needs exactly one PRIMARY KEY column")
	}
	ts.Name = def.Name
	for _, c := range def.Columns {
		ct, err := columnType(c.Type)
		if err != nil {
			return ts, err
		}
		ts.Columns = append(ts.Columns, murmurSchema.ColumnSchema{
			Name:     c.Name,
			Type:     ct,
			Nullable: c.Nullable && !c.PK,
		})
	}
	return ts, nil
}

func truncStmt(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 80 {
		s = s[:80] + "..."
	}
	return s
}

// stripSQLComments removes -- line comments and /* */ block comments
// outside single-, double-, and back-quoted runs.
func stripSQLComments(q string) string {
	var b strings.Builder
	b.Grow(len(q))
	i := 0
	for i < len(q) {
		c := q[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			j := i + 1
			for j < len(q) && q[j] != c {
				j++
			}
			if j < len(q) {
				j++
			}
			b.WriteString(q[i:j])
			i = j
		case c == '-' && i+1 < len(q) && q[i+1] == '-':
			j := i + 2
			for j < len(q) && q[j] != '\n' {
				j++
			}
			i = j
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			j := i + 2
			for j+1 < len(q) && !(q[j] == '*' && q[j+1] == '/') {
				j++
			}
			i = len(q)
			if j+1 < len(q) {
				i = j + 2
			}
			b.WriteByte(' ')
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}
