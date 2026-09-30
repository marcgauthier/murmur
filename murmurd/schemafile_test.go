package murmurd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	murmurSchema "github.com/marcgauthier/murmur/schema"
)

func writeSchemaFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseSchemaFile(t *testing.T) {
	dir := t.TempDir()
	path := writeSchemaFile(t, dir, "schema.sql", `-- leading comment
/* block
   comment */
CREATE TABLE IF NOT EXISTS docs (
  id BLOB PRIMARY KEY, -- inline comment
  title TEXT,
  n INTEGER NOT NULL
);

CREATE TABLE notes (
  id VARBINARY(16) PRIMARY KEY,
  body TEXT
);
`)
	tables, err := parseSchemaFile(path)
	if err != nil {
		t.Fatalf("valid schema rejected: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("tables = %d, want 2", len(tables))
	}
	docs := tables[0]
	if docs.Name != "docs" || len(docs.Columns) != 3 {
		t.Fatalf("docs = %+v", docs)
	}
	byName := map[string]murmurSchema.ColumnSchema{}
	for _, c := range docs.Columns {
		byName[strings.ToLower(c.Name)] = c
	}
	if byName["id"].Type != murmurSchema.ColBlob || byName["id"].Nullable {
		t.Fatalf("id = %+v, want blob NOT NULL", byName["id"])
	}
	if byName["title"].Type != murmurSchema.ColText || !byName["title"].Nullable {
		t.Fatalf("title = %+v, want nullable text", byName["title"])
	}
	if byName["n"].Type != murmurSchema.ColInteger || byName["n"].Nullable {
		t.Fatalf("n = %+v, want integer NOT NULL", byName["n"])
	}
	if tables[1].Name != "notes" || tables[1].Columns[0].Type != murmurSchema.ColBlob {
		t.Fatalf("notes = %+v", tables[1])
	}
}

func TestParseSchemaFileRejects(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"empty", ``, "declares no tables"},
		{"comments-only", "-- nothing here\n/* nor here */", "declares no tables"},
		{"insert", "CREATE TABLE t (id BLOB PRIMARY KEY);\nINSERT INTO t (id) VALUES (x'00');", "only CREATE TABLE"},
		{"select", "SELECT 1;", "only CREATE TABLE"},
		{"create-index", "CREATE TABLE t (id BLOB PRIMARY KEY);\nCREATE INDEX i ON t (id);", "local-only"},
		{"drop", "DROP TABLE t;", "destructive"},
		{"duplicate", "CREATE TABLE t (id BLOB PRIMARY KEY);\nCREATE TABLE t (id BLOB PRIMARY KEY);", "duplicate table"},
		{"duplicate-case", "CREATE TABLE t (id BLOB PRIMARY KEY);\nCREATE TABLE T (id BLOB PRIMARY KEY);", "duplicate table"},
		{"no-pk", "CREATE TABLE t (id BLOB, v TEXT);", "exactly one PRIMARY KEY"},
		{"pk-not-id", "CREATE TABLE t (id BLOB, v TEXT PRIMARY KEY);", "must be the `id` column"},
		{"text-pk", "CREATE TABLE t (id TEXT PRIMARY KEY);", "must be a blob type"},
		{"bad-type", "CREATE TABLE t (id BLOB PRIMARY KEY, v JSON);", "no murmur equivalent"},
		{"no-columns", "CREATE TABLE t ();", "at least one column"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeSchemaFile(t, t.TempDir(), "schema.sql", tc.body)
			_, err := parseSchemaFile(path)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got err %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestExampleSchemaFileParses keeps the shipped example honest:
// schema.example.sql must always be a bootable schema.
func TestExampleSchemaFileParses(t *testing.T) {
	tables, err := parseSchemaFile("schema.example.sql")
	if err != nil {
		t.Fatalf("schema.example.sql rejected: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("example tables = %d, want 2", len(tables))
	}
}

func TestParseSchemaFileMissing(t *testing.T) {
	_, err := parseSchemaFile(filepath.Join(t.TempDir(), "nope.sql"))
	if err == nil || !strings.Contains(err.Error(), "read schema file") {
		t.Fatalf("got err %v, want read failure", err)
	}
}

func TestStripSQLComments(t *testing.T) {
	in := "CREATE TABLE t (id BLOB PRIMARY KEY, -- trailing\nv TEXT /* inline */); -- done\n"
	got := stripSQLComments(in)
	if strings.Contains(got, "trailing") || strings.Contains(got, "inline") || strings.Contains(got, "done") {
		t.Fatalf("comments survive: %q", got)
	}
	if !strings.Contains(got, "CREATE TABLE") || !strings.Contains(got, "v TEXT") {
		t.Fatalf("code stripped: %q", got)
	}
	// Comment markers inside strings are code, not comments.
	lit := "SELECT '-- not a comment', '/* nor this */';"
	if stripSQLComments(lit) != lit {
		t.Fatalf("string literal mangled: %q", stripSQLComments(lit))
	}
	// Unterminated block comment eats to EOF (fail loud at parse).
	if got := stripSQLComments("CREATE TABLE t (id BLOB /* oops"); strings.Contains(got, "oops") {
		t.Fatalf("unterminated block kept: %q", got)
	}
}

// TestLoadConfigSchemaFileResolution covers absolute paths and
// relative-to-config-dir resolution at the LoadConfig level.
func TestLoadConfigSchemaFileResolution(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	abs := writeSchemaFile(t, dir, "abs.sql", validTestSchema)
	base := `
[murmur]
data_dir = "` + dataDir + `"

[murmur.encryption]
key_id = "k1"
key_hex = "` + testKeyHex + `"

[mysql]
enable = true
addr = "127.0.0.1:0"
database = "murmur"

[postgres]
enable = false

[schema]
file = "%s"
`
	// Absolute path.
	cfg, err := LoadConfig(writeTOMLWithSchema(t, strings.Replace(base, "%s", abs, 1), validTestSchema))
	if err != nil {
		t.Fatalf("absolute schema path rejected: %v", err)
	}
	if len(cfg.schemaFileTables) != 1 || cfg.schemaFileTables[0].Name != "docs" {
		t.Fatalf("parsed tables = %+v", cfg.schemaFileTables)
	}
	// Missing file fails closed.
	bad := strings.Replace(base, "%s", filepath.Join(dir, "missing.sql"), 1)
	if _, err := LoadConfig(writeTOML(t, bad)); err == nil || !strings.Contains(err.Error(), "read schema file") {
		t.Fatalf("got err %v, want read failure", err)
	}
	// Bad SQL fails closed with the statement identified.
	badSQL := strings.Replace(base, "%s", "schema.sql", 1)
	path := writeTOMLWithSchema(t, badSQL, "CREATE TABLE t (id BLOB);\n")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Fatalf("got err %v, want PK failure", err)
	}
}
