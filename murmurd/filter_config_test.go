package murmurd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTOML(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// writeTOMLWithSchema writes a config plus the schema.sql it
// references by relative path.
func writeTOMLWithSchema(t *testing.T, body, schemaSQL string) string {
	t.Helper()
	path := writeTOML(t, body)
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "schema.sql"), []byte(schemaSQL), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const validTestSchema = `CREATE TABLE docs (
  id BLOB PRIMARY KEY,
  title TEXT
);
`

func TestLoadConfigValidation(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
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
file = "schema.sql"
`
	cases := []struct {
		name    string
		mutate  func(string) string
		wantErr string
	}{
		{"valid", func(s string) string { return s }, ""},
		{"no-data-dir", func(s string) string {
			return strings.Replace(s, "data_dir = ", "#data_dir = ", 1)
		}, "data_dir"},
		{"no-key", func(s string) string {
			return strings.Replace(s, "key_hex = ", "#key_hex = ", 1)
		}, "key_hex or key_file"},
		{"bad-hex", func(s string) string {
			return strings.Replace(s, testKeyHex, "zzzz", 1)
		}, "not valid hex"},
		{"short-key", func(s string) string {
			return strings.Replace(s, testKeyHex, "abcd", 1)
		}, "16, 24, or 32 bytes"},
		{"no-frontends", func(s string) string {
			s = strings.Replace(s, "[mysql]\nenable = true", "[mysql]\nenable = false", 1)
			return s
		}, "at least one"},
		{"no-schema-file", func(s string) string {
			return strings.Replace(s, "file = \"schema.sql\"", "#file = \"schema.sql\"", 1)
		}, "[schema] file is required"},
		{"schema-tables-rejected", func(s string) string {
			return s + "[[schema.tables]]\nname = \"docs\"\n"
		}, "embedded-only"},
		{"peer-without-tls", func(s string) string {
			return s + "[[replication.peers]]\nnode_id = \"3fa85f64-5717-4562-b3fc-2c963f66afa6\"\naddrs = [\"127.0.0.1:4401\"]\n"
		}, "ca_cert_file"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Fresh data dir per case: identity persists across loads.
			body := strings.Replace(tc.mutate(base), dataDir, t.TempDir(), 1)
			_, err := LoadConfig(writeTOMLWithSchema(t, body, validTestSchema))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("valid config rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got err %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadConfigIdentity(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	body := `
[murmur]
data_dir = "` + data + `"

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
file = "schema.sql"
`
	path := writeTOMLWithSchema(t, body, validTestSchema)
	first, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if first.Murmur.NodeID == "" || first.Murmur.DBID == "" {
		t.Fatal("identity not generated on first boot")
	}
	// Second load reuses the persisted identity.
	second, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if second.Murmur.NodeID != first.Murmur.NodeID || second.Murmur.DBID != first.Murmur.DBID {
		t.Fatal("identity changed across loads")
	}
	// A configured ID that disagrees with the persisted one fails closed.
	other := strings.Replace(body, "[murmur]\ndata_dir = ", "[murmur]\nnode_id = \"3fa85f64-5717-4562-b3fc-2c963f66afa6\"\ndata_dir = ", 1)
	if _, err := LoadConfig(writeTOMLWithSchema(t, other, validTestSchema)); err == nil {
		t.Fatal("conflicting node_id accepted, want refusal")
	} else if !strings.Contains(err.Error(), "refusing to change identity") {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestKeyFile(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "key.hex")
	if err := os.WriteFile(keyFile, []byte(testKeyHex+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	body := `
[murmur]
data_dir = "` + filepath.Join(dir, "data") + `"

[murmur.encryption]
key_id = "k1"
key_file = "` + keyFile + `"

[mysql]
enable = true
addr = "127.0.0.1:0"
database = "murmur"

[postgres]
enable = false

[schema]
file = "schema.sql"
`
	cfg, err := LoadConfig(writeTOMLWithSchema(t, body, validTestSchema))
	if err != nil {
		t.Fatalf("key_file config rejected: %v", err)
	}
	key, err := cfg.storageKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 32 {
		t.Fatalf("key length = %d, want 32", len(key))
	}
	// Both sources at once is rejected.
	both := strings.Replace(body, "key_file = ", "key_hex = \""+testKeyHex+"\"\nkey_file = ", 1)
	if _, err := LoadConfig(writeTOMLWithSchema(t, both, validTestSchema)); err == nil {
		t.Fatal("key_hex + key_file accepted, want rejection")
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		q     string
		class StmtClass
	}{
		{"SELECT 1", ClassRead},
		{"select * from t where a = 1", ClassRead},
		{"WITH x AS (SELECT 1) SELECT * FROM x", ClassRead},
		{"WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", ClassWrite},
		{"VALUES (1), (2)", ClassRead},
		{"EXPLAIN SELECT 1", ClassRead},
		{"PRAGMA table_info(t)", ClassRead},
		{"INSERT INTO t VALUES (1)", ClassWrite},
		{"UPDATE t SET a = 1", ClassWrite},
		{"DELETE FROM t", ClassWrite},
		{"INSERT INTO t VALUES (1) RETURNING id", ClassReject},
		{"CREATE TABLE t (id BLOB PRIMARY KEY)", ClassCreateTable},
		{"TRUNCATE t", ClassTruncate},
		{"BEGIN", ClassTxnNoop},
		{"START TRANSACTION", ClassTxnNoop},
		{"COMMIT", ClassTxnNoop},
		{"ROLLBACK", ClassTxnNoop},
		{"SAVEPOINT s", ClassTxnNoop},
		{"SET x TO y", ClassSessionNoop},
		{"RESET x", ClassSessionNoop},
		{"DISCARD ALL", ClassSessionNoop},
		{"SELECT version()", ClassEmulated},
		{"select current_database() ; ", ClassEmulated},
		{"DROP TABLE t", ClassReject},
		{"ALTER TABLE t ADD COLUMN x", ClassReject},
		{"CREATE INDEX i ON t (a)", ClassReject},
		{"CREATE VIEW v AS SELECT 1", ClassReject},
		{"GRANT SELECT ON t TO u", ClassReject},
		{"COPY t FROM STDIN", ClassReject},
		{"VACUUM", ClassReject},
		{"LISTEN x", ClassReject},
		{"PREPARE p AS SELECT 1", ClassReject},
		{"SHOW ALL", ClassReject},
	}
	for _, tc := range cases {
		if got, _ := Classify(tc.q); got != tc.class {
			t.Errorf("Classify(%q) = %v, want %v", tc.q, got, tc.class)
		}
	}
}

func TestTranslateParams(t *testing.T) {
	got, n, err := TranslateParams("SELECT * FROM t WHERE a = $1 AND b = $2")
	if err != nil || n != 2 || got != "SELECT * FROM t WHERE a = ? AND b = ?" {
		t.Fatalf("got %q/%d/%v", got, n, err)
	}
	// Dollar-quoted bodies are untouched.
	got, n, err = TranslateParams("SELECT $tag$100$ $tag$, $1")
	if err != nil || n != 1 || got != "SELECT $tag$100$ $tag$, ?" {
		t.Fatalf("got %q/%d/%v", got, n, err)
	}
	// Strings are untouched.
	got, n, err = TranslateParams("SELECT '$1', $1")
	if err != nil || n != 1 || got != "SELECT '$1', ?" {
		t.Fatalf("got %q/%d/%v", got, n, err)
	}
	for _, bad := range []string{"SELECT $", "SELECT $0", "SELECT $2", "SELECT $1, $3"} {
		if _, _, err := TranslateParams(bad); err == nil {
			t.Errorf("%q accepted, want rejection", bad)
		}
	}
}

func TestSplitStatements(t *testing.T) {
	got := SplitStatements("SELECT 1; SELECT 'a;b'; SELECT $tag$x;$tag$;")
	if len(got) != 3 || got[1] != "SELECT 'a;b'" || got[2] != "SELECT $tag$x;$tag$" {
		t.Fatalf("got %#v", got)
	}
	if got := SplitStatements("-- just a comment"); len(got) != 1 {
		t.Fatalf("got %#v", got)
	}
}

func TestParseCreateTable(t *testing.T) {
	def, err := ParseCreateTable("CREATE TABLE docs (id BYTEA PRIMARY KEY, title TEXT, n INTEGER NOT NULL)")
	if err != nil {
		t.Fatal(err)
	}
	if def.Name != "docs" || len(def.Columns) != 3 {
		t.Fatalf("got %+v", def)
	}
	if !def.Columns[0].PK || def.Columns[0].Type != "blob" || def.Columns[0].Nullable {
		t.Fatalf("pk col: %+v", def.Columns[0])
	}
	if def.Columns[2].Nullable || def.Columns[1].Type != "text" || def.Columns[2].Type != "integer" {
		t.Fatalf("cols: %+v", def.Columns)
	}
	// Table-level PK, IF NOT EXISTS, sized types.
	def, err = ParseCreateTable("CREATE TABLE IF NOT EXISTS t2 (id BLOB, v VARCHAR(32), PRIMARY KEY (id))")
	if err != nil {
		t.Fatal(err)
	}
	if !def.IfNotExist || !def.Columns[0].PK || def.Columns[1].Type != "text" {
		t.Fatalf("got %+v", def)
	}
	for _, bad := range []string{
		"CREATE TABLE t (id BLOB, v TEXT)",                         // no key
		"CREATE TABLE t (a BLOB PRIMARY KEY, b TEXT PRIMARY KEY)",  // composite
		"CREATE TABLE t (id SERIAL PRIMARY KEY)",                   // autoincrement
		"CREATE TABLE t (id BLOB PRIMARY KEY, v JSON)",             // exotic type
		"CREATE TABLE t (id BLOB PRIMARY KEY, v TEXT DEFAULT 'x')", // default
		"CREATE TABLE t (id BLOB PRIMARY KEY, UNIQUE (v))",         // constraint
		"CREATE TABLE t (id BLOB PRIMARY KEY) WITH (x = 1)",        // options
	} {
		if _, err := ParseCreateTable(bad); err == nil {
			t.Errorf("%q accepted, want rejection", bad)
		}
	}
}
