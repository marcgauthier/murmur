package murmurd

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// This file proves the wire frontends enforce Murmur's schema rules:
// no secondary UNIQUE, no tables without a single NOT NULL `id` blob
// primary key, no autoincrement/defaults/checks/foreign keys. Every
// rejection is asserted live over the wire, with no residue left
// behind (no ghost table, no epoch advance, no sidecar change).

// wireCase is one statement that must be rejected.
type wireCase struct {
	name  string
	ddl   string
	want  string // required lowercase substring of the error
	table string // table that must not exist afterwards ("" for none)
}

func mysqlRuleCases() []wireCase {
	return []wireCase{
		{"inline unique key", "CREATE TABLE wu1 (id VARBINARY(16) PRIMARY KEY, v VARCHAR(32), UNIQUE KEY (v))", "unique", "wu1"},
		{"column unique", "CREATE TABLE wu2 (id VARBINARY(16) PRIMARY KEY, v VARCHAR(32) UNIQUE)", "unique", "wu2"},
		{"unique key on pk column", "CREATE TABLE wu4 (id VARBINARY(16) PRIMARY KEY, v TEXT, UNIQUE KEY (id))", "unique", "wu4"},
		{"plain secondary key", "CREATE TABLE wk1 (id VARBINARY(16) PRIMARY KEY, v VARCHAR(32), KEY (v))", "local-only", "wk1"},
		{"standalone create index", "CREATE INDEX wi1 ON wv (v)", "local-only", ""},
		{"standalone unique index", "CREATE UNIQUE INDEX wui1 ON wv (v)", "unique", ""},
		{"standalone index on text", "CREATE INDEX wi2 ON docs (title)", "murmur", ""},
		{"alter add unique", "ALTER TABLE wv ADD UNIQUE KEY (v)", "unique", ""},
		{"alter add key", "ALTER TABLE wv ADD KEY (v)", "local-only", ""},
		{"alter drop index", "ALTER TABLE wv DROP INDEX v", "murmur", ""},
		{"column default", "CREATE TABLE wd1 (id VARBINARY(16) PRIMARY KEY, v TEXT DEFAULT 'x')", "default", "wd1"},
		{"auto increment column", "CREATE TABLE wa1 (id VARBINARY(16) PRIMARY KEY AUTO_INCREMENT, v TEXT)", "auto_increment", "wa1"},
		{"auto increment table opt", "CREATE TABLE wa2 (id VARBINARY(16) PRIMARY KEY, v TEXT) AUTO_INCREMENT=100", "auto_increment", "wa2"},
		{"generated column", "CREATE TABLE wg1 (id VARBINARY(16) PRIMARY KEY, v TEXT GENERATED ALWAYS AS ('x'))", "murmur", "wg1"},
		{"check constraint", "CREATE TABLE wc1 (id VARBINARY(16) PRIMARY KEY, v TEXT, CHECK (v <> ''))", "check", "wc1"},
		{"foreign key", "CREATE TABLE wf1 (id VARBINARY(16) PRIMARY KEY, d VARBINARY(16), FOREIGN KEY (d) REFERENCES docs(id))", "foreign", "wf1"},
		{"temporary table", "CREATE TEMPORARY TABLE wt1 (id VARBINARY(16) PRIMARY KEY, v TEXT)", "murmur", "wt1"},
		{"no pk", "CREATE TABLE wn1 (v TEXT)", "primary key", "wn1"},
		{"composite pk", "CREATE TABLE wcp (a VARBINARY(16), b VARBINARY(16), PRIMARY KEY (a, b))", "exactly one", "wcp"},
		{"non-id pk", "CREATE TABLE wni (k VARBINARY(16) PRIMARY KEY, v TEXT)", "must be the `id` column", "wni"},
		{"non-blob id pk", "CREATE TABLE wib (id INTEGER PRIMARY KEY, v TEXT)", "blob", "wib"},
	}
}

func pgRuleCases() []wireCase {
	return []wireCase{
		{"standalone unique index", "CREATE UNIQUE INDEX wui ON docs (title)", "unique", ""},
		{"standalone index", "CREATE INDEX wi ON docs (title)", "local-only", ""},
		{"column unique", "CREATE TABLE pu1 (id BYTEA PRIMARY KEY, v TEXT UNIQUE)", "unique", "pu1"},
		{"table unique", "CREATE TABLE pu2 (id BYTEA PRIMARY KEY, v TEXT, UNIQUE (v))", "primary key", "pu2"},
		{"no pk", "CREATE TABLE pn1 (v TEXT)", "primary key", "pn1"},
		{"composite pk", "CREATE TABLE pc1 (a BYTEA PRIMARY KEY, b TEXT PRIMARY KEY)", "composite", "pc1"},
		{"non-id pk", "CREATE TABLE pni (k BYTEA PRIMARY KEY, v TEXT)", "`id`", "pni"},
		{"non-blob id pk", "CREATE TABLE pib (id INTEGER PRIMARY KEY, v TEXT)", "blob", "pib"},
		{"column default", "CREATE TABLE pd1 (id BYTEA PRIMARY KEY, v TEXT DEFAULT 'x')", "default", "pd1"},
		{"check constraint", "CREATE TABLE pc2 (id BYTEA PRIMARY KEY, v TEXT CHECK (v <> ''))", "check", "pc2"},
		{"references", "CREATE TABLE pf1 (id BYTEA PRIMARY KEY, d BYTEA REFERENCES docs(id))", "references", "pf1"},
		{"serial pk", "CREATE TABLE ps1 (id SERIAL PRIMARY KEY)", "autoincrement", "ps1"},
		{"drop table", "DROP TABLE docs", "destructive", ""},
		{"alter add column", "ALTER TABLE docs ADD COLUMN x TEXT", "destructive", ""},
	}
}

func mysqlTableSet(t *testing.T, my *sql.DB) map[string]bool {
	t.Helper()
	rows, err := my.Query("SHOW TABLES")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		out[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func pgTableCount(t *testing.T, pg *pgx.Conn, table string) int {
	t.Helper()
	var n int
	q := "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '" + table + "'"
	if err := pg.QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("sqlite_master probe for %s: %v", table, err)
	}
	return n
}

func sidecarBytes(t *testing.T, dir string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "data", "schema.json"))
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	return raw
}

func TestMySQLWireRuleRejections(t *testing.T) {
	dir := t.TempDir()
	ts := startDaemon(t, baseTOML(t, dir, docsSchema))
	my := dialMySQL(t, ts.srv.MySQLAddr())
	ctx := context.Background()

	// Valid helper table for standalone/ALTER index attempts.
	if _, err := my.Exec("CREATE TABLE wv (id VARBINARY(16) PRIMARY KEY, v VARCHAR(32))"); err != nil {
		t.Fatalf("helper create: %v", err)
	}
	epoch0 := ts.srv.DB().Status().SchemaEpoch
	sidecar0 := string(sidecarBytes(t, dir))

	for _, tc := range mysqlRuleCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, err := my.ExecContext(ctx, tc.ddl)
			if err == nil {
				t.Fatalf("%s succeeded, want rejection containing %q", tc.ddl, tc.want)
			}
			if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("%s error = %v, want containing %q", tc.ddl, err, tc.want)
			}
		})
	}

	// No residue: none of the rejected tables may exist.
	got := mysqlTableSet(t, my)
	for _, tc := range mysqlRuleCases() {
		if tc.table != "" && got[tc.table] {
			t.Errorf("rejected table %s exists (partial DDL commit)", tc.table)
		}
	}
	if !got["wv"] || !got["docs"] {
		t.Errorf("helper tables missing from %v", got)
	}
	// No epoch advance and no sidecar change from any rejection.
	if epoch := ts.srv.DB().Status().SchemaEpoch; epoch != epoch0 {
		t.Errorf("schema epoch advanced %d -> %d on rejected DDL", epoch0, epoch)
	}
	if sidecar := string(sidecarBytes(t, dir)); sidecar != sidecar0 {
		t.Errorf("sidecar changed on rejected DDL:\nbefore: %s\n after: %s", sidecar0, sidecar)
	}

	// Positive control: the daemon still accepts valid DDL + DML.
	if _, err := my.Exec("CREATE TABLE wok (id VARBINARY(16) PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("valid create after rejections: %v", err)
	}
	if _, err := my.Exec("INSERT INTO wok (id, v) VALUES (?, ?)", testID(61), "ok"); err != nil {
		t.Fatalf("insert after rejections: %v", err)
	}
	var n int
	if err := my.QueryRow("SELECT COUNT(*) FROM wok").Scan(&n); err != nil || n != 1 {
		t.Fatalf("count = %d, err = %v, want 1", n, err)
	}
}

func TestPGWireRuleRejections(t *testing.T) {
	dir := t.TempDir()
	ts := startDaemon(t, baseTOML(t, dir, docsSchema))
	ctx := context.Background()
	pg := dialPG(t, ts.srv.PGAddr())

	epoch0 := ts.srv.DB().Status().SchemaEpoch
	sidecar0 := string(sidecarBytes(t, dir))

	for _, tc := range pgRuleCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pg.Exec(ctx, tc.ddl)
			if err == nil {
				t.Fatalf("%s succeeded, want rejection containing %q", tc.ddl, tc.want)
			}
			if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("%s error = %v, want containing %q", tc.ddl, err, tc.want)
			}
		})
	}

	for _, tc := range pgRuleCases() {
		if tc.table != "" && pgTableCount(t, pg, tc.table) != 0 {
			t.Errorf("rejected table %s exists (partial DDL commit)", tc.table)
		}
	}
	if epoch := ts.srv.DB().Status().SchemaEpoch; epoch != epoch0 {
		t.Errorf("schema epoch advanced %d -> %d on rejected DDL", epoch0, epoch)
	}
	if sidecar := string(sidecarBytes(t, dir)); sidecar != sidecar0 {
		t.Errorf("sidecar changed on rejected DDL:\nbefore: %s\n after: %s", sidecar0, sidecar)
	}

	if _, err := pg.Exec(ctx, "CREATE TABLE pok (id BYTEA PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("valid create after rejections: %v", err)
	}
	if _, err := pg.Exec(ctx, "INSERT INTO pok (id, v) VALUES ($1, $2)", testID(62), "ok"); err != nil {
		t.Fatalf("insert after rejections: %v", err)
	}
	var n int
	if err := pg.QueryRow(ctx, "SELECT COUNT(*) FROM pok").Scan(&n); err != nil || n != 1 {
		t.Fatalf("count = %d, err = %v, want 1", n, err)
	}
}

// TestMySQLRedundantPKUniqueNormalized pins the one UNIQUE spelling the
// MySQL side accepts: a UNIQUE keyword merged into the PRIMARY KEY
// column itself. A primary key is already unique, the MySQL layer
// normalizes the redundancy away, and no secondary index may exist in
// any layer afterwards.
func TestMySQLRedundantPKUniqueNormalized(t *testing.T) {
	dir := t.TempDir()
	ts := startDaemon(t, baseTOML(t, dir, docsSchema))
	my := dialMySQL(t, ts.srv.MySQLAddr())
	ctx := context.Background()

	if _, err := my.Exec("CREATE TABLE wu3 (id VARBINARY(16) PRIMARY KEY UNIQUE, v TEXT)"); err != nil {
		t.Fatalf("redundant PK UNIQUE rejected: %v", err)
	}
	var name, ddl string
	if err := my.QueryRow("SHOW CREATE TABLE wu3").Scan(&name, &ddl); err != nil {
		t.Fatalf("show create: %v", err)
	}
	if !strings.Contains(ddl, "PRIMARY KEY") {
		t.Errorf("SHOW CREATE TABLE lacks PRIMARY KEY: %s", ddl)
	}
	if strings.Contains(strings.ToUpper(ddl), "UNIQUE") {
		t.Errorf("SHOW CREATE TABLE retains a secondary UNIQUE: %s", ddl)
	}
	var stored string
	q := "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'wu3'"
	row, err := ts.srv.DB().QueryContext(ctx, q)
	if err != nil {
		t.Fatalf("sqlite_master: %v", err)
	}
	defer row.Close()
	if !row.Next() {
		t.Fatal("wu3 missing from sqlite_master")
	}
	if err := row.Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToUpper(stored), "UNIQUE") {
		t.Errorf("stored DDL retains UNIQUE: %s", stored)
	}
}

// TestWirePKStoredNotNull proves the normalization both wires rely on:
// a PRIMARY KEY declared without the NOT NULL keyword is still stored
// explicitly NOT NULL (murmur never relies on implicit PK nullability).
func TestWirePKStoredNotNull(t *testing.T) {
	dir := t.TempDir()
	ts := startDaemon(t, baseTOML(t, dir, docsSchema))
	ctx := context.Background()
	my := dialMySQL(t, ts.srv.MySQLAddr())
	pg := dialPG(t, ts.srv.PGAddr())

	if _, err := my.Exec("CREATE TABLE mnn (id VARBINARY(16) PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("mysql create: %v", err)
	}
	if _, err := pg.Exec(ctx, "CREATE TABLE pnn (id BYTEA PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("pg create: %v", err)
	}
	if _, err := pg.Exec(ctx, "CREATE TABLE ptn (id BYTEA, v TEXT, PRIMARY KEY (id))"); err != nil {
		t.Fatalf("pg table-pk create: %v", err)
	}

	rows, err := ts.srv.DB().QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE name IN ('mnn', 'pnn', 'ptn')")
	if err != nil {
		t.Fatalf("sqlite_master: %v", err)
	}
	seen := map[string]bool{}
	for rows.Next() {
		var name, ddl string
		if err := rows.Scan(&name, &ddl); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		seen[name] = true
		if !strings.Contains(ddl, "PRIMARY KEY NOT NULL") {
			t.Errorf("%s stored DDL lacks explicit NOT NULL PK: %s", name, ddl)
		}
	}
	rows.Close()
	for _, want := range []string{"mnn", "pnn", "ptn"} {
		if !seen[want] {
			t.Errorf("table %s missing from sqlite_master", want)
		}
	}

	_, live, err := ts.srv.DB().LiveSchema()
	if err != nil {
		t.Fatalf("LiveSchema: %v", err)
	}
	for _, want := range []string{"mnn", "pnn", "ptn"} {
		matched := false
		for i := range live {
			if !strings.EqualFold(live[i].Name, want) {
				continue
			}
			matched = true
			pk := live[i].ColumnByID(live[i].PK)
			if pk == nil {
				t.Errorf("%s has no resolvable PK", want)
				continue
			}
			if pk.Nullable {
				t.Errorf("%s registry PK is nullable", want)
			}
		}
		if !matched {
			t.Errorf("table %s missing from live schema", want)
		}
	}
}

func TestClassifyCreateIndexMessages(t *testing.T) {
	class, ferr := Classify("CREATE UNIQUE INDEX i ON t (a)")
	if class != ClassReject || ferr == nil {
		t.Fatalf("CREATE UNIQUE INDEX not rejected: %v/%v", class, ferr)
	}
	if !strings.Contains(ferr.Reason, "UNIQUE") {
		t.Errorf("unique-index reason lacks UNIQUE: %q", ferr.Reason)
	}
	class, ferr = Classify("CREATE INDEX i ON t (a)")
	if class != ClassReject || ferr == nil {
		t.Fatalf("CREATE INDEX not rejected: %v/%v", class, ferr)
	}
	if !strings.Contains(ferr.Reason, "local-only") {
		t.Errorf("plain-index reason lacks local-only pointer: %q", ferr.Reason)
	}
}
