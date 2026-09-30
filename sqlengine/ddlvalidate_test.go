package sqlengine

import (
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/schema"
)

func TestParseCreateHead(t *testing.T) {
	cases := []struct {
		name     string
		ddl      string
		kind     string
		obj      string
		wantErr  bool
		replErr  bool // checkReplicatedDDLStatement must reject
		localErr bool // checkLocalDDLStatement must reject
	}{
		{"table", `CREATE TABLE t (a)`, "", "TABLE", false, false, false},
		{"table if not exists", `CREATE TABLE IF NOT EXISTS t (a)`, "", "TABLE", false, false, false},
		{"index", `CREATE INDEX i ON t (a)`, "", "INDEX", false, false, false},
		{"unique index", `CREATE UNIQUE INDEX i ON t (a)`, "UNIQUE", "INDEX", false, true, false},
		{"view", `CREATE VIEW v AS SELECT 1`, "", "VIEW", false, true, false},
		{"or replace view", `CREATE OR REPLACE VIEW v AS SELECT 1`, "", "VIEW", false, true, false},
		{"trigger", "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END", "", "TRIGGER", false, true, false},
		{"virtual", `CREATE VIRTUAL TABLE t USING fts5(a)`, "VIRTUAL", "TABLE", false, true, false},
		{"temp table", `CREATE TEMP TABLE t (a)`, "TEMP", "TABLE", false, true, false},
		{"temp view", `CREATE TEMP VIEW v AS SELECT 1`, "TEMP", "VIEW", false, true, false},
		{"lowercase", `create table t (a)`, "", "TABLE", false, false, false},
		{"leading comment", `/* x */ CREATE TABLE t (a)`, "", "TABLE", false, false, false},
		{"drop", `DROP TABLE t`, "", "", true, true, true},
		{"attach", `ATTACH DATABASE 'f' AS x`, "", "", true, true, true},
		{"pragma", `PRAGMA foreign_keys = ON`, "", "", true, true, true},
		{"insert", `INSERT INTO t VALUES (1)`, "", "", true, true, true},
		{"truncated", `CREATE`, "", "", true, true, true},
		{"empty", ``, "", "", true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind, obj, err := parseCreateHead(tc.ddl)
			if tc.wantErr && err == nil {
				t.Fatalf("parseCreateHead(%q) succeeded, want error", tc.ddl)
			}
			if !tc.wantErr && (err != nil || kind != tc.kind || obj != tc.obj) {
				t.Fatalf("parseCreateHead(%q) = %q/%q/%v, want %q/%q", tc.ddl, kind, obj, err, tc.kind, tc.obj)
			}
			if err := checkReplicatedDDLStatement(tc.ddl); (err != nil) != tc.replErr {
				t.Fatalf("replicated check(%q) err = %v, wantErr = %v", tc.ddl, err, tc.replErr)
			}
			if err := checkLocalDDLStatement(tc.ddl); (err != nil) != tc.localErr {
				t.Fatalf("local check(%q) err = %v, wantErr = %v", tc.ddl, err, tc.localErr)
			}
		})
	}
}

func TestCheckTableKeywords(t *testing.T) {
	base := `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT)`
	cases := []struct {
		name    string
		ddl     string
		wantErr string // "" means accept
	}{
		{"plain", base, ""},
		{"unique column", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT UNIQUE)`, "UNIQUE"},
		{"unique table", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT, UNIQUE (v))`, "UNIQUE"},
		{"unique in string", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT DEFAULT 'unique')`, ""},
		{"unique quoted ident", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, "unique" TEXT)`, ""},
		{"autoincrement", `CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, v TEXT)`, "AUTOINCREMENT"},
		{"autoincrement as name", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, autoincrement TEXT)`, ""},
		{"strict", base + ` STRICT`, "STRICT"},
		{"strict as column", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, strict TEXT)`, ""},
		{"without rowid", base + ` WITHOUT ROWID`, "WITHOUT ROWID"},
		{"virtual", `CREATE VIRTUAL TABLE t USING fts5(v)`, "virtual"},
		{"virtual as name", `CREATE TABLE virtual (id BLOB PRIMARY KEY NOT NULL)`, ""},
		{"lowercase unique", `create table t (id blob primary key not null, v text unique)`, "UNIQUE"},
		{"fk decl allowed", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT REFERENCES t(id) ON DELETE CASCADE)`, ""},
		{"default allowed", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT DEFAULT 'x')`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTableKeywords("t", tc.ddl)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("rejected %q: %v", tc.ddl, err)
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("accepted %q, want %s rejection", tc.ddl, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %v lacks %q", err, tc.wantErr)
				}
			}
		})
	}
}

func TestCheckTableChecks(t *testing.T) {
	cols := map[string]bool{"id": true, "status": true, "qty": true}
	cases := []struct {
		name    string
		ddl     string
		wantErr string
	}{
		{"none", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`, ""},
		{"single column", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, status TEXT CHECK (status IN ('active')))`, ""},
		{"function of own column", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, status TEXT CHECK (length(status) > 0))`, ""},
		{"quoted own column", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, status TEXT CHECK ("status" <> ''))`, ""},
		{"named column check", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, status TEXT CONSTRAINT ok CHECK (status <> ''))`, ""},
		{"cross column", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, status TEXT CHECK (status <> id))`, "references column"},
		{"two columns", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, qty INTEGER CHECK (qty > 0 AND status <> ''), status TEXT)`, "references column"},
		{"table level", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, qty INTEGER, CHECK (qty > 0))`, "table-level"},
		{"named table check", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, qty INTEGER, CONSTRAINT c CHECK (qty > 0))`, "table-level"},
		{"check in string", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, status TEXT DEFAULT 'CHECK (x)')`, ""},
		{"other column only", `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, status TEXT CHECK (qty > 0), qty INTEGER)`, "references column"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTableChecks("t", tc.ddl, cols)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("rejected %q: %v", tc.ddl, err)
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("accepted %q, want %s rejection", tc.ddl, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %v lacks %q", err, tc.wantErr)
				}
			}
		})
	}
}

func TestMurmurTypeForDecltype(t *testing.T) {
	cases := []struct {
		decl string
		want schema.ColumnType
		ok   bool
	}{
		{"BLOB", schema.ColBlob, true},
		{"BLOB(16)", schema.ColBlob, true},
		{"", schema.ColBlob, true},
		{"TEXT", schema.ColText, true},
		{"VARCHAR(32)", schema.ColText, true},
		{"CHARACTER VARYING(10)", schema.ColText, true},
		{"CLOB", schema.ColText, true},
		{"INTEGER", schema.ColInteger, true},
		{"INT", schema.ColInteger, true},
		{"BIGINT", schema.ColInteger, true},
		{"REAL", schema.ColReal, true},
		{"FLOAT", schema.ColReal, true},
		{"DOUBLE PRECISION", schema.ColReal, true},
		{"NUMERIC", 0, false},
		{"DECIMAL(10,2)", 0, false},
		{"DATETIME", 0, false},
		{"DATE", 0, false},
		{"BOOLEAN", 0, false},
	}
	for _, tc := range cases {
		if got, ok := murmurTypeForDecltype(tc.decl); ok != tc.ok || got != tc.want {
			t.Fatalf("decltype %q = %v/%v, want %v/%v", tc.decl, got, ok, tc.want, tc.ok)
		}
	}
}
