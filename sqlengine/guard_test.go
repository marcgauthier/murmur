package sqlengine

import (
	"testing"
)

func TestGuardFirstKeyword(t *testing.T) {
	cases := []struct {
		q    string
		want string
	}{
		{"SELECT 1", "SELECT"},
		{"  insert into t", "INSERT"},
		{"-- hi\nDROP TABLE t", "DROP"},
		{"/* x */ attach 'f' as g", "ATTACH"},
		{"/* unclosed", ""},
		{"", ""},
		{"   ", ""},
	}
	for _, tc := range cases {
		if got := firstKeyword(tc.q); got != tc.want {
			t.Fatalf("firstKeyword(%q) = %q, want %q", tc.q, got, tc.want)
		}
	}
}

func TestGuardWriteVerbScan(t *testing.T) {
	cases := []struct {
		q    string
		want bool
	}{
		{"WITH x AS (SELECT 1) SELECT * FROM x", false},
		{"WITH x AS (SELECT 1) UPDATE t SET a=1", true},
		{"WITH x AS (SELECT 1) DELETE FROM t", true},
		{"WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", true},
		{"WITH x AS (SELECT 1) REPLACE INTO t SELECT * FROM x", true},
		{"SELECT replace(a, 'b', 'c') FROM t", false},
		{"SELECT replace (a, 'b') FROM t", false},
		{"SELECT 'delete' FROM t", false},
		{`SELECT "update" FROM t`, false},
		{"SELECT a FROM t -- delete", false},
		{"SELECT a FROM t /* insert */", false},
		{"SELECT updated_at FROM t", false},
		{"SELECT * FROM t WHERE x IN (SELECT y FROM u)", false},
	}
	for _, tc := range cases {
		if got := containsWriteVerb(tc.q); got != tc.want {
			t.Fatalf("containsWriteVerb(%q) = %v, want %v", tc.q, got, tc.want)
		}
	}
}

func TestGuardPragmaAssigns(t *testing.T) {
	cases := []struct {
		q    string
		want bool
	}{
		{"PRAGMA table_info(t)", false},
		{"PRAGMA foreign_keys", false},
		{"PRAGMA foreign_keys = ON", true},
		{"PRAGMA foreign_keys=ON", true},
		{"PRAGMA user_version = 1", true},
		{`PRAGMA table_info('a=b')`, false},
	}
	for _, tc := range cases {
		if got := pragmaAssigns(tc.q); got != tc.want {
			t.Fatalf("pragmaAssigns(%q) = %v, want %v", tc.q, got, tc.want)
		}
	}
}

func TestGuardStatements(t *testing.T) {
	execReject := []string{
		"CREATE TABLE t (a)", "create unique index i on t(a)", "DROP TABLE t",
		"ALTER TABLE t ADD COLUMN a", "ATTACH DATABASE 'f' AS x", "DETACH x",
		"PRAGMA foreign_keys = ON", "PRAGMA table_info(t)", "VACUUM",
		"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT sp", "RELEASE sp",
		"-- c\nDROP TABLE t", "/* c */ attach 'f' as x", "begin immediate",
	}
	for _, q := range execReject {
		if err := checkExecAllowed(q); err == nil {
			t.Fatalf("exec allowed %q", q)
		}
	}
	execAllow := []string{
		"INSERT INTO t VALUES (1)", "UPDATE t SET a=1", "DELETE FROM t",
		"REPLACE INTO t VALUES (1)", "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
		"WITH x AS (SELECT 1) UPDATE t SET a=1", "WITH x AS (SELECT 1) SELECT * FROM x",
		"SELECT 1", "EXPLAIN SELECT 1", "VALUES (1)", "TABLE t",
		"ANALYZE", "REINDEX", "SELEC FROM WHERE",
	}
	for _, q := range execAllow {
		if err := checkExecAllowed(q); err != nil {
			t.Fatalf("exec rejected %q: %v", q, err)
		}
	}
	poolReject := []string{
		"INSERT INTO t VALUES (1)", "UPDATE t SET a=1", "DELETE FROM t",
		"REPLACE INTO t VALUES (1)", "CREATE TABLE t (a)", "ATTACH 'f' AS x",
		"PRAGMA foreign_keys = ON", "WITH x AS (SELECT 1) DELETE FROM t",
		"BEGIN", "VACUUM",
	}
	for _, q := range poolReject {
		if err := checkPoolQueryAllowed(q); err == nil {
			t.Fatalf("pool query allowed %q", q)
		}
	}
	poolAllow := []string{
		"SELECT 1", "WITH x AS (SELECT 1) SELECT * FROM x",
		"SELECT replace(a,'b') FROM t", "EXPLAIN UPDATE t SET a=1",
		"PRAGMA table_info(t)", "VALUES (1)", "TABLE t",
	}
	for _, q := range poolAllow {
		if err := checkPoolQueryAllowed(q); err != nil {
			t.Fatalf("pool query rejected %q: %v", q, err)
		}
	}
	txQueryReject := []string{
		"CREATE TABLE t (a)", "DROP TABLE t", "ATTACH 'f' AS x",
		"PRAGMA foreign_keys = ON", "SAVEPOINT sp", "COMMIT",
	}
	for _, q := range txQueryReject {
		if err := checkTxQueryAllowed(q); err == nil {
			t.Fatalf("tx query allowed %q", q)
		}
	}
	txQueryAllow := []string{
		"SELECT 1", "INSERT INTO t VALUES (1)", "UPDATE t SET a=1",
		"PRAGMA table_info(t)", "WITH x AS (SELECT 1) DELETE FROM t",
	}
	for _, q := range txQueryAllow {
		if err := checkTxQueryAllowed(q); err != nil {
			t.Fatalf("tx query rejected %q: %v", q, err)
		}
	}
}
