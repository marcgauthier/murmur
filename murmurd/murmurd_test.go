package murmurd

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/transport"
)

const testKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// testID returns a 16-byte murmur-style key.
func testID(seed byte) []byte {
	id := make([]byte, 16)
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// baseTOML writes a schema.sql plus a single-node config with both
// frontends on :0. The config references the schema file by relative
// path, exercising config-dir resolution.
func baseTOML(t *testing.T, dir, schemaSQL string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(schemaSQL), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := fmt.Sprintf(`
[murmur]
data_dir = %q

[murmur.encryption]
key_id = "test-key-1"
key_hex = %q

[mysql]
enable = true
addr = "127.0.0.1:0"
database = "murmur"

[postgres]
enable = true
addr = "127.0.0.1:0"
database = "murmur"

[schema]
file = "schema.sql"
`, filepath.Join(dir, "data"), testKeyHex)
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const docsSchema = `-- murmurd test schema: one docs table.
CREATE TABLE docs (
  id BLOB PRIMARY KEY,
  title TEXT
);
`

type testServer struct {
	srv    *Server
	cancel context.CancelFunc
	done   chan struct{}
	runErr error
}

func startDaemon(t *testing.T, configPath string) *testServer {
	t.Helper()
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ts := &testServer{srv: srv, cancel: cancel, done: make(chan struct{})}
	go func() {
		ts.runErr = srv.Run(ctx)
		close(ts.done)
	}()
	t.Cleanup(func() { ts.stop(t) })
	return ts
}

// stop cancels the daemon and waits for Run to return. It is safe to
// call more than once (done closes exactly once).
func (ts *testServer) stop(t *testing.T) {
	t.Helper()
	ts.cancel()
	select {
	case <-ts.done:
		if ts.runErr != nil {
			t.Logf("daemon run error: %v", ts.runErr)
		}
	case <-time.After(15 * time.Second):
		t.Error("daemon did not stop in 15s")
	}
}

func dialMySQL(t *testing.T, addr string) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("root:@tcp(%s)/murmur?timeout=5s&readTimeout=10s&writeTimeout=10s", addr)
	var handle *sql.DB
	deadline := time.Now().Add(20 * time.Second)
	for {
		var err error
		handle, err = sql.Open("mysql", dsn)
		if err != nil {
			t.Fatal(err)
		}
		if err = handle.Ping(); err == nil {
			t.Cleanup(func() { handle.Close() })
			return handle
		}
		handle.Close()
		if time.Now().After(deadline) {
			t.Fatalf("mysql ping %s: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func dialPG(t *testing.T, addr string) *pgx.Conn {
	t.Helper()
	url := fmt.Sprintf("postgres://murmur@%s/murmur?sslmode=disable", addr)
	deadline := time.Now().Add(20 * time.Second)
	for {
		conn, err := pgx.Connect(context.Background(), url)
		if err == nil {
			if perr := conn.Ping(context.Background()); perr == nil {
				t.Cleanup(func() { conn.Close(context.Background()) })
				return conn
			} else {
				err = perr
			}
			conn.Close(context.Background())
		}
		if time.Now().After(deadline) {
			t.Fatalf("pg connect %s: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestMySQLCRUD(t *testing.T) {
	dir := t.TempDir()
	ts := startDaemon(t, baseTOML(t, dir, docsSchema))
	my := dialMySQL(t, ts.srv.MySQLAddr())

	if _, err := my.Exec("CREATE TABLE notes (id VARBINARY(16) PRIMARY KEY, title TEXT, n INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Duplicate create must fail loudly, not silently replace.
	if _, err := my.Exec("CREATE TABLE notes (id VARBINARY(16) PRIMARY KEY, title TEXT)"); err == nil {
		t.Fatal("duplicate CREATE TABLE succeeded, want already-exists error")
	}
	id1, id2 := testID(1), testID(2)
	if _, err := my.Exec("INSERT INTO notes (id, title, n) VALUES (?, ?, ?), (?, ?, ?)", id1, "one", 1, id2, "two", 2); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, err := my.Query("SELECT id, title, n FROM notes ORDER BY n")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	type row struct {
		id    []byte
		title string
		n     int64
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.title, &r.n); err != nil {
			rows.Close()
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	rows.Close()
	if len(got) != 2 || !bytes.Equal(got[0].id, id1) || got[0].title != "one" || got[0].n != 1 ||
		!bytes.Equal(got[1].id, id2) || got[1].title != "two" || got[1].n != 2 {
		t.Fatalf("select got %+v, want both rows exact", got)
	}
	if _, err := my.Exec("UPDATE notes SET title = ?, n = ? WHERE n = ?", "ONE", 10, 1); err != nil {
		t.Fatalf("update: %v", err)
	}
	var title string
	var n int64
	if err := my.QueryRow("SELECT title, n FROM notes WHERE n = 10").Scan(&title, &n); err != nil {
		t.Fatalf("select after update: %v", err)
	}
	if title != "ONE" || n != 10 {
		t.Fatalf("after update got %q/%d, want ONE/10", title, n)
	}
	if _, err := my.Exec("DELETE FROM notes WHERE n = ?", 2); err != nil {
		t.Fatalf("delete: %v", err)
	}
	var count int
	if err := my.QueryRow("SELECT COUNT(*) FROM notes").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	// REPLACE rewrites to delete+insert over the provider.
	if _, err := my.Exec("REPLACE INTO notes (id, title, n) VALUES (?, ?, ?)", id1, "replaced", 99); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if err := my.QueryRow("SELECT title, n FROM notes WHERE n = 99").Scan(&title, &n); err != nil {
		t.Fatalf("select after replace: %v", err)
	}
	if title != "replaced" {
		t.Fatalf("after replace title = %q", title)
	}
	// Introspection is engine-provided on the MySQL side.
	rows, err = my.Query("SHOW TABLES")
	if err != nil {
		t.Fatalf("show tables: %v", err)
	}
	var tables []string
	for rows.Next() {
		var tn string
		if err := rows.Scan(&tn); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		tables = append(tables, tn)
	}
	rows.Close()
	found := false
	for _, tn := range tables {
		if tn == "notes" || tn == "docs" {
			found = true
		}
	}
	if !found {
		t.Fatalf("SHOW TABLES = %v, want notes/docs", tables)
	}
	if _, err := my.Exec("DESCRIBE notes"); err != nil {
		t.Fatalf("describe: %v", err)
	}
	// Destructive DDL is filtered: no murmur equivalent.
	if _, err := my.Exec("DROP TABLE notes"); err == nil {
		t.Fatal("DROP TABLE succeeded, want rejection")
	}
	if _, err := my.Exec("ALTER TABLE notes ADD COLUMN x TEXT"); err == nil {
		t.Fatal("ALTER TABLE succeeded, want rejection")
	}
	// Non-id key and exotic types are filtered with reasons.
	if _, err := my.Exec("CREATE TABLE bad (k INTEGER PRIMARY KEY, v TEXT)"); err == nil {
		t.Fatal("non-id PRIMARY KEY succeeded, want rejection")
	}
	if _, err := my.Exec("CREATE TABLE bad2 (id VARBINARY(16) PRIMARY KEY, v JSON)"); err == nil {
		t.Fatal("JSON column succeeded, want rejection")
	}
	// The table survived the rejected statements.
	if err := my.QueryRow("SELECT COUNT(*) FROM notes").Scan(&count); err != nil || count != 1 {
		t.Fatalf("after rejections count = %d, err = %v, want 1", count, err)
	}
}

func TestMySQLPersistRestart(t *testing.T) {
	dir := t.TempDir()
	path := baseTOML(t, dir, docsSchema)
	ts := startDaemon(t, path)
	my := dialMySQL(t, ts.srv.MySQLAddr())
	id := testID(7)
	if _, err := my.Exec("INSERT INTO docs (id, title) VALUES (?, ?)", id, "persist-me"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Stop and restart on the same directory: identity and data stay.
	ts.stop(t)
	if ts.runErr != nil {
		t.Fatalf("run: %v", ts.runErr)
	}
	ts2 := startDaemon(t, path)
	my2 := dialMySQL(t, ts2.srv.MySQLAddr())
	var title string
	var got []byte
	if err := my2.QueryRow("SELECT id, title FROM docs").Scan(&got, &title); err != nil {
		t.Fatalf("select after restart: %v", err)
	}
	if !bytes.Equal(got, id) || title != "persist-me" {
		t.Fatalf("after restart got %x/%q, want persisted row", got, title)
	}
}

func TestPGCRUD(t *testing.T) {
	dir := t.TempDir()
	ts := startDaemon(t, baseTOML(t, dir, docsSchema))
	ctx := context.Background()
	pg := dialPG(t, ts.srv.PGAddr())

	// Introspection shims every driver expects.
	var version, curdb string
	if err := pg.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatalf("version(): %v", err)
	}
	if version == "" {
		t.Fatal("empty version()")
	}
	if err := pg.QueryRow(ctx, "SELECT current_database()").Scan(&curdb); err != nil {
		t.Fatalf("current_database(): %v", err)
	}
	if curdb != "murmur" {
		t.Fatalf("current_database() = %q, want murmur", curdb)
	}
	// Session framing is absorbed (autocommit engine).
	if _, err := pg.Exec(ctx, "SET client_encoding TO 'UTF8'"); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if _, err := pg.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := pg.Exec(ctx, "COMMIT"); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}

	if _, err := pg.Exec(ctx, "CREATE TABLE pdocs (id BYTEA PRIMARY KEY, title TEXT, n INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	id1 := testID(11)
	// Extended protocol with $1 params and binary results (pgx default).
	if _, err := pg.Exec(ctx, "INSERT INTO pdocs (id, title, n) VALUES ($1, $2, $3)", id1, "pg-one", 41); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var title string
	var n int64
	var got []byte
	if err := pg.QueryRow(ctx, "SELECT id, title, n FROM pdocs WHERE n = $1", 41).Scan(&got, &title, &n); err != nil {
		t.Fatalf("select: %v", err)
	}
	if !bytes.Equal(got, id1) || title != "pg-one" || n != 41 {
		t.Fatalf("select got %x/%q/%d, want exact row", got, title, n)
	}
	if _, err := pg.Exec(ctx, "UPDATE pdocs SET n = $1 WHERE id = $2", 42, id1); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := pg.Exec(ctx, "DELETE FROM pdocs WHERE n = $1", 42); err != nil {
		t.Fatalf("delete: %v", err)
	}
	var count int64
	if err := pg.QueryRow(ctx, "SELECT COUNT(*) FROM pdocs").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
	// SQLite-subset filter rejections.
	for _, q := range []string{
		"DROP TABLE pdocs",
		"ALTER TABLE pdocs ADD COLUMN x TEXT",
		"CREATE INDEX i ON pdocs (n)",
		"INSERT INTO pdocs (id) VALUES ('ab') RETURNING id",
		"CREATE TABLE s (id SERIAL PRIMARY KEY)",
		"SELECT * FROM pdocs WHERE n::text = '1'",
		"GRANT SELECT ON pdocs TO murmur",
	} {
		if _, err := pg.Exec(ctx, q); err == nil {
			t.Fatalf("%q succeeded, want filter rejection", q)
		}
	}
}

func TestPGMySQLCrossTalk(t *testing.T) {
	dir := t.TempDir()
	ts := startDaemon(t, baseTOML(t, dir, docsSchema))
	ctx := context.Background()
	my := dialMySQL(t, ts.srv.MySQLAddr())
	pg := dialPG(t, ts.srv.PGAddr())

	id := testID(21)
	if _, err := pg.Exec(ctx, "INSERT INTO docs (id, title) VALUES ($1, $2)", id, "via-pg"); err != nil {
		t.Fatalf("pg insert: %v", err)
	}
	var title string
	if err := my.QueryRow("SELECT title FROM docs WHERE title = 'via-pg'").Scan(&title); err != nil {
		t.Fatalf("mysql read of pg write: %v", err)
	}
	id2 := testID(22)
	if _, err := my.Exec("INSERT INTO docs (id, title) VALUES (?, ?)", id2, "via-mysql"); err != nil {
		t.Fatalf("mysql insert: %v", err)
	}
	if err := pg.QueryRow(ctx, "SELECT title FROM docs WHERE title = 'via-mysql'").Scan(&title); err != nil {
		t.Fatalf("pg read of mysql write: %v", err)
	}
	if title != "via-mysql" {
		t.Fatalf("cross-talk title = %q", title)
	}
}

func TestReplicateOverMySQL(t *testing.T) {
	dir := t.TempDir()
	ca, err := transport.GenerateCA(24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	nodeA := db.NewNodeID()
	nodeB := db.NewNodeID()
	dbID := db.NewDBID()
	certA, keyA, err := ca.IssueNode(nodeA, 24*3600*1e9)
	if err != nil {
		t.Fatal(err)
	}
	certB, keyB, err := ca.IssueNode(nodeB, 24*3600*1e9)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	caFile := write("ca.crt", ca.CertPEM)
	certAFile, keyAFile := write("a.crt", certA), write("a.key", keyA)
	certBFile, keyBFile := write("b.crt", certB), write("b.key", keyB)
	if err := os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(docsSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	replA, replB := freePort(t), freePort(t)

	mkconfig := func(tag string, self db.NodeID, repl int, cert, key string, peer db.NodeID, peerRepl int) string {
		cfg := fmt.Sprintf(`
[murmur]
data_dir = %q
node_id = %q
db_id = %q

[murmur.encryption]
key_id = "test-key-1"
key_hex = %q

[mysql]
enable = true
addr = "127.0.0.1:0"
database = "murmur"

[postgres]
enable = false

[replication]
addr = "127.0.0.1:%d"
ca_cert_file = %q
node_cert_file = %q
node_key_file = %q

[[replication.peers]]
node_id = %q
addrs = ["127.0.0.1:%d"]

[schema]
file = "schema.sql"
`, filepath.Join(dir, tag), self.String(), dbID.String(), testKeyHex,
			repl, caFile, cert, key, peer.String(), peerRepl)
		path := filepath.Join(dir, tag+".toml")
		if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	tsA := startDaemon(t, mkconfig("a", nodeA, replA, certAFile, keyAFile, nodeB, replB))
	tsB := startDaemon(t, mkconfig("b", nodeB, replB, certBFile, keyBFile, nodeA, replA))
	myA := dialMySQL(t, tsA.srv.MySQLAddr())
	myB := dialMySQL(t, tsB.srv.MySQLAddr())

	id := testID(31)
	if _, err := myA.Exec("INSERT INTO docs (id, title) VALUES (?, ?)", id, "replicated"); err != nil {
		t.Fatalf("insert on A: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var title string
		err := myB.QueryRow("SELECT title FROM docs WHERE title = 'replicated'").Scan(&title)
		if err == nil && title == "replicated" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("row never replicated to B (last err: %v)", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
