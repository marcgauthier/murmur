package murmurd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	murmurSchema "github.com/marcgauthier/spedsql/schema"
)

// TestRestartPersistsSQLCreatedTables is the sidecar's reason to exist:
// DDL through SQL advances the stored schema, and the daemon must
// reopen it with data intact.
func TestRestartPersistsSQLCreatedTables(t *testing.T) {
	dir := t.TempDir()
	cfgPath := baseTOML(t, dir, docsSchema)

	ts := startDaemon(t, cfgPath)
	my := dialMySQL(t, ts.srv.MySQLAddr())
	if _, err := my.Exec("CREATE TABLE notes (id VARBINARY(16) PRIMARY KEY, title TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := my.Exec("INSERT INTO notes (id, title) VALUES (?, ?)", testID(9), "persist-me"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	ts.stop(t)

	// The sidecar must track the advanced schema.
	raw, err := os.ReadFile(filepath.Join(dir, "data", "schema.json"))
	if err != nil {
		t.Fatalf("sidecar missing after DDL: %v", err)
	}
	if !strings.Contains(string(raw), `"notes"`) {
		t.Fatalf("sidecar does not mention notes: %s", raw)
	}

	// Reboot from the same config + data dir: rows must survive.
	ts2 := startDaemon(t, cfgPath)
	my2 := dialMySQL(t, ts2.srv.MySQLAddr())
	var title string
	if err := my2.QueryRow("SELECT title FROM notes").Scan(&title); err != nil {
		t.Fatalf("select after restart: %v", err)
	}
	if title != "persist-me" {
		t.Fatalf("title = %q, want persist-me", title)
	}
	// And PG sees the SQL-created table too.
	pg := dialPG(t, ts2.srv.PGAddr())
	var n int
	if err := pg.QueryRow(context.Background(), "SELECT COUNT(*) FROM notes").Scan(&n); err != nil {
		t.Fatalf("pg count after restart: %v", err)
	}
	if n != 1 {
		t.Fatalf("pg count = %d, want 1", n)
	}
}

// TestBootMigratesSchemaFileAddedTables: schema.sql is desired additive
// state — a newly declared table migrates in on the next boot.
func TestBootMigratesSchemaFileAddedTables(t *testing.T) {
	dir := t.TempDir()
	cfgPath := baseTOML(t, dir, docsSchema)
	ts := startDaemon(t, cfgPath)
	dialMySQL(t, ts.srv.MySQLAddr())
	ts.stop(t)

	withExtra := docsSchema + `
CREATE TABLE orders (
  id BLOB PRIMARY KEY,
  total INTEGER
);
`
	cfgPath = baseTOML(t, dir, withExtra)
	ts2 := startDaemon(t, cfgPath)
	my := dialMySQL(t, ts2.srv.MySQLAddr())
	if _, err := my.Exec("INSERT INTO orders (id, total) VALUES (?, ?)", testID(3), 42); err != nil {
		t.Fatalf("insert into config-added table: %v", err)
	}
	var total int
	if err := my.QueryRow("SELECT total FROM orders").Scan(&total); err != nil {
		t.Fatalf("select: %v", err)
	}
	if total != 42 {
		t.Fatalf("total = %d, want 42", total)
	}
}

// TestBootMigratesSchemaFileAddedColumns: same for a column added to
// an existing table declaration.
func TestBootMigratesSchemaFileAddedColumns(t *testing.T) {
	dir := t.TempDir()
	ts := startDaemon(t, baseTOML(t, dir, docsSchema))
	dialMySQL(t, ts.srv.MySQLAddr())
	ts.stop(t)

	withCol := strings.Replace(docsSchema, "  title TEXT\n", "  title TEXT,\n  owner TEXT\n", 1)
	ts2 := startDaemon(t, baseTOML(t, dir, withCol))
	my := dialMySQL(t, ts2.srv.MySQLAddr())
	if _, err := my.Exec("INSERT INTO docs (id, title, owner) VALUES (?, ?, ?)", testID(4), "t", "o"); err != nil {
		t.Fatalf("insert with config-added column: %v", err)
	}
}

// TestBootRejectsSchemaFileColumnConflict: redefining a stored
// column's type in schema.sql must fail the boot loudly, not silently
// diverge.
func TestBootRejectsSchemaFileColumnConflict(t *testing.T) {
	dir := t.TempDir()
	ts := startDaemon(t, baseTOML(t, dir, docsSchema))
	dialMySQL(t, ts.srv.MySQLAddr())
	ts.stop(t)

	drifted := strings.Replace(docsSchema, "  title TEXT\n", "  title INTEGER\n", 1)
	cfg, err := LoadConfig(baseTOML(t, dir, drifted))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(cfg, nil); err == nil {
		t.Fatal("boot with conflicting column type succeeded, want loud failure")
	} else if !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("error does not name the conflict: %v", err)
	}
}

// TestCreateTableAfterAdoptedRevision simulates a replicated adoption
// (schema advanced behind the provider's back) and requires local
// CREATE TABLE to still work by rebasing on the live declaration.
func TestCreateTableAfterAdoptedRevision(t *testing.T) {
	dir := t.TempDir()
	ts := startDaemon(t, baseTOML(t, dir, docsSchema))
	my := dialMySQL(t, ts.srv.MySQLAddr())

	ctx := context.Background()
	_, live, err := ts.srv.DB().LiveSchema()
	if err != nil {
		t.Fatal(err)
	}
	adopted := append(append([]murmurSchema.TableSchema(nil), live...), murmurSchema.TableSchema{
		Name: "adopted",
		Columns: []murmurSchema.ColumnSchema{
			{Name: "id", Type: murmurSchema.ColBlob},
			{Name: "v", Type: murmurSchema.ColText, Nullable: true},
		},
	})
	if err := ts.srv.DB().Migrate(ctx, adopted); err != nil {
		t.Fatalf("simulated adoption: %v", err)
	}
	// Local DDL must rebase on the adopted revision, not fail on a
	// stale base or drop the adopted table.
	if _, err := my.Exec("CREATE TABLE local_after (id VARBINARY(16) PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create after adoption: %v", err)
	}
	for _, tbl := range []string{"adopted", "local_after"} {
		if _, err := my.Exec("INSERT INTO "+tbl+" (id) VALUES (?)", testID(5)); err != nil {
			t.Fatalf("insert into %s: %v", tbl, err)
		}
	}
	ts.stop(t)

	// Restart must reopen the twice-advanced schema.
	ts2 := startDaemon(t, filepath.Join(dir, "config.toml"))
	my2 := dialMySQL(t, ts2.srv.MySQLAddr())
	for _, tbl := range []string{"adopted", "local_after"} {
		var n int
		if err := my2.QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&n); err != nil {
			t.Fatalf("count %s after restart: %v", tbl, err)
		}
		if n != 1 {
			t.Fatalf("count %s = %d, want 1", tbl, n)
		}
	}
}
