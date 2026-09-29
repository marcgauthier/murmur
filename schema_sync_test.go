package replicateddb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marcgauthier/spedsql/schema"
)

// waitForRowsSync polls like waitForRows but tolerates the materializer
// going dirty while a node publishes an adopted/merged schema and rebuilds.
func waitForRowsSync(t *testing.T, db *DB, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := db.QueryContext(context.Background(), `SELECT name, phone FROM contacts ORDER BY name`)
		if err != nil {
			if errors.Is(err, ErrMaterializerDirty) || errors.Is(err, ErrNotReady) {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			t.Fatal(err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if n == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d rows", want)
}

// migrateTestTables returns the v1 declaration plus a nullable email column.
func migrateTestTables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "contacts",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "phone", Type: schema.ColText, Nullable: true},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
			{Name: "email", Type: schema.ColText, Nullable: true},
		},
	}}
}

func TestMigrateAdditive(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	cfg := testConfig(path)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "ann"); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, migrateTestTables()); err != nil {
		t.Fatal(err)
	}
	if got := db.Status().SchemaEpoch; got != 2 {
		t.Fatalf("epoch = %d, want 2", got)
	}
	// Old data intact, new column usable.
	rows := queryAll(t, db, `SELECT name, email FROM contacts`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	id2 := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name, email) VALUES (?, ?, ?)`,
		id2[:], "bob", "b@x"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// Reopen requires the migrated declaration, then serves all data.
	if _, err := Open(ctx, cfg); err == nil {
		t.Fatal("old config opened a migrated store")
	}
	cfg.Schema.Version = 2
	cfg.Schema.Tables = migrateTestTables()
	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if n := len(queryAll(t, db2, `SELECT id FROM contacts`)); n != 2 {
		t.Fatalf("want 2 rows, got %d", n)
	}
	// No-op migrate with identical content.
	if err := db2.Migrate(ctx, migrateTestTables()); err != nil {
		t.Fatal(err)
	}
	if got := db2.Status().SchemaEpoch; got != 2 {
		t.Fatalf("epoch after no-op = %d", got)
	}
}

func TestMigrateRejectsDestructive(t *testing.T) {
	ctx := context.Background()
	full := migrateTestTables()
	cases := map[string]func() []schema.TableSchema{
		"drop column": func() []schema.TableSchema {
			out := []schema.TableSchema{{
				Name: "contacts",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "name", Type: schema.ColText, Nullable: true},
				},
			}}
			return out
		},
		"change type": func() []schema.TableSchema {
			out := full
			out[0].Columns[3].Type = schema.ColText
			return out
		},
		"change nullability": func() []schema.TableSchema {
			out := full
			out[0].Columns[1].Nullable = false
			return out
		},
		"drop table": func() []schema.TableSchema { return nil },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			db, err := Open(ctx, testConfig(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Migrate(ctx, fn()); !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("err = %v, want ErrUnsupportedSchema", err)
			}
			if got := db.Status().SchemaEpoch; got != 1 {
				t.Fatalf("epoch = %d after rejected migration", got)
			}
		})
	}
}

func TestMigrateRejectsNonNullOnRows(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id) VALUES (?)`, id[:]); err != nil {
		t.Fatal(err)
	}
	withRequired := migrateTestTables()
	withRequired[0].Columns = append(withRequired[0].Columns,
		schema.ColumnSchema{Name: "req", Type: schema.ColText})
	if err := db.Migrate(ctx, withRequired); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("err = %v, want ErrUnsupportedSchema", err)
	}
	// Same migration on an empty table succeeds.
	db2, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if err := db2.Migrate(ctx, withRequired); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaAutoAdopt(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	dbA, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	dbB, err := Open(ctx, replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}}))
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	// Pre-migration row converges first.
	id0 := NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id0[:], "pre"); err != nil {
		t.Fatal(err)
	}
	waitForRows(t, dbB, 1, 15*time.Second)

	// A migrates; B adopts automatically, then data flows again.
	if err := dbA.Migrate(ctx, migrateTestTables()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for dbB.Status().SchemaEpoch != 2 {
		if time.Now().After(deadline) {
			t.Fatal("B never adopted the migration")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if dbB.Status().SchemaHash != dbA.Status().SchemaHash {
		t.Fatal("adopted hash differs")
	}
	id1, id2 := NewRowID(), NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, email) VALUES (?, ?, ?)`,
		id1[:], "a1", "a@x"); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := dbB.ExecContext(ctx, `INSERT INTO contacts (id, name, email) VALUES (?, ?, ?)`,
			id2[:], "b1", "b@x"); err == nil {
			break
		} else if !errors.Is(err, ErrMaterializerDirty) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitForRowsSync(t, dbA, 3, 30*time.Second)

	waitForRowsSync(t, dbB, 3, 30*time.Second)
}

func TestSchemaStrictAncestorApplies(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)
	strict := false

	dbA, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	cfgB := replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	cfgB.Schema.AcceptRemoteSchema = &strict
	dbB, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	// v1 rows converge while schemas match.
	id0 := NewRowID()
	if _, err := dbB.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id0[:], "v1"); err != nil {
		t.Fatal(err)
	}
	waitForRows(t, dbA, 1, 15*time.Second)

	// A migrates; strict B refuses to adopt and exchanges nothing further.
	if err := dbA.Migrate(ctx, migrateTestTables()); err != nil {
		t.Fatal(err)
	}
	id1 := NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id1[:], "v2"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second) // several handshake attempts must all refuse
	if got := dbB.Status().SchemaEpoch; got != 1 {
		t.Fatalf("strict B migrated to %d", got)
	}
	if n := len(queryAll(t, dbB, `SELECT id FROM contacts`)); n != 1 {
		t.Fatalf("strict B holds %d rows, want 1", n)
	}
	// A still serves its own rows.
	if n := len(queryAll(t, dbA, `SELECT id FROM contacts`)); n != 2 {
		t.Fatalf("A holds %d rows, want 2", n)
	}
}

func TestSchemaAncestorBatchApplies(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	dbA, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	dbB, err := Open(ctx, replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}}))
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()
	addrB := waitForAddr(t, dbB, 5*time.Second)
	waitForRows(t, dbB, 0, 5*time.Second)

	// Partition: B writes v1 rows nobody acknowledges.
	if err := dbA.RemovePeer(ctx, nodeB); err != nil {
		t.Fatal(err)
	}
	if err := dbB.RemovePeer(ctx, nodeA); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	idB := NewRowID()
	if _, err := dbB.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, idB[:], "offline"); err != nil {
		t.Fatal(err)
	}

	// A migrates while partitioned; B still only knows v1.
	if err := dbA.Migrate(ctx, migrateTestTables()); err != nil {
		t.Fatal(err)
	}

	// Reconnect: B adopts v2, then its retained v1 batches apply on A as
	// compatible ancestors with original provenance.
	if err := dbB.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{addrA}}); err != nil {
		t.Fatal(err)
	}
	if err := dbA.AddPeer(ctx, Peer{NodeID: nodeB, Addrs: []string{addrB}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)

	for dbB.Status().SchemaEpoch != 2 {
		if time.Now().After(deadline) {
			t.Fatal("B never adopted the migration")
		}
		time.Sleep(50 * time.Millisecond)
	}
	waitForRowsSync(t, dbA, 1, 30*time.Second)
	got := queryAll(t, dbA, `SELECT name FROM contacts`)
	if len(got) != 1 || got[0][0] != "offline" {
		t.Fatalf("ancestor batch did not apply: %v", got)
	}
}

func TestSchemaConcurrentMerge(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	// Offline branches: same epoch, disjoint additive content.
	dbA, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	dbB, err := Open(ctx, replConfig(t.TempDir(), nodeB, dbid, creds[nodeB], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	branchA := migrateTestTables() // adds email
	branchB := migrateTestTables()
	branchB[0].Columns[4] = schema.ColumnSchema{Name: "backup", Type: schema.ColText, Nullable: true}
	if err := dbA.Migrate(ctx, branchA); err != nil {
		t.Fatal(err)
	}
	if err := dbB.Migrate(ctx, branchB); err != nil {
		t.Fatal(err)
	}
	if dbA.Status().SchemaHash == dbB.Status().SchemaHash {
		t.Fatal("branches must differ")
	}
	idA, idB := NewRowID(), NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, email) VALUES (?, ?, ?)`,
		idA[:], "a", "a@x"); err != nil {
		t.Fatal(err)
	}
	if _, err := dbB.ExecContext(ctx, `INSERT INTO contacts (id, name, backup) VALUES (?, ?, ?)`,
		idB[:], "b", "b@x"); err != nil {
		t.Fatal(err)
	}

	// Connect: both must derive the identical canonical merge and converge.
	if err := dbB.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{addrA}}); err != nil {
		t.Fatal(err)
	}
	waitForRowsSync(t, dbA, 2, 60*time.Second)
	waitForRowsSync(t, dbB, 2, 60*time.Second)
	if dbA.Status().SchemaEpoch != 3 || dbB.Status().SchemaEpoch != 3 {
		t.Fatalf("epochs %d/%d, want 3/3", dbA.Status().SchemaEpoch, dbB.Status().SchemaEpoch)
	}
	if dbA.Status().SchemaHash != dbB.Status().SchemaHash {
		t.Fatal("merged hashes differ")
	}
	// Stable: no further version churn once converged.
	time.Sleep(2 * time.Second)
	if dbA.Status().SchemaEpoch != 3 || dbB.Status().SchemaEpoch != 3 {
		t.Fatal("schema churned after convergence")
	}
	// Both new columns usable on both nodes.
	rows := queryAll(t, dbA, `SELECT name, email, backup FROM contacts ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("want 2 merged rows, got %d", len(rows))
	}
}

// TestOrderRegistryLikeLocalPreservesPhysicalOrder covers the adoption
// column-order fix: a registry built from a wire manifest (ID-sorted
// columns) must take the live registry's existing order with new columns
// appended by ID, so the author and every adopter rebuild identical
// physical column order. Table "mc_rows" derives name < id, so ID order
// differs from declaration order and the test is sensitive to the bug.
func TestOrderRegistryLikeLocalPreservesPhysicalOrder(t *testing.T) {
	current, err := schema.BuildRegistry(1, []schema.TableSchema{{
		Name: "mc_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := schema.BuildRegistry(2, []schema.TableSchema{{
		Name: "mc_rows",
		Columns: []schema.ColumnSchema{
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "id", Type: schema.ColBlob},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
		},
	}, {
		Name: "new_table",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	orderRegistryLikeLocal(current, wire)
	got := wire.Tables[0].Columns
	want := []string{"id", "name", "score"}
	if len(got) != len(want) {
		t.Fatalf("columns = %d, want %d", len(got), len(want))
	}
	for i, name := range want {
		if got[i].Name != name {
			gotNames := []string{got[0].Name, got[1].Name, got[2].Name}
			t.Fatalf("column order = %v, want %v", gotNames, want)
		}
	}
	// Unknown tables keep manifest order.
	if wire.Tables[1].Name != "new_table" || wire.Tables[1].Columns[0].Name != "id" {
		t.Fatalf("new table mangled: %+v", wire.Tables[1])
	}
	// Nil-safe.
	orderRegistryLikeLocal(nil, wire)
	orderRegistryLikeLocal(current, nil)
}
