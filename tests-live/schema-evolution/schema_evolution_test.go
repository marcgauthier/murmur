// Rolling additive schema migration across a live mesh.
//
// Three daemons mesh on a two-column table. Node1 migrates first (adds a
// nullable score column) while node2/node3 stay behind: the mixed-version
// mesh must keep replicating in both directions without stalling. Then
// node2 and node3 migrate and the whole mesh converges on the full
// three-column state with equal digests.
package schemaevolution_test

import (
	"fmt"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

func baseTables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "evo_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}
}

func evolvedTables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "evo_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
		},
	}}
}

func TestRollingAdditiveMigration(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "schema-evolution",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema:      &db.SchemaConfig{Version: 1, Tables: baseTables()},
	})

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", 9000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO evo_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("n-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	waitNamesConverged(t, cluster, 5, 30*time.Second)

	// Rolling step 1: only node1 migrates, then writes the new column.
	if err := cluster.Migrate(0, evolvedTables()); err != nil {
		t.Fatalf("migrate node1: %v", err)
	}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", 9000+i)
		if err := cluster.ExecSQL(0, "UPDATE evo_rows SET score=? WHERE id=?", int64(100+i), id); err != nil {
			t.Fatalf("score write: %v", err)
		}
	}
	// Mixed-version mesh still flows both ways: old-schema writes from
	// node2 must reach migrated node1, and names stay converged.
	if err := cluster.ExecSQL(1, "INSERT INTO evo_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 9100), "late"); err != nil {
		t.Fatalf("mixed-version write: %v", err)
	}
	waitNamesConverged(t, cluster, 6, 30*time.Second)
	assertScore(t, cluster, 0, fmt.Sprintf("%032x", 9000), 100)

	// Rolling steps 2-3: the laggards migrate, then full convergence.
	if err := cluster.Migrate(1, evolvedTables()); err != nil {
		t.Fatalf("migrate node2: %v", err)
	}
	if err := cluster.Migrate(2, evolvedTables()); err != nil {
		t.Fatalf("migrate node3: %v", err)
	}
	waitFullConverged(t, cluster, 30*time.Second)
	assertScore(t, cluster, 1, fmt.Sprintf("%032x", 9001), 101)
	assertScore(t, cluster, 2, fmt.Sprintf("%032x", 9002), 102)
}

// waitNamesConverged polls id/name convergence (tolerates schema skew on
// the new column during the mixed-version window).
func waitNamesConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		counts := map[string]int{}
		for i := range c.Nodes {
			res, err := c.QuerySQL(i, "SELECT name FROM evo_rows ORDER BY name")
			if err != nil || len(res.Rows) != want {
				ok = false
				break
			}
			key := fmt.Sprintf("%v", res.Rows)
			counts[key]++
		}
		if ok && len(counts) == 1 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %d names within %v", want, timeout)
}

// waitFullConverged polls full-row digest equality once all peers share
// the evolved schema.
func waitFullConverged(t *testing.T, c *harness.Cluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "evo_rows")
			if err != nil || n != 6 {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "evo_rows", "name")
			if err != nil {
				ok = false
				break
			}
			if i == 0 {
				first = d
			} else if d != first {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("nodes did not fully converge within %v", timeout)
}

func assertScore(t *testing.T, c *harness.Cluster, idx int, idHex string, want int64) {
	t.Helper()
	res, err := c.QuerySQL(idx, "SELECT score FROM evo_rows WHERE id=?", idHex)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("node %d score read: %+v %v", idx, res, err)
	}
	got, _ := res.Rows[0][0].(float64)
	if int64(got) != want {
		t.Fatalf("node %d score = %v, want %d", idx, got, want)
	}
}
