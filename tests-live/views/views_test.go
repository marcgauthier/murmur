// Multi-process local-view acceptance: three spedsql daemon processes
// share replicated tables while each applies a local-only view definition;
// the views track replicated rows and rebuild across a node restart.
package views_test

import (
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

func TestLocalViewTracksReplicatedRowsAndRebuilds(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "views",
		NumNodes: 3,
		Schema: &db.SchemaConfig{
			Version: 1,
			Tables: []schema.TableSchema{{Name: "contacts", Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
				{Name: "phone", Type: schema.ColText, Nullable: true},
				{Name: "score", Type: schema.ColInteger, Nullable: true},
			}}},
			LocalDDL: []string{"CREATE VIEW enabled_contacts AS SELECT id, name FROM contacts WHERE score > 0"},
		},
	})

	for i, item := range []struct {
		name  string
		phone string
		score int
	}{{"ann", "111", 1}, {"bob", "222", 2}, {"hidden", "333", 0}} {
		id := ids.NewRowID()
		if err := cluster.ExecSQL(i, `INSERT INTO contacts (id, name, phone, score) VALUES (?, ?, ?, ?)`,
			hex.EncodeToString(id[:]), item.name, item.phone, item.score); err != nil {
			t.Fatalf("insert on node %d: %v", i, err)
		}
	}
	waitForViewRows(t, cluster, []int{0, 1, 2}, []string{"ann", "bob"})

	// Restarting one replica rebuilds the materialization and re-applies
	// its local-only view definition from the same configuration.
	cluster.StopNode(2)
	cluster.StartNode(2)
	cluster.WaitNodeReady(2)
	waitForViewRows(t, cluster, []int{2}, []string{"ann", "bob"})
}

func waitForViewRows(t *testing.T, cluster *harness.Cluster, nodes []int, want []string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		allMatch := true
		for _, i := range nodes {
			res, err := cluster.QuerySQL(i, `SELECT name FROM enabled_contacts ORDER BY name`)
			if err != nil {
				allMatch = false
				break
			}
			got := make([]string, 0, len(want))
			for _, r := range res.Rows {
				name, _ := r[0].(string)
				got = append(got, name)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for replicas' enabled_contacts view to contain %v", want)
}
