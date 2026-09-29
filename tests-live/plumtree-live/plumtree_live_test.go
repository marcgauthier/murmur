// Plumtree dissemination over a live mesh, plus mixed-mode refusal.
//
// All-Plumtree clusters must converge writes end to end. A gossip-mode
// node meshed with Plumtree peers must be refused at handshake time
// (required capability negotiation): no rows flow either way while the
// Plumtree pair converges, and capability refusals are recorded.
package plumtreelive_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
		Name: "pt_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}}
}

func TestPlumtreeMeshConverges(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "plumtree-live",
		NumNodes:    3,
		AwaitUnlock: true,
		Replication: &harness.ReplicationOptions{Dissemination: "plumtree"},
		Schema:      schemaConfig(),
	})
	for i := 0; i < 10; i++ {
		if err := cluster.ExecSQL(0, "INSERT INTO pt_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 11000+i), fmt.Sprintf("p-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	waitConverged(t, cluster, 10, 60*time.Second)
}

func TestMixedModeRefusesGossipPeer(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "plumtree-mixed",
		NumNodes:    3,
		AwaitUnlock: true,
		Replication: &harness.ReplicationOptions{Dissemination: "plumtree"},
		DisseminationByNode: map[int]string{
			0: "plumtree",
			1: "plumtree",
			2: "gossip",
		},
		Schema: schemaConfig(),
	})
	for i := 0; i < 10; i++ {
		if err := cluster.ExecSQL(0, "INSERT INTO pt_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 12000+i), fmt.Sprintf("p-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.ExecSQL(2, "INSERT INTO pt_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 12999), "gossip-only"); err != nil {
		t.Fatal(err)
	}
	// The Plumtree pair converges; the gossip node stays isolated.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		n0, _ := cluster.QueryRowCount(0, "pt_rows")
		n1, _ := cluster.QueryRowCount(1, "pt_rows")
		if n0 == 10 && n1 == 10 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(5 * time.Second) // let any leak arrive
	for i, want := range []int{10, 10, 1} {
		if n, _ := cluster.QueryRowCount(i, "pt_rows"); n != want {
			t.Fatalf("node %d count = %d, want %d (mode isolation broken?)", i, n, want)
		}
	}
	var refusals float64
	for i := range cluster.Nodes {
		refusals += metricValue(t, cluster.Nodes[i].APIAddr, "spedsql_repl_handshake_capability_refusals_total")
	}
	if refusals < 1 {
		t.Fatal("no handshake capability refusals recorded (negotiation silent?)")
	}
	t.Logf("capability refusals across mesh: %v", refusals)
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "pt_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "pt_rows", "name")
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
	t.Fatalf("nodes did not converge on %d rows within %v", want, timeout)
}

func metricValue(t *testing.T, apiAddr, name string) float64 {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, name+" ") && !strings.HasPrefix(line, name+"{") {
			continue
		}
		fields := strings.Fields(line)
		var v float64
		if _, err := fmt.Sscanf(fields[len(fields)-1], "%g", &v); err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		return v
	}
	t.Fatalf("metric %s not found", name)
	return 0
}
