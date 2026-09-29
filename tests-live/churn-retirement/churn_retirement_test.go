// Peer exclusion, restart persistence, and retention release.
//
// Three daemons mesh; node3 is excluded on the survivors via the admin
// API. The suites requires: sessions to node3 drop and its writes stop
// arriving, the excluded peer releases its GC retention pin, the
// exclusion survives a survivor restart (no rediscovery re-mesh), and
// re-adding the peer heals replication. SWIM suspect/dead/refutation is
// not covered: memberlist suspicion is not wired into the runtime.
package churnretirement_test

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

func TestExcludePersistsAndReleasesRetention(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "churn-retirement",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "ch_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})
	id2 := cluster.Nodes[2].NodeID.String()

	for i := 0; i < 5; i++ {
		if err := cluster.ExecSQL(0, "INSERT INTO ch_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 3000+i), fmt.Sprintf("a-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// 60s, not 30s: under a full parallel `go test ./...` the box is
	// saturated and even a 5-row baseline can take tens of seconds.
	waitConverged(t, cluster, 5, 60*time.Second)
	gatingBefore := metricValue(t, cluster.Nodes[0].APIAddr, "spedsql_gating_members")

	// Exclude node3 on both survivors.
	if err := cluster.RemovePeer(0, 2); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RemovePeer(1, 2); err != nil {
		t.Fatal(err)
	}
	// Sessions drop and node3 leaves the known-peer set (excluded peers
	// carry no metric series, so absence plus the count drop is the
	// signal).
	waitCondition(t, 60*time.Second, "sessions to node3 drop", func() bool {
		return !peerConnected(t, cluster.Nodes[0].APIAddr, id2) &&
			!peerConnected(t, cluster.Nodes[1].APIAddr, id2)
	})
	for _, i := range []int{0, 1} {
		if got := metricValue(t, cluster.Nodes[i].APIAddr, "spedsql_peer_count"); got != 1 {
			t.Fatalf("node %d peers = %v, want 1 after exclusion", i, got)
		}
	}
	// The retired peer releases its GC retention pin.
	if got := metricValue(t, cluster.Nodes[0].APIAddr, "spedsql_gating_members"); got >= gatingBefore {
		t.Fatalf("gating members = %v, want below baseline %v", got, gatingBefore)
	}
	// Node3's writes no longer arrive on survivors.
	if err := cluster.ExecSQL(2, "INSERT INTO ch_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 3999), "isolated"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	for _, i := range []int{0, 1} {
		if n, _ := cluster.QueryRowCount(i, "ch_rows"); n != 5 {
			t.Fatalf("node %d count = %d after isolation, want 5", i, n)
		}
	}

	// Exclusion survives a survivor restart (no rediscovery re-mesh).
	cluster.StopNode(0)
	cluster.StartNode(0)
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
	cluster.WaitNodeReady(0)
	time.Sleep(5 * time.Second)
	if peerConnected(t, cluster.Nodes[0].APIAddr, id2) {
		t.Fatal("node1 re-meshed excluded node3 after restart")
	}
	if got := metricValue(t, cluster.Nodes[0].APIAddr, "spedsql_peer_count"); got != 1 {
		t.Fatalf("node1 peers after restart = %v, want 1 (exclusion lost?)", got)
	}

	// Re-adding heals replication (removal was not destructive).
	if err := cluster.AddPeer(0, 2); err != nil {
		t.Fatal(err)
	}
	if err := cluster.AddPeer(1, 2); err != nil {
		t.Fatal(err)
	}
	waitConverged(t, cluster, 6, 60*time.Second)
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "ch_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "ch_rows", "name")
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

func waitCondition(t *testing.T, timeout time.Duration, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", timeout, what)
}

func metricValue(t *testing.T, apiAddr, name string) float64 {
	t.Helper()
	for _, line := range strings.Split(scrape(t, apiAddr), "\n") {
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

func peerConnected(t *testing.T, apiAddr, peerID string) bool {
	t.Helper()
	want := fmt.Sprintf("spedsql_peer_connected{peer=%q} 1", peerID)
	for _, line := range strings.Split(scrape(t, apiAddr), "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

func scrape(t *testing.T, apiAddr string) string {
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
	return string(raw)
}
