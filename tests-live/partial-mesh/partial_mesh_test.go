// Partial-mesh forwarding acceptance.
//
// The first test pins chain-only forwarding without a direct node1-node3
// session. The second uses bootstrap seeds to verify dynamic discovery and
// convergence. Both exercise the managed typed RIME record API.
package partialmesh_test

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestChainPeersConvergeViaForwarding(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "partial-mesh",
		NumNodes:     3,
		AwaitUnlock:  true,
		ManualPeers:  true,
		TypedRecords: true,
	})

	// Chain links only; nothing statically connects node1 to node3.
	link := func(a, b int) {
		t.Helper()
		if err := cluster.AddPeer(a, b); err != nil {
			t.Fatal(err)
		}
		if err := cluster.AddPeer(b, a); err != nil {
			t.Fatal(err)
		}
	}
	link(0, 1)
	link(1, 2)

	for i := 0; i < 10; i++ {
		if err := cluster.TypedInsert(0, fmt.Sprintf("d-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	waitConverged(t, cluster, 10, 60*time.Second)

	// No direct node1-node3 session exists: convergence arrived via
	// forwarding, and the peer counts are exactly the static links.
	id0, id2 := cluster.Nodes[0].NodeID.String(), cluster.Nodes[2].NodeID.String()
	if peerConnected(t, cluster.Nodes[0].APIAddr, id2) {
		t.Fatal("unexpected direct node1-node3 session (topology changed?)")
	}
	if peerConnected(t, cluster.Nodes[2].APIAddr, id0) {
		t.Fatal("unexpected direct node3-node1 session (topology changed?)")
	}
	for i, want := range []float64{1, 2, 1} {
		if got := metricValue(t, cluster.Nodes[i].APIAddr, "spedsql_peer_count"); got != want {
			t.Fatalf("node %d peers = %v, want %v", i, got, want)
		}
	}
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first []string
		for i := range c.Nodes {
			names, err := c.TypedNames(i)
			if err != nil || len(names) != want {
				ok = false
				break
			}
			if i == 0 {
				first = names
			} else if !equalNames(names, first) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.DumpForensics("converge-timeout")
	t.Fatalf("nodes did not converge on %d typed records within %v", want, timeout)
}

func equalNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func metricValue(t *testing.T, apiAddr, name string) float64 {
	t.Helper()
	if value, ok := harness.MetricValueFrom(scrape(t, apiAddr), name); ok {
		return value
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func peerConnected(t *testing.T, apiAddr, peerID string) bool {
	t.Helper()
	value, ok := harness.MetricValueWithLabels(scrape(t, apiAddr), "spedsql_peer_connected", map[string]string{"peer": peerID})
	return ok && value == 1
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

func TestDynamicBootstrapDiscovery(t *testing.T) {
	// Node 1 acts as bootstrap seed. Node 2 and 3 point to Node 1's repl address.
	// No static peers are configured; dynamic discovery must form cluster sessions.
	baseCluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:           "swim-discovery",
		NumNodes:       3,
		AwaitUnlock:    true,
		ManualPeers:    true,
		BootstrapSeeds: []int{0},
		TypedRecords:   true,
	})

	// Wait for SWIM dynamic discovery to find all cluster members without any explicit AddPeer calls
	waitForMembership(t, baseCluster, 3, 30*time.Second)

	// Write 10 rows on Node 0 (seed)
	for i := 0; i < 10; i++ {
		if err := baseCluster.TypedInsert(0, fmt.Sprintf("dyn-seed-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}

	// Write 5 rows on Node 1 (discovered peer)
	for i := 0; i < 5; i++ {
		if err := baseCluster.TypedInsert(1, fmt.Sprintf("dyn-node1-%d", i)); err != nil {
			t.Fatalf("node 1 write: %v", err)
		}
	}

	// Write 5 rows on Node 2 (discovered peer)
	for i := 0; i < 5; i++ {
		if err := baseCluster.TypedInsert(2, fmt.Sprintf("dyn-node2-%d", i)); err != nil {
			t.Fatalf("node 2 write: %v", err)
		}
	}

	// All 20 rows must converge across all 3 nodes. 150s, not 60s:
	// under a full parallel `go test ./...` the box runs ~10x slow
	// and exact convergence legitimately takes over a minute; the
	// proof (exact 20-row convergence) is unchanged.
	waitConverged(t, baseCluster, 20, 150*time.Second)
}

func waitForMembership(t *testing.T, c *harness.Cluster, wantMembers int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allDiscovered := true
		for _, node := range c.Nodes {
			count := metricValue(t, node.APIAddr, "spedsql_membership_count")
			if int(count) < wantMembers {
				allDiscovered = false
				break
			}
		}
		if allDiscovered {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	c.DumpForensics("membership-timeout")
	t.Fatalf("nodes did not discover %d members via SWIM within %v", wantMembers, timeout)
}
