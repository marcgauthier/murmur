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
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestPlumtreeMeshConverges(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "plumtree-live",
		NumNodes:     3,
		AwaitUnlock:  true,
		Replication:  &harness.ReplicationOptions{Dissemination: "plumtree"},
		TypedRecords: true,
	})
	for i := 0; i < 10; i++ {
		if err := cluster.TypedInsert(0, fmt.Sprintf("plum-%d", i)); err != nil {
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
		TypedRecords: true,
	})
	for i := 0; i < 10; i++ {
		if err := cluster.TypedInsert(0, fmt.Sprintf("mixed-plum-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.TypedInsert(2, "gossip-only"); err != nil {
		t.Fatal(err)
	}
	// The Plumtree pair converges; the gossip node stays isolated.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		names0, _ := cluster.TypedNames(0)
		names1, _ := cluster.TypedNames(1)
		n0, n1 := len(names0), len(names1)
		if n0 == 10 && n1 == 10 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(5 * time.Second) // let any leak arrive
	for i, want := range []int{10, 10, 1} {
		names, _ := cluster.TypedNames(i)
		if n := len(names); n != want {
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
		var first []string
		for i := range c.Nodes {
			names, err := c.TypedNames(i)
			if err != nil || len(names) != want {
				ok = false
				break
			}
			if i == 0 {
				first = names
			} else if !sameNames(names, first) {
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

func sameNames(a, b []string) bool {
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
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := harness.MetricValueFrom(string(raw), name); ok {
		return value
	}
	t.Fatalf("metric %s not found", name)
	return 0
}
