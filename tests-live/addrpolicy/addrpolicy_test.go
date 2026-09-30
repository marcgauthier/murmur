package addrpolicy_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// TestAddressPolicyAcrossProcesses exercises address admission through the
// shipped daemon configuration, QUIC listeners, and SQL service. Each node
// owns a separate process and node directory.
func TestAddressPolicyAcrossProcesses(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "addrpolicy",
		NumNodes:    3,
		AwaitUnlock: true,
		SchemaSQL: `CREATE TABLE IF NOT EXISTS items (
  id BLOB PRIMARY KEY NOT NULL,
  name TEXT NOT NULL DEFAULT ''
);`,
		// node1 admits loopback peers; node2's TEST-NET policy excludes every
		// actual loopback socket address. node3 is unrestricted and can provide
		// the allowed path between node1 and itself.
		AllowedNetworksByNode: map[int][]string{
			0: {"127.0.0.0/8"},
			1: {"198.51.100.0/24"},
		},
	})

	for _, node := range cluster.Nodes {
		t.Logf("%s pid=%d dir=%s repl=%s api=%s", node.Label,
			node.Process.Process.Pid, node.Dir, node.ReplAddr, node.APIAddr)
	}

	// Let QUIC dial retries settle, then confirm the allowed pair formed a
	// session while the denied node has no admitted path to either peer.
	waitForPeers(t, cluster, []int{1, 0, 1}, 20*time.Second)

	if err := cluster.ExecSQL(0, "INSERT INTO items (id, name) VALUES (?, ?)",
		fmt.Sprintf("%032x", 71001), "allowed-loopback"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecSQL(1, "INSERT INTO items (id, name) VALUES (?, ?)",
		fmt.Sprintf("%032x", 71002), "denied-testnet"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecSQL(2, "INSERT INTO items (id, name) VALUES (?, ?)",
		fmt.Sprintf("%032x", 71003), "unrestricted-loopback"); err != nil {
		t.Fatal(err)
	}

	waitForCount(t, cluster, 0, 2, 20*time.Second)
	waitForCount(t, cluster, 2, 2, 20*time.Second)
	time.Sleep(750 * time.Millisecond) // several dial/accept retries
	if got, err := cluster.QueryRowCount(1, "items"); err != nil || got != 1 {
		t.Fatalf("denied node state changed across network: count=%d err=%v; want only its local row", got, err)
	}
	if got, err := cluster.QueryRowCount(0, "items"); err != nil || got != 2 {
		t.Fatalf("admitted node did not receive both its local and node3-origin row: count=%d err=%v", got, err)
	}

	for i := range cluster.Nodes {
		if cluster.Nodes[i].Dir == "" {
			t.Fatalf("node%d has no isolated directory", i+1)
		}
	}
}

func waitForPeers(t *testing.T, cluster *harness.Cluster, want []int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for i, n := range cluster.Nodes {
			if got := connectedPeers(n.APIAddr); got != want[i] {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for i, n := range cluster.Nodes {
		t.Fatalf("%s connected_peers=%d, want %d", n.Label, connectedPeers(n.APIAddr), want[i])
	}
}

func connectedPeers(apiAddr string) int {
	resp, err := http.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var status struct {
		ConnectedPeers int `json:"connected_peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return -1
	}
	return status.ConnectedPeers
}

func waitForCount(t *testing.T, cluster *harness.Cluster, node, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, err := cluster.QueryRowCount(node, "items"); err == nil && got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.QueryRowCount(node, "items")
	t.Fatalf("%s row count=%d err=%v, want %d", cluster.Nodes[node].Label, got, err, want)
}
