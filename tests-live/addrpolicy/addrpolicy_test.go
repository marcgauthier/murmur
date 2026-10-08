package addrpolicy_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// TestAddressPolicyAcrossProcesses exercises address admission through the
// shipped daemon configuration, QUIC listeners, and typed RIME service. Each node
// owns a separate process and node directory.
func TestAddressPolicyAcrossProcesses(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "addrpolicy",
		NumNodes:     3,
		AwaitUnlock:  true,
		TypedRecords: true,
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

	if err := cluster.TypedInsert(0, "allowed-loopback"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.TypedInsert(1, "denied-testnet"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.TypedInsert(2, "unrestricted-loopback"); err != nil {
		t.Fatal(err)
	}

	waitForCount(t, cluster, 0, 2, 20*time.Second)
	waitForCount(t, cluster, 2, 2, 20*time.Second)
	time.Sleep(750 * time.Millisecond) // several dial/accept retries
	if got, err := cluster.TypedNames(1); err != nil || len(got) != 1 || got[0] != "denied-testnet" {
		t.Fatalf("denied node state changed across network: names=%v err=%v; want only its local row", got, err)
	}
	if got, err := cluster.TypedNames(0); err != nil || len(got) != 2 {
		t.Fatalf("admitted node did not receive both its local and node3-origin row: names=%v err=%v", got, err)
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
		if got, err := cluster.TypedNames(node); err == nil && len(got) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.TypedNames(node)
	t.Fatalf("%s record names=%v err=%v, want %d", cluster.Nodes[node].Label, got, err, want)
}
