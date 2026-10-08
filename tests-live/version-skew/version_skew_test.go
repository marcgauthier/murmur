package versionskew

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// A node advertising an unknown protocol version must be refused
// fail-closed: it exchanges no data in either direction, stays alive, and
// the compatible mesh keeps converging without it.
func TestUnknownProtocolVersionRefused(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "version-skew",
		NumNodes:     3,
		AwaitUnlock:  true,
		TypedRecords: true,
		ReplicationByNode: map[int]*harness.ReplicationOptions{
			2: {ProtocolVersionOverride: 99, MinProtocolVersionOverride: 99},
		},
	})

	for i := 0; i < 10; i++ {
		if err := cluster.TypedInsert(0, fmt.Sprintf("w-%d", i)); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	waitPairCount(t, cluster, 10, 30*time.Second)

	// The skewed node must never connect, in either direction, over a
	// window long enough for several dial/handshake rounds.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := connectedPeers(t, cluster.Nodes[0].APIAddr); got != 1 {
			t.Fatalf("node1 connected_peers=%d, want 1 (node2 only)", got)
		}
		if got := connectedPeers(t, cluster.Nodes[1].APIAddr); got != 1 {
			t.Fatalf("node2 connected_peers=%d, want 1 (node1 only)", got)
		}
		if got := connectedPeers(t, cluster.Nodes[2].APIAddr); got != 0 {
			t.Fatalf("skewed node3 connected_peers=%d, want 0", got)
		}
		if names, err := cluster.TypedNames(2); err != nil || len(names) != 0 {
			t.Fatalf("skewed node3 typed names=%v err=%v, want no inbound data", names, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// The skewed node stays alive and keeps serving local reads/writes;
	// its data just never leaves the node.
	for i := 0; i < 3; i++ {
		if err := cluster.TypedInsert(2, fmt.Sprintf("skewed-local-%d", i)); err != nil {
			t.Fatalf("skewed node local write failed (must stay writable): %v", err)
		}
	}
	time.Sleep(5 * time.Second)
	if names, err := cluster.TypedNames(0); err != nil || len(names) != 10 {
		t.Fatalf("node1 typed names=%d err=%v, want 10 (no outbound data from skewed node)", len(names), err)
	}
	if names, err := cluster.TypedNames(2); err != nil || len(names) != 3 {
		t.Fatalf("skewed node3 typed names=%d err=%v, want its 3 local rows", len(names), err)
	}
}

// waitPairCount waits until nodes 0 and 1 (the compatible pair) agree.
func waitPairCount(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		names0, e0 := c.TypedNames(0)
		names1, e1 := c.TypedNames(1)
		if e0 == nil && e1 == nil && len(names0) == want && len(names1) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("compatible pair counts=%d,%d, want %d (errors %v, %v)", len(names0), len(names1), want, e0, e1)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

var statusClient = &http.Client{Timeout: 5 * time.Second}

func connectedPeers(t *testing.T, apiAddr string) int {
	t.Helper()
	resp, err := statusClient.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		t.Fatalf("status %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	var st struct {
		ConnectedPeers int `json:"connected_peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode status %s: %v", apiAddr, err)
	}
	return st.ConnectedPeers
}
