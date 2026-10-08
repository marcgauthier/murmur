package crdtcontention_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestMultiProcessDisjointAndSharedCellContention(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "crdt-contention",
		NumNodes:        3,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
	})
	for _, node := range cluster.Nodes {
		t.Logf("%s pid=%d dir=%s repl=%s", node.Label, node.Process.Process.Pid, node.Dir, node.ReplAddr)
	}
	waitForPeers(t, cluster, 2, 20*time.Second)

	rowID := "03141592-6000-4000-8000-000000000000"
	if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{
		ID: rowID, Name: "baseline", Phone: "baseline", Score: 0,
	}); err != nil {
		t.Fatal(err)
	}
	waitForState(t, cluster, rowID, "baseline", "baseline", "0", 20*time.Second)

	// Concurrent edits to separate cells must all survive regardless of arrival
	// order at other processes.
	disjoint := []struct {
		node  int
		field string
		value any
	}{
		{0, "name", "node1-name"},
		{1, "phone", "node2-phone"},
		{2, "score", 42},
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(disjoint))
	for _, edit := range disjoint {
		edit := edit
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- cluster.TypedContentionUpdate(edit.node, rowID, edit.field, fmt.Sprint(edit.value))
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	waitForState(t, cluster, rowID, "node1-name", "node2-phone", "42", 20*time.Second)

	// All three processes now contend on the same name cell. The winning value
	// is intentionally not preselected; the LWW version must resolve to the
	// same application-visible result on every process.
	const updatesPerNode = 40
	errCh = make(chan error, len(cluster.Nodes)*updatesPerNode)
	wg = sync.WaitGroup{}
	for node := range cluster.Nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; seq < updatesPerNode; seq++ {
				value := fmt.Sprintf("node%d-cell-%03d", node+1, seq)
				errCh <- cluster.TypedContentionUpdate(node, rowID, "name", value)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	winner := waitForCellConvergence(t, cluster, rowID, "node2-phone", "42", 30*time.Second)
	if len(winner) == 0 {
		t.Fatal("same-cell contention resolved to an empty name")
	}
	if !strings.HasPrefix(winner, "node1-cell-") && !strings.HasPrefix(winner, "node2-cell-") && !strings.HasPrefix(winner, "node3-cell-") {
		t.Fatalf("same-cell winner %q did not come from the concurrent write set", winner)
	}
	first, err := cluster.TypedContentionRead(0, rowID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(cluster.Nodes); i++ {
		other, err := cluster.TypedContentionRead(i, rowID)
		if err != nil || other != first {
			t.Fatalf("logical typed row differs: node1=%+v node%d=%+v err=%v", first, i+1, other, err)
		}
	}
	t.Logf("three process cell conflict converged to %q after %d updates per node", winner, updatesPerNode)
}

func waitForPeers(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, node := range cluster.Nodes {
			if connectedPeers(node.APIAddr) != want {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, node := range cluster.Nodes {
		t.Fatalf("%s connected peers=%d, want %d", node.Label, connectedPeers(node.APIAddr), want)
	}
}

func connectedPeers(apiAddr string) int {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var status struct {
		ConnectedPeers int `json:"connected_peers"`
	}
	if json.NewDecoder(resp.Body).Decode(&status) != nil {
		return -1
	}
	return status.ConnectedPeers
}

func waitForState(t *testing.T, cluster *harness.Cluster, id, wantName, wantPhone, wantScore string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := make([]string, len(cluster.Nodes))
	for time.Now().Before(deadline) {
		ok := true
		for i := range cluster.Nodes {
			row, err := cluster.TypedContentionRead(i, id)
			last[i] = fmt.Sprintf("row=%+v err=%v", row, err)
			if err != nil || row.Name != wantName || row.Phone != wantPhone || fmt.Sprint(row.Score) != wantScore {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q/%q/%q on every process; last=%v", wantName, wantPhone, wantScore, last)
}

func waitForCellConvergence(t *testing.T, cluster *harness.Cluster, id, wantPhone, wantScore string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := make([]string, len(cluster.Nodes))
	for time.Now().Before(deadline) {
		var winner string
		converged := true
		for i := range cluster.Nodes {
			row, err := cluster.TypedContentionRead(i, id)
			if err != nil {
				last[i] = fmt.Sprintf("err=%v row=%+v", err, row)
				converged = false
				continue
			}
			name := row.Name
			last[i] = fmt.Sprintf("name=%q phone=%q score=%d", row.Name, row.Phone, row.Score)
			if row.Phone != wantPhone || fmt.Sprint(row.Score) != wantScore {
				converged = false
			}
			if i == 0 {
				winner = name
			} else if name != winner {
				converged = false
			}
		}
		if converged {
			return winner
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("same-cell contention did not converge across %d processes; last=%v", len(cluster.Nodes), last)
	return ""
}
