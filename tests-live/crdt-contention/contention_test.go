package crdtcontention_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	db "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/schema"
	"github.com/nomadsql/replicateddb/tests-live/harness"
)

func TestMultiProcessDisjointAndSharedCellContention(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "crdt-contention",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "contacts", Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "phone", Type: schema.ColText, Nullable: true},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
		}}}},
	})
	for _, node := range cluster.Nodes {
		t.Logf("%s pid=%d dir=%s repl=%s", node.Label, node.Process.Process.Pid, node.Dir, node.ReplAddr)
	}
	waitForPeers(t, cluster, 2, 20*time.Second)

	rowID := fmt.Sprintf("%032x", 31_415_926)
	if err := cluster.ExecSQL(0,
		"INSERT INTO contacts (id, name, phone, score) VALUES (?, ?, ?, ?)",
		rowID, "baseline", "baseline", 0); err != nil {
		t.Fatal(err)
	}
	waitForState(t, cluster, rowID, "baseline", "baseline", "0", 20*time.Second)

	// Concurrent edits to separate cells must all survive regardless of arrival
	// order at other processes.
	disjoint := []struct {
		node  int
		query string
		value any
	}{
		{0, "UPDATE contacts SET name = ? WHERE id = ?", "node1-name"},
		{1, "UPDATE contacts SET phone = ? WHERE id = ?", "node2-phone"},
		{2, "UPDATE contacts SET score = ? WHERE id = ?", 42},
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(disjoint))
	for _, edit := range disjoint {
		edit := edit
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- cluster.ExecSQL(edit.node, edit.query, edit.value, rowID)
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
				errCh <- cluster.ExecSQL(node, "UPDATE contacts SET name = ? WHERE id = ?", value, rowID)
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
	if got, err := cluster.ComputeTableDigest(0, "contacts", "id"); err != nil {
		t.Fatal(err)
	} else {
		for i := 1; i < len(cluster.Nodes); i++ {
			other, err := cluster.ComputeTableDigest(i, "contacts", "id")
			if err != nil || other != got {
				t.Fatalf("logical state digest differs: node1=%s node%d=%s err=%v", got, i+1, other, err)
			}
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
			res, err := cluster.QuerySQL(i, "SELECT name, phone, score FROM contacts WHERE id = ?", id)
			last[i] = fmt.Sprintf("err=%v rows=%v", err, res)
			if err != nil || len(res.Rows) != 1 || len(res.Rows[0]) < 3 ||
				fmt.Sprint(res.Rows[0][0]) != wantName || fmt.Sprint(res.Rows[0][1]) != wantPhone || fmt.Sprint(res.Rows[0][2]) != wantScore {
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
			res, err := cluster.QuerySQL(i, "SELECT name, phone, score FROM contacts WHERE id = ?", id)
			if err != nil || len(res.Rows) != 1 || len(res.Rows[0]) < 3 {
				last[i] = fmt.Sprintf("err=%v rows=%v", err, res)
				converged = false
				continue
			}
			name, phone, score := fmt.Sprint(res.Rows[0][0]), fmt.Sprint(res.Rows[0][1]), fmt.Sprint(res.Rows[0][2])
			last[i] = fmt.Sprintf("name=%q phone=%q score=%q", name, phone, score)
			if phone != wantPhone || score != wantScore {
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
