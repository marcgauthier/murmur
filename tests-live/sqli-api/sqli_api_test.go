package sqliapi_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// TestSQLApplicationRoutesRemoved pins the Phase 6 cutover at the running
// node boundary. Former SQL routes reject requests before parsing or applying
// their contents; the managed typed API remains writable and replicates.
func TestSQLApplicationRoutesRemoved(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "sqli-api-removed", NumNodes: 2, AwaitUnlock: true, TypedRecords: true,
	})
	if err := cluster.TypedInsert(0, "before-sql-probe"); err != nil {
		t.Fatal(err)
	}
	waitTypedCount(t, cluster, "before-sql-probe", 1)

	payloads := []string{
		`{"query":"SELECT * FROM records"}`,
		`{"query":"INSERT INTO records VALUES (1); DROP TABLE records"}`,
		`{"query":"' UNION SELECT secret FROM secrets --"}`,
	}
	for _, endpoint := range []string{"/v1/query", "/v1/exec"} {
		for _, payload := range payloads {
			body, status := postSQLRoute(t, "https://"+cluster.Nodes[0].APIAddr+endpoint, payload)
			if status != http.StatusGone {
				t.Fatalf("POST %s returned %d, want 410; body=%q", endpoint, status, body)
			}
			if !strings.Contains(strings.ToLower(body), "typed rime") {
				t.Fatalf("POST %s migration response lacks typed RIME guidance: %q", endpoint, body)
			}
		}
	}

	if err := cluster.TypedInsert(0, "after-sql-probe"); err != nil {
		t.Fatalf("typed write after removed SQL routes: %v", err)
	}
	waitTypedCount(t, cluster, "after-sql-probe", 1)
	for i := range cluster.Nodes {
		for _, name := range []string{"before-sql-probe", "after-sql-probe"} {
			count, err := cluster.TypedCount(i, name)
			if err != nil || count != 1 {
				t.Fatalf("node %d typed count for %q = %d, %v; want 1", i, name, count, err)
			}
		}
	}
}

func postSQLRoute(t *testing.T, endpoint, payload string) (string, int) {
	t.Helper()
	resp, err := http.Post(endpoint, "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST %s: %v", endpoint, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		t.Fatalf("read POST %s: %v", endpoint, err)
	}
	return string(body), resp.StatusCode
}

func waitTypedCount(t *testing.T, cluster *harness.Cluster, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		converged := true
		for i := range cluster.Nodes {
			count, err := cluster.TypedCount(i, name)
			if err != nil || count != want {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	counts := make([]string, len(cluster.Nodes))
	for i := range cluster.Nodes {
		count, err := cluster.TypedCount(i, name)
		counts[i] = fmt.Sprintf("%d:%d (%v)", i, count, err)
	}
	t.Fatalf("typed record %q did not converge: %s", name, strings.Join(counts, ", "))
}
