package scalemesh

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// Ten-node mesh with continuous writes: every node must converge, and the
// bounded-fanout scheduler must hold (default fanout 4) on every node for
// the whole run. Smaller suites never exceed the fanout, so they cannot
// prove selection/rotation at scale.
func TestTenNodeMeshConvergesBounded(t *testing.T) {
	const nodes = 10
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "scale-mesh",
		NumNodes:        nodes,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
	})

	// Bound sampling runs for the whole test. The sampler reports
	// breaches over a channel; only the test goroutine may FailNow.
	breaches := make(chan string, 64)
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				sampleFanoutBound(cluster, 4, breaches)
			}
		}
	}()

	for i := 0; i < 300; i++ {
		id := fmt.Sprintf("%032x", i)
		if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: id, Name: fmt.Sprintf("w-%d", i)}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// A second origin proves multi-hop forwarding, not just hub fan-out.
	for i := 300; i < 400; i++ {
		id := fmt.Sprintf("%032x", i)
		if err := cluster.TypedContentionInsert(7, harness.TypedContentionRow{ID: id, Name: fmt.Sprintf("w-%d", i)}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// 300s, not 120s: ten daemons converging 400 multi-hop rows need
	// headroom on a saturated box (a full `go test -race ./...`
	// exceeds two minutes on writes+convergence alone).
	waitAllCounts(t, cluster, 400, 300*time.Second)
	want, err := meshDigest(cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	for idx := 1; idx < nodes; idx++ {
		d, err := meshDigest(cluster, idx)
		if err != nil {
			t.Fatal(err)
		}
		if d != want {
			t.Fatalf("node%d digest differs after 10-node convergence", idx+1)
		}
	}
	sampleFanoutBound(cluster, 4, breaches)
	close(stop)
	for {
		select {
		case b := <-breaches:
			t.Errorf("fanout breach: %s", b)
		default:
			goto drained
		}
	}
drained:
}

func waitAllCounts(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		for idx := range c.Nodes {
			rows, err := c.TypedContentionRows(idx)
			if err != nil || len(rows) != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("10-node counts did not reach %d within %v", want, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// meshDigest returns the PK-ordered digest of the contention table on one node.
func meshDigest(c *harness.Cluster, idx int) (string, error) {
	rows, err := c.TypedContentionRows(idx)
	if err != nil {
		return "", err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	h := sha256.New()
	for _, row := range rows {
		fmt.Fprintf(h, "%s:%s:%s:%d\n", row.ID, row.Name, row.Phone, row.Score)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

var meshStatusClient = &http.Client{Timeout: 5 * time.Second}

// sampleFanoutBound reports a breach per node whose selected replication
// targets exceed the fanout. Safe for sampler goroutines: it never calls
// Fatalf.
func sampleFanoutBound(c *harness.Cluster, fanout int, breaches chan<- string) {
	for idx := range c.Nodes {
		got, err := selectedPeers(c.Nodes[idx].APIAddr)
		if err != nil {
			select {
			case breaches <- fmt.Sprintf("node%d status: %v", idx+1, err):
			default:
			}
			continue
		}
		if got > fanout {
			select {
			case breaches <- fmt.Sprintf("node%d selected_peers=%d exceeds fanout %d", idx+1, got, fanout):
			default:
			}
		}
	}
}

func selectedPeers(apiAddr string) (int, error) {
	resp, err := meshStatusClient.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var st struct {
		SelectedPeers int `json:"selected_peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return 0, err
	}
	return st.SelectedPeers, nil
}
