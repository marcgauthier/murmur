package resourceexhaustion_test

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// Huge values at production default budgets (16 MiB values, 64 MiB
// transactions): an 8 MiB value must replicate byte-exact, a 20 MiB value
// must be rejected as too large with no partial row and a healthy node,
// and a sustained large-value load must converge exactly.
func TestHugeTransactionsAtDefaultBudgets(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "resource-huge",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema:      rxSchema(),
	})

	// (a) 8 MiB value: accepted, byte-exact on both nodes.
	huge := strings.Repeat("H", 8<<20)
	wantDigest := sha256.Sum256([]byte(huge))
	hugeID := fmt.Sprintf("%032x", 1)
	if err := cluster.ExecSQL(0, "INSERT INTO rx_rows (id, name) VALUES (?, ?)", hugeID, huge); err != nil {
		t.Fatalf("8 MiB insert rejected, want accept: %v", err)
	}
	for _, i := range []int{0, 1} {
		got := readValue(t, cluster, i, hugeID, 2*time.Minute)
		if len(got) != len(huge) || sha256.Sum256([]byte(got)) != wantDigest {
			t.Fatalf("node %d value mismatch: len=%d digest=%x", i+1, len(got), sha256.Sum256([]byte(got)))
		}
	}
	t.Logf("8 MiB value byte-exact on both nodes (sha256 %x)", wantDigest)

	// (b) 20 MiB value: clean rejection, no partial row, node healthy.
	over := strings.Repeat("O", 20<<20)
	overID := fmt.Sprintf("%032x", 2)
	if err := cluster.ExecSQL(0, "INSERT INTO rx_rows (id, name) VALUES (?, ?)", overID, over); err == nil {
		t.Fatal("20 MiB insert accepted, want budget rejection")
	} else if !strings.Contains(strings.ToLower(err.Error()), "too large") {
		t.Fatalf("rejection error %q does not name the budget", err)
	}
	for _, i := range []int{0, 1} {
		if n, _ := cluster.QueryRowCount(i, tableName); n != 1 {
			t.Fatalf("node %d has %d rows after rejection, want 1 (partial apply?)", i+1, n)
		}
	}
	cluster.WaitNodeReady(0)
	if err := cluster.ExecSQL(0, "INSERT INTO rx_rows (id, name) VALUES (?, ?)",
		fmt.Sprintf("%032x", 3), "probe"); err != nil {
		t.Fatalf("probe write after rejection: %v", err)
	}
	t.Logf("20 MiB value rejected as too large, no partial row, writer healthy")

	// (c) Sustained large-value load: 20 x 512 KiB converge exactly.
	for i := 0; i < 20; i++ {
		v := strings.Repeat(fmt.Sprintf("%d", i%10), 512<<10)
		if err := cluster.ExecSQL(i%2, "INSERT INTO rx_rows (id, name) VALUES (?, ?)",
			fmt.Sprintf("%032x", 100+i), v); err != nil {
			t.Fatalf("load insert %d: %v", i, err)
		}
	}
	waitCounts(t, cluster, []int{0, 1}, 22, 3*time.Minute)
	d := waitDigests(t, cluster, []int{0, 1}, 3*time.Minute)
	t.Logf("PASS: 22 rows (8MiB + probe + 20x512KiB) converged, digest %s", d)
}

// readValue polls until id is readable on node idx and returns its value.
func readValue(t *testing.T, cluster *harness.Cluster, idx int, id string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res, err := cluster.QuerySQL(idx, "SELECT name FROM rx_rows WHERE id = ?", id)
		if err == nil && len(res.Rows) == 1 && len(res.Rows[0]) > 0 {
			if s, ok := res.Rows[0][0].(string); ok {
				return s
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("value %s never readable on node %d", id, idx+1)
	return ""
}
