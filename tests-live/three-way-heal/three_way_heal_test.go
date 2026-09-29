// Three-way-heal acceptance.
//
// Four fully meshed daemons split into isolated pairs, diverge with
// deterministic writes, then heal link-by-link in a caller-supplied
// order. The suite runs two different heal orders and requires the
// final digest to be identical across orders: heal order must not
// affect the converged state.
package threewayheal_test

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

const tableName = "twh_rows"

// crossLinks are the four node index pairs spanning the (0,1)|(2,3)
// split; a heal order is a permutation of these link indexes.
var crossLinks = [][2]int{{0, 2}, {0, 3}, {1, 2}, {1, 3}}

func TestHealOrderDoesNotAffectFinalDigest(t *testing.T) {
	orderA := parseOrder(t, "SPEDSQL_THREE_WAY_HEAL_ORDER", "0,1,2,3")
	orderB := parseOrder(t, "SPEDSQL_THREE_WAY_HEAL_ORDER_B", "3,2,1,0")

	digestA := runHealOrder(t, orderA, "a")
	digestB := runHealOrder(t, orderB, "b")
	t.Logf("final digest order %v: %s", orderA, digestA)
	t.Logf("final digest order %v: %s", orderB, digestB)
	if digestA != digestB {
		t.Fatalf("heal-order divergence: order %v -> %s, order %v -> %s", orderA, digestA, orderB, digestB)
	}
}

func runHealOrder(t *testing.T, order []int, tag string) string {
	t.Helper()
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "three-way-heal-" + tag,
		NumNodes:    4,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: tableName,
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	// Baseline converges before the split (honest-path control).
	// IDs/names are fixed so both heal orders must reach the same digest.
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%032x", 1000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO "+tableName+" (id, name) VALUES (?, ?)", id, fmt.Sprintf("base-%d", i)); err != nil {
			t.Fatalf("order %v baseline write: %v", order, err)
		}
	}
	waitConverged(t, cluster, 10, 150*time.Second)

	// Split into isolated pairs (0,1) and (2,3).
	for _, link := range crossLinks {
		if err := cluster.RemovePeer(link[0], link[1]); err != nil {
			t.Fatalf("order %v split %v: %v", order, link, err)
		}
		if err := cluster.RemovePeer(link[1], link[0]); err != nil {
			t.Fatalf("order %v split %v: %v", order, link, err)
		}
	}

	// Deterministic divergent writes per side.
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%032x", 2000+i)
		if err := cluster.ExecSQL(1, "INSERT INTO "+tableName+" (id, name) VALUES (?, ?)", id, fmt.Sprintf("sideA-%d", i)); err != nil {
			t.Fatalf("order %v sideA write: %v", order, err)
		}
		id = fmt.Sprintf("%032x", 3000+i)
		if err := cluster.ExecSQL(3, "INSERT INTO "+tableName+" (id, name) VALUES (?, ?)", id, fmt.Sprintf("sideB-%d", i)); err != nil {
			t.Fatalf("order %v sideB write: %v", order, err)
		}
	}

	// Each pair converges internally; equal counts but divergent digests
	// prove the split held (otherwise this test would be vacuous).
	waitConvergedOn(t, cluster, []int{0, 1}, 20, 150*time.Second)
	waitConvergedOn(t, cluster, []int{2, 3}, 20, 150*time.Second)
	dA, err := cluster.ComputeTableDigest(0, tableName, "name")
	if err != nil {
		t.Fatal(err)
	}
	dB, err := cluster.ComputeTableDigest(2, tableName, "name")
	if err != nil {
		t.Fatal(err)
	}
	if dA == dB {
		t.Fatalf("order %v: sides unexpectedly agree during split (%s)", order, dA)
	}

	// Heal link-by-link in the prescribed order, settling briefly
	// between links so the order is real, not nominal.
	for step, linkIdx := range order {
		link := crossLinks[linkIdx]
		if err := cluster.AddPeer(link[0], link[1]); err != nil {
			t.Fatalf("order %v heal %v: %v", order, link, err)
		}
		if err := cluster.AddPeer(link[1], link[0]); err != nil {
			t.Fatalf("order %v heal %v: %v", order, link, err)
		}
		t.Logf("order %v: healed link %d/%d (%d<->%d)", order, step+1, len(order), link[0], link[1])
		time.Sleep(time.Second)
	}

	waitConverged(t, cluster, 30, 150*time.Second)
	final, err := cluster.ComputeTableDigest(0, tableName, "name")
	if err != nil {
		t.Fatal(err)
	}
	return final
}

// parseOrder reads a comma-separated permutation of link indexes;
// invalid values fail fast rather than silently healing another order.
func parseOrder(t *testing.T, env, fallback string) []int {
	t.Helper()
	raw := os.Getenv(env)
	if raw == "" {
		raw = fallback
	}
	parts := strings.Split(raw, ",")
	if len(parts) != len(crossLinks) {
		t.Fatalf("%s=%q: want %d comma-separated link indexes", env, raw, len(crossLinks))
	}
	seen := make(map[int]bool)
	order := make([]int, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || v < 0 || v >= len(crossLinks) || seen[v] {
			t.Fatalf("%s=%q: want a permutation of 0..%d", env, raw, len(crossLinks)-1)
		}
		seen[v] = true
		order = append(order, v)
	}
	return order
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	waitConvergedOn(t, c, []int{0, 1, 2, 3}, want, timeout)
}

func waitConvergedOn(t *testing.T, c *harness.Cluster, idxs []int, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for k, idx := range idxs {
			n, err := c.QueryRowCount(idx, tableName)
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(idx, tableName, "name")
			if err != nil {
				ok = false
				break
			}
			if k == 0 {
				first = d
			} else if d != first {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			counts := make([]int, len(c.Nodes))
			for i := range c.Nodes {
				n, _ := c.QueryRowCount(i, tableName)
				counts[i] = n
			}
			t.Logf("converge progress: counts=%v want=%d", counts, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, idx := range idxs {
		n, _ := c.QueryRowCount(idx, tableName)
		d, _ := c.ComputeTableDigest(idx, tableName, "name")
		t.Logf("node %d at timeout: count=%d digest=%s", idx, n, d)
	}
	t.Fatalf("nodes %v did not converge on %d rows with equal digests within %v", idxs, want, timeout)
}
