// Three-way-heal acceptance.
//
// Four fully meshed daemons split into isolated pairs, diverge with
// deterministic writes (disjoint inserts plus conflicting updates to
// the same base rows), then heal link-by-link in a caller-supplied
// order. The suite runs two different heal orders and requires the
// final digest to be identical across orders: heal order must not
// affect the converged state, including LWW conflict winners.
package threewayheal_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// crossLinks are the four node index pairs spanning the (0,1)|(2,3)
// split; a heal order is a permutation of these link indexes.
var crossLinks = [][2]int{{0, 2}, {0, 3}, {1, 2}, {1, 3}}

func TestHealOrderDoesNotAffectFinalDigest(t *testing.T) {
	orderA := parseOrder(t, "MURMUR_THREE_WAY_HEAL_ORDER", "0,1,2,3")
	orderB := parseOrder(t, "MURMUR_THREE_WAY_HEAL_ORDER_B", "3,2,1,0")

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
		Name:            "three-way-heal-" + tag,
		NumNodes:        4,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
	})

	// Baseline converges before the split (honest-path control).
	// IDs/names are fixed so both heal orders must reach the same digest.
	for i := 0; i < 10; i++ {
		if err := insertRow(cluster, 0, 1000+i, fmt.Sprintf("base-%d", i)); err != nil {
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

	// Deterministic divergent writes per side: disjoint inserts plus
	// CONFLICTING updates to the same base rows (F2). Union-merge alone
	// cannot prove heal-order independence where it matters; LWW must
	// pick the same winner per cell regardless of arrival order. Side B
	// writes strictly after side A in wall time, so the relative HLC
	// order (and hence the winner) is deterministic across runs.
	for i := 0; i < 10; i++ {
		if err := insertRow(cluster, 1, 2000+i, fmt.Sprintf("sideA-%d", i)); err != nil {
			t.Fatalf("order %v sideA write: %v", order, err)
		}
		if err := insertRow(cluster, 3, 3000+i, fmt.Sprintf("sideB-%d", i)); err != nil {
			t.Fatalf("order %v sideB write: %v", order, err)
		}
	}
	for i := 0; i < 10; i++ {
		if err := cluster.TypedContentionUpdate(1, typedRowID(1000+i), "name", fmt.Sprintf("conflict-A-%d", i)); err != nil {
			t.Fatalf("order %v sideA conflict write: %v", order, err)
		}
	}
	for i := 0; i < 10; i++ {
		if err := cluster.TypedContentionUpdate(3, typedRowID(1000+i), "name", fmt.Sprintf("conflict-B-%d", i)); err != nil {
			t.Fatalf("order %v sideB conflict write: %v", order, err)
		}
	}

	// Each pair converges internally; equal counts but divergent digests
	// prove the split held (otherwise this test would be vacuous).
	waitConvergedOn(t, cluster, []int{0, 1}, 20, 150*time.Second)
	waitConvergedOn(t, cluster, []int{2, 3}, 20, 150*time.Second)
	rowsA, err := cluster.TypedContentionRows(0)
	if err != nil {
		t.Fatal(err)
	}
	rowsB, err := cluster.TypedContentionRows(2)
	if err != nil {
		t.Fatal(err)
	}
	dA, dB := digestRows(rowsA), digestRows(rowsB)
	if dA == dB {
		t.Fatalf("order %v: sides unexpectedly agree during split (%s)", order, dA)
	}

	// Heal link-by-link in the prescribed order. After each heal we
	// wait for real cross-side traffic on the new link (not a blind
	// sleep that could elapse before anything flows, making the orders
	// effectively identical).
	for step, linkIdx := range order {
		link := crossLinks[linkIdx]
		if err := cluster.AddPeer(link[0], link[1]); err != nil {
			t.Fatalf("order %v heal %v: %v", order, link, err)
		}
		if err := cluster.AddPeer(link[1], link[0]); err != nil {
			t.Fatalf("order %v heal %v: %v", order, link, err)
		}
		t.Logf("order %v: healed link %d/%d (%d<->%d)", order, step+1, len(order), link[0], link[1])
		waitLinkFlowed(t, cluster, order, link)
	}

	waitConverged(t, cluster, 30, 150*time.Second)
	rows, err := cluster.TypedContentionRows(0)
	if err != nil {
		t.Fatal(err)
	}
	// Log the LWW winner on a conflicted row: the digest comparison
	// across orders is the assertion, this names the value for forensics.
	final := digestRows(rows)
	for _, row := range rows {
		if row.ID == typedRowID(1000) {
			t.Logf("order %v: conflict winner row 1000 = %s", order, row.Name)
			break
		}
	}
	return final
}

// waitLinkFlowed blocks until a side-originated row is visible across the
// newly healed link in either direction: a sideA row (2xxx, written on
// node 1) on the sideB endpoint, or a sideB row (3xxx, written on node
// 3) on the sideA endpoint.
func waitLinkFlowed(t *testing.T, c *harness.Cluster, order []int, link [2]int) {
	t.Helper()
	sideA := typedRowID(2000)
	sideB := typedRowID(3000)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if hasRow(t, c, link[1], sideA) || hasRow(t, c, link[0], sideB) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("order %v: no cross-side traffic on healed link %v within 30s", order, link)
}

func hasRow(t *testing.T, c *harness.Cluster, idx int, id string) bool {
	t.Helper()
	rows, err := c.TypedContentionRows(idx)
	if err != nil {
		return false
	}
	for _, row := range rows {
		if row.ID == id {
			return true
		}
	}
	return false
}

// parseOrder reads a comma-separated permutation of link indexes;
// invalid values fail fast rather than silently healing another order.
func parseOrder(t *testing.T, env, fallback string) []int {
	t.Helper()
	raw := harness.GetEnv(env)
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
			rows, err := c.TypedContentionRows(idx)
			if err != nil || len(rows) != want {
				ok = false
				break
			}
			d := digestRows(rows)
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
				rows, _ := c.TypedContentionRows(i)
				counts[i] = len(rows)
			}
			t.Logf("converge progress: counts=%v want=%d", counts, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, idx := range idxs {
		rows, _ := c.TypedContentionRows(idx)
		t.Logf("node %d at timeout: count=%d digest=%s", idx, len(rows), digestRows(rows))
	}
	t.Fatalf("nodes %v did not converge on %d rows with equal digests within %v", idxs, want, timeout)
}

func insertRow(c *harness.Cluster, node, id int, name string) error {
	return c.TypedContentionInsert(node, harness.TypedContentionRow{ID: typedRowID(id), Name: name})
}

func typedRowID(id int) string { return fmt.Sprintf("%08x-0000-4000-8000-%012x", id, id) }

func digestRows(rows []harness.TypedContentionRow) string {
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	h := sha256.New()
	for _, row := range rows {
		_, _ = fmt.Fprintf(h, "%q\x00%q\x00%q\x00%d\n", row.ID, row.Name, row.Phone, row.Score)
	}
	return hex.EncodeToString(h.Sum(nil))
}
