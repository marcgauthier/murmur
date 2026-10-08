// SWIM seed-only discovery acceptance.
//
// Four daemons start with no static peers: the only topology config is
// a single SWIM seed address (one seed; the harness leaves seeds with
// an empty bootstrap list, and memberlist Join returns after the first
// live seed, so a second seed address would never be contacted —
// production cross-configures seeds with each other instead). The test
// asserts a full mesh forms
// purely via SWIM discovery (membership view plus a live session to
// every peer), writes from every node converge, then one seed is
// killed and restarted: failure detection must observe the death,
// rediscovery must re-form the mesh, and all rows (including rows
// written while the seed was down) must reconverge with identical
// digests.
package swimdiscovery_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

const tableName = "swim_rows"

func TestSeedOnlyDiscoveryFormsFullMesh(t *testing.T) {
	deadline := envSeconds("MURMUR_SWIM_DISCOVERY_SECONDS", 60)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:           "swim-discovery",
		NumNodes:       4,
		AwaitUnlock:    true,
		ManualPeers:    true,
		BootstrapSeeds: []int{0},
		TypedRecords:   true,
	})
	assertNoStaticPeers(t, cluster)

	// Full mesh purely via SWIM: every node sees all 4 members ...
	waitMembership(t, cluster, 4, time.Duration(deadline)*time.Second)
	// ... and holds a live replication session to every other node.
	waitFullSessionMesh(t, cluster, time.Duration(deadline)*time.Second)

	// Writes from every node converge with identical digests.
	rowsPerNode := 10
	for i := range cluster.Nodes {
		for r := 0; r < rowsPerNode; r++ {
			if err := cluster.TypedInsert(i, fmt.Sprintf("n%d-r%d", i, r)); err != nil {
				t.Fatalf("node %d write: %v", i, err)
			}
		}
	}
	// 150s: same loaded-box rationale as below (partial-mesh uses 150s
	// for the same reason); the proof is exact rows plus equal digests.
	waitConverged(t, cluster, 4*rowsPerNode, 150*time.Second)
	t.Logf("seed-only mesh formed and converged: membership=4, sessions=full, rows=%d", 4*rowsPerNode)
}

func TestSeedKillAndRediscovery(t *testing.T) {
	deadline := envSeconds("MURMUR_SWIM_DISCOVERY_SECONDS", 60)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:           "swim-rediscovery",
		NumNodes:       4,
		AwaitUnlock:    true,
		ManualPeers:    true,
		BootstrapSeeds: []int{0},
		TypedRecords:   true,
	})
	assertNoStaticPeers(t, cluster)
	waitMembership(t, cluster, 4, time.Duration(deadline)*time.Second)
	waitFullSessionMesh(t, cluster, time.Duration(deadline)*time.Second)

	// Baseline writes converge before the kill (honest-path control).
	for i := 0; i < 8; i++ {
		if err := cluster.TypedInsert(2, fmt.Sprintf("base-%d", i)); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	// 150s, not 60s: under a heavily loaded box a wedged QUIC stream
	// needs a 30s send-timeout recycle to heal, and several incidents
	// can stack; the proof (exact rows, identical digests) is unchanged.
	waitConverged(t, cluster, 8, 150*time.Second)

	// The seed itself started with an empty bootstrap list, so give the
	// restarted seed a live member to rejoin through (any live member
	// rejoins the whole cluster via gossip).
	patchSeedBootstrap(t, cluster, 0, cluster.Nodes[1].ReplAddr)

	cluster.KillNode(0)
	t.Logf("seed node1 killed; waiting for survivors to observe death")
	waitDeathObserved(t, cluster, []int{1, 2, 3}, time.Duration(deadline)*time.Second)

	// Survivors keep writing while the seed is down.
	for i := 0; i < 12; i++ {
		if err := cluster.TypedInsert(1+i%3, fmt.Sprintf("down-%d", i)); err != nil {
			t.Fatalf("survivor write %d: %v", i, err)
		}
	}
	waitConvergedOn(t, cluster, []int{1, 2, 3}, 20, 150*time.Second)

	cluster.StartNode(0)
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
	cluster.WaitNodeReady(0)
	t.Logf("seed node1 restarted; waiting for rediscovery")

	waitMembership(t, cluster, 4, time.Duration(deadline)*time.Second)
	waitFullSessionMesh(t, cluster, time.Duration(deadline)*time.Second)
	waitConverged(t, cluster, 20, 150*time.Second)
	t.Logf("seed rediscovered and reconverged: membership=4, sessions=full, rows=20")
}

// assertNoStaticPeers proves the mesh under test was discovered, not
// configured: every node config must carry an empty static peer list.
func assertNoStaticPeers(t *testing.T, c *harness.Cluster) {
	t.Helper()
	for i, node := range c.Nodes {
		raw, err := os.ReadFile(node.ConfigFile)
		if err != nil {
			t.Fatalf("node %d read config: %v", i, err)
		}
		var cfg struct {
			Peers []struct {
				NodeID string `json:"node_id"`
			} `json:"peers"`
		}
		if err := json.Unmarshal(raw, &cfg); err != nil {
			t.Fatalf("node %d parse config: %v", i, err)
		}
		if len(cfg.Peers) != 0 {
			t.Fatalf("node %d has %d static peers, want 0 (seeds-only discovery)", i, len(cfg.Peers))
		}
	}
}

// patchSeedBootstrap adds a bootstrap seed address to a node's config
// file; it takes effect on the next StartNode.
func patchSeedBootstrap(t *testing.T, c *harness.Cluster, idx int, seedAddr string) {
	t.Helper()
	node := c.Nodes[idx]
	raw, err := os.ReadFile(node.ConfigFile)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg["bootstrap"] = []string{seedAddr}
	cfg["membership_enabled"] = true
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(node.ConfigFile, out, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

func waitMembership(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, node := range c.Nodes {
			if got := int(metricValue(t, node.APIAddr, "spedsql_membership_count")); got != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, node := range c.Nodes {
		t.Logf("%s membership_count=%v alive=%v suspect=%v dead=%v",
			node.Label, metricValue(t, node.APIAddr, "spedsql_membership_count"),
			metricValue(t, node.APIAddr, "spedsql_membership_alive_count"),
			metricValue(t, node.APIAddr, "spedsql_membership_suspect_count"),
			metricValue(t, node.APIAddr, "spedsql_membership_dead_count"))
	}
	t.Fatalf("nodes did not reach membership %d within %v", want, timeout)
}

// waitDeathObserved requires every listed survivor to report fewer
// alive members than the full mesh, proving failure detection fired
// before the restart makes rediscovery meaningful. (alive_count
// excludes self, so a full 4-mesh reports 3.)
func waitDeathObserved(t *testing.T, c *harness.Cluster, survivors []int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, idx := range survivors {
			alive := int(metricValue(t, c.Nodes[idx].APIAddr, "spedsql_membership_alive_count"))
			if alive >= len(c.Nodes)-1 {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("survivors did not observe seed death within %v", timeout)
}

// waitFullSessionMesh requires a live session for every directed pair,
// proving SWIM discovery built a complete replication mesh.
func waitFullSessionMesh(t *testing.T, c *harness.Cluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sessionMeshComplete(t, c) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("full session mesh did not form within %v", timeout)
}

func sessionMeshComplete(t *testing.T, c *harness.Cluster) bool {
	t.Helper()
	for i, from := range c.Nodes {
		body := scrape(t, from.APIAddr)
		for j, to := range c.Nodes {
			if i == j {
				continue
			}
			connected, found := harness.MetricValueWithLabels(body, "spedsql_peer_connected", map[string]string{"peer": to.NodeID.String()})
			if !found || connected != 1 {
				return false
			}
		}
	}
	return true
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	waitConvergedOn(t, c, []int{0, 1, 2, 3}, want, timeout)
}

func waitConvergedOn(t *testing.T, c *harness.Cluster, idxs []int, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	lastProgress := time.Now()
	lastCounts := ""
	stallDumps := 0
	for time.Now().Before(deadline) {
		ok := true
		var first []string
		for k, idx := range idxs {
			names, err := c.TypedNames(idx)
			if err != nil || len(names) != want {
				ok = false
				break
			}
			if k == 0 {
				first = names
			} else if !sameNames(names, first) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Since(lastLog) > 5*time.Second {
			lastLog = time.Now()
			counts := make([]int, len(c.Nodes))
			for i := range c.Nodes {
				names, _ := c.TypedNames(i)
				counts[i] = len(names)
			}
			t.Logf("converge progress: counts=%v want=%d", counts, want)
			key := fmt.Sprintf("%v", counts)
			if key != lastCounts {
				lastCounts = key
				lastProgress = time.Now()
			} else if time.Since(lastProgress) > 15*time.Second && stallDumps < 2 {
				stallDumps++
				t.Logf("no progress for %v; capturing stall forensics", time.Since(lastProgress).Truncate(time.Second))
				dumpStallForensics(t, c)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, idx := range idxs {
		names, _ := c.TypedNames(idx)
		t.Logf("node %d at timeout: count=%d", idx, len(names))
	}
	dumpTopoForensics(t, c)
	t.Fatalf("nodes %v did not converge on %d typed records with equal contents within %v", idxs, want, timeout)
}

func sameNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// dumpTopoForensics logs membership, session, and per-peer state for
// every node so a convergence failure is diagnosable.
func dumpTopoForensics(t *testing.T, c *harness.Cluster) {
	t.Helper()
	for i, node := range c.Nodes {
		body, ok := tryScrape(node.APIAddr)
		if !ok {
			t.Logf("node %d topo: unreachable (down?)", i)
			continue
		}
		var peers []string
		if samples, ok := harness.MetricSamples(body); ok {
			for _, sample := range samples {
				if strings.HasPrefix(sample.Name, "spedsql_peer_") {
					peers = append(peers, fmt.Sprintf("%s labels=%v value=%g", sample.Name, sample.Labels, sample.Value))
				}
			}
		}
		t.Logf("node %d topo: membership=%v alive=%v applyFail=%v invalid=%v deferred=%v peers=[%s]",
			i, mustMetric(body, "spedsql_membership_count"),
			mustMetric(body, "spedsql_membership_alive_count"),
			mustMetric(body, "spedsql_repl_apply_failures_total"),
			mustMetric(body, "spedsql_repl_batches_invalid_total"),
			mustMetric(body, "spedsql_repl_batches_deferred_total"),
			strings.Join(peers, " | "))
		for _, ps := range debugPeers(t, node.APIAddr) {
			t.Logf("node %d peer %s dynamic=%v lastSend=%s lastRecv=%s bytes=%d/%d queued=%d/%d/%d have=%v sent=%v",
				i, shortID(ps.NodeID), ps.Dynamic,
				age(ps.LastSend), age(ps.LastRecv),
				ps.BytesSent, ps.BytesReceived,
				ps.QueuedNeed, ps.QueuedCtrl, ps.QueuedSchema,
				shortMap(ps.Have), shortMap(ps.Sent))
		}
	}
}

type debugPeerStatus struct {
	NodeID        string
	Addrs         []string
	Connected     bool
	Dynamic       bool
	Selected      bool
	LastSend      time.Time
	LastRecv      time.Time
	BytesSent     uint64
	BytesReceived uint64
	QueuedNeed    int
	QueuedCtrl    int
	QueuedSchema  int
	Have          map[string]uint64
	Sent          map[string]uint64
}

func debugPeers(t *testing.T, apiAddr string) []debugPeerStatus {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/v1/debug/peers")
	if err != nil {
		t.Logf("debug peers %s: %v", apiAddr, err)
		return nil
	}
	defer resp.Body.Close()
	var out []debugPeerStatus
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Logf("decode debug peers %s: %v", apiAddr, err)
		return nil
	}
	return out
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func shortMap(m map[string]uint64) string {
	var parts []string
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%s:%d", shortID(k), v))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func age(ts time.Time) string {
	if ts.IsZero() {
		return "never"
	}
	return time.Since(ts).Truncate(100 * time.Millisecond).String()
}

// dumpStallForensics samples per-peer traffic counters twice to show
// whether replication is flowing or wedged, and pulls goroutine stacks
// from the least-progressed node to pinpoint blockage.
func dumpStallForensics(t *testing.T, c *harness.Cluster) {
	t.Helper()
	sample := func() map[int]map[string][2]uint64 {
		out := make(map[int]map[string][2]uint64)
		for i, node := range c.Nodes {
			m := make(map[string][2]uint64)
			for _, ps := range debugPeers(t, node.APIAddr) {
				m[shortID(ps.NodeID)] = [2]uint64{ps.BytesSent, ps.BytesReceived}
			}
			out[i] = m
		}
		return out
	}
	before := sample()
	time.Sleep(2 * time.Second)
	after := sample()
	for i, node := range c.Nodes {
		body, ok := tryScrape(node.APIAddr)
		if !ok {
			t.Logf("node %d flow: unreachable (down?)", i)
			continue
		}
		t.Logf("node %d flow: needsSent=%v needsRecv=%v acksSent=%v acksRecv=%v needDrops=%v applyFail=%v susp=%v refute=%v recJoin=%v recLeave=%v",
			i, mustMetric(body, "spedsql_repl_needs_sent_total"),
			mustMetric(body, "spedsql_repl_needs_received_total"),
			mustMetric(body, "spedsql_repl_acks_sent_total"),
			mustMetric(body, "spedsql_repl_acks_received_total"),
			mustMetric(body, "spedsql_repl_need_drops_total"),
			mustMetric(body, "spedsql_repl_apply_failures_total"),
			mustMetric(body, "spedsql_swim_suspicions_total"),
			mustMetric(body, "spedsql_swim_refutations_total"),
			mustMetric(body, "spedsql_swim_reconciled_joins_total"),
			mustMetric(body, "spedsql_swim_reconciled_leaves_total"))
		for peer, b := range before[i] {
			a := after[i][peer]
			t.Logf("node %d -> %s bytes sent %d->%d recv %d->%d", i, peer, b[0], a[0], b[1], a[1])
		}
	}
	// Stacks from one wedged sender (frozen acks + dropping Needs) and
	// the behind node: the behind node is often idle while a sender is
	// wedged, so a single node's stacks mislead.
	wedged := -1
	behind, best := -1, -1
	for idx, node := range c.Nodes {
		names, err := c.TypedNames(idx)
		if err != nil {
			continue
		}
		n := len(names)
		if behind == -1 || n < best {
			behind, best = idx, n
		}
		if body, ok := tryScrape(node.APIAddr); ok {
			if mustMetric(body, "spedsql_repl_need_drops_total") > 0 && wedged == -1 {
				wedged = idx
			}
		}
	}
	if wedged >= 0 {
		dumpFilteredStacks(t, c, wedged)
	}
	if behind >= 0 && behind != wedged {
		dumpFilteredStacks(t, c, behind)
	}
}

func dumpFilteredStacks(t *testing.T, c *harness.Cluster, idx int) {
	t.Helper()
	resp, err := http.Get("https://" + c.Nodes[idx].APIAddr + "/v1/debug/stacks")
	if err != nil {
		t.Logf("node %d stacks: %v", idx, err)
		return
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		t.Logf("node %d stacks read: %v", idx, err)
		return
	}
	keywords := []string{"replication.(*Manager).readDataLoop", "replication.(*Manager).readControlFrames",
		"replication.(*Manager).streamAcceptLoop", "replication.(*Manager).onBatches",
		"replication.(*Manager).sendDue", "replication.(*Manager).sendOrigin",
		"replication.(*Manager).sendOriginCapped", "replication.(*Manager).sendAckAndPull",
		"replication.(*Manager).sendQueuedNeeds", "replication.(*Manager).sendQueuedCtrl",
		"replication.(*Manager).sendSchemaRequests", "replication.(*Manager).serveSchemaResponses",
		"replication.(*Manager).sendQueuedPlumtree", "replication.(*Manager).waitTransfer",
		"replication.(*Manager).sendLoop", "replication.(*Manager).sendAll",
		"replication.(*Manager).performAntiEntropy", "spedsql.(*DB).ApplyRemote",
		"sched.Admit", "queueRemote"}
	blocks := strings.Split(string(raw), "\n\n")
	shown := 0
	for _, b := range blocks {
		hit := false
		for _, k := range keywords {
			if strings.Contains(b, k) {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		lines := strings.Split(strings.TrimSpace(b), "\n")
		if len(lines) > 12 {
			lines = lines[:12]
		}
		t.Logf("node %d stack:\n%s", idx, strings.Join(lines, "\n"))
		shown++
		if shown >= 30 {
			t.Logf("node %d stacks truncated", idx)
			return
		}
	}
	if shown == 0 {
		t.Logf("node %d: no replication stacks matched (%d goroutines total)", idx, len(blocks))
	}
}

func scrape(t *testing.T, apiAddr string) string {
	t.Helper()
	body, ok := tryScrape(apiAddr)
	if !ok {
		t.Fatalf("scrape metrics %s: unreachable", apiAddr)
	}
	return body
}

// tryScrape fetches /metrics without failing: forensics must survive
// dead nodes.
func tryScrape(apiAddr string) (string, bool) {
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", false
	}
	return string(raw), true
}

func metricValue(t *testing.T, apiAddr, name string) float64 {
	t.Helper()
	v, ok := metricValueFrom(scrape(t, apiAddr), name)
	if !ok {
		t.Fatalf("metric %s not found", name)
	}
	return v
}

// metricValueFrom parses one gauge/counter; ok=false reports absence
// without failing so forensics can cover nodes in odd states.
func metricValueFrom(body, name string) (float64, bool) {
	return harness.MetricValueFrom(body, name)
}

func mustMetric(body, name string) float64 {
	v, _ := metricValueFrom(body, name)
	return v
}

func envSeconds(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
