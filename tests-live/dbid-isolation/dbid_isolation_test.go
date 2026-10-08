// Database-identity isolation acceptance: nodes from two different
// databases (distinct DBIDs) never peer with each other, even when the
// TLS layer would admit them.
//
// Setup: two independent 2-node clusters with byte-identical structured
// Schema, so schema agreement cannot be the refusal cause. The clusters
// are then given shared CA trust (each cluster's CA certificate is
// appended to every node's tls/ca.crt, then all nodes restart): without
// this, a cross-peer dial would die at the TLS layer and the test would
// prove nothing about database identity. With shared trust, cross
// handshakes reach replication/manager.go validateIdentity and must be
// refused with "db id mismatch".
//
// Flow:
//  1. Assert the two cluster DBIDs actually differ.
//  2. Converge one marker per cluster (positive controls: each mesh is
//     internally healthy, with equal digests).
//  3. Cross-AddPeer every A node to every B node and vice versa.
//  4. Over a >=10s window, assert every cross peer entry stays
//     Connected=false, connected_peers never exceeds the internal count
//     (1), and neither side's marker ever appears on the other side.
//     spedsql_repl_handshake_identity_refusals_total must advance on all
//     nodes (proving validateIdentity refused), while
//     spedsql_repl_handshake_schema_refusals_total must not advance
//     (proving schema was never the cause).
//  5. Cleanup with RemovePeer for every cross edge, then re-verify each
//     cluster still converges internally.
//
// Knob: MURMUR_DBID_ISO_WINDOW_S (default 10, minimum 10) sets the
// isolation observation window. Default runtime is well under 3 minutes.
package dbidisolation_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestCrossDatabasePeersNeverConnect(t *testing.T) {
	window := time.Duration(envInt("MURMUR_DBID_ISO_WINDOW_S", 10)) * time.Second
	if window < 10*time.Second {
		t.Fatalf("isolation window %v below the 10s minimum", window)
	}

	clusterA := harness.NewCluster(t, harness.ClusterOptions{
		Name: "dbid-iso-a", NumNodes: 2, AwaitUnlock: true, TypedRecords: true,
	})
	clusterB := harness.NewCluster(t, harness.ClusterOptions{
		Name: "dbid-iso-b", NumNodes: 2, AwaitUnlock: true, TypedRecords: true,
	})

	if clusterA.DBID == clusterB.DBID {
		t.Fatalf("clusters share DBID %s; isolation test is vacuous", clusterA.DBID)
	}
	t.Logf("cluster A DBID=%s cluster B DBID=%s", clusterA.DBID, clusterB.DBID)

	// Share CA trust so cross dials pass TLS and reach the DBID check.
	crossTrustCAs(t, clusterA, clusterB)

	// Positive controls: each cluster converges internally.
	if err := clusterA.TypedInsert(0, "cluster-a-marker"); err != nil {
		t.Fatalf("A marker insert: %v", err)
	}
	if err := clusterB.TypedInsert(0, "cluster-b-marker"); err != nil {
		t.Fatalf("B marker insert: %v", err)
	}
	waitConverged(t, clusterA, 1, 60*time.Second)
	waitConverged(t, clusterB, 1, 60*time.Second)
	waitPeerConnected(t, clusterA.Nodes[0].APIAddr, clusterA.Nodes[1].NodeID.String(), true, 30*time.Second)
	waitPeerConnected(t, clusterB.Nodes[0].APIAddr, clusterB.Nodes[1].NodeID.String(), true, 30*time.Second)

	baseIdentity := map[string]int64{}
	baseSchema := map[string]int64{}
	for _, c := range []*harness.Cluster{clusterA, clusterB} {
		for _, n := range c.Nodes {
			baseIdentity[n.APIAddr] = metricInt(t, n.APIAddr, "spedsql_repl_handshake_identity_refusals_total")
			baseSchema[n.APIAddr] = metricInt(t, n.APIAddr, "spedsql_repl_handshake_schema_refusals_total")
		}
	}

	// Cross-AddPeer in both directions (full bipartite).
	for _, a := range clusterA.Nodes {
		for _, b := range clusterB.Nodes {
			crossAddPeer(t, a, b)
			crossAddPeer(t, b, a)
		}
	}

	// Isolation window: cross peers never connect, markers never cross.
	windowEnd := time.Now().Add(window)
	for time.Now().Before(windowEnd) {
		assertNoCrossConnection(t, clusterA, clusterB)
		assertMarkersIsolated(t, clusterA, clusterB)
		time.Sleep(500 * time.Millisecond)
	}
	assertNoCrossConnection(t, clusterA, clusterB)
	assertMarkersIsolated(t, clusterA, clusterB)

	// The refusal cause must be identity (DBID), on every node, with no
	// schema refusals (identical schemas agree).
	waitMetricAdvanced(t, "spedsql_repl_handshake_identity_refusals_total", 30*time.Second,
		append(append([]*harness.Node{}, clusterA.Nodes...), clusterB.Nodes...), baseIdentity)
	for _, c := range []*harness.Cluster{clusterA, clusterB} {
		for _, n := range c.Nodes {
			if got := metricInt(t, n.APIAddr, "spedsql_repl_handshake_schema_refusals_total"); got != baseSchema[n.APIAddr] {
				t.Fatalf("schema refusals advanced on %s (%d -> %d); schemas should agree",
					n.APIAddr, baseSchema[n.APIAddr], got)
			}
		}
	}

	// Cleanup: remove every cross edge, then re-verify internal health.
	for _, a := range clusterA.Nodes {
		for _, b := range clusterB.Nodes {
			crossRemovePeer(t, a, b)
			crossRemovePeer(t, b, a)
		}
	}
	waitConverged(t, clusterA, 1, 60*time.Second)
	waitConverged(t, clusterB, 1, 60*time.Second)
	assertNoCrossConnection(t, clusterA, clusterB)
}

// crossTrustCAs appends each cluster's CA certificate to every node of
// the other cluster and restarts all nodes so both the replication TLS
// pool and the API client roots pick it up. Node certificates are
// untouched, so existing API client credentials keep working.
func crossTrustCAs(t *testing.T, a, b *harness.Cluster) {
	t.Helper()
	for _, n := range a.Nodes {
		appendPEM(t, filepath.Join(n.TLSDir, "ca.crt"), b.CA.CertPEM)
	}
	for _, n := range b.Nodes {
		appendPEM(t, filepath.Join(n.TLSDir, "ca.crt"), a.CA.CertPEM)
	}
	for _, c := range []*harness.Cluster{a, b} {
		for i := range c.Nodes {
			c.StopNode(i)
		}
		for i := range c.Nodes {
			c.StartNode(i)
			c.UnlockNode(i, c.Nodes[i].KeyHex)
			c.WaitNodeReady(i)
		}
	}
}

func appendPEM(t *testing.T, path string, pem []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
		defer f.Close()
	}
	if _, err := f.Write(pem); err != nil {
		t.Fatalf("append %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
}

func apiClient() *http.Client {
	// The harness routes DefaultTransport to per-node mTLS clients;
	// reuse it with a bound. Both clusters registered their API addrs.
	return &http.Client{Timeout: 15 * time.Second, Transport: http.DefaultTransport}
}

func crossAddPeer(t *testing.T, from, to *harness.Node) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"node_id": to.NodeID.String(),
		"addrs":   []string{to.ReplAddr},
	})
	resp, err := apiClient().Post("https://"+from.APIAddr+"/v1/admin/add_peer", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("cross add_peer %s -> %s: %v", from.APIAddr, to.NodeID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("cross add_peer %s -> %s status=%d: %s", from.APIAddr, to.NodeID, resp.StatusCode, raw)
	}
}

func crossRemovePeer(t *testing.T, from, to *harness.Node) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"node_id": to.NodeID.String()})
	resp, err := apiClient().Post("https://"+from.APIAddr+"/v1/admin/remove_peer", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("cross remove_peer %s -> %s: %v", from.APIAddr, to.NodeID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("cross remove_peer %s -> %s status=%d: %s", from.APIAddr, to.NodeID, resp.StatusCode, raw)
	}
}

type peerJSON struct {
	NodeID    string `json:"NodeID"`
	Connected bool   `json:"Connected"`
}

func fetchPeers(t *testing.T, apiAddr string) []peerJSON {
	t.Helper()
	resp, err := apiClient().Get("https://" + apiAddr + "/v1/debug/peers")
	if err != nil {
		t.Fatalf("debug/peers %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	var peers []peerJSON
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		t.Fatalf("decode peers %s: %v", apiAddr, err)
	}
	return peers
}

func connectedPeers(t *testing.T, apiAddr string) int {
	t.Helper()
	resp, err := apiClient().Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		t.Fatalf("status %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	var st struct {
		Connected int `json:"connected_peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode status %s: %v", apiAddr, err)
	}
	return st.Connected
}

// assertNoCrossConnection fails fast if any cross-cluster session exists:
// every cross entry must be absent or Connected=false, and no node may
// report more live sessions than its one internal peer.
func assertNoCrossConnection(t *testing.T, a, b *harness.Cluster) {
	t.Helper()
	bIDs := map[string]bool{}
	for _, n := range b.Nodes {
		bIDs[n.NodeID.String()] = true
	}
	aIDs := map[string]bool{}
	for _, n := range a.Nodes {
		aIDs[n.NodeID.String()] = true
	}
	for _, n := range a.Nodes {
		for _, p := range fetchPeers(t, n.APIAddr) {
			if bIDs[p.NodeID] && p.Connected {
				t.Fatalf("CROSS CONNECTED: A node %s reports cross peer %s Connected=true", n.APIAddr, p.NodeID)
			}
		}
		if got := connectedPeers(t, n.APIAddr); got > 1 {
			t.Fatalf("A node %s connected_peers=%d, want <=1 (internal only)", n.APIAddr, got)
		}
	}
	for _, n := range b.Nodes {
		for _, p := range fetchPeers(t, n.APIAddr) {
			if aIDs[p.NodeID] && p.Connected {
				t.Fatalf("CROSS CONNECTED: B node %s reports cross peer %s Connected=true", n.APIAddr, p.NodeID)
			}
		}
		if got := connectedPeers(t, n.APIAddr); got > 1 {
			t.Fatalf("B node %s connected_peers=%d, want <=1 (internal only)", n.APIAddr, got)
		}
	}
}

func assertMarkersIsolated(t *testing.T, a, b *harness.Cluster) {
	t.Helper()
	for i := range a.Nodes {
		names, err := a.TypedNames(i)
		if err != nil || len(names) != 1 || names[0] != "cluster-a-marker" {
			t.Fatalf("A node %d names=%v (err=%v), want only its own marker", i, names, err)
		}
	}
	for i := range b.Nodes {
		names, err := b.TypedNames(i)
		if err != nil || len(names) != 1 || names[0] != "cluster-b-marker" {
			t.Fatalf("B node %d names=%v (err=%v), want only its own marker", i, names, err)
		}
	}
}

func waitPeerConnected(t *testing.T, apiAddr, nodeID string, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, p := range fetchPeers(t, apiAddr) {
			if p.NodeID == nodeID && p.Connected == want {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("peer %s Connected != %v on %s within %v", nodeID, want, apiAddr, timeout)
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			names, err := c.TypedNames(i)
			if err != nil || len(names) != want {
				ok = false
				break
			}
			d := strings.Join(names, "\x00")
			if i == 0 {
				first = d
			} else if d != first {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %d typed records with equal names within %v", want, timeout)
}

func metricInt(t *testing.T, apiAddr, name string) int64 {
	t.Helper()
	resp, err := apiClient().Get("https://" + apiAddr + "/metrics")
	if err != nil {
		t.Fatalf("metrics %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := harness.MetricValueFrom(string(raw), name); ok {
		return int64(value)
	}
	t.Fatalf("counter %s not present in /metrics", name)
	return 0
}

func waitMetricAdvanced(t *testing.T, name string, timeout time.Duration, nodes []*harness.Node, before map[string]int64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, n := range nodes {
			if metricInt(t, n.APIAddr, name) <= before[n.APIAddr] {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	for _, n := range nodes {
		t.Logf("%s %s = %d (baseline %d)", n.APIAddr, name, metricInt(t, n.APIAddr, name), before[n.APIAddr])
	}
	t.Fatalf("%s did not advance on every node within %v", name, timeout)
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
