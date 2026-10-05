// Certificate-lifecycle acceptance: a node whose certificate expires is
// rejected on fresh replication handshakes, and rotation restores the mesh.
//
// Flow (2 nodes, structured Schema marker table):
//  1. Converge a marker (positive control: healthy mesh).
//  2. Stop node B, replace tls/node.crt + tls/node.key with a short-expiry
//     certificate minted via cluster.CA.IssueNode for the SAME NodeID,
//     restart, re-unlock, and assert the nodes are connected pre-expiry
//     (debug/peers Connected=true plus a second converged marker).
//  3. After the served certificate's NotAfter passes, restart B so both
//     sides must re-handshake, then assert B is rejected: the survivor's
//     debug/peers shows Connected=false, connected_peers drops 1->0, and
//     a marker written on A never reaches B. Non-vacuity: the certificate
//     B actually serves is parsed at assert time and its NotAfter must be
//     in the past.
//  4. Rotate (fresh 24h cert + restart) and assert full reconvergence
//     with equal ordered digests.
//
// Why step 3 restarts B: an already-established QUIC/TLS session is not
// re-validated mid-connection (TLS property), and the replication dial
// loop only dials sessionless peers (replication/manager.go tryDial
// returns early when a session is alive). Expiry is therefore enforced
// on fresh handshakes, which the restart forces on both sides.
//
// Knobs: MURMUR_CERT_LIFECYCLE_TTL_S (default 25) sets the short-cert
// lifetime; MURMUR_CERT_LIFECYCLE_WINDOW_S (default 10) sets the
// post-expiry isolation window. Default runtime is well under 3 minutes.
package certlifecycle_test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestCertExpiryRejectsAndRotationHeals(t *testing.T) {
	ttl := time.Duration(envInt("MURMUR_CERT_LIFECYCLE_TTL_S", 25)) * time.Second
	window := time.Duration(envInt("MURMUR_CERT_LIFECYCLE_WINDOW_S", 10)) * time.Second
	if ttl < 15*time.Second {
		t.Fatalf("short-cert TTL %v too small to converge pre-expiry; raise MURMUR_CERT_LIFECYCLE_TTL_S", ttl)
	}

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "cert-lifecycle",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "markers",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "val", Type: schema.ColText, Nullable: true},
			},
		}}},
	})
	a, b := 0, 1
	nodeB := cluster.Nodes[b]

	// Phase 0: positive control, healthy mesh converges.
	if err := cluster.ExecSQL(a, "INSERT INTO markers (id, val) VALUES (?, ?)", fmt.Sprintf("%032x", 0), "init"); err != nil {
		t.Fatalf("phase0 insert: %v", err)
	}
	waitConverged(t, cluster, "markers", 1, 60*time.Second)
	waitPeerConnected(t, cluster.Nodes[a].APIAddr, nodeB.NodeID.String(), true, 30*time.Second)
	if got := connectedPeers(t, cluster.Nodes[a].APIAddr); got != 1 {
		t.Fatalf("pre-expiry connected_peers(A) = %d, want 1", got)
	}

	// Phase 1: stop B, swap in a short-expiry cert for the same NodeID.
	cluster.StopNode(b)
	shortCertPEM, shortKeyPEM, err := cluster.CA.IssueNode(nodeB.NodeID, ttl)
	if err != nil {
		t.Fatalf("mint short cert: %v", err)
	}
	expectedNotAfter := parseNotAfter(t, shortCertPEM)
	t.Logf("short cert expires at %s (in %v)", expectedNotAfter.Format(time.RFC3339), time.Until(expectedNotAfter).Round(time.Second))
	writeCertPair(t, nodeB, shortCertPEM, shortKeyPEM)
	cluster.StartNode(b)
	cluster.UnlockNode(b, nodeB.KeyHex)
	cluster.WaitNodeReady(b)

	// Prove B actually serves the short cert (not a stale one): the
	// served leaf's expiry must match the minted cert.
	served := fetchServedCert(t, nodeB.APIAddr)
	if !served.NotAfter.Equal(expectedNotAfter) {
		t.Fatalf("served cert NotAfter = %s, want minted %s", served.NotAfter, expectedNotAfter)
	}
	if time.Until(served.NotAfter) <= 0 {
		t.Fatalf("short cert already expired at %s", served.NotAfter)
	}

	// Assert connected pre-expiry: session plus a converged marker.
	preDeadline := served.NotAfter.Add(-5 * time.Second).Sub(time.Now())
	if preDeadline <= 0 {
		t.Fatalf("no pre-expiry budget left (NotAfter %s)", served.NotAfter)
	}
	waitPeerConnected(t, cluster.Nodes[a].APIAddr, nodeB.NodeID.String(), true, preDeadline)
	if err := cluster.ExecSQL(a, "INSERT INTO markers (id, val) VALUES (?, ?)", fmt.Sprintf("%032x", 1), "pre-expiry"); err != nil {
		t.Fatalf("phase1 insert: %v", err)
	}
	waitConverged(t, cluster, "markers", 2, preDeadline)

	// Phase 2: wait out the TTL, then force fresh handshakes by
	// restarting B. B's daemon starts fine with an expired cert (PEM
	// parsing does not check expiry); its handshakes must fail.
	t.Logf("waiting for cert expiry at %s", expectedNotAfter.Format(time.RFC3339))
	deadline := expectedNotAfter.Add(2 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
	}
	cluster.StopNode(b)
	cluster.StartNode(b)
	insecure := insecureAPIClient(t, cluster)
	unlockVia(t, insecure, nodeB.APIAddr, nodeB.KeyHex)
	waitReadyVia(t, insecure, nodeB.APIAddr)

	// B is rejected: survivor sees Connected=false and connected_peers
	// drops 1->0.
	waitPeerConnected(t, cluster.Nodes[a].APIAddr, nodeB.NodeID.String(), false, 45*time.Second)
	waitConnectedPeers(t, cluster.Nodes[a].APIAddr, 0, 45*time.Second)
	// B's own view agrees (read via the insecure client: B's HTTPS
	// server cert is expired, so the verifying harness client fails).
	waitPeerConnectedVia(t, insecure, nodeB.APIAddr, cluster.Nodes[a].NodeID.String(), false, 45*time.Second)

	// Non-vacuity: at assert time, the cert B serves is expired.
	servedAfter := fetchServedCert(t, nodeB.APIAddr)
	if !servedAfter.NotAfter.Before(time.Now()) {
		t.Fatalf("served cert NotAfter = %s, want it in the past", servedAfter.NotAfter)
	}
	t.Logf("rejection confirmed while serving expired cert (NotAfter %s)", servedAfter.NotAfter.Format(time.RFC3339))

	// A-side marker written post-expiry must never reach B during the
	// isolation window, while A keeps serving it.
	if err := cluster.ExecSQL(a, "INSERT INTO markers (id, val) VALUES (?, ?)", fmt.Sprintf("%032x", 2), "post-expiry"); err != nil {
		t.Fatalf("phase2 insert: %v", err)
	}
	assertNeverOnB := func() {
		t.Helper()
		if n := queryCountVia(t, insecure, nodeB.APIAddr, "SELECT count(*) FROM markers WHERE id = '00000000000000000000000000000002'"); n != 0 {
			t.Fatalf("post-expiry marker reached expired node B (count=%d)", n)
		}
	}
	windowEnd := time.Now().Add(window)
	for time.Now().Before(windowEnd) {
		assertNeverOnB()
		if got := connectedPeers(t, cluster.Nodes[a].APIAddr); got != 0 {
			t.Fatalf("connected_peers(A) = %d during isolation window, want 0", got)
		}
		time.Sleep(500 * time.Millisecond)
	}
	assertNeverOnB()
	if n, err := cluster.QueryRowCount(a, "markers"); err != nil || n != 3 {
		t.Fatalf("A markers = %d (err=%v), want 3", n, err)
	}

	// Phase 3: rotate to a fresh cert and assert full reconvergence.
	cluster.StopNode(b)
	freshCertPEM, freshKeyPEM, err := cluster.CA.IssueNode(nodeB.NodeID, 24*time.Hour)
	if err != nil {
		t.Fatalf("mint fresh cert: %v", err)
	}
	writeCertPair(t, nodeB, freshCertPEM, freshKeyPEM)
	cluster.StartNode(b)
	cluster.UnlockNode(b, nodeB.KeyHex)
	cluster.WaitNodeReady(b)
	waitPeerConnected(t, cluster.Nodes[a].APIAddr, nodeB.NodeID.String(), true, 60*time.Second)
	waitConverged(t, cluster, "markers", 3, 60*time.Second)
	// Bidirectional proof: a B-side write converges too.
	if err := cluster.ExecSQL(b, "INSERT INTO markers (id, val) VALUES (?, ?)", fmt.Sprintf("%032x", 3), "rotated"); err != nil {
		t.Fatalf("phase3 insert: %v", err)
	}
	waitConverged(t, cluster, "markers", 4, 60*time.Second)
	d0, err := cluster.ComputeTableDigest(a, "markers", "id")
	if err != nil {
		t.Fatal(err)
	}
	d1, err := cluster.ComputeTableDigest(b, "markers", "id")
	if err != nil {
		t.Fatal(err)
	}
	if d0 != d1 {
		t.Fatalf("post-rotation digests differ: A=%s B=%s", d0, d1)
	}
	t.Logf("rotation healed mesh; digest=%s", d0)
}

func writeCertPair(t *testing.T, n *harness.Node, certPEM, keyPEM []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(n.TLSDir, "node.crt"), certPEM, 0644); err != nil {
		t.Fatalf("write node.crt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(n.TLSDir, "node.key"), keyPEM, 0600); err != nil {
		t.Fatalf("write node.key: %v", err)
	}
}

func parseNotAfter(t *testing.T, certPEM []byte) time.Time {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("decode minted cert PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse minted cert: %v", err)
	}
	return cert.NotAfter
}

// fetchServedCert returns the leaf certificate a node's HTTPS API
// currently serves, without verifying it (it may be expired).
func fetchServedCert(t *testing.T, apiAddr string) *x509.Certificate {
	t.Helper()
	conn, err := tls.Dial("tcp", apiAddr, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // intentional: inspecting the served cert itself
	if err != nil {
		t.Fatalf("dial API %s: %v", apiAddr, err)
		defer conn.Close()
	}
	st := conn.ConnectionState()
	if len(st.PeerCertificates) == 0 {
		t.Fatalf("no served certificate on %s", apiAddr)
	}
	return st.PeerCertificates[0]
}

// insecureAPIClient talks to a node whose server certificate is expired:
// server verification is skipped but a fresh CA-issued client certificate
// is still presented (the daemon verifies clients against its CA).
func insecureAPIClient(t *testing.T, cluster *harness.Cluster) *http.Client {
	t.Helper()
	certPEM, keyPEM, err := cluster.CA.IssueNode(db.NewNodeID(), time.Hour)
	if err != nil {
		t.Fatalf("mint API client cert: %v", err)
	}
	clientCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load API client cert: %v", err)
	}
	return &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // intentional: server cert is the expired subject under test
		Certificates:       []tls.Certificate{clientCert},
	}}}
}

func unlockVia(t *testing.T, client *http.Client, apiAddr, keyHex string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]string{"key_hex": keyHex, "cipher": "chacha20"})
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Post("https://"+apiAddr+"/v1/admin/unlock", "application/json", bytes.NewReader(payload))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("insecure unlock of %s timed out", apiAddr)
}

func waitReadyVia(t *testing.T, client *http.Client, apiAddr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("https://" + apiAddr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("insecure readiness of %s timed out", apiAddr)
}

func queryCountVia(t *testing.T, client *http.Client, apiAddr, query string) int {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"query": query})
	resp, err := client.Post("https://"+apiAddr+"/v1/query", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("insecure query %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("insecure query %s status=%d", apiAddr, resp.StatusCode)
	}
	var res harness.QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("decode insecure query: %v", err)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return 0
	}
	n, _ := strconv.Atoi(fmt.Sprint(res.Rows[0][0]))
	return n
}

type peerJSON struct {
	NodeID    string `json:"NodeID"`
	Connected bool   `json:"Connected"`
}

func peersVia(t *testing.T, client *http.Client, apiAddr string) []peerJSON {
	t.Helper()
	resp, err := client.Get("https://" + apiAddr + "/v1/debug/peers")
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

func peerConnected(peers []peerJSON, nodeID string) (bool, bool) {
	for _, p := range peers {
		if p.NodeID == nodeID {
			return p.Connected, true
		}
	}
	return false, false
}

func waitPeerConnected(t *testing.T, apiAddr, nodeID string, want bool, timeout time.Duration) {
	t.Helper()
	waitPeerConnectedVia(t, http.DefaultClient, apiAddr, nodeID, want, timeout)
}

func waitPeerConnectedVia(t *testing.T, client *http.Client, apiAddr, nodeID string, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, ok := peerConnected(peersVia(t, client, apiAddr), nodeID); ok && got == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("peer %s Connected != %v on %s within %v", nodeID, want, apiAddr, timeout)
}

func connectedPeers(t *testing.T, apiAddr string) int {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/v1/status")
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

func waitConnectedPeers(t *testing.T, apiAddr string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := connectedPeers(t, apiAddr); got == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("connected_peers != %d on %s within %v", want, apiAddr, timeout)
}

func waitConverged(t *testing.T, c *harness.Cluster, table string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, table)
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, table, "id")
			if err != nil {
				ok = false
				break
			}
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
	t.Fatalf("nodes did not converge on %d %s rows with equal digests within %v", want, table, timeout)
}

func envInt(name string, fallback int) int {
	return harness.EnvInt(name, fallback)
}
