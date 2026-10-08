// TLS-floor acceptance: the daemon HTTPS API refuses TLS below 1.2.
//
// Live proofs in this suite:
//  1. API negative: an mTLS client pinned to TLS 1.1 (MinVersion and
//     MaxVersion both 1.1, so the ClientHello is genuinely emitted)
//     fails the handshake against a node's API; the failure must be a
//     server-side rejection ("remote error"), not a local config error.
//     TLS 1.0 is probed the same way.
//  2. API positive: the identical client shape at TLS 1.2 succeeds and
//     reaches the live status endpoint; the negotiated version is asserted to be 1.2.
//  3. Mesh positive control: a 2-node cluster converges a marker over
//     QUIC (TLS 1.3), proving the floor does not break replication.
//
// QUIC replication floor gap (documented, not silently skipped): the raw
// negative QUIC dial with TLS MaxVersion 1.2 was attempted and is
// infeasible as a live server probe: the repo's quic-go stack
// (github.com/quic-go/quic-go v0.63.0) refuses the dial locally in under
// a millisecond ("tls: no supported versions satisfy MinVersion and
// MaxVersion") without emitting any packet, because QUIC mandates
// TLS 1.3 (RFC 9001) and the stack pins it internally. No client that can
// emit a sub-1.3 QUIC handshake exists, so a server-side QUIC floor probe
// cannot be constructed. As a compile-coupled guard instead, the suite
// asserts the product's own QUIC TLS constructors
// (transport.Credentials ServerTLSConfig/ClientTLSConfig, backed by
// transport/quic.go) pin MinVersion to TLS 1.3; the converged live mesh
// is the TLS 1.3 positive control.
package tlsfloor_test

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/tests-live/harness"
	"github.com/marcgauthier/murmur/transport"
)

func TestAPIFloorRejectsBelowTLS12(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "tls-floor",
		NumNodes:     2,
		AwaitUnlock:  true,
		TypedRecords: true,
	})
	node := cluster.Nodes[0]

	// Mesh positive control: marker converges over QUIC (TLS 1.3).
	if err := cluster.TypedInsert(0, "tls-version-floor-marker"); err != nil {
		t.Fatalf("marker insert: %v", err)
	}
	waitConverged(t, cluster, 1, 60*time.Second)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cluster.CA.CertPEM) {
		t.Fatal("parse cluster CA")
	}
	clientCert, err := tls.LoadX509KeyPair(
		filepath.Join(node.TLSDir, "node.crt"),
		filepath.Join(node.TLSDir, "node.key"),
	)
	if err != nil {
		t.Fatalf("load node client cert: %v", err)
	}
	baseURL := "https://" + node.APIAddr

	// API negative: TLS 1.1 and 1.0 handshakes must fail with a
	// server-side rejection. Both Min and Max are pinned so the client
	// genuinely emits the old ClientHello (Go's default client minimum
	// is 1.2, so Max-only pinning would fail locally and prove nothing).
	for _, ver := range []uint16{tls.VersionTLS11, tls.VersionTLS10} {
		client := &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				RootCAs: roots, Certificates: []tls.Certificate{clientCert},
				MinVersion: ver, MaxVersion: ver,
			}},
		}
		resp, err := client.Get(baseURL + "/v1/status")
		if err == nil {
			_ = resp.Body.Close()
			t.Fatalf("TLS %#04x client unexpectedly connected (status=%d)", ver, resp.StatusCode)
		}
		t.Logf("TLS %#04x rejected: %v", ver, err)
		if !isRemoteRejection(err) {
			t.Fatalf("TLS %#04x failure is not a server-side rejection (%v); "+
				"the toolchain cannot emit this version, so the floor is unproven", ver, err)
		}
	}

	// API positive: the same client at TLS 1.2 reaches a live typed node.
	conn, err := tls.Dial("tcp", node.APIAddr, &tls.Config{
		RootCAs: roots, Certificates: []tls.Certificate{clientCert},
		MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("TLS 1.2 mTLS dial: %v", err)
	}
	negotiated := conn.ConnectionState().Version
	_ = conn.Close()
	if negotiated != tls.VersionTLS12 {
		t.Fatalf("negotiated version = %#04x, want TLS 1.2", negotiated)
	}
	tls12Client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs: roots, Certificates: []tls.Certificate{clientCert},
			MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12,
		}},
	}
	resp, err := tls12Client.Get(baseURL + "/v1/status")
	if err != nil {
		t.Fatalf("TLS 1.2 status: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("TLS 1.2 status=%d, want 200", resp.StatusCode)
	}
	resp, err = tls12Client.Get(baseURL + "/v1/status")
	if err != nil {
		t.Fatalf("TLS 1.2 status: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("TLS 1.2 status=%d, want 200", resp.StatusCode)
	}
	t.Logf("TLS 1.2 client reached the typed API successfully")
}

// TestQUICFloorPinsTLS13 guards the replication floor at the product's
// QUIC TLS constructors: both the server (listener) and client (dialer)
// configs must pin MinVersion to TLS 1.3. A live sub-1.3 QUIC probe is
// infeasible (see the file header); the converged 2-node mesh in
// TestAPIFloorRejectsBelowTLS12 is the live TLS 1.3 positive control.
func TestQUICFloorPinsTLS13(t *testing.T) {
	nodeID := db.NewNodeID()
	ca, err := transport.GenerateCA(time.Hour)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	certPEM, keyPEM, err := ca.IssueNode(nodeID, time.Hour)
	if err != nil {
		t.Fatalf("mint cert: %v", err)
	}
	creds, err := transport.CredentialsFromPEM(certPEM, keyPEM, ca.CertPEM, nil)
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}
	if got := creds.ServerTLSConfig().MinVersion; got != tls.VersionTLS13 {
		t.Fatalf("QUIC server MinVersion = %#04x, want TLS 1.3", got)
	}
	if got := creds.ClientTLSConfig(nodeID).MinVersion; got != tls.VersionTLS13 {
		t.Fatalf("QUIC client MinVersion = %#04x, want TLS 1.3", got)
	}
}

// isRemoteRejection reports whether a handshake failure came from the
// server (a rejection alert) rather than from local config validation.
func isRemoteRejection(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "remote error") ||
		strings.Contains(msg, "protocol version") ||
		strings.Contains(msg, "handshake failure")
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first []string
		for i := range c.Nodes {
			names, err := c.TypedNames(i)
			if err != nil || len(names) != want {
				ok = false
				break
			}
			if i == 0 {
				first = names
			} else if !reflect.DeepEqual(names, first) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %d typed records within %v", want, timeout)
}
