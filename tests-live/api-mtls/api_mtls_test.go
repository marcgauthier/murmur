package apimtls_test

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/tests-live/harness"
	"github.com/marcgauthier/murmur/transport"
)

func TestDaemonAPIRequiresMTLSExceptHealth(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{Name: "api-mtls", NumNodes: 1})
	node := cluster.Nodes[0]
	cluster.WaitNodeReady(0)
	baseURL := "https://" + node.APIAddr

	noCert := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs: clusterRoots(t, cluster.CA.CertPEM), MinVersion: tls.VersionTLS12,
		}},
	}
	for _, path := range []string{"/v1/status", "/metrics"} {
		resp, err := noCert.Get(baseURL + path)
		if err != nil {
			t.Fatalf("request %s without client certificate: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("request %s without client certificate status=%d, want 401", path, resp.StatusCode)
		}
	}
	resp, err := noCert.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("HTTPS health without client certificate: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTPS health status=%d, want 200", resp.StatusCode)
	}

	rogueCA, err := transport.GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	rogueCertPEM, rogueKeyPEM, err := rogueCA.IssueNode(db.NewNodeID(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	rogueCert, err := tls.X509KeyPair(rogueCertPEM, rogueKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	wrongCA := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs: clusterRoots(t, cluster.CA.CertPEM), Certificates: []tls.Certificate{rogueCert},
		}},
	}
	resp, err = wrongCA.Get(baseURL + "/v1/status")
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("untrusted client certificate status=%d, want rejection", resp.StatusCode)
		}
	}
	expiredCertPEM, expiredKeyPEM, err := cluster.CA.IssueNode(db.NewNodeID(), -time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	expiredCert, err := tls.X509KeyPair(expiredCertPEM, expiredKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	expiredClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs: clusterRoots(t, cluster.CA.CertPEM), Certificates: []tls.Certificate{expiredCert},
		}},
	}
	if resp, err = expiredClient.Get(baseURL + "/v1/status"); err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expired client certificate status=%d, want rejection", resp.StatusCode)
		}
	}

	resp, err = http.DefaultClient.Get(baseURL + "/v1/status")
	if err != nil {
		t.Fatalf("trusted client certificate could not access API: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("trusted client certificate status=%d, want 200", resp.StatusCode)
	}
	cli := exec.Command(cluster.BinaryPath, "status",
		"--api", baseURL,
		"--ca-cert", filepath.Join(node.TLSDir, "ca.crt"),
		"--client-cert", filepath.Join(node.TLSDir, "node.crt"),
		"--client-key", filepath.Join(node.TLSDir, "node.key"),
	)
	cliOut, err := cli.CombinedOutput()
	if err != nil || !strings.Contains(string(cliOut), "node_id") {
		t.Fatalf("mTLS status CLI failed: err=%v output=%s", err, cliOut)
	}
	badCLI := exec.Command(cluster.BinaryPath, "status",
		"--api", fmt.Sprintf("http://%s", node.APIAddr),
		"--ca-cert", filepath.Join(node.TLSDir, "ca.crt"),
		"--client-cert", filepath.Join(node.TLSDir, "node.crt"),
		"--client-key", filepath.Join(node.TLSDir, "node.key"),
	)
	if err := badCLI.Run(); err == nil {
		t.Fatal("CLI accepted a plaintext HTTP API URL")
	}
	plainResp, err := http.Get("http://" + node.APIAddr + "/healthz")
	if err == nil {
		plainResp.Body.Close()
		if plainResp.StatusCode == http.StatusOK {
			t.Fatal("plaintext HTTP unexpectedly reached the HTTPS API")
		}
	}
}

func clusterRoots(t *testing.T, caPEM []byte) *x509.CertPool {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid test CA")
	}
	return roots
}
