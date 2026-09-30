package unlockabuse_test

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// certlessClient trusts the cluster CA but presents no client certificate.
func certlessClient(t *testing.T, cluster *harness.Cluster) *http.Client {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cluster.CA.CertPEM) {
		t.Fatal("invalid test CA")
	}
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs: roots,
		}},
	}
}
