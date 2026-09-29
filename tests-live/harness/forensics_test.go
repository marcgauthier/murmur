package harness

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDumpForensicsWritesFiles proves the shared forensics helper
// captures every endpoint to per-node files without a live daemon.
func TestDumpForensicsWritesFiles(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "body-for-%s", r.URL.Path)
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "https://")
	liveAPIRouter.mu.Lock()
	liveAPIRouter.byTarget[host] = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	liveAPIRouter.mu.Unlock()
	defer func() {
		liveAPIRouter.mu.Lock()
		delete(liveAPIRouter.byTarget, host)
		liveAPIRouter.mu.Unlock()
	}()

	dir := t.TempDir()
	c := &Cluster{T: t, Name: "forensics-unit", Nodes: []*Node{{APIAddr: host, Dir: dir}}}
	c.DumpForensics("unit")

	for name, wantPath := range map[string]string{
		"peers.json":  "/v1/debug/peers",
		"metrics.txt": "/metrics",
		"stacks.txt":  "/v1/debug/stacks",
	} {
		raw, err := os.ReadFile(filepath.Join(dir, "forensics-unit-"+name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(raw), "body-for-"+wantPath) {
			t.Fatalf("%s = %q, want body for %s", name, raw, wantPath)
		}
	}
}

// TestFetchPathUnreachableNeverFails proves forensics survives dead
// nodes: unreachable endpoints yield "" instead of failing or hanging.
func TestFetchPathUnreachableNeverFails(t *testing.T) {
	c := &Cluster{T: t, Name: "forensics-dead", Nodes: []*Node{{APIAddr: "127.0.0.1:1", Dir: t.TempDir()}}}
	if got := c.FetchPath(0, "/v1/status"); got != "" {
		t.Fatalf("FetchPath(dead) = %q, want empty", got)
	}
	if got := c.FetchPath(99, "/v1/status"); got != "" {
		t.Fatalf("FetchPath(bad index) = %q, want empty", got)
	}
	// Full dump over a dead node must also stay silent and fatal-free.
	c.DumpForensics("dead")
}
