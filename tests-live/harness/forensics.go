package harness

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// forensicsClient bounds every dump fetch: a wedged daemon may never
// answer, and an unbounded fetch would hang the suite past its own
// deadline into the go test timeout.
var forensicsClient = &http.Client{Timeout: 10 * time.Second}

// DumpForensics captures per-node diagnostic state for post-failure
// analysis: status and peer summaries go to the test log (truncated),
// while full peer states, metrics, and goroutine stacks go to per-node
// files under each node directory (preserved with failure artifacts).
// Every fetch is time-bounded and a fetch failure is logged, never
// fatal: forensics must survive dead and wedged nodes alike. The
// stacks endpoint takes no daemon locks, so it answers even when the
// status endpoint hangs on a wedged mutex.
func (c *Cluster) DumpForensics(reason string) {
	for idx, node := range c.Nodes {
		label := fmt.Sprintf("node%d", idx+1)
		c.T.Logf("forensics(%s): %s status: %s", reason, label, truncateString(c.FetchPath(idx, "/v1/status"), 2048))
		peers := c.FetchPath(idx, "/v1/debug/peers")
		c.T.Logf("forensics(%s): %s peers: %s", reason, label, truncateString(peers, 8192))
		c.writeForensicFile(node, reason, "peers.json", peers)
		c.writeForensicFile(node, reason, "metrics.txt", c.FetchPath(idx, "/metrics"))
		if stacks := c.FetchPath(idx, "/v1/debug/stacks"); stacks != "" {
			c.writeForensicFile(node, reason, "stacks.txt", stacks)
		}
	}
}

// FetchPath returns the full body of a daemon HTTP endpoint, or "" when
// the node is unreachable or the fetch fails. It never fails the test.
func (c *Cluster) FetchPath(idx int, path string) string {
	if idx < 0 || idx >= len(c.Nodes) {
		return ""
	}
	resp, err := forensicsClient.Get("https://" + c.Nodes[idx].APIAddr + path)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return ""
	}
	return string(raw)
}

func (c *Cluster) writeForensicFile(node *Node, reason, name, body string) {
	if body == "" {
		return
	}
	path := filepath.Join(node.Dir, "forensics-"+reason+"-"+name)
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		c.T.Logf("forensics(%s): %s write failed: %v", reason, name, err)
	}
}

func truncateString(s string, n int) string {
	if len(s) > n {
		return s[:n] + "...<truncated>"
	}
	return s
}
