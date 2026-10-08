package unlockabuse_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// NOTE: the suite writes the replicated typed contention fixture table so
// cross-node convergence assertions exercise the replication log;
// node-local writes would make them vacuous (and fail).

const wrongKeyHex = "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0"

func TestUnlockAbuse(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "unlock-abuse",
		NumNodes:        2,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
	})

	// Positive control: honest path works before any abuse.
	baseID := fmt.Sprintf("%032x", 1)
	if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: baseID, Name: "baseline"}); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	waitAllCount(t, cluster, 1, 10*time.Second)
	assertEqualDigests(t, cluster)

	// Lock node0: restart without unlock. Truncate its log so the audit
	// entry asserted below must come from the upcoming unlock.
	node := cluster.Nodes[0]
	cluster.StopNode(0)
	if err := os.Truncate(node.LogFile, 0); err != nil {
		t.Fatalf("truncate node log: %v", err)
	}
	cluster.StartNode(0)
	cluster.WaitNodeReady(0)

	unlockURL := fmt.Sprintf("https://%s/v1/admin/unlock", node.APIAddr)

	// Unauthenticated admin access is refused (certless client).
	noCert := certlessClient(t, cluster)
	for _, tc := range []struct {
		method, path string
		body         []byte
	}{
		{http.MethodPost, "/v1/admin/unlock", []byte(`{"key_hex":"` + node.KeyHex + `"}`)},
		{http.MethodGet, "/v1/admin/status", nil},
		{http.MethodPost, "/v1/admin/lock", []byte(`{}`)},
		{http.MethodGet, "/v1/status", nil},
		{http.MethodPost, "/v1/exec", []byte(`{"query":"SELECT 1"}`)},
	} {
		var body io.Reader
		if tc.body != nil {
			body = bytes.NewReader(tc.body)
		}
		req, err := http.NewRequest(tc.method, "https://"+node.APIAddr+tc.path, body)
		if err != nil {
			t.Fatalf("build %s %s: %v", tc.method, tc.path, err)
		}
		if tc.body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := noCert.Do(req)
		if err != nil {
			t.Fatalf("certless %s %s: %v", tc.method, tc.path, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("certless %s %s status=%d, want 401", tc.method, tc.path, resp.StatusCode)
		}
	}
	// The one documented certless endpoint still answers.
	resp, err := noCert.Get("https://" + node.APIAddr + "/healthz")
	if err != nil {
		t.Fatalf("certless /healthz: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("certless /healthz status=%d, want 200", resp.StatusCode)
	}

	// Wrong-key and unknown-key-id attempts: generic 401s with no oracle.
	wrongKey := unlockAttempt(t, unlockURL, wrongKeyHex, "remote-unlock-key")
	unknownID := unlockAttempt(t, unlockURL, wrongKeyHex, "no-such-key-9")
	rightKeyWrongID := unlockAttempt(t, unlockURL, node.KeyHex, "no-such-key-9")
	assertNoOracle(t, "wrong-key", wrongKey, node)
	assertNoOracle(t, "unknown-key-id", unknownID, node)
	assertNoOracle(t, "right-key-unknown-id", rightKeyWrongID, node)
	if wrongKey.body != unknownID.body || wrongKey.body != rightKeyWrongID.body {
		t.Fatalf("unlock oracle: response bodies differ:\nwrong-key=%q\nunknown-id=%q\nright-key-unknown-id=%q",
			wrongKey.body, unknownID.body, rightKeyWrongID.body)
	}
	if wrongKey.contentType != unknownID.contentType {
		t.Fatalf("unlock oracle: content types differ: %q vs %q", wrongKey.contentType, unknownID.contentType)
	}

	// Honest unlock succeeds (and proves the 401s above reflect real key
	// enforcement, not a wedged node): response differs, node serves data.
	okBody := doUnlock(t, unlockURL, node.KeyHex, "")
	if !strings.Contains(okBody, `"unlocked":true`) {
		t.Fatalf("honest unlock body=%q, want unlocked:true", okBody)
	}
	waitLogContains(t, node.LogFile, "Database unlocked and online", 10*time.Second)

	rows, err := cluster.TypedContentionRows(0)
	if err != nil {
		t.Fatalf("post-unlock count: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("post-unlock count=%d, want 1 (data intact)", len(rows))
	}
	postID := fmt.Sprintf("%032x", 2)
	if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: postID, Name: "post-unlock"}); err != nil {
		t.Fatalf("post-unlock insert: %v", err)
	}
	waitAllCount(t, cluster, 2, 10*time.Second)
	assertEqualDigests(t, cluster)
}

type unlockResult struct {
	status      int
	body        string
	contentType string
}

func unlockAttempt(t *testing.T, url, keyHex, keyID string) unlockResult {
	t.Helper()
	payload, _ := json.Marshal(map[string]string{"key_hex": keyHex, "key_id": keyID, "cipher": "chacha20"})
	resp, err := http.DefaultClient.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("unlock attempt: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return unlockResult{status: resp.StatusCode, body: string(b), contentType: resp.Header.Get("Content-Type")}
}

func doUnlock(t *testing.T, url, keyHex, keyID string) string {
	t.Helper()
	payload, _ := json.Marshal(map[string]string{"key_hex": keyHex, "key_id": keyID, "cipher": "chacha20"})
	resp, err := http.DefaultClient.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("honest unlock: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("honest unlock status=%d body=%q, want 200", resp.StatusCode, string(b))
	}
	return string(b)
}

func assertNoOracle(t *testing.T, name string, r unlockResult, node *harness.Node) {
	t.Helper()
	if r.status != http.StatusUnauthorized {
		t.Fatalf("%s: status=%d, want 401", name, r.status)
	}
	if !strings.HasPrefix(r.body, "unlock failed") {
		t.Fatalf("%s: body=%q, want generic \"unlock failed\" prefix", name, r.body)
	}
	// Only key MATERIAL is secret. Key IDs ("live-key-1",
	// "remote-unlock-key") are non-secret identifiers, and the failure
	// body legitimately names the store's recorded key ID for operator
	// diagnostics. The anti-oracle property is enforced separately by
	// the identical-bodies assertion below: all failure modes must be
	// indistinguishable, so the echoed ID reveals nothing about which
	// IDs exist or which part of the guess was wrong.
	for _, secret := range []string{node.KeyHex, wrongKeyHex} {
		if strings.Contains(r.body, secret) {
			t.Fatalf("%s: body leaks key material %q: %q", name, secret, r.body)
		}
	}
}

func waitAllCount(t *testing.T, cluster *harness.Cluster, expected int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allMatch := true
		for i := range cluster.Nodes {
			rows, err := cluster.TypedContentionRows(i)
			if err != nil || len(rows) != expected {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cluster failed to converge to %d rows within %v", expected, timeout)
}

func assertEqualDigests(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	var digests []string
	for i := range cluster.Nodes {
		rows, err := cluster.TypedContentionRows(i)
		if err != nil {
			t.Fatalf("node %d digest: %v", i, err)
		}
		sort.Slice(rows, func(a, b int) bool { return rows[a].ID < rows[b].ID })
		h := sha256.New()
		for _, row := range rows {
			fmt.Fprintf(h, "%s:%s:%s:%d\n", row.ID, row.Name, row.Phone, row.Score)
		}
		digests = append(digests, hex.EncodeToString(h.Sum(nil)))
	}
	for i := 1; i < len(digests); i++ {
		if digests[i] != digests[0] {
			t.Fatalf("node %d digest %s != node 0 digest %s", i, digests[i], digests[0])
		}
	}
}

func waitLogContains(t *testing.T, path, substr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(b), substr) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("log %s missing %q within %v", path, substr, timeout)
}
