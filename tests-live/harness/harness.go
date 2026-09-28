package harness

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	db "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/transport"
)

// Node represents a running spedsql daemon instance in the test cluster.
type Node struct {
	Index         int
	Label         string
	NodeID        db.NodeID
	Dir           string
	PebbleDir     string
	LogsDir       string
	SchemaDir     string
	TLSDir        string
	ConfigFile    string
	LogFile       string
	ReplAddr      string
	APIAddr       string
	FetchAddr     string
	KeyHex        string
	KeyID         string
	AdminToken    string
	Process       *exec.Cmd
	LogFileWriter *os.File
}

// Cluster manages multiple discrete node instances in their own directories.
type Cluster struct {
	T          *testing.T
	Name       string
	RuntimeDir string
	BinaryPath string
	DBID       db.DBID
	CA         *transport.CA
	Nodes      []*Node
	failed     bool
}

// ClusterOptions configure the live test cluster.
type ClusterOptions struct {
	Name                  string
	NumNodes              int
	AwaitUnlock           bool
	Schema                *db.SchemaConfig
	SchemaSQL             string
	BaseReplPort          int
	BaseAPIPort           int
	AllowedNetworks       []string
	AllowedNetworksByNode map[int][]string
	AllowedPeersByNode    map[int][]db.NodeID
	NodeIDs               []db.NodeID
	Files                 *FilesOptions
	Bridge                *BridgeOptions
	// ManualPeers omits the automatic full-mesh peer list so the test can
	// establish peering later via AddPeer (delayed-mesh scenarios).
	ManualPeers bool
}

var liveHTTPClient = &http.Client{Timeout: 15 * time.Second}

// NewCluster spins up a multi-process live test cluster with discrete node folders.
func NewCluster(t *testing.T, opts ClusterOptions) *Cluster {
	t.Helper()

	if opts.Name == "" {
		opts.Name = t.Name()
	}
	if opts.NumNodes <= 0 {
		opts.NumNodes = 3
	}
	if opts.BaseReplPort <= 0 {
		opts.BaseReplPort = 17440
	}
	if opts.BaseAPIPort <= 0 {
		opts.BaseAPIPort = 18080
	}

	// Locate spedsql binary, build if not found
	binPath := findOrBuildSpedSQL(t)

	// Runtime root: the override env names a root shared by concurrent
	// runs, so each cluster still nests under its own name (as does the
	// default) to keep multi-cluster scenarios from clobbering each other.
	runtimeRoot := os.Getenv("SPEDSQL_LIVE_RUNTIME")
	if runtimeRoot == "" {
		runtimeRoot = filepath.Join(repoRoot(t), "tests-live", "runtime", opts.Name)
	} else {
		runtimeRoot = filepath.Join(runtimeRoot, opts.Name)
	}
	_ = os.RemoveAll(runtimeRoot)
	_ = os.MkdirAll(runtimeRoot, 0755)

	ca, err := transport.GenerateCA(24 * time.Hour)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}

	dbID := db.NewDBID()
	cluster := &Cluster{
		T:          t,
		Name:       opts.Name,
		RuntimeDir: runtimeRoot,
		BinaryPath: binPath,
		DBID:       dbID,
		CA:         ca,
	}

	t.Cleanup(func() {
		cluster.Cleanup()
	})

	labels := []string{"node1", "node2", "node3", "node4", "node5", "node6", "node7", "node8"}
	for i := 0; i < opts.NumNodes; i++ {
		label := fmt.Sprintf("node%d", i+1)
		if i < len(labels) {
			label = labels[i]
		}
		nodeDir := filepath.Join(runtimeRoot, label)
		pebbleDir := filepath.Join(nodeDir, "pebble")
		logsDir := filepath.Join(nodeDir, "logs")
		schemaDir := filepath.Join(nodeDir, "schema")
		tlsDir := filepath.Join(nodeDir, "tls")

		_ = os.MkdirAll(pebbleDir, 0755)
		_ = os.MkdirAll(logsDir, 0755)
		_ = os.MkdirAll(schemaDir, 0755)
		_ = os.MkdirAll(tlsDir, 0755)

		nodeID := db.NewNodeID()
		if i < len(opts.NodeIDs) && opts.NodeIDs[i] != (db.NodeID{}) {
			nodeID = opts.NodeIDs[i]
		}
		certPEM, keyPEM, err := ca.IssueNode(nodeID, 24*time.Hour)
		if err != nil {
			t.Fatalf("issue node cert: %v", err)
		}

		caFile := filepath.Join(tlsDir, "ca.crt")
		certFile := filepath.Join(tlsDir, "node.crt")
		keyFile := filepath.Join(tlsDir, "node.key")

		_ = os.WriteFile(caFile, ca.CertPEM, 0644)
		_ = os.WriteFile(certFile, certPEM, 0644)
		_ = os.WriteFile(keyFile, keyPEM, 0600)

		if opts.SchemaSQL != "" {
			_ = os.WriteFile(filepath.Join(schemaDir, "schema.sql"), []byte(opts.SchemaSQL), 0644)
		}

		replPort := getFreePort(t)
		apiPort := getFreePort(t)
		replAddr := fmt.Sprintf("127.0.0.1:%d", replPort)
		apiAddr := fmt.Sprintf("127.0.0.1:%d", apiPort)
		fetchAddr := ""
		if opts.Files != nil {
			fetchAddr = fmt.Sprintf("127.0.0.1:%d", getFreePort(t))
		}

		keyHex := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		adminToken := "0123456789abcdef0123456789abcdef"

		node := &Node{
			Index:      i,
			Label:      label,
			NodeID:     nodeID,
			Dir:        nodeDir,
			PebbleDir:  pebbleDir,
			LogsDir:    logsDir,
			SchemaDir:  schemaDir,
			TLSDir:     tlsDir,
			ConfigFile: filepath.Join(nodeDir, "config.json"),
			LogFile:    filepath.Join(logsDir, "node.log"),
			ReplAddr:   replAddr,
			APIAddr:    apiAddr,
			FetchAddr:  fetchAddr,
			KeyHex:     keyHex,
			KeyID:      "live-key-1",
			AdminToken: adminToken,
		}
		cluster.Nodes = append(cluster.Nodes, node)
	}

	// Write config files connecting all nodes as peers
	for i, node := range cluster.Nodes {
		var peerConfigs []map[string]any
		if !opts.ManualPeers {
			for j, peer := range cluster.Nodes {
				if i != j {
					peerConfigs = append(peerConfigs, map[string]any{
						"node_id": peer.NodeID.String(),
						"addrs":   []string{peer.ReplAddr},
					})
				}
			}
		}

		allowedNetworks := opts.AllowedNetworks
		if perNode, ok := opts.AllowedNetworksByNode[i]; ok {
			allowedNetworks = perNode
		}
		var allowedPeers []string
		for _, peerID := range opts.AllowedPeersByNode[i] {
			allowedPeers = append(allowedPeers, peerID.String())
		}
		cfgJSON := map[string]any{
			"node_id":            node.NodeID.String(),
			"db_id":              cluster.DBID.String(),
			"data_dir":           node.Dir,
			"listen_addr":        node.ReplAddr,
			"api_addr":           node.APIAddr,
			"await_unlock":       opts.AwaitUnlock,
			"key_hex":            node.KeyHex,
			"key_id":             node.KeyID,
			"admin_token":        node.AdminToken,
			"service_token":      "0123456789abcdef0123456789abcdef",
			"schema_path":        node.SchemaDir,
			"tls_ca_cert_file":   filepath.Join(node.TLSDir, "ca.crt"),
			"tls_node_cert_file": filepath.Join(node.TLSDir, "node.crt"),
			"tls_node_key_file":  filepath.Join(node.TLSDir, "node.key"),
			"peers":              peerConfigs,
			"allowed_networks":   allowedNetworks,
			"allowed_peers":      allowedPeers,
		}
		if opts.Schema != nil {
			cfgJSON["schema"] = opts.Schema
		}
		if opts.Files != nil {
			var fetchPeers []map[string]any
			if i > 0 {
				hub := cluster.Nodes[0]
				fetchPeers = append(fetchPeers, map[string]any{
					"node_id": hub.NodeID.String(),
					"addrs":   []string{hub.FetchAddr},
				})
			}
			cfgJSON["files"] = map[string]any{
				"enabled":           true,
				"object_key_hex":    opts.Files.ObjectKeyHex,
				"fetch_addr":        node.FetchAddr,
				"fetch_peers":       fetchPeers,
				"fetch_interval_ms": opts.Files.FetchIntervalMs,
				"max_file_bytes":    opts.Files.MaxFileBytes,
			}
		}
		if opts.Bridge != nil {
			for _, bi := range opts.Bridge.BridgeNodes() {
				if i != bi {
					continue
				}
				b := opts.Bridge
				cfgJSON["bridge"] = map[string]any{
					"role":               b.Role,
					"stream":             b.Stream,
					"staging_dir":        b.StagingDir,
					"outbox_dir":         filepath.Join(node.Dir, "outbox"),
					"inbox_dir":          filepath.Join(node.Dir, "inbox"),
					"signer_key_file":    b.SignerKeyFile,
					"recipient_pub_file": b.RecipientPubFile,
					"recipient_key_file": b.RecipientKeyFile,
					"signer_pub_file":    b.SignerPubFile,
					"signer_pub_files":   b.SignerPubFiles,
				}
			}
		}

		data, err := json.MarshalIndent(cfgJSON, "", "  ")
		if err != nil {
			t.Fatalf("marshal config: %v", err)
		}
		if err := os.WriteFile(node.ConfigFile, data, 0644); err != nil {
			t.Fatalf("write config: %v", err)
		}
	}

	// Start all nodes
	for _, node := range cluster.Nodes {
		cluster.StartNode(node.Index)
		if opts.AwaitUnlock {
			cluster.UnlockNode(node.Index, node.KeyHex)
		}
		cluster.WaitNodeReady(node.Index)
	}

	return cluster
}

func (c *Cluster) StartNode(idx int) {
	c.T.Helper()
	node := c.Nodes[idx]

	logFile, err := os.OpenFile(node.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		c.T.Fatalf("open log file %s: %v", node.LogFile, err)
	}
	node.LogFileWriter = logFile

	cmd := exec.Command(c.BinaryPath, "agent", "--config", node.ConfigFile)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		c.T.Fatalf("start node %s: %v", node.Label, err)
	}
	node.Process = cmd
}

func (c *Cluster) StopNode(idx int) {
	node := c.Nodes[idx]
	if node.Process != nil && node.Process.Process != nil {
		_ = node.Process.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- node.Process.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = node.Process.Process.Kill()
			<-done
		}
		node.Process = nil
	}
	if node.LogFileWriter != nil {
		_ = node.LogFileWriter.Close()
		node.LogFileWriter = nil
	}
}

func (c *Cluster) KillNode(idx int) {
	node := c.Nodes[idx]
	if node.Process != nil && node.Process.Process != nil {
		_ = node.Process.Process.Kill()
		_ = node.Process.Wait()
		node.Process = nil
	}
	if node.LogFileWriter != nil {
		_ = node.LogFileWriter.Close()
		node.LogFileWriter = nil
	}
}

func (c *Cluster) UnlockNode(idx int, keyHex string) {
	c.T.Helper()
	node := c.Nodes[idx]

	payload, _ := json.Marshal(map[string]string{
		"key_hex": keyHex,
		"cipher":  "chacha20",
	})

	url := fmt.Sprintf("http://%s/v1/admin/unlock", node.APIAddr)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := liveHTTPClient.Post(url, "application/json", bytes.NewReader(payload))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.T.Fatalf("failed to unlock node %s at %s within timeout", node.Label, url)
}

func (c *Cluster) WaitNodeReady(idx int) {
	c.T.Helper()
	node := c.Nodes[idx]
	url := fmt.Sprintf("http://%s/healthz", node.APIAddr)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := liveHTTPClient.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.T.Fatalf("node %s at %s failed readiness check", node.Label, url)
}

func (c *Cluster) ExecSQL(idx int, query string, args ...any) error {
	node := c.Nodes[idx]
	url := fmt.Sprintf("http://%s/v1/exec", node.APIAddr)
	payload, err := json.Marshal(map[string]any{
		"query": query,
		"args":  args,
	})
	if err != nil {
		return err
	}
	resp, err := liveHTTPClient.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("node %s exec POST: %w", node.Label, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("node %s exec failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	return nil
}

type QueryResult struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

func (c *Cluster) QuerySQL(idx int, query string, args ...any) (*QueryResult, error) {
	node := c.Nodes[idx]
	url := fmt.Sprintf("http://%s/v1/query", node.APIAddr)
	payload, err := json.Marshal(map[string]any{
		"query": query,
		"args":  args,
	})
	if err != nil {
		return nil, err
	}
	resp, err := liveHTTPClient.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("node %s query POST: %w", node.Label, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("node %s query failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	var res QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *Cluster) QueryRowCount(idx int, table string) (int, error) {
	res, err := c.QuerySQL(idx, fmt.Sprintf("SELECT count(*) FROM %s", table))
	if err != nil {
		return 0, err
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return 0, nil
	}
	val := fmt.Sprintf("%v", res.Rows[0][0])
	var count int
	_, _ = fmt.Sscanf(val, "%d", &count)
	return count, nil
}

func (c *Cluster) AddPeer(fromIdx, targetIdx int) error {
	fromNode := c.Nodes[fromIdx]
	targetNode := c.Nodes[targetIdx]
	url := fmt.Sprintf("http://%s/v1/admin/add_peer", fromNode.APIAddr)
	payload, _ := json.Marshal(map[string]any{
		"node_id": targetNode.NodeID.String(),
		"addrs":   []string{targetNode.ReplAddr},
	})
	resp, err := liveHTTPClient.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add peer failed (%d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *Cluster) RemovePeer(fromIdx, targetIdx int) error {
	fromNode := c.Nodes[fromIdx]
	targetNode := c.Nodes[targetIdx]
	url := fmt.Sprintf("http://%s/v1/admin/remove_peer", fromNode.APIAddr)
	payload, _ := json.Marshal(map[string]any{
		"node_id": targetNode.NodeID.String(),
	})
	resp, err := liveHTTPClient.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remove peer failed (%d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *Cluster) ComputeTableDigest(idx int, table string, orderBy string) (string, error) {
	res, err := c.QuerySQL(idx, fmt.Sprintf("SELECT * FROM %s ORDER BY %s", table, orderBy))
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, row := range res.Rows {
		for _, cell := range row {
			h.Write([]byte(fmt.Sprintf("%v:", cell)))
		}
		h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (c *Cluster) MarkFailed(reason string) {
	c.failed = true
	c.T.Errorf("[LIVE-FAIL] %s", reason)
}

func (c *Cluster) Cleanup() {
	for i := range c.Nodes {
		c.StopNode(i)
	}
	if c.T.Failed() || c.failed {
		failuresDir := filepath.Join(repoRoot(c.T), "tests-live", "failures")
		_ = os.MkdirAll(failuresDir, 0755)
		dest := filepath.Join(failuresDir, fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102T150405Z"), c.Name))
		_ = os.Rename(c.RuntimeDir, dest)
		fmt.Printf("RESULT: FAIL scenario=%s | retained artifacts at %s\n", c.Name, dest)
	} else {
		_ = os.RemoveAll(c.RuntimeDir)
		fmt.Printf("RESULT: PASS scenario=%s (runtime cleaned up)\n", c.Name)
	}
}

func findOrBuildSpedSQL(t *testing.T) string {
	t.Helper()
	root := repoRoot(t)
	bin := filepath.Join(root, "bin", "spedsql")
	if _, err := os.Stat(bin); err == nil {
		return bin
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/spedsql")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build spedsql binary: %v, output: %s", err, string(out))
	}
	return bin
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repository root")
		}
		dir = parent
	}
}

// claimedPorts tracks ephemeral ports already handed to node configs in
// this test process. Parallel daemon suites churn thousands of sockets
// (TIME_WAIT pileup), so bare bind(:0)-then-close allocation can hand the
// same port to two nodes and silently break the mesh; the registry makes
// intra-process duplicates impossible by construction.
var (
	claimedPortsMu sync.Mutex
	claimedPorts   = make(map[int]bool)
)

// claimPort records port as allocated for a node config. It reports false
// when the port is unusable or was already claimed, in which case the
// caller must draw another.
func claimPort(port int) bool {
	if port <= 0 || port > 65535 {
		return false
	}
	claimedPortsMu.Lock()
	defer claimedPortsMu.Unlock()
	if claimedPorts[port] {
		return false
	}
	claimedPorts[port] = true
	return true
}

func getFreePort(t *testing.T) int {
	t.Helper()
	// Retry a bounded number of draws: under ephemeral-port pressure the
	// kernel recycles aggressively and repeats are likely.
	for i := 0; i < 50; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen for free port: %v", err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
		if claimPort(port) {
			return port
		}
	}
	t.Fatal("could not draw an unclaimed ephemeral port after 50 attempts")
	return 0
}
