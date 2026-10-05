package harness

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/transport"
)

// Node represents a running murmur daemon instance in the test cluster.
type Node struct {
	OriginKey ed25519.PrivateKey
	Index     int
	Label     string
	NodeID    db.NodeID
	// BinaryPath is the daemon executable this node starts. It
	// defaults to the cluster binary; SetNodeBinary switches it
	// while the node is stopped (rolling-upgrade tests).
	BinaryPath    string
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
	Process       *exec.Cmd
	LogFileWriter *os.File
	// Pids records every daemon PID started for this node so Cleanup
	// can reap stragglers a normal stop missed.
	pidMu sync.Mutex
	Pids  []int
}

// Cluster manages multiple discrete node instances in their own directories.
type Cluster struct {
	originMu   sync.Mutex
	originKeys map[db.NodeID]ed25519.PrivateKey
	T          *testing.T
	Name       string
	RuntimeDir string
	BinaryPath string
	DBID       db.DBID
	CA         *transport.CA
	Nodes      []*Node
	nodeEnv    map[int][]string
	failed     bool
}

// ClusterOptions configure the live test cluster.
type ClusterOptions struct {
	// TrustedSnapshotSourcesByNode overrides the explicit all-node fixture policy.
	// An explicitly empty entry disables snapshots for that receiver.
	TrustedSnapshotSourcesByNode map[int][]int
	Name                         string
	NumNodes                     int
	AwaitUnlock                  bool
	Schema                       *db.SchemaConfig
	SchemaSQL                    string
	BaseReplPort                 int
	BaseAPIPort                  int
	AllowedNetworks              []string
	AllowedNetworksByNode        map[int][]string
	AllowedPeersByNode           map[int][]db.NodeID
	NodeIDs                      []db.NodeID
	Files                        *FilesOptions
	Bridge                       *BridgeOptions
	// ManualPeers omits the automatic full-mesh peer list so the test can
	// establish peering later via AddPeer (delayed-mesh scenarios).
	ManualPeers bool
	// Replication carries optional retention overrides for scenarios
	// that must force log expiry (snapshot resync, GC gating). Nil
	// selects production defaults.
	Replication *ReplicationOptions
	// Limits carries optional commit-path budget overrides for
	// scenarios that must deterministically trip budget rejection.
	// Nil selects production defaults.
	Limits *LimitsOptions
	// Pebble carries optional storage overrides for scenarios that
	// must shrink the cache/memtables or stall compactions. Nil
	// selects production defaults.
	Pebble *PebbleOptions
	// PebbleByNode replaces the pebble section per node. Nodes with
	// no entry fall back to opts.Pebble; a nil entry and nil base
	// omit the section.
	PebbleByNode map[int]*PebbleOptions
	// NodeEnv appends KEY=VALUE pairs to the daemon process
	// environment per node (resource-pressure scenarios, e.g.
	// GOMEMLIMIT). Entries replace same-key inherited variables.
	NodeEnv map[int][]string
	// DisseminationByNode overrides the dissemination mode per node
	// (mixed-mode capability tests). Empty entries inherit
	// Replication.Dissemination (or the default when unset).
	DisseminationByNode map[int]string
	// ReplicationByNode replaces the replication section per node
	// (version-skew tests). Nodes with no entry fall back to
	// opts.Replication; a nil entry and nil base omit the section.
	ReplicationByNode map[int]*ReplicationOptions
	// BridgeByNode overrides the bridge configuration per node
	// (multi-stream importers). Entries replace opts.Bridge for that
	// node; nodes with no entry fall back to opts.Bridge.
	BridgeByNode map[int]*BridgeOptions
	// Bootstrap is an optional list of QUIC seed addresses for dynamic discovery across nodes.
	Bootstrap []string
	// BootstrapByNode overrides the bootstrap seed addresses per node.
	BootstrapByNode map[int][]string
	// BootstrapSeeds specifies node indices (e.g. []int{0}) that act as bootstrap seeds.
	// All cluster nodes have membership enabled, and non-seed nodes automatically
	// receive the seed nodes' replication addresses in their bootstrap configuration.
	BootstrapSeeds []int
	// BinaryByNode overrides the daemon executable per node
	// (mixed-version clusters: old binaries alongside the
	// current build). Empty entries inherit the cluster binary.
	BinaryByNode map[int]string
}

// ReplicationOptions mirrors the daemon's replication config section.
// Zero values select production defaults; positive values override.
type ReplicationOptions struct {
	MinLogRetentionMs        int64
	MaxOfflineLogRetentionMs int64
	MinRetainedBatches       uint64
	Dissemination            string
	// ProtocolVersionOverride/MinProtocolVersionOverride replace the
	// advertised handshake versions (interoperability testing only).
	ProtocolVersionOverride    uint16
	MinProtocolVersionOverride uint16
}

// LimitsOptions mirrors the daemon's limits config section (commit-path
// budgets). Zero values select production defaults.
type LimitsOptions struct {
	MaxValueBytes       int
	MaxTransactionBytes int64
	MaxBatchMutations   int
}

// PebbleOptions mirrors the daemon's pebble config section (storage
// sizing and compaction control). Zero values select production
// defaults; per-node entries replace the cluster-wide section.
type PebbleOptions struct {
	CacheBytes                  int64
	MemTableBytes               uint64
	MemTableCount               int
	MaxOpenFiles                int
	MaxConcurrentCompactions    int
	DisableAutomaticCompactions bool
}

var liveHTTPClient = &http.Client{Timeout: 15 * time.Second}

type liveAPITransport struct {
	mu       sync.RWMutex
	byTarget map[string]http.RoundTripper
}

func (t *liveAPITransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.RLock()
	rt := t.byTarget[req.URL.Host]
	t.mu.RUnlock()
	if rt == nil {
		return nil, fmt.Errorf("no live mTLS client configured for API %q", req.URL.Host)
	}
	return rt.RoundTrip(req)
}

var liveAPIRouter = &liveAPITransport{byTarget: make(map[string]http.RoundTripper)}

func init() { http.DefaultTransport = liveAPIRouter }

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

	// Locate testnode binary, build if not found. MURMUR_BIN overrides
	// the shared binary so experimental daemon builds can be exercised
	// without disturbing parallel suites.
	var binPath string
	if override := getEnv("MURMUR_BIN"); override != "" {
		binPath = override
	} else {
		binPath = findOrBuildTestNode(t)
	}

	// Runtime root: the override env names a root shared by concurrent
	// runs, so each cluster still nests under its own name (as does the
	// default) to keep multi-cluster scenarios from clobbering each other.
	runtimeRoot := getEnv("MURMUR_LIVE_RUNTIME")
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
		originKeys: make(map[db.NodeID]ed25519.PrivateKey),
		T:          t,
		Name:       opts.Name,
		RuntimeDir: runtimeRoot,
		BinaryPath: binPath,
		DBID:       dbID,
		CA:         ca,
		nodeEnv:    opts.NodeEnv,
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

		nodeBin := binPath
		if override, ok := opts.BinaryByNode[i]; ok && override != "" {
			if _, err := os.Stat(override); err != nil {
				t.Fatalf("node %s binary %q: %v", label, override, err)
			}
			nodeBin = override
		}
		node := &Node{
			Index:      i,
			Label:      label,
			NodeID:     nodeID,
			BinaryPath: nodeBin,
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
		}
		cluster.Nodes = append(cluster.Nodes, node)
	}

	for _, node := range cluster.Nodes {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		node.OriginKey = key
		cluster.originKeys[node.NodeID] = key
		if err := os.WriteFile(filepath.Join(node.TLSDir, "origin.key"), key, 0600); err != nil {
			t.Fatal(err)
		}
	}

	// Live API requests use HTTPS and the first node's CA-issued certificate.
	apiRoots := x509.NewCertPool()
	if !apiRoots.AppendCertsFromPEM(ca.CertPEM) {
		t.Fatal("parse live API CA")
	}
	for _, node := range cluster.Nodes {
		clientCert, err := tls.LoadX509KeyPair(
			filepath.Join(node.TLSDir, "node.crt"),
			filepath.Join(node.TLSDir, "node.key"),
		)
		if err != nil {
			t.Fatalf("load live API client certificate for %s: %v", node.Label, err)
		}
		liveAPIRouter.mu.Lock()
		liveAPIRouter.byTarget[node.APIAddr] = &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12, RootCAs: apiRoots, Certificates: []tls.Certificate{clientCert},
		}}
		liveAPIRouter.mu.Unlock()
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
		originKeys := make(map[string]string)
		var snapshotSources []string
		for _, peer := range cluster.Nodes {
			originKeys[peer.NodeID.String()] = hex.EncodeToString(peer.OriginKey.Public().(ed25519.PublicKey))
			snapshotSources = append(snapshotSources, peer.NodeID.String())
		}
		if indices, ok := opts.TrustedSnapshotSourcesByNode[i]; ok {
			snapshotSources = nil
			for _, idx := range indices {
				snapshotSources = append(snapshotSources, cluster.Nodes[idx].NodeID.String())
			}
		}
		cfgJSON := map[string]any{
			"origin_signing_key_file":  filepath.Join(node.TLSDir, "origin.key"),
			"origin_public_keys":       originKeys,
			"trusted_snapshot_sources": snapshotSources,
			"node_id":                  node.NodeID.String(),
			"db_id":                    cluster.DBID.String(),
			"data_dir":                 node.Dir,
			"listen_addr":              node.ReplAddr,
			"api_addr":                 node.APIAddr,
			"await_unlock":             opts.AwaitUnlock,
			"key_hex":                  node.KeyHex,
			"key_id":                   node.KeyID,
			"schema_path":              node.SchemaDir,
			"tls_ca_cert_file":         filepath.Join(node.TLSDir, "ca.crt"),
			"tls_node_cert_file":       filepath.Join(node.TLSDir, "node.crt"),
			"tls_node_key_file":        filepath.Join(node.TLSDir, "node.key"),
			"peers":                    peerConfigs,
			"allowed_networks":         allowedNetworks,
			"allowed_peers":            allowedPeers,
		}
		if len(opts.BootstrapSeeds) > 0 {
			cfgJSON["membership_enabled"] = true
			isSeed := false
			for _, sIdx := range opts.BootstrapSeeds {
				if sIdx == i {
					isSeed = true
					break
				}
			}
			if !isSeed {
				var seedAddrs []string
				for _, sIdx := range opts.BootstrapSeeds {
					if sIdx >= 0 && sIdx < len(cluster.Nodes) {
						seedAddrs = append(seedAddrs, cluster.Nodes[sIdx].ReplAddr)
					}
				}
				cfgJSON["bootstrap"] = seedAddrs
			}
		} else if boot, ok := opts.BootstrapByNode[i]; ok {
			cfgJSON["bootstrap"] = boot
			cfgJSON["membership_enabled"] = true
		} else if len(opts.Bootstrap) > 0 {
			cfgJSON["bootstrap"] = opts.Bootstrap
			cfgJSON["membership_enabled"] = true
		}
		if opts.Schema != nil {
			cfgJSON["schema"] = opts.Schema
		}
		repOpts := opts.Replication
		if perNode, ok := opts.ReplicationByNode[i]; ok {
			repOpts = perNode
		}
		if repOpts != nil || len(opts.DisseminationByNode) > 0 {
			rep := map[string]any{}
			if repOpts != nil {
				rep["min_log_retention_ms"] = repOpts.MinLogRetentionMs
				rep["max_offline_log_retention_ms"] = repOpts.MaxOfflineLogRetentionMs
				rep["min_retained_batches"] = repOpts.MinRetainedBatches
				rep["dissemination"] = repOpts.Dissemination
				rep["protocol_version_override"] = repOpts.ProtocolVersionOverride
				rep["min_protocol_version_override"] = repOpts.MinProtocolVersionOverride
			}
			if mode, ok := opts.DisseminationByNode[i]; ok {
				rep["dissemination"] = mode
			}
			cfgJSON["replication"] = rep
		}
		if opts.Limits != nil {
			cfgJSON["limits"] = map[string]any{
				"max_value_bytes":       opts.Limits.MaxValueBytes,
				"max_transaction_bytes": opts.Limits.MaxTransactionBytes,
				"max_batch_mutations":   opts.Limits.MaxBatchMutations,
			}
		}
		pebOpts := opts.Pebble
		if perNode, ok := opts.PebbleByNode[i]; ok {
			pebOpts = perNode
		}
		if pebOpts != nil {
			cfgJSON["pebble"] = map[string]any{
				"cache_bytes":                   pebOpts.CacheBytes,
				"memtable_bytes":                pebOpts.MemTableBytes,
				"memtable_count":                pebOpts.MemTableCount,
				"max_open_files":                pebOpts.MaxOpenFiles,
				"max_concurrent_compactions":    pebOpts.MaxConcurrentCompactions,
				"disable_automatic_compactions": pebOpts.DisableAutomaticCompactions,
			}
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
		if b, ok := opts.BridgeByNode[i]; ok && b != nil {
			cfgJSON["bridge"] = bridgeJSON(node, b)
		} else if opts.Bridge != nil {
			for _, bi := range opts.Bridge.BridgeNodes() {
				if i != bi {
					continue
				}
				cfgJSON["bridge"] = bridgeJSON(node, opts.Bridge)
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

func bridgeJSON(node *Node, b *BridgeOptions) map[string]any {
	return map[string]any{
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

// SetNodeBinary switches the daemon executable a node starts next.
// The node must be stopped; the switch takes effect on StartNode.
// Rolling-upgrade tests flip nodes from the previous release to the
// current build one at a time.
func (c *Cluster) SetNodeBinary(idx int, path string) {
	c.T.Helper()
	if path == "" {
		c.T.Fatalf("node %s: empty binary path", c.Nodes[idx].Label)
	}
	if _, err := os.Stat(path); err != nil {
		c.T.Fatalf("node %s binary %q: %v", c.Nodes[idx].Label, path, err)
	}
	c.Nodes[idx].BinaryPath = path
}

func (c *Cluster) StartNode(idx int) {
	c.T.Helper()
	node := c.Nodes[idx]

	// Defensive: never orphan a live process by starting over it.
	if node.Process != nil && node.Process.Process != nil {
		c.StopNode(idx)
	}

	logFile, err := os.OpenFile(node.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		c.T.Fatalf("open log file %s: %v", node.LogFile, err)
	}
	node.LogFileWriter = logFile

	bin := node.BinaryPath
	if bin == "" {
		bin = c.BinaryPath
	}
	cmd := exec.Command(bin, "agent", "--config", node.ConfigFile)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if extra := c.nodeEnv[idx]; len(extra) > 0 {
		cmd.Env = mergeEnv(os.Environ(), extra)
	}

	if err := cmd.Start(); err != nil {
		c.T.Fatalf("start node %s: %v", node.Label, err)
	}
	node.Process = cmd
	node.pidMu.Lock()
	node.Pids = append(node.Pids, cmd.Process.Pid)
	node.pidMu.Unlock()
}

// mergeEnv overlays KEY=VALUE pairs onto a base environment, replacing
// same-key entries. Malformed entries (no '=') are ignored.
func mergeEnv(base, overlay []string) []string {
	out := append([]string(nil), base...)
	for _, kv := range overlay {
		key, _, ok := strings.Cut(kv, "=")
		if !ok || key == "" {
			continue
		}
		replaced := false
		for i, existing := range out {
			if ek, _, ok := strings.Cut(existing, "="); ok && ek == key {
				out[i] = kv
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, kv)
		}
	}
	return out
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
	c.UnlockNodeWithKeyID(idx, "", keyHex)
}

// UnlockNodeWithKeyID unlocks with explicit key identity: required
// after a rotation, when the registry only honors the new key ID.
func (c *Cluster) UnlockNodeWithKeyID(idx int, keyID, keyHex string) {
	c.T.Helper()
	node := c.Nodes[idx]

	payload, _ := json.Marshal(map[string]string{
		"key_id":  keyID,
		"key_hex": keyHex,
		"cipher":  "chacha20",
	})

	url := fmt.Sprintf("https://%s/v1/admin/unlock", node.APIAddr)
	deadline := time.Now().Add(10 * time.Second)
	var lastFailure string
	for time.Now().Before(deadline) {
		resp, err := liveHTTPClient.Post(url, "application/json", bytes.NewReader(payload))
		if err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
				_ = resp.Body.Close()
				return
			}
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			lastFailure = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
		} else {
			lastFailure = err.Error()
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.T.Fatalf("failed to unlock node %s at %s within timeout; last error: %s", node.Label, url, lastFailure)
}

func (c *Cluster) WaitNodeReady(idx int) {
	c.T.Helper()
	node := c.Nodes[idx]
	url := fmt.Sprintf("https://%s/healthz", node.APIAddr)
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
	url := fmt.Sprintf("https://%s/v1/exec", node.APIAddr)
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
	url := fmt.Sprintf("https://%s/v1/query", node.APIAddr)
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
	url := fmt.Sprintf("https://%s/v1/admin/add_peer", fromNode.APIAddr)
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
	url := fmt.Sprintf("https://%s/v1/admin/remove_peer", fromNode.APIAddr)
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
	// A wedged shutdown or a test bug can strand a daemon past the
	// normal stop loop; sweep recorded PIDs so no testnode outlives
	// its suite to collide with the next run.
	c.reapStragglers()
	if c.T.Failed() || c.failed {
		failuresDir := getEnv("MURMUR_LIVE_FAILURES")
		if failuresDir == "" {
			failuresDir = filepath.Join(repoRoot(c.T), "tests-live", "failures")
		}
		_ = os.MkdirAll(failuresDir, 0755)
		dest := filepath.Join(failuresDir, fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102T150405Z"), c.Name))
		_ = os.Rename(c.RuntimeDir, dest)
		fmt.Printf("RESULT: FAIL scenario=%s | retained artifacts at %s\n", c.Name, dest)
	} else {
		_ = os.RemoveAll(c.RuntimeDir)
		fmt.Printf("RESULT: PASS scenario=%s (runtime cleaned up)\n", c.Name)
	}
}

// GetEnv returns the environment variable for name.
func GetEnv(name string) string {
	return os.Getenv(name)
}

// EnvInt reads an integer environment variable with fallback.
func EnvInt(name string, fallback int) int {
	if val := GetEnv(name); val != "" {
		if v, err := strconv.Atoi(val); err == nil && v > 0 {
			return v
		}
	}
	return fallback
}

// EnvSeconds reads a seconds-duration environment variable with fallback.
func EnvSeconds(name string, fallback int) time.Duration {
	return time.Duration(EnvInt(name, fallback)) * time.Second
}

// EnvMillis reads a milliseconds-duration environment variable with fallback.
func EnvMillis(name string, fallback int) time.Duration {
	return time.Duration(EnvInt(name, fallback)) * time.Millisecond
}

func getEnv(name string) string {
	return GetEnv(name)
}

func findOrBuildTestNode(t *testing.T) string {
	t.Helper()
	root := repoRoot(t)
	bin := filepath.Join(root, "tests-live", "bin", "testnode")
	// The default CGO build requires the SQLite feature tags; MURMUR_TAGS
	// overrides them (e.g. MURMUR_TAGS=modernc for the pure-Go backend).
	// Without an override, a CGO-disabled environment implies the
	// pure-Go backend so bare `CGO_ENABLED=0 go test -tags modernc`
	// runs build a working daemon instead of a mattn/modernc mix.
	tags := getEnv("MURMUR_TAGS")
	if tags == "" {
		tags = "sqlite_preupdate_hook sqlite_fts5"
		if os.Getenv("CGO_ENABLED") == "0" {
			tags = "modernc"
		}
	}
	// A bare `go test ./tests-live/<scenario>` must never silently reuse a
	// binary built from older sources: rebuild when any build input is
	// newer than the binary or the tags stamp disagrees.
	if reason := testNodeStaleReason(bin, root, tags); reason == "" {
		return bin
	} else {
		t.Logf("rebuilding testnode binary: %s", reason)
	}
	if err := os.MkdirAll(filepath.Dir(bin), 0o755); err != nil {
		t.Fatalf("create test bin dir: %v", err)
	}
	cmd := exec.Command("go", "build", "-tags", tags, "-o", bin, "./tests-live/harness/testnode")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build testnode binary: %v, output: %s", err, string(out))
	}
	if err := os.WriteFile(bin+".tags", []byte(tags), 0o644); err != nil {
		t.Fatalf("write testnode tags stamp: %v", err)
	}
	return bin
}

// testNodeStaleReason returns "" when bin exists, was built with tags, and is
// newer than every build input under root. Otherwise it returns a
// human-readable reason so the caller can log why a rebuild happens.
func testNodeStaleReason(bin, root, tags string) string {
	st, err := os.Stat(bin)
	if err != nil {
		return "binary missing"
	}
	stamp, err := os.ReadFile(bin + ".tags")
	if err != nil || strings.TrimSpace(string(stamp)) != tags {
		return "build tags changed or unknown"
	}
	newer, err := newestSourceAfter(root, st.ModTime())
	if err != nil {
		return "source scan failed, rebuilding to be safe"
	}
	if newer != "" {
		rel, rerr := filepath.Rel(root, newer)
		if rerr == nil {
			newer = rel
		}
		return "source newer than binary: " + newer
	}
	return ""
}

var errFoundNewer = errors.New("found source newer than binary")

// newestSourceAfter returns the first build input under root (.go files plus
// go.mod/go.sum, which pin external module versions) modified after t, or ""
// when the tree is older. Only metadata is read, so the scan costs ~1s.
func newestSourceAfter(root string, t time.Time) (string, error) {
	found := ""
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") && name != "go.mod" && name != "go.sum" {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.ModTime().After(t) {
			found = path
			return errFoundNewer
		}
		return nil
	})
	if err == errFoundNewer {
		return found, nil
	}
	return "", err
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

// Port claims coordinate ephemeral-port allocation across the test
// processes of a parallel run (and across checkouts on one box): each
// `go test` package runs in its own process, so an in-memory registry
// cannot stop two suites from handing the same port to two daemons,
// and the loser dies with "bind: address already in use".
//
// A claim is a file named by the port under a shared temp directory,
// created with O_EXCL (atomic on every platform, no locking needed),
// holding "<pid> <claim-unixnano>". Claims expire after portClaimTTL:
// long enough to cover the longest suites (the 2h soak), short enough
// that crashed runs eventually release their ports. Claimants never
// delete their claims (the daemon outlives any safe delete point);
// expiry plus an opportunistic sweep bounds the directory size.
const portClaimTTL = 4 * time.Hour

// claimPort records port as allocated for a node config. It reports
// false when the port is unusable or is claimed by a live-enough
// entry, in which case the caller must draw another. Expired claims
// are removed and reported unclaimed (the caller redraws; the freed
// port may come up again on a later draw).
func claimPort(port int) bool {
	if port <= 0 || port > 65535 {
		return false
	}
	dir := filepath.Join(os.TempDir(), "spedsql-portclaims")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return false
	}
	name := filepath.Join(dir, strconv.Itoa(port))
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err == nil {
		_, werr := fmt.Fprintf(f, "%d %d\n", os.Getpid(), time.Now().UnixNano())
		_ = f.Close()
		return werr == nil
	}
	if !os.IsExist(err) {
		return false
	}
	raw, rerr := os.ReadFile(name)
	if rerr != nil {
		return false
	}
	var pid int64
	var nano int64
	if _, serr := fmt.Sscanf(string(raw), "%d %d", &pid, &nano); serr != nil {
		return false
	}
	if time.Since(time.Unix(0, nano)) < portClaimTTL {
		return false
	}
	_ = os.Remove(name)
	return false
}

var portSweepCount atomic.Uint64

// sweepPortClaims deletes expired claim files. It runs on a small
// fraction of draws to bound directory growth; expiry itself needs no
// sweep (expired entries simply lose).
func sweepPortClaims(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	now := time.Now()
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var pid, nano int64
		if _, err := fmt.Sscanf(string(raw), "%d %d", &pid, &nano); err != nil {
			continue
		}
		if now.Sub(time.Unix(0, nano)) >= portClaimTTL {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

func getFreePort(t *testing.T) int {
	t.Helper()
	if portSweepCount.Add(1)%256 == 0 {
		sweepPortClaims(filepath.Join(os.TempDir(), "murmur-portclaims"))
	}
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

// MergeOperation invokes an explicit transaction operation on a live daemon.
func (c *Cluster) MergeOperation(node int, request map[string]string) error {
	raw, err := json.Marshal(request)
	if err != nil {
		return err
	}
	response, err := liveHTTPClient.Post(fmt.Sprintf("https://%s/v1/crdt", c.Nodes[node].APIAddr), "application/json", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 204 {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("merge operation HTTP %d: %s", response.StatusCode, body)
	}
	return nil
}
func (c *Cluster) MergeState(node int, table, row string) (map[string]string, error) {
	response, err := liveHTTPClient.Get(fmt.Sprintf("https://%s/v1/crdt/state?table=%s&row=%s", c.Nodes[node].APIAddr, table, row))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		body, _ := io.ReadAll(response.Body)
		return nil, fmt.Errorf("state HTTP %d: %s", response.StatusCode, body)
	}
	var state map[string]string
	err = json.NewDecoder(response.Body).Decode(&state)
	return state, err
}
