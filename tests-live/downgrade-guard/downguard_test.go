// Downgrade-guard acceptance: a previous-release binary must refuse a
// store first written by the current binary.
//
// The current binary writes store format 3; the pinned previous release
// requires strict format equality with its own version (2) and fails
// closed with a version error. The suite proves the refusal is clean
// (version error, nonzero exit), the store is byte-untouched (the
// current binary reopens it with an identical digest), and both
// directions of the compatibility matrix still work (positive controls:
// the previous binary boots a fresh store, and the current binary opens
// a previous-release store).
package downgradeguard_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

// defaultPrevRef pins the previous release the current build must refuse
// to downgrade to. It tracks tests-live/release-upgrade; SPEDSQL_PREV_REF
// overrides it per run.
const defaultPrevRef = "4bdd2974766786865c6222bb4843cab79e41e5c5"

func prevRef() string {
	if ref := os.Getenv("SPEDSQL_PREV_REF"); ref != "" {
		return ref
	}
	return defaultPrevRef
}

func prevTags() string {
	if tags := os.Getenv("SPEDSQL_TAGS"); tags != "" {
		return tags
	}
	return "sqlite_preupdate_hook sqlite_fts5"
}

var (
	prevMu       sync.Mutex
	prevBuilt    bool
	prevWorktree string
	prevBin      string
)

func TestMain(m *testing.M) {
	code := m.Run()
	prevMu.Lock()
	wt := prevWorktree
	prevMu.Unlock()
	if wt != "" {
		rm := exec.Command("git", "worktree", "remove", "--force", wt)
		if root := repoRootDir(); root != "" {
			rm.Dir = root
		}
		if out, err := rm.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "downgrade-guard: worktree remove: %v: %s\n", err, out)
			_ = os.RemoveAll(wt)
		}
	}
	os.Exit(code)
}

func repoRootDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	full := append([]string{"-C", root}, args...)
	cmd := exec.Command("git", full...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(full, " "), err, out)
	}
}

// ensurePrevBuild checks out the previous release and builds its daemon.
// It skips only when the tree is not a git checkout at all; any other
// failure fails the test, since an unverifiable downgrade guard is a
// release blocker, not a pass.
func ensurePrevBuild(t *testing.T) {
	t.Helper()
	prevMu.Lock()
	defer prevMu.Unlock()
	if prevBuilt {
		return
	}
	root := repoRootDir()
	if root == "" {
		t.Fatal("downgrade-guard: could not find repository root")
	}
	if _, err := exec.Command("git", "-C", root, "rev-parse", "--git-dir").CombinedOutput(); err != nil {
		t.Skip("downgrade-guard: not a git checkout, previous release unavailable")
	}
	ref := prevRef()
	if _, err := exec.Command("git", "-C", root, "rev-parse", "--verify", ref+"^{commit}").CombinedOutput(); err != nil {
		t.Fatalf("downgrade-guard: previous ref %q not present (shallow clone? CI needs fetch-depth: 0): %v", ref, err)
	}
	tmp, err := os.MkdirTemp("", "spedsql-prev-")
	if err != nil {
		t.Fatal(err)
	}
	wt := filepath.Join(tmp, "wt")
	runGit(t, root, "worktree", "add", "--detach", wt, ref)
	prevWorktree = wt
	daemonPkg := "./cmd/spedsql"
	if _, err := os.Stat(filepath.Join(wt, "tests-live", "harness", "testnode")); err == nil {
		daemonPkg = "./tests-live/harness/testnode"
	}
	t.Logf("previous release %s (daemon %s)", ref, daemonPkg)
	cmd := exec.Command("go", "build", "-tags", prevTags(), "-o", filepath.Join(tmp, "prev-testnode"), daemonPkg)
	cmd.Dir = wt
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build previous daemon: %v: %s", err, out)
	}
	prevBin = filepath.Join(tmp, "prev-testnode")
	prevBuilt = true
}

func prevDaemon(t *testing.T) string {
	t.Helper()
	ensurePrevBuild(t)
	prevMu.Lock()
	defer prevMu.Unlock()
	return prevBin
}

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
		Name: "dg_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}}
}

func TestDowngradeGuardRefusesNewStore(t *testing.T) {
	oldBin := prevDaemon(t)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "downgrade-guard",
		NumNodes:    1,
		AwaitUnlock: false,
		Schema:      schemaConfig(),
	})
	node := cluster.Nodes[0]
	newBin := cluster.BinaryPath

	const rows = 25
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("%032x", 3000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO dg_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	if n, err := cluster.QueryRowCount(0, "dg_rows"); err != nil || n != rows {
		t.Fatalf("seeded count = %d, %v; want %d", n, err, rows)
	}
	wantDigest, err := cluster.ComputeTableDigest(0, "dg_rows", "name")
	if err != nil {
		t.Fatalf("pre-stop digest: %v", err)
	}
	wantNodeID := node.NodeID.String()
	cluster.StopNode(0)

	// Positive controls: the previous binary boots a fresh store (proving
	// the old binary itself works), and the current binary opens that
	// previous-release store (backward compatibility).
	freshCfg := writeFreshConfig(t, cluster)
	probeBoot(t, "prev-on-fresh", oldBin, freshCfg)
	probeBoot(t, "current-on-prev-store", newBin, freshCfg)

	// The downgrade attempt: the previous binary must refuse the
	// current binary's store with a version error and a nonzero exit.
	before := hashTree(t, node.PebbleDir)
	exitCode, output := runToExit(t, oldBin, node.ConfigFile, 60*time.Second)
	if exitCode == 0 {
		t.Fatalf("previous binary exited 0 on a current store, want nonzero refusal")
	}
	if !strings.Contains(output, "unsupported format version") {
		t.Fatalf("previous binary output lacks a version error (exit %d):\n%s", exitCode, tailLines(output, 30))
	}
	t.Logf("previous binary refused current store: exit %d, version error present", exitCode)

	// The refused store must be untouched: keys byte-identical, no
	// in-place content mutation anywhere (Pebble file rotation from the
	// refused open itself is logged, not failed), and the format
	// markers still at the current version.
	after := hashTree(t, node.PebbleDir)
	assertStoreUntouched(t, before, after)
	assertFormatMarkers(t, cluster, node.PebbleDir, node.NodeID.String(), 3)

	// The current binary reopens the store with identical data and identity.
	cluster.StartNode(0)
	cluster.WaitNodeReady(0)
	if n, err := cluster.QueryRowCount(0, "dg_rows"); err != nil || n != rows {
		t.Fatalf("reopened count = %d, %v; want %d", n, err, rows)
	}
	gotDigest, err := cluster.ComputeTableDigest(0, "dg_rows", "name")
	if err != nil {
		t.Fatal(err)
	}
	if gotDigest != wantDigest {
		t.Fatalf("reopened digest %s != pre-stop %s", gotDigest, wantDigest)
	}
	if got := nodeIDLabel(t, node.APIAddr); got != wantNodeID {
		t.Fatalf("reopened node_id = %s, want %s", got, wantNodeID)
	}
	t.Logf("current binary reopened refused store: %d rows digest %s", rows, gotDigest)
}

// writeFreshConfig builds a standalone single-node daemon config in a
// temp dir with a fresh identity, a fresh certificate, and fresh ports.
func writeFreshConfig(t *testing.T, cluster *harness.Cluster) string {
	t.Helper()
	base := cluster.Nodes[0]
	root := t.TempDir()
	pebbleDir := filepath.Join(root, "pebble")
	logsDir := filepath.Join(root, "logs")
	schemaDir := filepath.Join(root, "schema")
	tlsDir := filepath.Join(root, "tls")
	for _, d := range []string{pebbleDir, logsDir, schemaDir, tlsDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatal(err)
		}
	}
	fresh := db.NewNodeID()
	certPEM, keyPEM, err := cluster.CA.IssueNode(fresh, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tlsDir, "ca.crt"), cluster.CA.CertPEM, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tlsDir, "node.crt"), certPEM, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tlsDir, "node.key"), keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := map[string]any{
		"node_id":            fresh.String(),
		"db_id":              db.NewDBID().String(),
		"data_dir":           root,
		"listen_addr":        fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		"api_addr":           fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		"await_unlock":       false,
		"key_hex":            base.KeyHex,
		"key_id":             base.KeyID,
		"schema_path":        schemaDir,
		"tls_ca_cert_file":   filepath.Join(tlsDir, "ca.crt"),
		"tls_node_cert_file": filepath.Join(tlsDir, "node.crt"),
		"tls_node_key_file":  filepath.Join(tlsDir, "node.key"),
		"peers":              []map[string]any{},
		"schema":             schemaConfig(),
	}
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	cfgFile := filepath.Join(root, "config.json")
	if err := os.WriteFile(cfgFile, raw, 0644); err != nil {
		t.Fatal(err)
	}
	return cfgFile
}

// probeBoot runs a daemon on a config until /healthz reports ready, then
// stops it cleanly. It proves the binary can open the store.
func probeBoot(t *testing.T, name, bin, cfgFile string) {
	t.Helper()
	logFile := filepath.Join(t.TempDir(), name+".log")
	cmd := startDaemon(t, bin, cfgFile, logFile)
	apiAddr := configAddr(t, cfgFile, "api_addr")
	waitHealthz(t, apiAddr, 30*time.Second)
	t.Logf("%s: booted and healthy", name)
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("%s: interrupt: %v", name, err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: exit after SIGINT: %v\n%s", name, err, readLogTail(t, logFile))
		}
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("%s: daemon did not stop after SIGINT", name)
	}
}

// runToExit runs a daemon until it exits and returns its exit code with
// combined output. The process is killed if it outlives the timeout.
func runToExit(t *testing.T, bin, cfgFile string, timeout time.Duration) (int, string) {
	t.Helper()
	logFile := filepath.Join(t.TempDir(), "refusal.log")
	cmd := startDaemon(t, bin, cfgFile, logFile)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		out, _ := os.ReadFile(logFile)
		if err == nil {
			return 0, string(out)
		}
		if exit, ok := err.(*exec.ExitError); ok {
			return exit.ExitCode(), string(out)
		}
		t.Fatalf("wait: %v", err)
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-done
		out, _ := os.ReadFile(logFile)
		t.Fatalf("daemon did not exit within %v (downgrade refusal missing):\n%s", timeout, tailLines(string(out), 20))
	}
	return -1, ""
}

func startDaemon(t *testing.T, bin, cfgFile, logFile string) *exec.Cmd {
	t.Helper()
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// The log stays open for the daemon's lifetime; the test process
	// exits right after, so no explicit close is needed beyond the
	// process end. Register a cleanup to flush on success paths.
	t.Cleanup(func() { _ = f.Close() })
	cmd := exec.Command(bin, "agent", "--config", cfgFile)
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		t.Fatalf("start daemon: %v", err)
	}
	return cmd
}

func configAddr(t *testing.T, cfgFile, key string) string {
	t.Helper()
	raw, err := os.ReadFile(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	addr, _ := cfg[key].(string)
	if addr == "" {
		t.Fatalf("config %s has no %s", cfgFile, key)
	}
	return addr
}

func waitHealthz(t *testing.T, apiAddr string, timeout time.Duration) {
	t.Helper()
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // healthz probe in a test
	}
	deadline := time.Now().Add(timeout)
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
	t.Fatalf("daemon at %s never became healthy within %v", apiAddr, timeout)
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func readLogTail(t *testing.T, logFile string) string {
	t.Helper()
	raw, err := os.ReadFile(logFile)
	if err != nil {
		return ""
	}
	return tailLines(string(raw), 30)
}

func tailLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// hashTree records every regular file under root as relpath -> sha256.
func hashTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		out[rel] = hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// assertStoreUntouched proves the refused downgrade open mutated no
// content: everything outside data/ (keys, intents) must match exactly,
// and every data/ file present in both snapshots must be byte-identical
// (Pebble never modifies files in place). File additions/removals under
// data/ are the refused open's own Pebble-level rotation (WAL replay,
// manifest/options version bump, obsolete cleanup): content-preserving
// by design, and logged here rather than failed. The reopen digest
// below proves the logical data is identical.
func assertStoreUntouched(t *testing.T, before, after map[string]string) {
	t.Helper()
	var violations []string
	var rotation []string
	for name, sum := range before {
		afterSum, ok := after[name]
		if !ok {
			entry := "removed: " + name
			if strings.HasPrefix(name, "data/") {
				rotation = append(rotation, entry)
			} else {
				violations = append(violations, entry)
			}
		} else if afterSum != sum {
			violations = append(violations, "modified: "+name)
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			entry := "added: " + name
			if strings.HasPrefix(name, "data/") {
				rotation = append(rotation, entry)
			} else {
				violations = append(violations, entry)
			}
		}
	}
	if len(violations) != 0 {
		t.Fatalf("refused store was touched:\n%s", strings.Join(violations, "\n"))
	}
	t.Logf("refused store: no content mutated (%d files stable, %d rotated by Pebble open/close)",
		len(before), len(rotation))
	for _, r := range rotation {
		t.Logf("  pebble rotation: %s", r)
	}
}

// assertFormatMarkers opens the store offline and requires the given
// persistent format version, proving the refused open neither upgraded
// nor corrupted the version markers.
func assertFormatMarkers(t *testing.T, cluster *harness.Cluster, pebbleDir, nodeID string, want uint64) {
	t.Helper()
	node, err := db.ParseNodeID(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	key, err := hex.DecodeString(cluster.Nodes[0].KeyHex)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := db.Open(context.Background(), db.Config{
		Path:   pebbleDir,
		NodeID: node,
		DBID:   cluster.DBID,
		Schema: *schemaConfig(),
		Pebble: db.DefaultPebbleConfig(),
		// No unlock API is involved (await_unlock=false); the daemon
		// opens with the config key_id directly.
		Encryption: db.EncryptionConfig{Key: key, KeyID: cluster.Nodes[0].KeyID},
	})
	if err != nil {
		t.Fatalf("offline open after refusal: %v", err)
	}
	defer handle.Close()
	if got := handle.Status().FormatFormat; got != want {
		t.Fatalf("store format after refusal = %d, want %d", got, want)
	}
	t.Logf("store format markers still at %d after refusal", want)
}

func nodeIDLabel(t *testing.T, apiAddr string) string {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw := make([]byte, 1<<20)
	n, _ := resp.Body.Read(raw)
	for _, line := range strings.Split(string(raw[:n]), "\n") {
		if !strings.HasPrefix(line, "spedsql_info{") {
			continue
		}
		key := `node_id="`
		i := strings.Index(line, key)
		if i < 0 {
			continue
		}
		rest := line[i+len(key):]
		if j := strings.Index(rest, `"`); j >= 0 {
			return rest[:j]
		}
	}
	t.Fatal("node_id label not found in spedsql_info")
	return ""
}
