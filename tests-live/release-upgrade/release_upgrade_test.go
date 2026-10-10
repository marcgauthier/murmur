// Release and upgrade acceptance: the current build must reject
// incompatible on-disk formats from the previous release.
//
// The suite checks out the pinned previous release (MURMUR_PREV_REF)
// into a scratch worktree, builds its daemon, and verifies that the
// current binary rejects its incompatible store and backup formats.
package releaseupgrade_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

// defaultPrevRef is the previous release the current build must
// upgrade from. Bump it to the last release commit whenever a new
// release is cut; MURMUR_PREV_REF overrides it per run.
const defaultPrevRef = "4bdd2974766786865c6222bb4843cab79e41e5c5"

func prevRef() string {
	if ref := harness.GetEnv("MURMUR_PREV_REF"); ref != "" {
		return ref
	}
	return defaultPrevRef
}

func prevTags() string {
	if tags := harness.GetEnv("MURMUR_TAGS"); tags != "" {
		return tags
	}
	// The pinned previous release uses mattn/go-sqlite3 and requires this
	// tag for its replication capture hook. This tag is isolated to the
	// historical fixture build; current typed binaries use the runner's
	// CGO-disabled configuration.
	return "sqlite_preupdate_hook"
}

// The previous release is checked out and built once per package run;
// every test shares the binaries. The worktree is removed by TestMain.
var (
	prevMu       sync.Mutex
	prevBuilt    bool
	prevWorktree string
	prevBin      string
	prevHelper   string
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
			fmt.Fprintf(os.Stderr, "release-upgrade: worktree remove: %v: %s\n", err, out)
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

func runGoBuild(t *testing.T, dir, output, pkg string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-tags", prevTags(), "-o", output, pkg)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s at previous release: %v: %s", pkg, err, out)
	}
}

// ensurePrevBuild checks out the previous release and builds its
// daemon plus the offline-backup helper. It skips only when the tree
// is not a git checkout at all; any other failure (unknown ref,
// shallow clone, build break) fails the test, since an unverifiable
// upgrade is a release blocker, not a pass.
func ensurePrevBuild(t *testing.T) {
	t.Helper()
	prevMu.Lock()
	defer prevMu.Unlock()
	if prevBuilt {
		return
	}
	root := repoRootDir()
	if root == "" {
		t.Fatal("release-upgrade: could not find repository root")
	}
	if _, err := exec.Command("git", "-C", root, "rev-parse", "--git-dir").CombinedOutput(); err != nil {
		t.Skip("release-upgrade: not a git checkout, previous release unavailable")
	}
	ref := prevRef()
	if _, err := exec.Command("git", "-C", root, "rev-parse", "--verify", ref+"^{commit}").CombinedOutput(); err != nil {
		t.Fatalf("release-upgrade: previous ref %q not present (shallow clone? CI needs fetch-depth: 0): %v", ref, err)
	}
	tmp, err := os.MkdirTemp("", "spedsql-prev-")
	if err != nil {
		t.Fatal(err)
	}
	wt := filepath.Join(tmp, "wt")
	// The mutex stays held for the whole build: the tests in this
	// package run sequentially, and dropping the lock around runCmd
	// would let a t.Fatalf unwind past an unlocked mutex.
	runGit(t, root, "worktree", "add", "--detach", wt, ref)
	prevWorktree = wt
	// The daemon entrypoint moved from cmd/spedsql to the internal
	// test-node fixture; build whichever the old revision has.
	daemonPkg := "./cmd/spedsql"
	if _, err := os.Stat(filepath.Join(wt, "tests-live", "harness", "testnode")); err == nil {
		daemonPkg = "./tests-live/harness/testnode"
	}
	t.Logf("previous release %s (daemon %s)", ref, daemonPkg)
	runGoBuild(t, wt, filepath.Join(tmp, "prev-testnode"), daemonPkg)
	helperDir := filepath.Join(wt, "prevbackuphelper")
	if err := os.MkdirAll(helperDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(helperDir, "main.go"), []byte(prevBackupHelperSrc), 0644); err != nil {
		t.Fatal(err)
	}
	runGoBuild(t, wt, filepath.Join(tmp, "prev-backup"), "./prevbackuphelper")
	prevBin = filepath.Join(tmp, "prev-testnode")
	prevHelper = filepath.Join(tmp, "prev-backup")
	prevBuilt = true
	t.Logf("previous daemon %s backup-helper %s", shortHash(t, prevBin), shortHash(t, prevHelper))
}

// prevDaemon returns the previous-release daemon binary, building it
// on first use.
func prevDaemon(t *testing.T) string {
	t.Helper()
	ensurePrevBuild(t)
	prevMu.Lock()
	defer prevMu.Unlock()
	return prevBin
}

// prevBackupBin returns a helper built against the previous release
// that takes an offline backup of a stopped node directory:
// prev-backup <dataDir> <nodeID> <dbid> <keyHex> <schemaJSON> <destDir>.
func prevBackupBin(t *testing.T) string {
	t.Helper()
	ensurePrevBuild(t)
	prevMu.Lock()
	defer prevMu.Unlock()
	return prevHelper
}

// prevBackupHelperSrc is compiled inside the previous release's
// worktree, so it links the old writer. It mirrors the offline backup
// flow (same key ID the daemon unlock path uses) and must only use
// APIs that exist in the pinned release. Its import path is the
// PREVIOUS release's module path: do not rewrite it in a module
// rename (it must resolve inside the old checkout, not this one).
const prevBackupHelperSrc = `package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	db "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/backup"
)

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "prev-backup: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) != 7 {
		fail("usage: prev-backup <dataDir> <nodeID> <dbid> <keyHex> <schemaJSON> <destDir>")
	}
	dataDir, nodeIDS, dbidS, keyHex, schemaFile, destDir := os.Args[1], os.Args[2], os.Args[3], os.Args[4], os.Args[5], os.Args[6]
	nodeID, err := db.ParseNodeID(nodeIDS)
	if err != nil {
		fail("parse node id: %v", err)
	}
	dbid, err := db.ParseDBID(dbidS)
	if err != nil {
		fail("parse db id: %v", err)
	}
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		fail("decode key: %v", err)
	}
	schemaRaw, err := os.ReadFile(schemaFile)
	if err != nil {
		fail("read schema: %v", err)
	}
	var schemaCfg db.SchemaConfig
	if err := json.Unmarshal(schemaRaw, &schemaCfg); err != nil {
		fail("parse schema: %v", err)
	}
	dest, err := backup.NewLocalDestination(destDir)
	if err != nil {
		fail("destination: %v", err)
	}
	ctx := context.Background()
	handle, err := db.Open(ctx, db.Config{
		Path:       dataDir,
		NodeID:     nodeID,
		DBID:       dbid,
		Schema:     schemaCfg,
		Encryption: db.EncryptionConfig{Key: key, KeyID: "remote-unlock-key"},
	})
	if err != nil {
		fail("open: %v", err)
	}
	defer handle.Close()
	meta, err := handle.Backup(ctx, backup.Config{Destination: dest})
	if err != nil {
		fail("backup: %v", err)
	}
	fmt.Printf("backup %s bytes=%d\n", meta.BackupID, meta.TotalBytes)
}
`

func shortHash(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:12]
}

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
		Name: "ru_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}}
}

// The current binary rejects opening a legacy Pebble store:
// fails closed with a clear error without modifying the directory.
func TestNewBinaryRejectsOldPebbleStore(t *testing.T) {
	oldBin := prevDaemon(t)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "release-upgrade-open",
		NumNodes:     1,
		AwaitUnlock:  true,
		Schema:       schemaConfig(),
		BinaryByNode: map[int]string{0: oldBin},
	})
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%032x", 100+i)
		if err := execPreviousReleaseSQL(cluster, 0, "INSERT INTO ru_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("old-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	cluster.StopNode(0)

	node := cluster.Nodes[0]
	entries1, _ := os.ReadDir(node.Dir)
	var names1 []string
	for _, e := range entries1 {
		names1 = append(names1, e.Name())
	}
	t.Logf("node.Dir entries: %v", names1)
	entries2, _ := os.ReadDir(node.DataDir)
	var names2 []string
	for _, e := range entries2 {
		names2 = append(names2, e.Name())
	}
	t.Logf("node.DataDir entries: %v", names2)
	cfg := offlineConfig(t, node.Dir, cluster, node.NodeID.String())
	if _, err := db.Open(context.Background(), cfg); err == nil {
		t.Fatalf("expected db.Open to reject legacy Pebble directory, got nil")
	} else if !strings.Contains(err.Error(), "legacy Pebble database") {
		t.Fatalf("expected legacy Pebble rejection error, got: %v", err)
	} else {
		t.Logf("legacy Pebble store properly rejected: %v", err)
	}
}

// A backup taken by a legacy Pebble writer is explicitly rejected by the
// current Spool-only restore engine.
func TestOldBackupRejectedOnNewBinary(t *testing.T) {
	ctx := context.Background()
	oldBin := prevDaemon(t)
	helper := prevBackupBin(t)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "release-upgrade-backup",
		NumNodes:     1,
		AwaitUnlock:  true,
		Schema:       schemaConfig(),
		BinaryByNode: map[int]string{0: oldBin},
	})
	node := cluster.Nodes[0]
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("%032x", 7000+i)
		if err := execPreviousReleaseSQL(cluster, 0, "INSERT INTO ru_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	cluster.StopNode(0)

	schemaRaw, err := json.Marshal(schemaConfig())
	if err != nil {
		t.Fatal(err)
	}
	schemaFile := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(schemaFile, schemaRaw, 0644); err != nil {
		t.Fatal(err)
	}
	backupDir := t.TempDir()
	cmd := exec.Command(helper, node.DataDir, node.NodeID.String(), cluster.DBID.String(), node.KeyHex, schemaFile, backupDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("previous-release backup: %v: %s", err, out)
	} else {
		t.Logf("previous writer: %s", strings.TrimSpace(string(out)))
	}

	if err := os.RemoveAll(node.DataDir); err != nil {
		t.Fatal(err)
	}
	fresh := db.NewNodeID()
	if _, err := backup.Restore(ctx, backup.RestoreConfig{
		Source:      mustLocalDest(t, backupDir),
		TargetPath:  node.DataDir,
		FreshNodeID: fresh.String(),
		Mode:        backup.RestoreClone,
		Overwrite:   true,
	}); err == nil {
		t.Fatalf("expected restore to reject legacy Pebble backup, got nil")
	} else if !strings.Contains(err.Error(), "legacy Pebble backup") {
		t.Fatalf("expected legacy Pebble backup error, got: %v", err)
	} else {
		t.Logf("legacy Pebble backup properly rejected: %v", err)
	}
}

// execPreviousReleaseSQL seeds fixtures only through the pinned historical
// binary. Current Murmur nodes have no SQL client or materializer.
func execPreviousReleaseSQL(c *harness.Cluster, idx int, query string, args ...any) error {
	node := c.Nodes[idx]
	payload, err := json.Marshal(map[string]any{"query": query, "args": args})
	if err != nil {
		return err
	}
	url := "https://" + node.APIAddr + "/v1/exec"
	resp, err := c.APIClient().Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("previous daemon SQL seed request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("previous daemon SQL seed failed (%d): %s", resp.StatusCode, body)
	}
	return nil
}

func mustLocalDest(t *testing.T, dir string) *backup.LocalDestination {
	t.Helper()
	d, err := backup.NewLocalDestination(dir)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func mustParseNodeID(t *testing.T, s string) db.NodeID {
	t.Helper()
	id, err := db.ParseNodeID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func rewriteNodeConfig(t *testing.T, node *harness.Node, freshNodeID string) {
	t.Helper()
	raw, err := os.ReadFile(node.ConfigFile)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	_, freshKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	node.OriginKey = freshKey
	if err := os.WriteFile(filepath.Join(node.TLSDir, "origin.key"), freshKey, 0600); err != nil {
		t.Fatal(err)
	}
	cfg["node_id"] = freshNodeID
	if registry, ok := cfg["origin_public_keys"].(map[string]any); ok {
		registry[freshNodeID] = hex.EncodeToString(node.OriginKey.Public().(ed25519.PublicKey))
	}
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(node.ConfigFile, out, 0644); err != nil {
		t.Fatal(err)
	}
	node.NodeID = mustParseNodeID(t, freshNodeID)
}

func reissueNodeCert(t *testing.T, cluster *harness.Cluster, node *harness.Node, freshNodeID string) {
	cluster.ProvisionOrigin(node.NodeID, node.OriginKey)
	t.Helper()
	id := mustParseNodeID(t, freshNodeID)
	certPEM, keyPEM, err := cluster.CA.IssueNode(id, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(node.TLSDir, "node.crt"), certPEM, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(node.TLSDir, "node.key"), keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
}

func infoLabel(t *testing.T, apiAddr, label string) string {
	t.Helper()
	samples, ok := harness.MetricSamples(scrapeMetrics(t, apiAddr))
	if !ok {
		t.Fatal("decode metrics JSON")
	}
	for _, sample := range samples {
		if sample.Name == "spedsql_info" {
			if value := sample.Labels[label]; value != "" {
				return value
			}
		}
	}
	t.Fatalf("label %s not found in spedsql_info", label)
	return ""
}

func scrapeMetrics(t *testing.T, apiAddr string) string {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

type ruRow struct {
	ID ids.RowID `rime:"primary"`
}

func offlineConfig(t *testing.T, dataDir string, cluster *harness.Cluster, nodeID string) db.Config {
	t.Helper()
	typed, err := db.Model[ruRow](db.ModelOptions{
		Name: "ru_rows", TableID: 1,
		RecordOptions: db.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	node, err := db.ParseNodeID(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	var signingKey ed25519.PrivateKey
	registry, _ := origin.NewKeyRegistry(nil)
	for _, n := range cluster.Nodes {
		_ = registry.Add(n.NodeID, n.OriginKey.Public().(ed25519.PublicKey))
		if n.NodeID == node {
			signingKey = n.OriginKey
		}
	}
	if len(signingKey) == 0 {
		_, signingKey, _ = ed25519.GenerateKey(rand.Reader)
		_ = registry.Add(node, signingKey.Public().(ed25519.PublicKey))
	}
	return db.Config{
		Path:          dataDir,
		NodeID:        node,
		DBID:          cluster.DBID,
		Tables:        []db.TableDefinition{typed},
		OriginSigning: db.OriginSigningConfig{PrivateKey: signingKey, TrustedKeys: registry},
		Spool:         db.DefaultSpoolConfig(),
		Encryption:    db.EncryptionConfig{Key: keyBytes(t, cluster.Nodes[0].KeyHex), KeyID: "remote-unlock-key"},
	}
}

func keyBytes(t *testing.T, keyHex string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(keyHex)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
