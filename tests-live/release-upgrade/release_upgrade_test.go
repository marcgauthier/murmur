// Release and upgrade acceptance: previous-release binaries must
// interoperate with the current build.
//
// The suite checks out the pinned previous release (MURMUR_PREV_REF)
// into a scratch worktree, builds its daemon, and proves three
// upgrade paths against the current binary: a rolling upgrade with
// continuous writes, a current-binary open of a previous-release
// store, and a fresh-identity restore (by the current library) of a
// backup taken by the previous release's writer.
package releaseupgrade_test

import (
	"context"
	"crypto/ed25519"
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
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/backup"
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
	if os.Getenv("CGO_ENABLED") == "0" {
		return "modernc"
	}
	return "sqlite_preupdate_hook sqlite_fts5"
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
// prev-backup <pebbleDir> <nodeID> <dbid> <keyHex> <schemaJSON> <destDir>.
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
		fail("usage: prev-backup <pebbleDir> <nodeID> <dbid> <keyHex> <schemaJSON> <destDir>")
	}
	pebbleDir, nodeIDS, dbidS, keyHex, schemaFile, destDir := os.Args[1], os.Args[2], os.Args[3], os.Args[4], os.Args[5], os.Args[6]
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
		Path:       pebbleDir,
		NodeID:     nodeID,
		DBID:       dbid,
		Schema:     schemaCfg,
		Pebble:     db.DefaultPebbleConfig(),
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

// Signed replication requires a coordinated offline cutover. Every legacy
// node converges first, then all writers stop before baseline migration.
func TestCoordinatedSignedCutoverLosesNoWrites(t *testing.T) {
	oldBin := prevDaemon(t)
	cluster := harness.NewCluster(t, harness.ClusterOptions{Name: "release-upgrade", NumNodes: 3, AwaitUnlock: true, Schema: schemaConfig(), BinaryByNode: map[int]string{0: oldBin, 1: oldBin, 2: oldBin}})
	for node := range cluster.Nodes {
		for i := 0; i < 5; i++ {
			if err := cluster.ExecSQL(node, "INSERT INTO ru_rows (id,name) VALUES (?,?)", fmt.Sprintf("%032x", node*100+i), fmt.Sprintf("old-%d-%d", node, i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	waitCounts(t, cluster, 15, 60*time.Second)
	before, err := cluster.ComputeTableDigest(0, "ru_rows", "id")
	if err != nil {
		t.Fatal(err)
	}
	for i := range cluster.Nodes {
		cluster.StopNode(i)
	}
	for i := range cluster.Nodes {
		migrateNodeBaseline(t, cluster, i)
		cluster.SetNodeBinary(i, cluster.BinaryPath)
	}
	for i := range cluster.Nodes {
		cluster.StartNode(i)
		cluster.UnlockNode(i, cluster.Nodes[i].KeyHex)
		cluster.WaitNodeReady(i)
	}
	waitCounts(t, cluster, 15, 30*time.Second)
	for i := range cluster.Nodes {
		got, err := cluster.ComputeTableDigest(i, "ru_rows", "id")
		if err != nil || got != before {
			t.Fatalf("baseline changed: %s %v", got, err)
		}
	}
	for i := range cluster.Nodes {
		if err := cluster.ExecSQL(i, "INSERT INTO ru_rows (id,name) VALUES (?,?)", fmt.Sprintf("%032x", 1000+i), "signed"); err != nil {
			t.Fatal(err)
		}
	}
	waitCounts(t, cluster, 18, 30*time.Second)
	waitConvergedCounts(t, cluster, 30*time.Second)
}

func migrateNodeBaseline(t *testing.T, c *harness.Cluster, idx int) {
	t.Helper()
	cmd := exec.Command(c.BinaryPath, "migrate-origin-baseline", "--config", c.Nodes[idx].ConfigFile)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("offline origin baseline migration: %v: %s", err, out)
	}
}

// The current binary opens a store written by the previous release:
// same identity, same directory, data byte-identical.
func TestNewBinaryOpensOldStore(t *testing.T) {
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
		if err := cluster.ExecSQL(0, "INSERT INTO ru_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("old-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	waitCounts(t, cluster, 10, 60*time.Second)
	want, err := cluster.ComputeTableDigest(0, "ru_rows", "id")
	if err != nil {
		t.Fatal(err)
	}
	cluster.StopNode(0)

	migrateNodeBaseline(t, cluster, 0)
	cluster.SetNodeBinary(0, cluster.BinaryPath)
	cluster.StartNode(0)
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
	cluster.WaitNodeReady(0)
	waitCounts(t, cluster, 10, 30*time.Second)
	got, err := cluster.ComputeTableDigest(0, "ru_rows", "id")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("current binary read digest %s, old writer wrote %s", got, want)
	}
}

// A backup taken by the previous release's writer restores under the
// current library with a fresh identity, and the current binary serves
// the restored data with no lost rows.
func TestOldBackupRestoresOnNewBinary(t *testing.T) {
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
		if err := cluster.ExecSQL(0, "INSERT INTO ru_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	waitCounts(t, cluster, 20, 60*time.Second)
	want, err := cluster.ComputeTableDigest(0, "ru_rows", "id")
	if err != nil {
		t.Fatal(err)
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
	cmd := exec.Command(helper, node.PebbleDir, node.NodeID.String(), cluster.DBID.String(), node.KeyHex, schemaFile, backupDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("previous-release backup: %v: %s", err, out)
	} else {
		t.Logf("previous writer: %s", strings.TrimSpace(string(out)))
	}

	if err := os.RemoveAll(node.PebbleDir); err != nil {
		t.Fatal(err)
	}
	fresh := db.NewNodeID()
	if _, err := backup.Restore(ctx, backup.RestoreConfig{
		Source:      mustLocalDest(t, backupDir),
		TargetPath:  node.PebbleDir,
		FreshNodeID: fresh.String(),
		Mode:        backup.RestoreClone,
		Overwrite:   true,
	}); err != nil {
		t.Fatalf("current restore of previous-release backup: %v", err)
	}
	rewriteNodeConfig(t, node, fresh.String())
	reissueNodeCert(t, cluster, node, fresh.String())

	migrateNodeBaseline(t, cluster, 0)
	cluster.SetNodeBinary(0, cluster.BinaryPath)
	cluster.StartNode(0)
	cluster.UnlockNode(0, node.KeyHex)
	cluster.WaitNodeReady(0)
	waitCounts(t, cluster, 20, 90*time.Second)
	got, err := cluster.ComputeTableDigest(0, "ru_rows", "id")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("restored digest %s != pre-backup %s", got, want)
	}
	if id := infoLabel(t, node.APIAddr, "node_id"); id != fresh.String() {
		t.Fatalf("node identity = %s, want fresh %s", id, fresh.String())
	}
}

// waitCounts waits until every node reports the exact row count.
func waitCounts(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		for idx := range c.Nodes {
			n, err := c.QueryRowCount(idx, "ru_rows")
			if err != nil || n != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("row counts did not reach %d within %v", want, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitRowOnNode waits until one node serves a row written elsewhere
// (mixed-version replication proof).
func waitRowOnNode(t *testing.T, c *harness.Cluster, idx int, id string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		res, err := c.QuerySQL(idx, "SELECT count(*) FROM ru_rows WHERE id = ?", id)
		if err == nil && len(res.Rows) > 0 && len(res.Rows[0]) > 0 && fmt.Sprintf("%v", res.Rows[0][0]) == "1" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node%d did not replicate row %s within %v (last err %v)", idx+1, id, timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// quiesce pauses all writers, waits for exact digest agreement, then
// resumes. Callers must hold no pause themselves.
func quiesce(t *testing.T, c *harness.Cluster, paused *[3]atomic.Bool, timeout time.Duration) {
	t.Helper()
	for i := range paused {
		paused[i].Store(true)
	}
	time.Sleep(300 * time.Millisecond)
	deadline := time.Now().Add(timeout)
	for {
		want, err := c.ComputeTableDigest(0, "ru_rows", "id")
		if err == nil {
			match := true
			for idx := 1; idx < len(c.Nodes); idx++ {
				d, err := c.ComputeTableDigest(idx, "ru_rows", "id")
				if err != nil || d != want {
					match = false
					break
				}
			}
			if match {
				for i := range paused {
					paused[i].Store(false)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			dumpIDDiff(t, c)
			t.Fatalf("digests did not agree within %v", timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitConvergedCounts waits until all nodes agree on the same row count
// (the absolute value floats while writers run).
func waitConvergedCounts(t *testing.T, c *harness.Cluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		first, err := c.QueryRowCount(0, "ru_rows")
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatalf("node1 count query: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		agree := true
		for idx := 1; idx < len(c.Nodes); idx++ {
			n, err := c.QueryRowCount(idx, "ru_rows")
			if err != nil || n != first {
				agree = false
				break
			}
		}
		if agree {
			time.Sleep(500 * time.Millisecond)
			still := true
			for idx := range c.Nodes {
				n, err := c.QueryRowCount(idx, "ru_rows")
				if err != nil || n != first {
					still = false
					break
				}
			}
			if still {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("row counts did not converge within %v", timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// dumpIDDiff logs the symmetric row-id difference between nodes 1 and
// 2 plus the first ordered row difference (failure diagnostics).
func dumpIDDiff(t *testing.T, c *harness.Cluster) {
	t.Helper()
	ids := func(idx int) map[string]bool {
		m := map[string]bool{}
		res, err := c.QuerySQL(idx, "SELECT id FROM ru_rows")
		if err != nil {
			t.Logf("divergence: node%d ids query: %v", idx+1, err)
			return m
		}
		for _, row := range res.Rows {
			if len(row) > 0 {
				m[fmt.Sprintf("%v", row[0])] = true
			}
		}
		return m
	}
	a, b := ids(0), ids(1)
	var onlyA, onlyB int
	for id := range a {
		if !b[id] {
			onlyA++
		}
	}
	for id := range b {
		if !a[id] {
			onlyB++
		}
	}
	t.Logf("divergence: only-node1=%d only-node2=%d", onlyA, onlyB)
	r0, _ := c.QuerySQL(0, "SELECT * FROM ru_rows ORDER BY id")
	r1, _ := c.QuerySQL(1, "SELECT * FROM ru_rows ORDER BY id")
	for i := 0; i < len(r0.Rows) && i < len(r1.Rows); i++ {
		a, b := fmt.Sprintf("%v", r0.Rows[i]), fmt.Sprintf("%v", r1.Rows[i])
		if a != b {
			t.Logf("divergence: first diff at row %d:\n  node1=%q\n  node2=%q", i, a, b)
			break
		}
	}
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
	for _, line := range strings.Split(scrapeMetrics(t, apiAddr), "\n") {
		if !strings.HasPrefix(line, "spedsql_info{") {
			continue
		}
		key := label + `="`
		i := strings.Index(line, key)
		if i < 0 {
			continue
		}
		rest := line[i+len(key):]
		if j := strings.Index(rest, `"`); j >= 0 {
			return rest[:j]
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
