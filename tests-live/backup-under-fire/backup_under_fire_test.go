// Backup-under-fire acceptance: online backups during sustained writes.
//
// A 2-node daemon mesh takes continuous writes while a suite-local third
// member (backupagent, in ./backupagent) joins over QUIC replication and
// takes repeated ONLINE backups — the live checkpoint pipeline, zero
// downtime, daemons never stopping. One throttled backup is SIGKILLed
// mid-stream: no completed artifact may appear and the partial remnant
// must be rejected. A final backup restores under a fresh identity with
// zero lost rows and a digest identical to the survivors.
package backupunderfire_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

type backupFireRecord struct {
	ID    ids.RowID `rime:"primary"`
	Name  string
	Count int64
	Tags  []string
	Peak  int64
	Floor float64
}

func typedDefinition(t *testing.T) db.TableDefinition {
	t.Helper()
	definition, err := db.Define[backupFireRecord]("live_typed_records", 901, db.RecordOptions{
		PrimaryField:  "ID",
		FieldIDs:      map[string]uint32{"ID": 1, "Name": 2, "Count": 3, "Tags": 4, "Peak": 5, "Floor": 6},
		MergePolicies: map[string]db.RecordMergePolicy{"Count": db.RecordMergeCounter, "Tags": db.RecordMergeORSet, "Peak": db.RecordMergeMax, "Floor": db.RecordMergeMin},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
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

// buildAgent compiles the native typed backup agent without SQLite tags.
func buildAgent(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "backupagent")
	cmd := exec.Command("go", "build", "-o", out, "./tests-live/backup-under-fire/backupagent")
	cmd.Dir = repoRoot(t)
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build backupagent: %v: %s", err, raw)
	}
	return out
}

func writeSeconds(t *testing.T) time.Duration {
	t.Helper()
	return harness.EnvSeconds("MURMUR_BACKUP_UNDER_FIRE_WRITE_SECONDS", 30)
}

// agent wraps a running backupagent child process.
type agent struct {
	cmd   *exec.Cmd
	stdin io.Writer
	lines chan agentLine
}

type agentLine struct {
	text string
	err  error
}

func startAgent(t *testing.T, bin string, cluster *harness.Cluster, replAddr string) *agent {
	t.Helper()
	id := db.NewNodeID()
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := cluster.CA.IssueNode(id, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(root, "ca.crt")
	certFile := filepath.Join(root, "node.crt")
	keyFile := filepath.Join(root, "node.key")
	originKeyFile := filepath.Join(root, "origin.key")
	originKeysFile := filepath.Join(root, "origin-public-keys.json")
	signing := cluster.OriginSigning(id)
	cluster.ProvisionOrigin(id, signing.PrivateKey)
	if err := os.WriteFile(originKeyFile, signing.PrivateKey, 0600); err != nil {
		t.Fatal(err)
	}
	publicKeys := make(map[string]string)
	for _, originID := range append([]db.NodeID{id}, clusterNodeIDs(cluster)...) {
		publicKey, ok := signing.TrustedKeys.Lookup(originID)
		if !ok {
			t.Fatal("missing origin public key")
		}
		publicKeys[originID.String()] = hex.EncodeToString(publicKey)
	}
	originKeysJSON, err := json.Marshal(publicKeys)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(originKeysFile, originKeysJSON, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, cluster.CA.CertPEM, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, certPEM, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
	var peers []string
	for _, n := range cluster.Nodes {
		peers = append(peers, n.NodeID.String()+"="+n.ReplAddr)
	}
	stderrFile := filepath.Join(root, "agent.stderr")
	stderr, err := os.OpenFile(stderrFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stderr.Close() })
	cmd := exec.Command(bin,
		"--data-dir", dataDir,
		"--node-id", id.String(),
		"--db-id", cluster.DBID.String(),
		"--key-hex", cluster.Nodes[0].KeyHex,
		"--key-id", "remote-unlock-key",
		"--repl-addr", replAddr,
		"--peers", strings.Join(peers, ","),
		"--ca", caFile,
		"--cert", certFile,
		"--key", keyFile,
		"--origin-key", originKeyFile,
		"--origin-public-keys", originKeysFile,
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start backupagent: %v", err)
	}
	a := &agent{cmd: cmd, stdin: stdin, lines: make(chan agentLine, 64)}
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 64*1024), 64*1024)
		for scanner.Scan() {
			a.lines <- agentLine{text: scanner.Text()}
		}
		a.lines <- agentLine{err: scanner.Err()}
	}()
	t.Cleanup(func() {
		// Best-effort kill for failure paths; the main flow always
		// reaps the process explicitly.
		_ = cmd.Process.Kill()
	})
	first := a.next(t, 60*time.Second)
	if first != "READY" {
		raw, _ := os.ReadFile(stderrFile)
		t.Fatalf("backupagent boot: %q (stderr: %s)", first, raw)
	}
	addAgentPeer(t, cluster, id.String(), replAddr)
	return a
}

func clusterNodeIDs(cluster *harness.Cluster) []db.NodeID {
	var out []db.NodeID
	for _, node := range cluster.Nodes {
		out = append(out, node.NodeID)
	}
	return out
}

func (a *agent) next(t *testing.T, timeout time.Duration) string {
	t.Helper()
	select {
	case l := <-a.lines:
		if l.err != nil {
			t.Fatalf("backupagent output ended: %v", l.err)
		}
		return l.text
	case <-time.After(timeout):
		t.Fatalf("backupagent response timeout after %v", timeout)
	}
	return ""
}

func (a *agent) roundTrip(t *testing.T, cmd string, timeout time.Duration) string {
	t.Helper()
	if _, err := io.WriteString(a.stdin, cmd+"\n"); err != nil {
		t.Fatalf("backupagent write %q: %v", cmd, err)
	}
	return a.next(t, timeout)
}

// adminClient speaks to daemon admin endpoints with the cluster's mTLS
// material (same credentials the harness uses for its own API calls).
func adminClient(t *testing.T, cluster *harness.Cluster) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(cluster.CA.CertPEM) {
		t.Fatal("parse CA")
	}
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(cluster.Nodes[0].TLSDir, "node.crt"),
		filepath.Join(cluster.Nodes[0].TLSDir, "node.key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}}},
	}
}

func addAgentPeer(t *testing.T, cluster *harness.Cluster, agentID, replAddr string) {
	t.Helper()
	client := adminClient(t, cluster)
	body, _ := json.Marshal(map[string]any{"node_id": agentID, "addrs": []string{replAddr}})
	for _, n := range cluster.Nodes {
		url := fmt.Sprintf("https://%s/v1/admin/add_peer", n.APIAddr)
		var lastErr error
		for attempt := 0; attempt < 10; attempt++ {
			resp, err := client.Post(url, "application/json", bytes.NewReader(body))
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					lastErr = nil
					break
				}
				lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			} else {
				lastErr = err
			}
			time.Sleep(200 * time.Millisecond)
		}
		if lastErr != nil {
			t.Fatalf("add agent peer on %s: %v", n.Label, lastErr)
		}
	}
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

func TestBackupUnderFire(t *testing.T) {
	ctx := context.Background()
	agentBin := buildAgent(t)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "backup-under-fire", NumNodes: 2, AwaitUnlock: true, TypedRecords: true,
	})
	writeDur := writeSeconds(t)

	// Sustained writes on both mesh nodes for the whole load phase.
	stopWriters := make(chan struct{})
	var writersWG sync.WaitGroup
	var okWrites, failedWrites atomic.Int64
	for w := 0; w < 2; w++ {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			ticker := time.NewTicker(25 * time.Millisecond)
			defer ticker.Stop()
			seq := 0
			for {
				select {
				case <-stopWriters:
					return
				case <-ticker.C:
					seq++
					if err := cluster.TypedInsert(w, fmt.Sprintf("w%d-%d", w, seq)); err != nil {
						failedWrites.Add(1)
					} else {
						okWrites.Add(1)
					}
				}
			}
		}(w)
	}
	loadStart := time.Now()

	// Agent run A: join mid-load, take two fast online backups, then a
	// throttled backup that dies to SIGKILL mid-stream.
	agentA := startAgent(t, agentBin, cluster, fmt.Sprintf("127.0.0.1:%d", freePort(t)))
	snapNames, err := cluster.TypedNames(0)
	if err != nil {
		t.Fatal(err)
	}
	snap := len(snapNames)
	target := snap
	if target < 100 {
		target = 100
	}
	if got := agentA.roundTrip(t, fmt.Sprintf("WAIT %d 120", target), 130*time.Second); !strings.HasPrefix(got, "WAIT_OK") {
		t.Fatalf("agent A converge: %q", got)
	} else {
		t.Logf("agent A converged mid-load: %s", got)
	}
	backupRoot := t.TempDir()
	b1 := filepath.Join(backupRoot, "b1")
	b1Name := fastBackup(t, agentA, b1)
	b2 := filepath.Join(backupRoot, "b2")
	b2Name := fastBackup(t, agentA, b2)
	t.Logf("online backups mid-load: b1=%s b2=%s", b1Name, b2Name)

	b3 := filepath.Join(backupRoot, "b3")
	if err := os.MkdirAll(b3, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(agentA.stdin, fmt.Sprintf("BACKUP %s 16384 200\n", b3)); err != nil {
		t.Fatal(err)
	}
	remnant := waitPartial(t, b3, 30*time.Second)
	t.Logf("killing agent mid-backup with %d-byte partial stream", remnant.size)
	if err := agentA.cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL agent: %v", err)
	}
	_ = agentA.cmd.Wait()
	assertNoArchive(t, b3)
	assertPartialRejected(ctx, t, remnant.path)

	// Keep the load up for the configured duration, then quiesce.
	if elapsed := time.Since(loadStart); elapsed < writeDur {
		time.Sleep(writeDur - elapsed)
	}
	close(stopWriters)
	writersWG.Wait()
	total := int(okWrites.Load())
	if got := failedWrites.Load(); got != 0 {
		t.Fatalf("%d writes failed during backup load, want zero-downtime", got)
	}
	t.Logf("load phase: %d writes, zero failures", total)
	waitConverged(t, cluster, total, 90*time.Second)
	survivorNames, err := cluster.TypedNames(0)
	if err != nil {
		t.Fatal(err)
	}
	survivorDigest := namesDigest(survivorNames)

	// Agent run B: rejoin fresh, converge on the quiesced total, take the
	// final online backup, and exit cleanly.
	agentB := startAgent(t, agentBin, cluster, fmt.Sprintf("127.0.0.1:%d", freePort(t)))
	if got := agentB.roundTrip(t, fmt.Sprintf("WAIT %d 120", total), 130*time.Second); !strings.HasPrefix(got, "WAIT_OK") {
		t.Fatalf("agent B converge: %q", got)
	}
	b4 := filepath.Join(backupRoot, "b4")
	b4Name := fastBackup(t, agentB, b4)
	if got := agentB.roundTrip(t, "EXIT", 30*time.Second); got != "BYE" {
		t.Fatalf("agent B exit: %q", got)
	}
	if err := agentB.cmd.Wait(); err != nil {
		t.Fatalf("agent B exit code: %v", err)
	}
	t.Logf("final online backup: b4=%s", b4Name)

	// Restore every completed backup: intermediate snapshots must be
	// non-empty, non-decreasing, and subsets of the final data; the
	// final backup must match the survivors exactly (zero lost rows).
	finalNames := make(map[string]bool, len(survivorNames))
	for _, name := range survivorNames {
		finalNames[name] = true
	}
	c1 := restoreAndCount(ctx, t, cluster, b1, b1Name)
	c2 := restoreAndCount(ctx, t, cluster, b2, b2Name)
	if c1 <= 0 || c2 < c1 {
		t.Fatalf("intermediate backup counts %d/%d not positive non-decreasing", c1, c2)
	}
	assertSubset(t, cluster, b1, b1Name, finalNames)
	assertSubset(t, cluster, b2, b2Name, finalNames)
	c4, d4 := restoreCountDigest(ctx, t, cluster, b4, b4Name)
	if c4 != total {
		t.Fatalf("final backup count = %d, want %d (lost rows)", c4, total)
	}
	if d4 != survivorDigest {
		t.Fatalf("final backup digest %s != survivors %s", d4, survivorDigest)
	}
	t.Logf("restored b1=%d b2=%d b4=%d rows; final digest matches survivors", c1, c2, c4)
}

func fastBackup(t *testing.T, a *agent, subdir string) string {
	t.Helper()
	got := a.roundTrip(t, fmt.Sprintf("BACKUP %s 0 0", subdir), 120*time.Second)
	name, ok := strings.CutPrefix(got, "BACKUP_OK ")
	if !ok {
		t.Fatalf("online backup: %q", got)
	}
	return name
}

type partialFile struct {
	path string
	size int64
}

// waitPartial waits for an in-progress backup stream (.tmp-*) to reach a
// size proving the archive is genuinely mid-stream, and returns it.
func waitPartial(t *testing.T, dir string, timeout time.Duration) partialFile {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.Contains(e.Name(), ".tmp-") {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			if info.Size() >= 2048 {
				return partialFile{path: filepath.Join(dir, e.Name()), size: info.Size()}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no partial backup stream appeared in %s within %v", dir, timeout)
	return partialFile{}
}

// assertNoArchive requires that the killed backup published no completed
// artifact (the local destination's tmp+rename protocol must not have
// completed) and that the remnant is invisible to backup listing.
func assertNoArchive(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tar.gz") {
			t.Fatalf("killed backup published completed artifact %s (throttle too fast?)", e.Name())
		}
	}
	dest, err := backup.NewLocalDestination(dir)
	if err != nil {
		t.Fatal(err)
	}
	list, err := dest.ListBackups(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("killed backup listed %d backups, want zero", len(list))
	}
	t.Log("killed backup published no artifact; remnant invisible to listing")
}

// assertPartialRejected proves defense in depth: even if a partial stream
// were mistaken for a completed artifact (as a non-atomic destination
// would leave behind), restore fails closed and publishes no intent.
func assertPartialRejected(ctx context.Context, t *testing.T, remnant string) {
	t.Helper()
	scratch := t.TempDir()
	fake := filepath.Join(scratch, "partial.tar.gz")
	raw, err := os.ReadFile(remnant)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fake, raw, 0600); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	_, err = backup.Restore(ctx, backup.RestoreConfig{
		Source:      mustLocalDest(t, scratch),
		BackupName:  "partial.tar.gz",
		TargetPath:  target,
		FreshNodeID: db.NewNodeID().String(),
		Mode:        backup.RestoreClone,
	})
	if err == nil {
		t.Fatal("partial-artifact restore succeeded, want rejection")
	}
	t.Logf("partial artifact rejected: %v", err)
	if _, statErr := os.Stat(filepath.Join(target, backup.RestoreIntentFileName)); !os.IsNotExist(statErr) {
		t.Fatalf("restore intent present after partial rejection (err=%v)", statErr)
	}
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		var first string
		for i := range c.Nodes {
			names, err := c.TypedNames(i)
			if err != nil || len(names) != want {
				ready = false
				break
			}
			d := namesDigest(names)
			if i == 0 {
				first = d
			} else if d != first {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("mesh did not converge on %d rows within %v", want, timeout)
}

func namesDigest(names []string) string {
	copyNames := append([]string(nil), names...)
	sort.Strings(copyNames)
	h := sha256.New()
	for _, name := range copyNames {
		fmt.Fprintf(h, "%s\n", name)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func mustLocalDest(t *testing.T, dir string) *backup.LocalDestination {
	t.Helper()
	d, err := backup.NewLocalDestination(dir)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func keyBytes(t *testing.T, keyHex string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(keyHex)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func offlineOpen(t *testing.T, ctx context.Context, cluster *harness.Cluster, dataDir, nodeID string) *db.DB {
	t.Helper()
	node, err := db.ParseNodeID(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := db.Open(ctx, db.Config{
		OriginSigning: cluster.OriginSigning(node),
		Path:          dataDir,
		NodeID:        node,
		DBID:          cluster.DBID,
		Schema:        db.SchemaConfig{Version: 1},
		Tables:        []db.TableDefinition{typedDefinition(t)},
		Spool:         db.DefaultSpoolConfig(),
		// The agent stores use the same key ID the daemon unlock path
		// uses, by suite construction.
		Encryption: db.EncryptionConfig{Key: keyBytes(t, cluster.Nodes[0].KeyHex), KeyID: "remote-unlock-key"},
	})
	if err != nil {
		t.Fatalf("offline open: %v", err)
	}
	return handle
}

func restoreTo(t *testing.T, ctx context.Context, srcDir, name string) (target, fresh string) {
	t.Helper()
	target = t.TempDir()
	fresh = db.NewNodeID().String()
	if _, err := backup.Restore(ctx, backup.RestoreConfig{
		Source:      mustLocalDest(t, srcDir),
		BackupName:  name,
		TargetPath:  target,
		FreshNodeID: fresh,
		Mode:        backup.RestoreClone,
	}); err != nil {
		t.Fatalf("restore %s: %v", name, err)
	}
	return target, fresh
}

func restoreAndCount(ctx context.Context, t *testing.T, cluster *harness.Cluster, srcDir, name string) int {
	t.Helper()
	target, fresh := restoreTo(t, ctx, srcDir, name)
	handle := offlineOpen(t, ctx, cluster, target, fresh)
	defer handle.Close()
	rows, err := db.TableOf[backupFireRecord](handle, "live_typed_records")
	if err != nil {
		t.Fatal(err)
	}
	n, err := rows.Where().Count()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func restoreCountDigest(ctx context.Context, t *testing.T, cluster *harness.Cluster, srcDir, name string) (int, string) {
	t.Helper()
	target, fresh := restoreTo(t, ctx, srcDir, name)
	handle := offlineOpen(t, ctx, cluster, target, fresh)
	defer handle.Close()
	rows, err := db.TableOf[backupFireRecord](handle, "live_typed_records")
	if err != nil {
		t.Fatal(err)
	}
	n, err := rows.Where().Count()
	if err != nil {
		t.Fatal(err)
	}
	return n, offlineDigest(ctx, t, handle)
}

// offlineDigest uses the same canonical unique-name digest as TypedNames.
func offlineDigest(ctx context.Context, t *testing.T, handle *db.DB) string {
	t.Helper()
	rows, err := db.TableOf[backupFireRecord](handle, "live_typed_records")
	if err != nil {
		t.Fatal(err)
	}
	records, err := rows.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(records))
	for _, row := range records {
		names = append(names, row.Name)
	}
	return namesDigest(names)
}

// daemonIDSet returns unique marker names visible on node 0.
func daemonIDSet(t *testing.T, cluster *harness.Cluster) map[string]bool {
	t.Helper()
	names, err := cluster.TypedNames(0)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]bool, len(names))
	for _, name := range names {
		out[name] = true
	}
	return out
}

// assertSubset requires every row ID in a restored intermediate backup to
// exist in the survivors' final set (no phantom rows from online reads).
func assertSubset(t *testing.T, cluster *harness.Cluster, srcDir, name string, final map[string]bool) {
	t.Helper()
	ctx := context.Background()
	target, fresh := restoreTo(t, ctx, srcDir, name)
	handle := offlineOpen(t, ctx, cluster, target, fresh)
	defer handle.Close()
	rows, err := db.TableOf[backupFireRecord](handle, "live_typed_records")
	if err != nil {
		t.Fatal(err)
	}
	records, err := rows.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, row := range records {
		if !final[row.Name] {
			t.Fatalf("backup %s restores phantom row %s", name, row.Name)
		}
		checked++
	}
	t.Logf("backup %s: %d rows all present in final set", name, checked)
}
