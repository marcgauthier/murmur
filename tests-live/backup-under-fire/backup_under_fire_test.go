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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/backup"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
		Name: "buf_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}}
}

func agentTags() string {
	if tags := os.Getenv("SPEDSQL_TAGS"); tags != "" {
		return tags
	}
	return "sqlite_preupdate_hook sqlite_fts5"
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

// buildAgent compiles the suite-local backup agent with the same tags as
// the daemon under test (SPEDSQL_TAGS-aware, so the modernc config builds
// a matching agent).
func buildAgent(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "backupagent")
	cmd := exec.Command("go", "build", "-tags", agentTags(), "-o", out, "./tests-live/backup-under-fire/backupagent")
	cmd.Dir = repoRoot(t)
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build backupagent: %v: %s", err, raw)
	}
	return out
}

func writeSeconds(t *testing.T) time.Duration {
	t.Helper()
	if v := os.Getenv("SPEDSQL_BACKUP_UNDER_FIRE_WRITE_SECONDS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("bad SPEDSQL_BACKUP_UNDER_FIRE_WRITE_SECONDS=%q", v)
		}
		return time.Duration(n) * time.Second
	}
	return 30 * time.Second
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
	pebbleDir := filepath.Join(root, "pebble")
	if err := os.MkdirAll(pebbleDir, 0755); err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := cluster.CA.IssueNode(id, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(root, "ca.crt")
	certFile := filepath.Join(root, "node.crt")
	keyFile := filepath.Join(root, "node.key")
	if err := os.WriteFile(caFile, cluster.CA.CertPEM, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, certPEM, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
	schemaRaw, err := json.Marshal(schemaConfig())
	if err != nil {
		t.Fatal(err)
	}
	schemaFile := filepath.Join(root, "schema.json")
	if err := os.WriteFile(schemaFile, schemaRaw, 0644); err != nil {
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
		"--pebble-dir", pebbleDir,
		"--node-id", id.String(),
		"--db-id", cluster.DBID.String(),
		"--key-hex", cluster.Nodes[0].KeyHex,
		"--key-id", "remote-unlock-key",
		"--schema", schemaFile,
		"--repl-addr", replAddr,
		"--peers", strings.Join(peers, ","),
		"--ca", caFile,
		"--cert", certFile,
		"--key", keyFile,
		"--table", "buf_rows",
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
		Name:        "backup-under-fire",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema:      schemaConfig(),
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
					id := fmt.Sprintf("%02x%030x", w, seq)
					seq++
					if err := cluster.ExecSQL(w, "INSERT INTO buf_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("w%d-%d", w, seq)); err != nil {
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
	snap, err := cluster.QueryRowCount(0, "buf_rows")
	if err != nil {
		t.Fatal(err)
	}
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
	survivorDigest, err := cluster.ComputeTableDigest(0, "buf_rows", "id")
	if err != nil {
		t.Fatal(err)
	}

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
	finalIDs := daemonIDSet(t, cluster)
	c1 := restoreAndCount(ctx, t, cluster, b1, b1Name)
	c2 := restoreAndCount(ctx, t, cluster, b2, b2Name)
	if c1 <= 0 || c2 < c1 {
		t.Fatalf("intermediate backup counts %d/%d not positive non-decreasing", c1, c2)
	}
	assertSubset(t, cluster, b1, b1Name, finalIDs)
	assertSubset(t, cluster, b2, b2Name, finalIDs)
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
			n, err := c.QueryRowCount(i, "buf_rows")
			if err != nil || n != want {
				ready = false
				break
			}
			d, err := c.ComputeTableDigest(i, "buf_rows", "id")
			if err != nil {
				ready = false
				break
			}
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

func offlineOpen(t *testing.T, ctx context.Context, cluster *harness.Cluster, pebbleDir, nodeID string) *db.DB {
	t.Helper()
	node, err := db.ParseNodeID(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := db.Open(ctx, db.Config{
		Path:   pebbleDir,
		NodeID: node,
		DBID:   cluster.DBID,
		Schema: *schemaConfig(),
		Pebble: db.DefaultPebbleConfig(),
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
	rows, err := handle.QueryContext(ctx, "SELECT count(*) FROM buf_rows")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("no count row")
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func restoreCountDigest(ctx context.Context, t *testing.T, cluster *harness.Cluster, srcDir, name string) (int, string) {
	t.Helper()
	target, fresh := restoreTo(t, ctx, srcDir, name)
	handle := offlineOpen(t, ctx, cluster, target, fresh)
	defer handle.Close()
	rows, err := handle.QueryContext(ctx, "SELECT count(*) FROM buf_rows")
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
	}
	_ = rows.Close()
	return n, offlineDigest(ctx, t, handle)
}

// offlineDigest mirrors harness.ComputeTableDigest exactly, including its
// JSON value rendering (see tampered-backup for the rationale).
func offlineDigest(ctx context.Context, t *testing.T, handle *db.DB) string {
	t.Helper()
	rows, err := handle.QueryContext(ctx, "SELECT * FROM buf_rows ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols := rows.Columns()
	h := sha256.New()
	for rows.Next() {
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		for _, cell := range dest {
			raw, err := json.Marshal(cell)
			if err != nil {
				t.Fatal(err)
			}
			var v any
			if err := json.Unmarshal(raw, &v); err != nil {
				t.Fatal(err)
			}
			h.Write([]byte(fmt.Sprintf("%v:", v)))
		}
		h.Write([]byte("\n"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// daemonIDSet returns the hex row IDs visible on node 0 (HTTP values
// render blobs as base64).
func daemonIDSet(t *testing.T, cluster *harness.Cluster) map[string]bool {
	t.Helper()
	res, err := cluster.QuerySQL(0, "SELECT id FROM buf_rows ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]bool, len(res.Rows))
	for _, row := range res.Rows {
		s, _ := row[0].(string)
		raw, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			t.Fatalf("decode id %q: %v", s, err)
		}
		out[hex.EncodeToString(raw)] = true
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
	rows, err := handle.QueryContext(ctx, "SELECT id FROM buf_rows ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	checked := 0
	for rows.Next() {
		var cell any
		if err := rows.Scan(&cell); err != nil {
			t.Fatal(err)
		}
		var hx string
		switch v := cell.(type) {
		case []byte:
			hx = hex.EncodeToString(v)
		case string:
			hx = hex.EncodeToString([]byte(v))
		default:
			t.Fatalf("id cell type %T", cell)
		}
		if !final[hx] {
			t.Fatalf("backup %s restores phantom row %s", name, hx)
		}
		checked++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("backup %s: %d rows all present in final set", name, checked)
}
