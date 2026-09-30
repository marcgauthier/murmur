// Plaintext audit: unique high-entropy marker rows must never appear in
// plaintext anywhere on disk (data files, WAL, tmp, scratch, logs), and
// key material must not leak outside the provisioned config file. Backup
// artifacts must not contain marker plaintext either.
package plaintextaudit_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/backup"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
		Name: "audit_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "value", Type: schema.ColText, Nullable: true},
		},
	}}}
}

func TestPlaintextAuditMarkersAndKeysAbsentOnDisk(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "plaintext-audit",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema:      schemaConfig(),
	})
	node := cluster.Nodes[0]

	// Unique high-entropy markers: random per run, so a stale-file false
	// positive from another run is impossible.
	markers := make([]string, 8)
	for i := range markers {
		markers[i] = "SPEDSQL-AUDIT-" + randHex(t, 32) + fmt.Sprintf("-ROW%d", i)
		if err := cluster.ExecSQL(0, "INSERT INTO audit_rows (id, value) VALUES (?, ?)",
			fmt.Sprintf("%032x", 7000+i), markers[i]); err != nil {
			t.Fatalf("insert marker %d: %v", i, err)
		}
	}

	// Sustained load around the markers to exercise WAL, memtables,
	// compaction, and replication paths.
	for i := 0; i < 200; i++ {
		if err := cluster.ExecSQL(i%2, "INSERT INTO audit_rows (id, value) VALUES (?, ?)",
			fmt.Sprintf("%032x", 8000+i), fmt.Sprintf("load-filler-%d-padding-%s", i, strings.Repeat("x", 64))); err != nil {
			t.Fatalf("load write %d: %v", i, err)
		}
	}
	waitConverged(t, cluster, 208, 60*time.Second)

	// Crash mid-life so tmp/scratch/crash artifacts exist, then restart.
	cluster.KillNode(0)
	cluster.StartNode(0)
	cluster.UnlockNode(0, node.KeyHex)
	cluster.WaitNodeReady(0)
	waitConverged(t, cluster, 208, 60*time.Second)

	// Positive control: every marker round-trips byte-identical through
	// SQL, proving the rows exist and the scan below is meaningful.
	for i, want := range markers {
		res, err := cluster.QuerySQL(0, "SELECT value FROM audit_rows WHERE id = ?", fmt.Sprintf("%032x", 7000+i))
		if err != nil || len(res.Rows) != 1 {
			t.Fatalf("marker %d query: err=%v res=%v", i, err, res)
		}
		if got := fmt.Sprintf("%v", res.Rows[0][0]); got != want {
			t.Fatalf("marker %d = %q, want %q", i, got, want)
		}
	}
	t.Logf("positive control: all %d markers round-trip intact", len(markers))

	// Stop both nodes so the on-disk image is quiescent, then scan the
	// ENTIRE node directory of BOTH nodes: pebble, WAL, keys, tmp,
	// scratch, logs. Node 1 learned the rows via replication apply (not
	// local commit), so its disk image exercises different code paths
	// and must be audited too.
	cluster.StopNode(0)
	cluster.StopNode(1)
	for n := range cluster.Nodes {
		nodedir := cluster.Nodes[n].Dir
		for i, m := range markers {
			if hit, err := scanTree(nodedir, []byte(m), nil); err != nil {
				t.Fatalf("scan node%d marker %d: %v", n, i, err)
			} else if hit != "" {
				t.Fatalf("PLAINTEXT LEAK: marker %d found on disk in %s", i, hit)
			}
		}
		t.Logf("markers absent from entire node dir %s", nodedir)
	}

	// Key material must not leak outside the provisioned config file,
	// where the harness itself places the operator key.
	keyHex := cluster.Nodes[0].KeyHex
	keyRaw, err := hex.DecodeString(keyHex)
	if err != nil {
		t.Fatal(err)
	}
	for n := range cluster.Nodes {
		nnode := cluster.Nodes[n]
		skipConfig := map[string]bool{nnode.ConfigFile: true}
		for _, needle := range [][]byte{[]byte(keyHex), []byte(strings.ToUpper(keyHex)), keyRaw} {
			if hit, err := scanTree(nnode.Dir, needle, skipConfig); err != nil {
				t.Fatalf("scan node%d key material: %v", n, err)
			} else if hit != "" {
				t.Fatalf("KEY LEAK: key material found on disk in %s", hit)
			}
		}
	}
	t.Logf("key material absent outside provisioned config.json")

	// Backup artifacts must not contain marker plaintext (or the key).
	ctx := context.Background()
	backupParent := t.TempDir()
	backupDir := filepath.Join(backupParent, "dest")
	offline := openNodeDir(t, ctx, cluster, node)
	if _, err := offline.Backup(ctx, backup.Config{Destination: mustLocalDest(t, backupDir)}); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := offline.Close(); err != nil {
		t.Fatalf("close offline: %v", err)
	}
	for i, m := range markers {
		if hit, err := scanTree(backupDir, []byte(m), nil); err != nil {
			t.Fatalf("scan backup marker %d: %v", i, err)
		} else if hit != "" {
			t.Fatalf("PLAINTEXT LEAK: marker %d found in backup artifact %s", i, hit)
		}
	}
	if hit, err := scanTree(backupDir, []byte(keyHex), nil); err != nil {
		t.Fatalf("scan backup key: %v", err)
	} else if hit != "" {
		t.Fatalf("KEY LEAK: key material found in backup artifact %s", hit)
	}
	t.Logf("backup artifacts clean: no marker plaintext, no key material")

	// Restart and reconverge: the audit must not disturb honest behavior.
	cluster.StartNode(0)
	cluster.UnlockNode(0, node.KeyHex)
	cluster.WaitNodeReady(0)
	cluster.StartNode(1)
	cluster.UnlockNode(1, cluster.Nodes[1].KeyHex)
	cluster.WaitNodeReady(1)
	waitConverged(t, cluster, 208, 60*time.Second)
}

func randHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// scanTree reports the first regular file under root containing needle.
// Files in skip are not read. Reads stream with overlap so matches split
// across chunk boundaries are still found.
func scanTree(root string, needle []byte, skip map[string]bool) (string, error) {
	var hit string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || hit != "" {
			return err
		}
		if entry.IsDir() || skip[path] {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		found, err := fileContains(path, needle)
		if err != nil {
			return err
		}
		if found {
			hit = path
		}
		return nil
	})
	return hit, err
}

func fileContains(path string, needle []byte) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	const chunk = 64 * 1024
	buf := make([]byte, 0, chunk+len(needle))
	tmp := make([]byte, chunk)
	for {
		n, rerr := f.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			if bytes.Contains(buf, needle) {
				return true, nil
			}
			if len(buf) > len(needle) {
				buf = append(buf[:0], buf[len(buf)-len(needle):]...)
			}
		}
		if rerr == io.EOF {
			return false, nil
		}
		if rerr != nil {
			return false, rerr
		}
	}
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "audit_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "audit_rows", "value")
			if err != nil {
				ok = false
				break
			}
			if i == 0 {
				first = d
			} else if d != first {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %d rows within %v", want, timeout)
}

func mustLocalDest(t *testing.T, dir string) *backup.LocalDestination {
	t.Helper()
	d, err := backup.NewLocalDestination(dir)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func openNodeDir(t *testing.T, ctx context.Context, cluster *harness.Cluster, node *harness.Node) *db.DB {
	t.Helper()
	raw, err := hex.DecodeString(cluster.Nodes[0].KeyHex)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := db.Open(ctx, db.Config{
		Path:       node.PebbleDir,
		NodeID:     node.NodeID,
		DBID:       cluster.DBID,
		Schema:     *schemaConfig(),
		Pebble:     db.DefaultPebbleConfig(),
		Encryption: db.EncryptionConfig{Key: raw, KeyID: "remote-unlock-key"},
	})
	if err != nil {
		t.Fatalf("open node dir: %v", err)
	}
	return handle
}
