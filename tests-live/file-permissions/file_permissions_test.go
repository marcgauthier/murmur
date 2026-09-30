// File-permission acceptance: private key material created or owned by the
// product must carry no group/other permission bits (0600 files, 0700 dirs),
// after fresh init and again after backup plus restore.
//
// Scope: this suite asserts the paths the PRODUCT controls: the encrypted
// key-registry directory (pebble/keys), the registry files, and
// backup/restore artifacts. Operator-owned node directories, public
// certificates, log files, and the TLS private key are created by the test
// harness and daemon fixture (node.key is minted and written by the
// harness at 0600); the suite records their observed modes for audit but
// does not assert on them, because no product change can alter
// fixture-chosen modes.
package filepermissions_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
		Name: "perm_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}}
}

func TestFilePermissionsFreshInitAndBackupRestore(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "file-permissions",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema:      schemaConfig(),
	})

	// Phase 1: fresh init. Every node's private key material must already
	// be locked down before any data is written.
	for i := range cluster.Nodes {
		assertSecretPathsLockedDown(t, cluster, i, "fresh-init")
	}
	logModeTable(t, cluster)

	// Positive control: honest writes replicate and converge, proving the
	// permission posture does not break the database.
	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("%032x", 9000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO perm_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	waitConverged(t, cluster, 25, 30*time.Second)

	// Phase 2: backup plus restore through the product library, then
	// re-assert: backup artifacts, the restored tree, and the live nodes.
	ctx := context.Background()
	cluster.StopNode(1)
	backupParent := t.TempDir()
	backupDir := filepath.Join(backupParent, "dest")
	offline := openNodeDir(t, ctx, cluster, cluster.Nodes[1])
	meta, err := offline.Backup(ctx, backup.Config{Destination: mustLocalDest(t, backupDir)})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	t.Logf("backup %s bytes=%d", meta.BackupID, meta.TotalBytes)
	if err := offline.Close(); err != nil {
		t.Fatalf("close offline: %v", err)
	}
	assertTreeLockedDown(t, backupDir, "backup-artifacts")

	staging := t.TempDir()
	restoreTarget := filepath.Join(staging, "restored")
	fresh := db.NewNodeID()
	if _, err := backup.Restore(ctx, backup.RestoreConfig{
		Source:      mustLocalDest(t, backupDir),
		TargetPath:  restoreTarget,
		FreshNodeID: fresh.String(),
		Mode:        backup.RestoreClone,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	assertTreeLockedDown(t, restoreTarget, "restored-tree")
	// The restored key registry in particular must be private.
	assertTreeLockedDown(t, filepath.Join(restoreTarget, "keys"), "restored-keys")

	cluster.StartNode(1)
	cluster.UnlockNode(1, cluster.Nodes[1].KeyHex)
	cluster.WaitNodeReady(1)
	waitConverged(t, cluster, 25, 60*time.Second)

	for i := range cluster.Nodes {
		assertSecretPathsLockedDown(t, cluster, i, "post-backup-restore")
	}
	t.Logf("PASS: private key material locked down (0600/0700) fresh and post restore")
}

// assertSecretPathsLockedDown fails when any product-owned secret path on
// node idx carries group/other permission bits. node.key is deliberately
// absent: it is harness-minted fixture material, audit-logged in
// logModeTable instead of asserted here.
func assertSecretPathsLockedDown(t *testing.T, cluster *harness.Cluster, idx int, phase string) {
	t.Helper()
	node := cluster.Nodes[idx]
	secretPaths := []string{
		filepath.Join(node.PebbleDir, "keys"),
	}
	for _, p := range secretPaths {
		assertTreeLockedDown(t, p, fmt.Sprintf("%s node%d %s", phase, idx, p))
	}
}

// assertTreeLockedDown walks root (a file or directory) and fails on the
// first path whose mode grants anything to group or other.
func assertTreeLockedDown(t *testing.T, root, label string) {
	t.Helper()
	var violations []string
	walkErr := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().Perm()&0o077 != 0 {
			violations = append(violations, fmt.Sprintf("%s mode=%04o", path, info.Mode().Perm()))
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("%s: walk %s: %v", label, root, walkErr)
	}
	if len(violations) > 0 {
		t.Fatalf("%s: %d paths grant group/other access:\n  %s",
			label, len(violations), joinLines(violations, 10))
	}
	t.Logf("%s: %s locked down (no group/other bits)", label, root)
}

func joinLines(lines []string, max int) string {
	out := ""
	for i, l := range lines {
		if i >= max {
			out += fmt.Sprintf("\n  ... and %d more", len(lines)-max)
			break
		}
		if i > 0 {
			out += "\n  "
		}
		out += l
	}
	return out
}

// logModeTable records the observed modes of operator-owned paths for audit
// evidence. These modes are chosen by the test fixture, not the product.
func logModeTable(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	for i, node := range cluster.Nodes {
		for _, p := range []string{
			node.Dir, node.PebbleDir, node.LogsDir, node.SchemaDir,
			node.TLSDir, node.ConfigFile, node.LogFile,
			filepath.Join(node.TLSDir, "ca.crt"),
			filepath.Join(node.TLSDir, "node.crt"),
			filepath.Join(node.TLSDir, "node.key"),
		} {
			fi, err := os.Stat(p)
			if err != nil {
				t.Logf("node%d %s: stat error: %v", i, p, err)
				continue
			}
			t.Logf("node%d %s: mode=%04o dir=%v", i, p, fi.Mode().Perm(), fi.IsDir())
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
			n, err := c.QueryRowCount(i, "perm_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "perm_rows", "name")
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
