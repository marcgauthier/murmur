// File-permission acceptance: private key material created or owned by the
// product must carry no group/other permission bits (0600 files, 0700 dirs),
// after fresh init and again after backup plus restore.
//
// Scope: this suite asserts the paths the PRODUCT controls: the encrypted
// key container (data/keys.enc), the data directory, and
// backup/restore artifacts. Operator-owned node directories, public
// certificates, log files, and the TLS private key are created by the test
// harness and daemon fixture (node.key is minted and written by the
// harness at 0600); the suite records their observed modes for audit but
// does not assert on them, because no product change can alter
// fixture-chosen modes.
package filepermissions_test

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

type permissionRecord struct {
	ID    ids.RowID `rime:"primary"`
	Name  string
	Count int64
	Tags  []string
	Peak  int64
	Floor float64
}

type permissionLocalRecord struct {
	ID   ids.RowID `rime:"primary"`
	Name string
}

func tableDefinitions() ([]db.TableDefinition, error) {
	replicated, err := db.Define[permissionRecord]("live_typed_records", 901, db.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2, "Count": 3, "Tags": 4, "Peak": 5, "Floor": 6},
		MergePolicies: map[string]db.RecordMergePolicy{
			"Count": db.RecordMergeCounter, "Tags": db.RecordMergeORSet,
			"Peak": db.RecordMergeMax, "Floor": db.RecordMergeMin,
		},
	})
	if err != nil {
		return nil, err
	}
	local, err := db.Define[permissionLocalRecord]("live_node_local_records", 902, db.RecordOptions{
		PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}, Scope: db.TableScopeNodeLocal,
	})
	if err != nil {
		return nil, err
	}
	return []db.TableDefinition{replicated, local}, nil
}

func TestFilePermissionsFreshInitAndBackupRestore(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file permissions (0600/0700) do not apply to Windows NTFS ACLs")
	}
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "file-permissions",
		NumNodes:     2,
		AwaitUnlock:  true,
		TypedRecords: true,
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
		if err := cluster.TypedInsert(0, fmt.Sprintf("row-%d", i)); err != nil {
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
	// The restored key container and data directory in particular must be private.
	assertTreeLockedDown(t, filepath.Join(restoreTarget, "data"), "restored-data")

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
		filepath.Join(node.Dir, "data", "keys.enc"),
		filepath.Join(node.Dir, "data"),
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
			node.Dir, node.DataDir, node.LogsDir, node.SchemaDir,
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
		var first []string
		for i := range c.Nodes {
			names, err := c.TypedNames(i)
			if err != nil || len(names) != want {
				ok = false
				break
			}
			if i == 0 {
				first = names
			} else if !reflect.DeepEqual(names, first) {
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

func mustTableDefinitions(t *testing.T) []db.TableDefinition {
	t.Helper()
	tables, err := tableDefinitions()
	if err != nil {
		t.Fatal(err)
	}
	return tables
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
	// The store pins the origin signing key under the NodeID; the
	// offline open must present the same identity as the daemon.
	registry, _ := origin.NewKeyRegistry(nil)
	for _, n := range cluster.Nodes {
		_ = registry.Add(n.NodeID, n.OriginKey.Public().(ed25519.PublicKey))
	}
	// NOTE: no testdb.Configure here — it overwrites OriginSigning with
	// deterministic fixture keys, but this store pinned the daemon's
	// real key. Offline opens carry no replication peers, so Configure
	// would contribute nothing else.
	handle, err := db.Open(ctx, db.Config{
		Path:          node.Dir,
		NodeID:        node.NodeID,
		DBID:          cluster.DBID,
		OriginSigning: db.OriginSigningConfig{PrivateKey: node.OriginKey, TrustedKeys: registry},
		Tables:        mustTableDefinitions(t),
		Spool:         db.DefaultSpoolConfig(),
		Encryption:    db.EncryptionConfig{Key: raw, KeyID: "remote-unlock-key"},
	})
	if err != nil {
		t.Fatalf("open node dir: %v", err)
	}
	return handle
}
