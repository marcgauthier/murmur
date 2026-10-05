// Backup, disaster, fresh-identity restore, and mesh rejoin acceptance.
//
// Three spedsql daemons mesh; node3 stops and the test takes an offline
// backup of its durable directory through the library, wipes the
// directory to simulate total loss, restores the backup under a fresh
// writer identity (reissued TLS certificate included), and restarts the
// daemon on the restored data. The mesh must reconverge with no lost
// rows. A second phase proves rollback safety: opening restored data
// under any NodeID other than the restore intent's fresh identity is
// rejected.
package backuprestore_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
		Name: "br_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}}
}

func TestBackupRestoreRejoinMesh(t *testing.T) {
	ctx := context.Background()
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "backup-restore",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema:      schemaConfig(),
	})
	node3 := cluster.Nodes[2]

	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("%032x", 5000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO br_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	waitConverged(t, cluster, 20, 30*time.Second)

	// Offline backup of the stopped node through the library.
	cluster.StopNode(2)
	backupDir := t.TempDir()
	offline := openNodeDir(t, ctx, cluster, node3)
	meta, err := offline.Backup(ctx, backup.Config{Destination: mustLocalDest(t, backupDir)})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	t.Logf("backup %s bytes=%d", meta.BackupID, meta.TotalBytes)
	if err := offline.Close(); err != nil {
		t.Fatalf("close offline: %v", err)
	}

	// Total loss of node3's durable directory, then fresh-identity
	// restore into the same location the daemon will reopen.
	if err := os.RemoveAll(node3.PebbleDir); err != nil {
		t.Fatal(err)
	}
	fresh := db.NewNodeID()
	if _, err := backup.Restore(ctx, backup.RestoreConfig{
		Source:      mustLocalDest(t, backupDir),
		TargetPath:  node3.PebbleDir,
		FreshNodeID: fresh.String(),
		Mode:        backup.RestoreClone,
		Overwrite:   true,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	rewriteNodeConfig(t, node3, fresh.String())
	reissueNodeCert(t, cluster, node3, fresh.String())

	// Restart on restored data: the daemon must adopt the fresh identity
	// and the mesh must reconverge with no lost rows.
	cluster.StartNode(2)
	cluster.UnlockNode(2, node3.KeyHex)
	cluster.WaitNodeReady(2)
	waitConverged(t, cluster, 20, 90*time.Second)
	if got := infoLabel(t, node3.APIAddr, "node_id"); got != fresh.String() {
		t.Fatalf("node3 identity = %s, want fresh %s", got, fresh.String())
	}
}

func TestRestoreRejectsStaleIdentity(t *testing.T) {
	ctx := context.Background()
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "backup-restore-id",
		NumNodes:    1,
		AwaitUnlock: true,
		Schema:      schemaConfig(),
	})
	node := cluster.Nodes[0]
	if err := cluster.ExecSQL(0, "INSERT INTO br_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 9), "solo"); err != nil {
		t.Fatal(err)
	}
	cluster.StopNode(0)

	backupDir := t.TempDir()
	offline := openNodeDir(t, ctx, cluster, node)
	if _, err := offline.Backup(ctx, backup.Config{Destination: mustLocalDest(t, backupDir)}); err != nil {
		t.Fatal(err)
	}
	if err := offline.Close(); err != nil {
		t.Fatal(err)
	}

	staging := t.TempDir()
	fresh := db.NewNodeID()
	if _, err := backup.Restore(ctx, backup.RestoreConfig{
		Source:      mustLocalDest(t, backupDir),
		TargetPath:  staging,
		FreshNodeID: fresh.String(),
		Mode:        backup.RestoreClone,
	}); err != nil {
		t.Fatal(err)
	}
	// Opening under the OLD identity must be rejected (rollback safety).
	badCfg := offlineConfig(t, staging, cluster, node.NodeID.String())
	if _, err := db.Open(ctx, badCfg); err == nil {
		t.Fatal("open under stale identity succeeded, want rejection")
	} else {
		t.Logf("stale identity rejected: %v", err)
	}
	// Opening under the fresh identity succeeds.
	goodCfg := offlineConfig(t, staging, cluster, fresh.String())
	restored, err := db.Open(ctx, goodCfg)
	if err != nil {
		t.Fatalf("open under fresh identity: %v", err)
	}
	_ = restored.Close()
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

func offlineConfig(t *testing.T, pebbleDir string, cluster *harness.Cluster, nodeID string) db.Config {
	t.Helper()
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
		Path:          pebbleDir,
		NodeID:        node,
		DBID:          cluster.DBID,
		OriginSigning: db.OriginSigningConfig{PrivateKey: signingKey, TrustedKeys: registry},
		Schema:        *schemaConfig(),
		Pebble:        db.DefaultPebbleConfig(),
		// The daemon unlocks with key_id "remote-unlock-key" (see
		// handleAdminUnlock); the offline open must use the same ID.
		Encryption: db.EncryptionConfig{Key: keyBytes(t, cluster.Nodes[0].KeyHex), KeyID: "remote-unlock-key"},
	}
}

func openNodeDir(t *testing.T, ctx context.Context, cluster *harness.Cluster, node *harness.Node) *db.DB {
	t.Helper()
	handle, err := db.Open(ctx, offlineConfig(t, node.PebbleDir, cluster, node.NodeID.String()))
	if err != nil {
		t.Fatalf("open node dir: %v", err)
	}
	return handle
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
	if originKeys, ok := cfg["origin_public_keys"].(map[string]any); ok {
		originKeys[freshNodeID] = hex.EncodeToString(node.OriginKey.Public().(ed25519.PublicKey))
		cfg["origin_public_keys"] = originKeys
	}
	if sources, ok := cfg["trusted_snapshot_sources"].([]any); ok {
		cfg["trusted_snapshot_sources"] = append(sources, freshNodeID)
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

func mustParseNodeID(t *testing.T, s string) db.NodeID {
	t.Helper()
	id, err := db.ParseNodeID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
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

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "br_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "br_rows", "name")
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

func infoLabel(t *testing.T, apiAddr, label string) string {
	t.Helper()
	body := scrapeMetrics(t, apiAddr)
	for _, line := range strings.Split(body, "\n") {
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
