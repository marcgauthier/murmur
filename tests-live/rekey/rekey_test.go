// Multi-process storage-key rotation: one spedsql daemon process rotates
// its storage key without downtime, restarts on the new key with state
// intact, and rejects the retired key.
package rekey_test

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestStorageKeyRekeySurvivesRestart(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "rekey",
		NumNodes: 1,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "contacts", Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "phone", Type: schema.ColText, Nullable: true},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
		}}}},
	})
	oldKeyHex := cluster.Nodes[0].KeyHex

	for i := 0; i < 25; i++ {
		id := ids.NewRowID()
		if err := cluster.ExecSQL(0, `INSERT INTO contacts (id, name, phone, score) VALUES (?, ?, ?, ?)`,
			hex.EncodeToString(id[:]), fmt.Sprintf("person-%02d", i), fmt.Sprintf("phone-%02d", i), i); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	// Zero-downtime rotation: the running node rekeys and keeps serving.
	newKey := make([]byte, 32)
	if _, err := rand.Read(newKey); err != nil {
		t.Fatal(err)
	}
	newKeyHex := hex.EncodeToString(newKey)
	if err := cluster.RotateKey(0, "live-new-key", newKeyHex, string(db.AES256GCM)); err != nil {
		t.Fatalf("rotate key: %v", err)
	}
	st, err := cluster.EncryptionStatus(0)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := st["ApplicationKeyID"].(string); got != "live-new-key" {
		t.Fatalf("active application key id = %q, want live-new-key", got)
	}
	afterID := ids.NewRowID()
	if err := cluster.ExecSQL(0, `INSERT INTO contacts (id, name) VALUES (?, ?)`,
		hex.EncodeToString(afterID[:]), "after-rotate"); err != nil {
		t.Fatalf("post-rotate write: %v", err)
	}

	// Restart on the new key: state intact, new writes accepted.
	cluster.StopNode(0)
	rewriteKeyConfig(t, cluster, "live-new-key", newKeyHex)
	cluster.StartNode(0)
	cluster.WaitNodeReady(0)
	if n := rowCount(t, cluster); n != 26 {
		t.Fatalf("reopened row count = %d, want 26", n)
	}
	res, err := cluster.QuerySQL(0, `SELECT phone FROM contacts WHERE name = ?`, "person-17")
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("reopened query: %+v %v", res, err)
	}
	if phone, _ := res.Rows[0][0].(string); phone != "phone-17" {
		t.Fatalf("reopened row phone = %q", phone)
	}
	postID := ids.NewRowID()
	if err := cluster.ExecSQL(0, `INSERT INTO contacts (id, name) VALUES (?, ?)`,
		hex.EncodeToString(postID[:]), "after-rekey"); err != nil {
		t.Fatalf("post-rekey write: %v", err)
	}

	// The retired key no longer unlocks the database.
	cluster.StopNode(0)
	rewriteKeyConfig(t, cluster, "live-old-key", oldKeyHex)
	cluster.StartNode(0)
	assertNeverReady(t, cluster, 0, 8*time.Second)
}

// TestRekeyAwaitUnlockRestartsWithKeyID pins the rotate-then-restart
// path for await-unlock nodes: after rotation the registry only honors
// the new key ID, so the post-restart unlock must present (id, material)
// together. Unlocking with the new material but no ID 401s.
func TestRekeyAwaitUnlockRestartsWithKeyID(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "rekey-unlock-id",
		NumNodes:    1,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "contacts", Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		}}}},
	})
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
	cluster.WaitNodeReady(0)
	id := ids.NewRowID()
	if err := cluster.ExecSQL(0, `INSERT INTO contacts (id, name) VALUES (?, ?)`,
		hex.EncodeToString(id[:]), "pre-rotate"); err != nil {
		t.Fatalf("pre-rotate write: %v", err)
	}
	newKey := make([]byte, 32)
	if _, err := rand.Read(newKey); err != nil {
		t.Fatal(err)
	}
	newKeyHex := hex.EncodeToString(newKey)
	if err := cluster.RotateKey(0, "live-new-key", newKeyHex, string(db.AES256GCM)); err != nil {
		t.Fatalf("rotate key: %v", err)
	}
	// SIGKILL (not graceful stop): persistence of the rotation must
	// not depend on a clean shutdown.
	cluster.KillNode(0)
	rewriteKeyConfig(t, cluster, "live-new-key", newKeyHex)
	cluster.StartNode(0)
	cluster.UnlockNodeWithKeyID(0, "live-new-key", newKeyHex)
	cluster.WaitNodeReady(0)
	if n := rowCount(t, cluster); n != 1 {
		t.Fatalf("reopened row count = %d, want 1", n)
	}
	postID := ids.NewRowID()
	if err := cluster.ExecSQL(0, `INSERT INTO contacts (id, name) VALUES (?, ?)`,
		hex.EncodeToString(postID[:]), "post-restart"); err != nil {
		t.Fatalf("post-restart write: %v", err)
	}
}

func rowCount(t *testing.T, cluster *harness.Cluster) int {
	t.Helper()
	res, err := cluster.QuerySQL(0, `SELECT count(*) FROM contacts`)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("row count: %+v %v", res, err)
	}
	count, _ := res.Rows[0][0].(float64)
	return int(count)
}

func rewriteKeyConfig(t *testing.T, cluster *harness.Cluster, keyID, keyHex string) {
	t.Helper()
	path := cluster.Nodes[0].ConfigFile
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	cfg["key_id"] = keyID
	cfg["key_hex"] = keyHex
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0644); err != nil {
		t.Fatal(err)
	}
}

// assertNeverReady starts from an already-started node and requires the
// process to exit (failed unlock) without ever serving health.
func assertNeverReady(t *testing.T, cluster *harness.Cluster, idx int, timeout time.Duration) {
	t.Helper()
	node := cluster.Nodes[idx]
	deadline := time.Now().Add(timeout)
	exited := make(chan error, 1)
	go func() { exited <- node.Process.Wait() }()
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			return // failed unlock exited the daemon: old key invalidated
		default:
		}
		resp, err := http.Get(fmt.Sprintf("https://%s/healthz", node.APIAddr))
		if err == nil {
			resp.Body.Close()
			t.Fatal("node with retired key became ready")
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("node with retired key neither exited nor stayed down")
}
