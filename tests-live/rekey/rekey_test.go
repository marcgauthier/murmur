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
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestTypedStorageKeyRekeySurvivesRestart(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-rekey", NumNodes: 1, TypedRecords: true,
	})
	oldKeyHex := cluster.Nodes[0].KeyHex
	if err := cluster.TypedInsert(0, "typed-before-rotate"); err != nil {
		t.Fatalf("insert typed record: %v", err)
	}

	newKey := make([]byte, 32)
	if _, err := rand.Read(newKey); err != nil {
		t.Fatal(err)
	}
	newKeyHex := hex.EncodeToString(newKey)
	if err := cluster.RotateKey(0, "typed-new-key", newKeyHex, string(db.AES256GCM)); err != nil {
		t.Fatalf("rotate typed database key: %v", err)
	}
	status, err := cluster.EncryptionStatus(0)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := status["ApplicationKeyID"].(string); got != "typed-new-key" {
		t.Fatalf("active typed key ID = %q, want typed-new-key", got)
	}
	if err := cluster.TypedInsert(0, "typed-after-rotate"); err != nil {
		t.Fatalf("typed write after rotation: %v", err)
	}

	cluster.StopNode(0)
	rewriteKeyConfig(t, cluster, "typed-new-key", newKeyHex)
	cluster.StartNode(0)
	cluster.WaitNodeReady(0)
	waitTypedRecordCount(t, cluster, "typed-before-rotate", 1)
	waitTypedRecordCount(t, cluster, "typed-after-rotate", 1)

	// The pre-rotation key no longer opens the typed Spool after rotation.
	cluster.StopNode(0)
	rewriteKeyConfig(t, cluster, "live-old-key", oldKeyHex)
	cluster.StartNode(0)
	assertNeverReady(t, cluster, 0, 8*time.Second)
}

func waitTypedRecordCount(t *testing.T, cluster *harness.Cluster, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got, err := cluster.TypedCount(0, name)
		if err == nil && got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.TypedCount(0, name)
	t.Fatalf("typed count for %q = %d, %v; want %d", name, got, err, want)
}

// TestRekeyAwaitUnlockRestartsWithKeyID pins the rotate-then-restart
// path for await-unlock nodes: after rotation the registry only honors
// the new key ID, so the post-restart unlock must present (id, material)
// together. Unlocking with the new material but no ID 401s.
func TestRekeyAwaitUnlockRestartsWithKeyID(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "rekey-unlock-id",
		NumNodes:     1,
		AwaitUnlock:  true,
		TypedRecords: true,
	})
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
	cluster.WaitNodeReady(0)
	if err := cluster.TypedInsert(0, "pre-rotate"); err != nil {
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
	if n, err := cluster.TypedCount(0, "pre-rotate"); err != nil || n != 1 {
		t.Fatalf("reopened typed row count = %d, %v; want 1", n, err)
	}
	if err := cluster.TypedInsert(0, "post-restart"); err != nil {
		t.Fatalf("post-restart write: %v", err)
	}
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
