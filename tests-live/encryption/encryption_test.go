package encryption_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestTypedEncryptedStoreKeepsRecordsEncryptedAcrossRestart(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-encryption", NumNodes: 1, TypedRecords: true, AwaitUnlock: true,
	})
	cluster.WaitNodeReady(0)

	const secretMarker = "TYPED-CONFIDENTIAL-PLAINTEXT-SECRET-246813579"
	if err := cluster.TypedInsert(0, secretMarker); err != nil {
		t.Fatalf("insert typed secret: %v", err)
	}
	if got, err := cluster.TypedCount(0, secretMarker); err != nil || got != 1 {
		t.Fatalf("typed secret before restart = %d, %v; want 1", got, err)
	}

	cluster.StopNode(0)
	hasPlaintext, err := containsPlaintext(cluster.Nodes[0].Dir, secretMarker)
	if err != nil {
		t.Fatalf("scan typed node directory: %v", err)
	}
	if hasPlaintext {
		t.Fatalf("typed plaintext secret marker found on disk in %s", cluster.Nodes[0].Dir)
	}

	cluster.StartNode(0)
	cluster.WaitNodeReady(0)
	wrongKey := hex.EncodeToString(make([]byte, 32))
	payload, _ := json.Marshal(map[string]string{"key_hex": wrongKey, "cipher": "chacha20"})
	unlockURL := fmt.Sprintf("https://%s/v1/admin/unlock", cluster.Nodes[0].APIAddr)
	resp, err := http.Post(unlockURL, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("wrong-key unlock request: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("typed database unexpectedly accepted the wrong key")
	}
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
	if got, err := cluster.TypedCount(0, secretMarker); err != nil || got != 1 {
		t.Fatalf("typed secret after restart = %d, %v; want 1", got, err)
	}
}

func containsPlaintext(root, marker string) (bool, error) {
	var found bool
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), marker) {
			found = true
		}
		return nil
	})
	return found, err
}
