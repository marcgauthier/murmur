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

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestEncryptedStoreRejectsWrongKeyAndReopens(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "encryption",
		NumNodes:    1,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{
			Version: 1,
			Tables: []schema.TableSchema{
				{
					Name: "secrets",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "value", Type: schema.ColText, Nullable: true},
					},
				},
			},
		},
	})

	const secretMarker = "SUPER-CONFIDENTIAL-PLAINTEXT-SECRET-987654321"
	rowID := fmt.Sprintf("%032x", 12345)

	// Write secret into node 1
	if err := cluster.ExecSQL(0, "INSERT INTO secrets (id, value) VALUES (?, ?)", rowID, secretMarker); err != nil {
		t.Fatalf("insert secret: %v", err)
	}

	// Stop node process
	t.Logf("Stopping node 1 to inspect at-rest ciphertext...")
	cluster.StopNode(0)

	// Inspect all files in node1 directory: verify secretMarker is NEVER present in plaintext on disk
	node1PebbleDir := cluster.Nodes[0].PebbleDir
	hasPlaintext, err := containsPlaintext(node1PebbleDir, secretMarker)
	if err != nil {
		t.Fatalf("scan pebble dir for plaintext: %v", err)
	}
	if hasPlaintext {
		t.Fatalf("SECURITY VIOLATION: plaintext secret marker found unencrypted on disk in %s", node1PebbleDir)
	}
	t.Logf("Confidentiality confirmed: 0 occurrences of secret marker in %s", node1PebbleDir)

	// Restart node 1 with await-unlock
	t.Logf("Restarting node 1 and testing wrong-key rejection...")
	cluster.StartNode(0)
	cluster.WaitNodeReady(0)

	// Attempt unlock with wrong key: must be rejected with 401 Unauthorized
	wrongKey := hex.EncodeToString(make([]byte, 32))
	unlockURL := fmt.Sprintf("https://%s/v1/admin/unlock", cluster.Nodes[0].APIAddr)
	payload, _ := json.Marshal(map[string]string{
		"key_hex": wrongKey,
		"cipher":  "chacha20",
	})
	resp, err := http.Post(unlockURL, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("unlock request: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("node unexpectedly accepted wrong key (status 200)")
	}
	t.Logf("Wrong key properly rejected with HTTP %d", resp.StatusCode)

	// Unlock with correct key
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)

	// Verify query succeeds and secret is intact
	res, err := cluster.QuerySQL(0, "SELECT value FROM secrets WHERE id = ?", rowID)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("query secret after correct unlock: err=%v, res=%v", err, res)
	}
	gotVal := fmt.Sprintf("%v", res.Rows[0][0])
	if gotVal != secretMarker {
		t.Fatalf("retrieved secret = %q, want %q", gotVal, secretMarker)
	}

	t.Logf("Node successfully decrypted and returned confidential secret.")
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
