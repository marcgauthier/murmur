// Multi-process two-Low/two-High encrypted file bridge acceptance scenario.
//
// Four spedsql daemon processes (two Low mesh peers, two High mesh peers)
// in discrete node directories replicate how the application works: files
// cross HTTP endpoints, mesh replication and peer fetch run over real
// sockets, and bridge bundles plus recipient-sealed file chunks cross a
// shared staging directory standing in for air-gap media.
package filesbridge_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/tests-live/harness"
)

const (
	bridgeFileName = "shared/field-map.bin"
	bridgeStream   = "field-files"
	lowObjectKey   = "1111111111111111111111111111111111111111111111111111111111111111"
	highObjectKey  = "2222222222222222222222222222222222222222222222222222222222222222"
)

func TestTwoLowTwoHighFileBridgeLive(t *testing.T) {
	workDir := t.TempDir()
	keys := harness.GenerateBridgeKeys(t, filepath.Join(workDir, "keys"))
	staging := filepath.Join(workDir, "staging")
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatal(err)
	}

	low := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "files-bridge-low",
		NumNodes: 2,
		// Negative fetch interval disables the background worker so each
		// fetch below is an explicit, deterministic step.
		Files: &harness.FilesOptions{ObjectKeyHex: lowObjectKey, MaxFileBytes: 4 << 20, FetchIntervalMs: -1},
		Bridge: &harness.BridgeOptions{
			Role: "low-exporter", Stream: bridgeStream, NodeIndex: 0,
			StagingDir:    staging,
			SignerKeyFile: keys.SignerKeyFile, RecipientPubFile: keys.RecipientPubFile,
		},
	})
	high := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "files-bridge-high",
		NumNodes: 2,
		Files:    &harness.FilesOptions{ObjectKeyHex: highObjectKey, MaxFileBytes: 4 << 20, FetchIntervalMs: -1},
		Bridge: &harness.BridgeOptions{
			Role: "high-importer", Stream: bridgeStream, NodeIndex: 0,
			StagingDir:       staging,
			RecipientKeyFile: keys.RecipientKeyFile, SignerPubFile: keys.SignerPubFile,
		},
	})

	data := make([]byte, 384<<10)
	for i := range data {
		data[i] = byte((i*37 + i/251) % 253)
	}
	wantHex := sha256.Sum256(data)
	wantDigest := hex.EncodeToString(wantHex[:])

	// Low upload over HTTP, then Low mesh metadata + peer fetch.
	up, err := low.UploadFile(0, bridgeFileName, data)
	if err != nil {
		t.Fatalf("Low upload: %v", err)
	}
	if up.Digest != wantDigest {
		t.Fatalf("upload digest %s, want %s", up.Digest, wantDigest)
	}
	if err := low.WaitFileAvailable(1, bridgeFileName, false, 30*time.Second); err != nil {
		t.Fatalf("Low mesh metadata: %v", err)
	}
	if _, err := low.FetchFile(1, bridgeFileName); err != nil {
		t.Fatalf("Low peer fetch: %v", err)
	}
	assertNodeDigest(t, low, 1, data)

	// Export Low-1 to staging: a signed bundle plus sealed chunks.
	exp, err := low.BridgeExport(0)
	if err != nil {
		t.Fatalf("bridge export: %v", err)
	}
	t.Logf("export: %+v", exp)
	staged := stagingFiles(t, staging)
	if len(staged) < 2 {
		t.Fatalf("staged %d files, want bundle plus sealed file chunks", len(staged))
	}
	assertNoPlaintext(t, staged, data)

	// Import into High-1: metadata applies, chunks install, and the object
	// re-encrypts under the High object key.
	imp, err := high.BridgeImport(0)
	if err != nil {
		t.Fatalf("bridge import: %v", err)
	}
	t.Logf("import: %+v", imp)
	if err := high.WaitFileAvailable(0, bridgeFileName, true, 30*time.Second); err != nil {
		t.Fatalf("High-1 import: %v", err)
	}
	assertNodeDigest(t, high, 0, data)
	assertReencrypted(t, low.Nodes[0].PebbleDir, high.Nodes[0].PebbleDir)

	// High mesh carries metadata to High-2, which fetches bytes from High-1.
	if err := high.WaitFileAvailable(1, bridgeFileName, false, 30*time.Second); err != nil {
		t.Fatalf("High mesh metadata: %v", err)
	}
	if _, err := high.FetchFile(1, bridgeFileName); err != nil {
		t.Fatalf("High-2 peer fetch: %v", err)
	}
	assertNodeDigest(t, high, 1, data)

	// Search across the replicated metadata.
	found, err := high.SearchFiles(1, "shared/", "")
	if err != nil {
		t.Fatalf("High search: %v", err)
	}
	if len(found) != 1 || found[0].Digest != wantDigest {
		t.Fatalf("search found %+v, want one file %s", found, wantDigest)
	}

	// Delete cascade: Low tombstone replicates, exports, and imports.
	if err := low.DeleteFile(0, bridgeFileName); err != nil {
		t.Fatalf("Low delete: %v", err)
	}
	if err := low.WaitFileDeleted(1, bridgeFileName, 30*time.Second); err != nil {
		t.Fatalf("Low delete mesh: %v", err)
	}
	if _, err := low.BridgeExport(0); err != nil {
		t.Fatalf("delete export: %v", err)
	}
	if _, err := high.BridgeImport(0); err != nil {
		t.Fatalf("delete import: %v", err)
	}
	if err := high.WaitFileDeleted(0, bridgeFileName, 30*time.Second); err != nil {
		t.Fatalf("High-1 delete: %v", err)
	}
	if err := high.WaitFileDeleted(1, bridgeFileName, 30*time.Second); err != nil {
		t.Fatalf("High-2 delete: %v", err)
	}
}

func assertNodeDigest(t *testing.T, c interface {
	DownloadFile(idx int, name string) ([]byte, error)
}, idx int, want []byte,
) {
	t.Helper()
	got, err := c.DownloadFile(idx, bridgeFileName)
	if err != nil {
		t.Fatalf("download node %d: %v", idx, err)
	}
	if !bytes.Equal(got, want) {
		sum := sha256.Sum256(got)
		t.Fatalf("node %d bytes differ: got sha256 %x", idx, sum)
	}
}

func stagingFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && !strings.Contains(e.Name(), ".tmp-") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out
}

// assertNoPlaintext verifies staged artifacts carry no document plaintext:
// several 1KB windows of the payload must be absent from every staged file.
func assertNoPlaintext(t *testing.T, staged []string, data []byte) {
	t.Helper()
	windows := [][]byte{data[0:1024], data[100000:101024], data[300000:301024]}
	for _, path := range staged {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for i, w := range windows {
			if bytes.Contains(raw, w) {
				t.Fatalf("staged %s contains plaintext window %d", filepath.Base(path), i)
			}
		}
	}
}

// assertReencrypted verifies High at-rest bytes differ from Low at-rest
// bytes for the same verified content (separate domain object keys).
func assertReencrypted(t *testing.T, lowPebble, highPebble string) {
	t.Helper()
	lowObj := firstObjectFile(t, lowPebble)
	highObj := firstObjectFile(t, highPebble)
	lowRaw, err := os.ReadFile(lowObj)
	if err != nil {
		t.Fatal(err)
	}
	highRaw, err := os.ReadFile(highObj)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(lowRaw, highRaw) {
		t.Fatal("High object bytes identical to Low object bytes; expected re-encryption")
	}
}

func firstObjectFile(t *testing.T, pebbleDir string) string {
	t.Helper()
	var found string
	_ = filepath.Walk(filepath.Join(pebbleDir, "files"), func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(info.Name(), ".spfo") && found == "" {
			found = path
		}
		return nil
	})
	if found == "" {
		t.Fatalf("no object file under %s", pebbleDir)
	}
	return found
}
