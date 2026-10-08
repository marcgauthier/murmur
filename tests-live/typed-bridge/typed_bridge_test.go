package typedbridge_test

import (
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

// TestBridgeImportsIntoNativeRIME verifies the bridge applies imported
// records through the typed durable path and that the result survives a
// High-node restart.
func TestBridgeImportsIntoNativeRIME(t *testing.T) {
	workDir := t.TempDir()
	keys := harness.GenerateBridgeKeys(t, workDir+"/keys")
	staging := workDir + "/staging"
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatal(err)
	}

	low := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-bridge-low", NumNodes: 1, TypedRecords: true,
		Bridge: &harness.BridgeOptions{Role: "low-exporter", Stream: "typed-stream", NodeIndex: 0,
			StagingDir: staging, SignerKeyFile: keys.SignerKeyFile, RecipientPubFile: keys.RecipientPubFile},
	})
	high := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-bridge-high", NumNodes: 1, TypedRecords: true,
		Bridge: &harness.BridgeOptions{Role: "high-importer", Stream: "typed-stream", NodeIndex: 0,
			StagingDir: staging, RecipientKeyFile: keys.RecipientKeyFile, SignerPubFile: keys.SignerPubFile},
	})

	rowID := ids.NewRowID()
	if err := low.TypedInsertWithID(0, rowID, "bridge-native-record"); err != nil {
		t.Fatalf("insert low record: %v", err)
	}
	exported, err := low.BridgeExport(0)
	if err != nil || exported["published"].(float64) < 1 {
		t.Fatalf("bridge export = %v, %v; want published records", exported, err)
	}
	imported, err := high.BridgeImport(0)
	if err != nil || imported["imported"].(float64) < 1 {
		t.Fatalf("bridge import = %v, %v; want imported records", imported, err)
	}
	waitCount(t, high, 1)
	if err := high.TypedRename(0, "bridge-native-record", "high-owned-record"); err != nil {
		t.Fatalf("High typed field override: %v", err)
	}
	if err := low.TypedRename(0, "bridge-native-record", "low-update-before-release"); err != nil {
		t.Fatalf("Low typed field update: %v", err)
	}
	if _, err := low.BridgeExport(0); err != nil {
		t.Fatalf("export Low update while field is High-owned: %v", err)
	}
	if _, err := high.BridgeImport(0); err != nil {
		t.Fatalf("import Low update while field is High-owned: %v", err)
	}
	waitNamedCount(t, high, "high-owned-record", 1)
	waitNamedCount(t, high, "low-update-before-release", 0)
	if err := high.BridgeRelease(0, "live_typed_records", hex.EncodeToString(rowID[:]), "Name"); err != nil {
		t.Fatalf("release typed field ownership: %v", err)
	}
	if err := low.TypedRename(0, "low-update-before-release", "low-after-release"); err != nil {
		t.Fatalf("Low typed field update after release: %v", err)
	}
	if _, err := low.BridgeExport(0); err != nil {
		t.Fatalf("export Low update after release: %v", err)
	}
	if _, err := high.BridgeImport(0); err != nil {
		t.Fatalf("import Low update after release: %v", err)
	}
	waitNamedCount(t, high, "low-after-release", 1)

	high.StopNode(0)
	high.StartNode(0)
	high.WaitNodeReady(0)
	waitNamedCount(t, high, "low-after-release", 1)
}

func waitNamedCount(t *testing.T, cluster *harness.Cluster, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		got, err := cluster.TypedCount(0, name)
		if err == nil && got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.TypedCount(0, name)
	t.Fatalf("typed row count for %q = %d, %v; want %d", name, got, err, want)
}

func waitCount(t *testing.T, cluster *harness.Cluster, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		got, err := cluster.TypedCount(0, "bridge-native-record")
		if err == nil && got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.TypedCount(0, "bridge-native-record")
	t.Fatalf("native record count = %d, %v; want %d", got, err, want)
}
