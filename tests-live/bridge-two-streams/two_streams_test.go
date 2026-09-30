// Two Low domains importing into one High cluster.
//
// Low-A and Low-B are independent domains (distinct DBIDs, CAs, keys,
// streams, staging directories). High node1 imports stream-a and High
// node2 imports stream-b; the High mesh then converges rows from both
// domains. A second export round on stream-a only must arrive without
// disturbing stream-b progress. Cross-domain row-identity collision is
// covered separately once its quarantine behavior is pinned.
package twostreams_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
		Name: "ts_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}}
}

func TestTwoLowDomainsIntoOneHigh(t *testing.T) {
	workDir := t.TempDir()
	keysA := harness.GenerateBridgeKeys(t, workDir+"/keys-a")
	keysB := harness.GenerateBridgeKeys(t, workDir+"/keys-b")
	stagingA := workDir + "/staging-a"
	stagingB := workDir + "/staging-b"
	if err := os.MkdirAll(stagingA, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(stagingB, 0755); err != nil {
		t.Fatal(err)
	}

	lowA := harness.NewCluster(t, harness.ClusterOptions{
		Name: "two-streams-low-a", NumNodes: 1, Schema: schemaConfig(),
		Bridge: &harness.BridgeOptions{Role: "low-exporter", Stream: "stream-a", NodeIndex: 0,
			StagingDir: stagingA, SignerKeyFile: keysA.SignerKeyFile, RecipientPubFile: keysA.RecipientPubFile},
	})
	lowB := harness.NewCluster(t, harness.ClusterOptions{
		Name: "two-streams-low-b", NumNodes: 1, Schema: schemaConfig(),
		Bridge: &harness.BridgeOptions{Role: "low-exporter", Stream: "stream-b", NodeIndex: 0,
			StagingDir: stagingB, SignerKeyFile: keysB.SignerKeyFile, RecipientPubFile: keysB.RecipientPubFile},
	})
	high := harness.NewCluster(t, harness.ClusterOptions{
		Name: "two-streams-high", NumNodes: 2, Schema: schemaConfig(),
		BridgeByNode: map[int]*harness.BridgeOptions{
			0: {Role: "high-importer", Stream: "stream-a", NodeIndex: 0,
				StagingDir: stagingA, RecipientKeyFile: keysA.RecipientKeyFile, SignerPubFile: keysA.SignerPubFile},
			1: {Role: "high-importer", Stream: "stream-b", NodeIndex: 1,
				StagingDir: stagingB, RecipientKeyFile: keysB.RecipientKeyFile, SignerPubFile: keysB.SignerPubFile},
		},
	})

	// Round 1: disjoint rows from both domains land on both High nodes.
	for i := 0; i < 5; i++ {
		if err := lowA.ExecSQL(0, "INSERT INTO ts_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 0xA000+i), fmt.Sprintf("a-%d", i)); err != nil {
			t.Fatal(err)
		}
		if err := lowB.ExecSQL(0, "INSERT INTO ts_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 0xB000+i), fmt.Sprintf("b-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	exportOnce(t, lowA, 0)
	exportOnce(t, lowB, 0)
	importOnce(t, high, 0)
	importOnce(t, high, 1)
	waitHighConverged(t, high, 10, 60*time.Second)
	assertNames(t, high, []string{"a-0", "a-1", "a-2", "a-3", "a-4", "b-0", "b-1", "b-2", "b-3", "b-4"})
	assertLowOwned(t, high, fmt.Sprintf("%032x", 0xA000))
	assertLowOwned(t, high, fmt.Sprintf("%032x", 0xB000))
	progressBBefore := fmt.Sprintf("%v", bridgeProgress(t, high, 1))

	// Round 2: stream-a only. New A rows arrive; stream-b progress on
	// High node2 is byte-identical (per-stream isolation).
	for i := 5; i < 8; i++ {
		if err := lowA.ExecSQL(0, "INSERT INTO ts_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 0xA000+i), fmt.Sprintf("a-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	exportOnce(t, lowA, 0)
	importOnce(t, high, 0)
	waitHighConverged(t, high, 13, 60*time.Second)
	if got := fmt.Sprintf("%v", bridgeProgress(t, high, 1)); got != progressBBefore {
		t.Fatalf("stream-b progress changed without a stream-b import:\nbefore: %s\nafter:  %s", progressBBefore, got)
	}
}

func TestCrossDomainIdentityCollisionFailsClosed(t *testing.T) {
	workDir := t.TempDir()
	keysA := harness.GenerateBridgeKeys(t, workDir+"/keys-a")
	keysB := harness.GenerateBridgeKeys(t, workDir+"/keys-b")
	stagingA := workDir + "/staging-a"
	stagingB := workDir + "/staging-b"
	if err := os.MkdirAll(stagingA, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(stagingB, 0755); err != nil {
		t.Fatal(err)
	}

	lowA := harness.NewCluster(t, harness.ClusterOptions{
		Name: "two-streams-col-a", NumNodes: 1, Schema: schemaConfig(),
		Bridge: &harness.BridgeOptions{Role: "low-exporter", Stream: "sa", NodeIndex: 0,
			StagingDir: stagingA, SignerKeyFile: keysA.SignerKeyFile, RecipientPubFile: keysA.RecipientPubFile},
	})
	lowB := harness.NewCluster(t, harness.ClusterOptions{
		Name: "two-streams-col-b", NumNodes: 1, Schema: schemaConfig(),
		Bridge: &harness.BridgeOptions{Role: "low-exporter", Stream: "sb", NodeIndex: 0,
			StagingDir: stagingB, SignerKeyFile: keysB.SignerKeyFile, RecipientPubFile: keysB.RecipientPubFile},
	})
	high := harness.NewCluster(t, harness.ClusterOptions{
		Name: "two-streams-col-h", NumNodes: 2, Schema: schemaConfig(),
		BridgeByNode: map[int]*harness.BridgeOptions{
			0: {Role: "high-importer", Stream: "sa", NodeIndex: 0,
				StagingDir: stagingA, RecipientKeyFile: keysA.RecipientKeyFile, SignerPubFile: keysA.SignerPubFile},
			1: {Role: "high-importer", Stream: "sb", NodeIndex: 1,
				StagingDir: stagingB, RecipientKeyFile: keysB.RecipientKeyFile, SignerPubFile: keysB.SignerPubFile},
		},
	})

	// Domain A lands first; domain B's same-ID row must fail closed.
	row := fmt.Sprintf("%032x", 0xC001)
	if err := lowA.ExecSQL(0, "INSERT INTO ts_rows (id, name) VALUES (?, ?)", row, "from-a"); err != nil {
		t.Fatal(err)
	}
	exportOnce(t, lowA, 0)
	importOnce(t, high, 0)
	waitHighConverged(t, high, 1, 30*time.Second)

	if err := lowB.ExecSQL(0, "INSERT INTO ts_rows (id, name) VALUES (?, ?)", row, "from-b"); err != nil {
		t.Fatal(err)
	}
	exportOnce(t, lowB, 0)
	if _, err := high.BridgeImport(1); err == nil {
		t.Fatal("colliding import succeeded, want identity-collision failure")
	} else if !strings.Contains(strings.ToLower(err.Error()), "collides") {
		t.Fatalf("collision error %q does not name the collision", err)
	}
	// First-writer state stands untouched on both High nodes.
	for i := range high.Nodes {
		res, err := high.QuerySQL(i, "SELECT name FROM ts_rows")
		if err != nil || len(res.Rows) != 1 || res.Rows[0][0].(string) != "from-a" {
			t.Fatalf("high-%d state = %+v err=%v, want intact from-a", i, res, err)
		}
	}
}

func exportOnce(t *testing.T, c *harness.Cluster, idx int) {
	t.Helper()
	res, err := c.BridgeExport(idx)
	if err != nil {
		t.Fatalf("bridge export: %v", err)
	}
	if res["published"].(float64) < 1 {
		t.Fatalf("export published nothing: %v", res)
	}
}

func importOnce(t *testing.T, c *harness.Cluster, idx int) {
	t.Helper()
	res, err := c.BridgeImport(idx)
	if err != nil {
		t.Fatalf("bridge import: %v", err)
	}
	if res["imported"].(float64) < 1 {
		t.Fatalf("import applied nothing: %v", res)
	}
}

func bridgeProgress(t *testing.T, c *harness.Cluster, idx int) any {
	t.Helper()
	res, err := c.BridgeStatus(idx)
	if err != nil {
		t.Fatalf("bridge status: %v", err)
	}
	return res["progress"]
}

func waitHighConverged(t *testing.T, high *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range high.Nodes {
			n, err := high.QueryRowCount(i, "ts_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := high.ComputeTableDigest(i, "ts_rows", "name")
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
	t.Fatalf("high nodes did not converge on %d rows within %v", want, timeout)
}

func assertNames(t *testing.T, high *harness.Cluster, want []string) {
	t.Helper()
	for i := range high.Nodes {
		res, err := high.QuerySQL(i, "SELECT name FROM ts_rows ORDER BY name")
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Rows) != len(want) {
			t.Fatalf("high-%d rows = %d, want %d", i, len(res.Rows), len(want))
		}
		for j, row := range res.Rows {
			if row[0].(string) != want[j] {
				t.Fatalf("high-%d row %d = %v, want %s", i, j, row[0], want[j])
			}
		}
	}
}

func assertLowOwned(t *testing.T, high *harness.Cluster, rowHex string) {
	t.Helper()
	for i := range high.Nodes {
		present, owner, err := high.Provenance(i, "ts_rows", rowHex, "name")
		if err != nil || !present || owner != "low" {
			t.Fatalf("high-%d provenance(%s) = present=%v owner=%q err=%v, want low-owned", i, rowHex, present, owner, err)
		}
	}
}
