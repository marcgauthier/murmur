// Multi-process High-owned field convergence across two meshed High peers.
//
// Peer A imports bundle 1, takes a High SQL override, then imports bundle 2
// (override-then-import). Peer B imports both bundles before the mesh
// delivers A's override (import-then-override). Both peers must converge on
// the High bytes with High-owned provenance, and an explicit release on A
// must converge both peers back to the Low value.
//
// Adversarial HLC ordering (Low update newer than the override) is covered
// at the DB layer by TestBridgeShadowReorderConvergence, which crafts
// batches directly; real SQL writes cannot control cross-node HLC order, so
// this test asserts real-path convergence while the unit test pins the
// hostile interleaving.
package highlow_test

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

func TestHighLowOwnershipReorderAcrossPeersLive(t *testing.T) {
	workDir := t.TempDir()
	keys := makeLiveKeys(t, filepath.Join(workDir, "keys"))
	staging := filepath.Join(workDir, "staging")
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatal(err)
	}
	low := newLowCluster(t, "highlow-own-low", staging, keys, liveContactsSchema())
	high := harness.NewCluster(t, harness.ClusterOptions{
		Name: "highlow-own-high", NumNodes: 2, ManualPeers: true,
		Schema: liveSchemaConfig(liveContactsSchema()),
		Bridge: &harness.BridgeOptions{
			Role: "high-importer", Stream: liveStream, NodeIndices: []int{0, 1},
			StagingDir:       staging,
			RecipientKeyFile: keys.files.RecipientKeyFile, SignerPubFile: keys.files.SignerPubFile,
		},
	})

	row := ids.NewRowID()
	rowHex := hex.EncodeToString(row[:])
	writeContact(t, low, 0, row, "low-1", 1)
	if _, published := exportOnce(t, low, 0); published < 1 {
		t.Fatalf("round 1 published %d, want >= 1", published)
	}

	// Peer A: override-then-import.
	if _, imported := importOnce(t, high, 0); imported < 1 {
		t.Fatalf("A round 1 imported %d, want >= 1", imported)
	}
	if err := high.ExecSQL(0, `UPDATE contacts SET name=? WHERE id=?`, "high", rowHex); err != nil {
		t.Fatalf("A override: %v", err)
	}
	updateContact(t, low, 0, row, "low-2", 2)
	if _, published := exportOnce(t, low, 0); published < 1 {
		t.Fatalf("round 2 published %d, want >= 1", published)
	}
	if _, imported := importOnce(t, high, 0); imported < 1 {
		t.Fatalf("A round 2 imported %d, want >= 1", imported)
	}
	if got := contactName(t, high, 0, rowHex); got != "high" {
		t.Fatalf("A before mesh: name=%q, want High-owned %q", got, "high")
	}

	// Peer B: import-then-override (override arrives later over the mesh).
	if _, imported := importOnce(t, high, 1); imported < 2 {
		t.Fatalf("B imported %d, want >= 2", imported)
	}
	if got := contactName(t, high, 1, rowHex); got != "low-2" {
		t.Fatalf("B before mesh: name=%q, want %q", got, "low-2")
	}

	// Mesh the High peers only after both orders are established locally.
	if err := high.AddPeer(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := high.AddPeer(1, 0); err != nil {
		t.Fatal(err)
	}
	waitForOwnership(t, high, rowHex, "high", "high", 30*time.Second)
	assertContactsEqual(t, contacts(t, high, 0), contacts(t, high, 1))

	// Explicit release on A converges both peers back to the Low value.
	if err := high.ReleaseOwnership(0, "contacts", rowHex, "name"); err != nil {
		t.Fatalf("release: %v", err)
	}
	waitForOwnership(t, high, rowHex, "low-2", "low", 30*time.Second)
	assertContactsEqual(t, contacts(t, high, 0), contacts(t, high, 1))
}

func contactName(t *testing.T, c *harness.Cluster, idx int, rowHex string) string {
	t.Helper()
	res, err := c.QuerySQL(idx, `SELECT name FROM contacts WHERE id=?`, rowHex)
	if err != nil {
		t.Fatalf("contact name: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("contact rows = %+v, want one", res.Rows)
	}
	name, _ := res.Rows[0][0].(string)
	return name
}

func waitForOwnership(t *testing.T, c *harness.Cluster, rowHex, wantName, wantOwner string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for i := 0; i < 2; i++ {
			res, err := c.QuerySQL(i, `SELECT name FROM contacts WHERE id=?`, rowHex)
			if err != nil || len(res.Rows) != 1 {
				ready = false
				break
			}
			if name, _ := res.Rows[0][0].(string); name != wantName {
				ready = false
				break
			}
			present, owner, err := c.Provenance(i, "contacts", rowHex, "name")
			if err != nil || !present || owner != wantOwner {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("peers did not converge on %q owned by %q within %v", wantName, wantOwner, timeout)
}
