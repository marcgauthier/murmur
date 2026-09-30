// Multi-process High/Low bridge acceptance scenarios. Real Low and High
// spedsql daemon processes (distinct DBIDs, no mesh between them) move
// logical writes across a directory drop, driven only through HTTP:
// disconnected transfer, gap delivery with duplicate/reordered recovery,
// two-receiver convergence, forgery rejection, restart resumption,
// schema-hold survival with local migration release, and signer rotation
// plus outage catch-up.
package highlow_test

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

const liveStream = "field-records"

func liveContactsSchema(extra ...schema.ColumnSchema) []schema.TableSchema {
	cols := []schema.ColumnSchema{
		{Name: "id", Type: schema.ColBlob, Nullable: false},
		{Name: "name", Type: schema.ColText, Nullable: true},
		{Name: "score", Type: schema.ColInteger, Nullable: true},
	}
	return []schema.TableSchema{{
		Name:    "contacts",
		Columns: append(cols, extra...),
	}}
}

func liveSchemaConfig(tables []schema.TableSchema) *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: tables}
}

type liveKeys struct {
	files harness.BridgeKeyFiles
}

func makeLiveKeys(t *testing.T, dir string) liveKeys {
	t.Helper()
	return liveKeys{files: harness.GenerateBridgeKeys(t, dir)}
}

func newLowCluster(t *testing.T, name, staging string, keys liveKeys, tables []schema.TableSchema) *harness.Cluster {
	t.Helper()
	return harness.NewCluster(t, harness.ClusterOptions{
		Name:     name,
		NumNodes: 1,
		Schema:   liveSchemaConfig(tables),
		Bridge: &harness.BridgeOptions{
			Role: "low-exporter", Stream: liveStream, NodeIndex: 0,
			StagingDir:    staging,
			SignerKeyFile: keys.files.SignerKeyFile, RecipientPubFile: keys.files.RecipientPubFile,
		},
	})
}

func newHighCluster(t *testing.T, name, staging string, keys liveKeys, tables []schema.TableSchema, nodes int, indices []int) *harness.Cluster {
	t.Helper()
	return harness.NewCluster(t, harness.ClusterOptions{
		Name:     name,
		NumNodes: nodes,
		Schema:   liveSchemaConfig(tables),
		Bridge: &harness.BridgeOptions{
			Role: "high-importer", Stream: liveStream, NodeIndices: indices,
			StagingDir:       staging,
			RecipientKeyFile: keys.files.RecipientKeyFile, SignerPubFile: keys.files.SignerPubFile,
		},
	})
}

func writeContact(t *testing.T, c *harness.Cluster, idx int, row ids.RowID, name string, score int64) {
	t.Helper()
	if err := c.ExecSQL(idx, `INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)`, hex.EncodeToString(row[:]), name, score); err != nil {
		t.Fatalf("insert contact: %v", err)
	}
}

func updateContact(t *testing.T, c *harness.Cluster, idx int, row ids.RowID, name string, score int64) {
	t.Helper()
	if err := c.ExecSQL(idx, `UPDATE contacts SET name=?, score=? WHERE id=?`, name, score, hex.EncodeToString(row[:])); err != nil {
		t.Fatalf("update contact: %v", err)
	}
}

func contacts(t *testing.T, c *harness.Cluster, idx int) map[string]float64 {
	t.Helper()
	res, err := c.QuerySQL(idx, `SELECT name, score FROM contacts`)
	if err != nil {
		t.Fatalf("query contacts: %v", err)
	}
	out := make(map[string]float64)
	for _, r := range res.Rows {
		name, _ := r[0].(string)
		score, _ := r[1].(float64)
		out[name] = score
	}
	return out
}

func assertContactsEqual(t *testing.T, a map[string]float64, b map[string]float64) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("contact states diverged: %v vs %v", a, b)
	}
	for name, score := range a {
		if b[name] != score {
			t.Fatalf("contact states diverged: %v vs %v", a, b)
		}
	}
}

func exportOnce(t *testing.T, c *harness.Cluster, idx int) (captured, published int) {
	t.Helper()
	res, err := c.BridgeExport(idx)
	if err != nil {
		t.Fatalf("bridge export: %v", err)
	}
	capF, _ := res["captured"].(float64)
	pubF, _ := res["published"].(float64)
	return int(capF), int(pubF)
}

func importOnce(t *testing.T, c *harness.Cluster, idx int) (received, imported int) {
	t.Helper()
	res, err := c.BridgeImport(idx)
	if err != nil {
		t.Fatalf("bridge import: %v", err)
	}
	recF, _ := res["received"].(float64)
	impF, _ := res["imported"].(float64)
	return int(recF), int(impF)
}

func stagingNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && !strings.Contains(e.Name(), ".tmp-") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

func bridgeProgress(t *testing.T, c *harness.Cluster, idx int) []any {
	t.Helper()
	st, err := c.BridgeStatus(idx)
	if err != nil {
		t.Fatalf("bridge status: %v", err)
	}
	prog, _ := st["progress"].([]any)
	return prog
}

func TestHighLowDisconnectedTransferLive(t *testing.T) {
	workDir := t.TempDir()
	keys := makeLiveKeys(t, filepath.Join(workDir, "keys"))
	staging := filepath.Join(workDir, "staging")
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatal(err)
	}
	low := newLowCluster(t, "highlow-low", staging, keys, liveContactsSchema())
	high := newHighCluster(t, "highlow-high", staging, keys, liveContactsSchema(), 1, []int{0})
	if low.DBID == high.DBID {
		t.Fatal("Low and High share a DBID; domains must differ")
	}

	row := ids.NewRowID()
	writeContact(t, low, 0, row, "ann", 1)
	if _, published := exportOnce(t, low, 0); published < 1 {
		t.Fatalf("published %d bundles, want >= 1", published)
	}
	if _, imported := importOnce(t, high, 0); imported < 1 {
		t.Fatalf("imported %d bundles, want >= 1", imported)
	}
	assertContactsEqual(t, contacts(t, low, 0), contacts(t, high, 0))
	prog := bridgeProgress(t, high, 0)
	if len(prog) != 1 {
		t.Fatalf("progress = %+v, want one stream", prog)
	}
	if applied, _ := prog[0].(map[string]any)["Applied"].(float64); applied < 1 {
		t.Fatalf("progress = %+v, want applied >= 1", prog)
	}
}

func TestHighLowDuplicateReorderRecoveryLive(t *testing.T) {
	workDir := t.TempDir()
	keys := makeLiveKeys(t, filepath.Join(workDir, "keys"))
	staging := filepath.Join(workDir, "staging")
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatal(err)
	}
	low := newLowCluster(t, "highlow-gap-low", staging, keys, liveContactsSchema())
	high := newHighCluster(t, "highlow-gap-high", staging, keys, liveContactsSchema(), 1, []int{0})

	for i, name := range []string{"ann", "bob", "cid"} {
		writeContact(t, low, 0, ids.NewRowID(), name, int64(i))
		if _, published := exportOnce(t, low, 0); published < 1 {
			t.Fatalf("round %d published %d, want >= 1", i, published)
		}
	}
	files := stagingNames(t, staging)
	if len(files) != 3 {
		t.Fatalf("staged %d files, want 3 bundles", len(files))
	}

	// Withhold the middle bundle: the importer applies the first and
	// records a gap instead of skipping ahead.
	withheld := filepath.Join(workDir, "withheld.spb")
	if err := os.Rename(filepath.Join(staging, files[1]), withheld); err != nil {
		t.Fatal(err)
	}
	if _, imported := importOnce(t, high, 0); imported != 1 {
		t.Fatalf("gap import applied %d, want 1", imported)
	}
	prog := bridgeProgress(t, high, 0)
	missing, _ := prog[0].(map[string]any)["Missing"].([]any)
	if len(missing) != 1 || missing[0].(float64) != 2 {
		t.Fatalf("progress = %+v, want missing [2]", prog)
	}

	// Restoring the bundle fills the gap; a duplicate redelivery is a
	// no-op and all three bundles converge.
	if err := os.Rename(withheld, filepath.Join(staging, files[1])); err != nil {
		t.Fatal(err)
	}
	if _, imported := importOnce(t, high, 0); imported != 2 {
		t.Fatalf("catch-up import applied %d, want 2", imported)
	}
	if _, imported := importOnce(t, high, 0); imported != 0 {
		t.Fatalf("duplicate import applied %d, want 0", imported)
	}
	assertContactsEqual(t, contacts(t, low, 0), contacts(t, high, 0))
	prog = bridgeProgress(t, high, 0)
	if applied, _ := prog[0].(map[string]any)["Applied"].(float64); applied != 3 {
		t.Fatalf("progress = %+v, want applied 3", prog)
	}
}

func TestHighLowTwoReceiversConvergeLive(t *testing.T) {
	workDir := t.TempDir()
	keys := makeLiveKeys(t, filepath.Join(workDir, "keys"))
	staging := filepath.Join(workDir, "staging")
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatal(err)
	}
	low := newLowCluster(t, "highlow-2r-low", staging, keys, liveContactsSchema())
	high := newHighCluster(t, "highlow-2r-high", staging, keys, liveContactsSchema(), 2, []int{0, 1})

	writeContact(t, low, 0, ids.NewRowID(), "ann", 1)
	if _, published := exportOnce(t, low, 0); published < 1 {
		t.Fatalf("published %d, want >= 1", published)
	}
	// Each receiver imports the same staging independently; the High mesh
	// then converges both to identical state.
	if _, imported := importOnce(t, high, 0); imported < 1 {
		t.Fatalf("receiver 0 imported %d, want >= 1", imported)
	}
	if _, imported := importOnce(t, high, 1); imported < 1 {
		t.Fatalf("receiver 1 imported %d, want >= 1", imported)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		a, b := contacts(t, high, 0), contacts(t, high, 1)
		if len(a) == 1 && len(b) == 1 {
			assertContactsEqual(t, a, b)
			assertContactsEqual(t, a, contacts(t, low, 0))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("receivers did not converge: %v vs %v", a, b)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestHighLowForgeriesRejectedLive(t *testing.T) {
	workDir := t.TempDir()
	keys := makeLiveKeys(t, filepath.Join(workDir, "keys"))
	staging := filepath.Join(workDir, "staging")
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatal(err)
	}
	low := newLowCluster(t, "highlow-forge-low", staging, keys, liveContactsSchema())
	high := newHighCluster(t, "highlow-forge-high", staging, keys, liveContactsSchema(), 1, []int{0})
	writeContact(t, low, 0, ids.NewRowID(), "ann", 1)
	if _, published := exportOnce(t, low, 0); published < 1 {
		t.Fatalf("published %d, want >= 1", published)
	}
	good := stagingNames(t, staging)
	if len(good) != 1 {
		t.Fatalf("staged %d files, want 1 bundle", len(good))
	}
	goodRaw, err := os.ReadFile(filepath.Join(staging, good[0]))
	if err != nil {
		t.Fatal(err)
	}

	// Rogue domain: its own signer and recipient, exporting one bundle.
	rogueKeys := makeLiveKeys(t, filepath.Join(workDir, "rogue-keys"))
	rogueStaging := filepath.Join(workDir, "rogue-staging")
	if err := os.MkdirAll(rogueStaging, 0755); err != nil {
		t.Fatal(err)
	}
	rogue := newLowCluster(t, "highlow-forge-rogue", rogueStaging, rogueKeys, liveContactsSchema())
	writeContact(t, rogue, 0, ids.NewRowID(), "mallory", 9)
	if _, published := exportOnce(t, rogue, 0); published < 1 {
		t.Fatalf("rogue published %d, want >= 1", published)
	}
	rogueFiles := stagingNames(t, rogueStaging)
	rogueRaw, err := os.ReadFile(filepath.Join(rogueStaging, rogueFiles[0]))
	if err != nil {
		t.Fatal(err)
	}

	// Wrong-recipient domain: main signer, unknown recipient.
	otherKeys := makeLiveKeys(t, filepath.Join(workDir, "other-keys"))
	otherStaging := filepath.Join(workDir, "other-staging")
	if err := os.MkdirAll(otherStaging, 0755); err != nil {
		t.Fatal(err)
	}
	other := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "highlow-forge-other",
		NumNodes: 1,
		Schema:   liveSchemaConfig(liveContactsSchema()),
		Bridge: &harness.BridgeOptions{
			Role: "low-exporter", Stream: liveStream, NodeIndex: 0,
			StagingDir:    otherStaging,
			SignerKeyFile: keys.files.SignerKeyFile, RecipientPubFile: otherKeys.files.RecipientPubFile,
		},
	})
	writeContact(t, other, 0, ids.NewRowID(), "carol", 3)
	if _, published := exportOnce(t, other, 0); published < 1 {
		t.Fatalf("other-recipient published %d, want >= 1", published)
	}
	otherFiles := stagingNames(t, otherStaging)
	otherRaw, err := os.ReadFile(filepath.Join(otherStaging, otherFiles[0]))
	if err != nil {
		t.Fatal(err)
	}

	// Clear the good bundle; each hostile artifact is presented alone and
	// must fail closed without partial apply or recorded progress.
	if err := os.Remove(filepath.Join(staging, good[0])); err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), goodRaw...)
	tampered[len(tampered)-1] ^= 0xFF
	oversized := append(append([]byte(nil), goodRaw...), make([]byte, 17<<20)...)
	hostile := map[string][]byte{
		"rogue.spb":  rogueRaw,
		"sealed.spb": otherRaw,
		"evil.spb":   tampered,
		"big.spb":    oversized,
		".hidden":    goodRaw,
	}
	for name, raw := range hostile {
		path := filepath.Join(staging, name)
		if err := os.WriteFile(path, raw, 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := high.BridgeImport(0); err == nil {
			t.Fatalf("hostile artifact %s accepted", name)
		}
		if got := contacts(t, high, 0); len(got) != 0 {
			t.Fatalf("hostile artifact %s partially applied: %v", name, got)
		}
		if prog := bridgeProgress(t, high, 0); len(prog) != 0 {
			t.Fatalf("hostile artifact %s recorded progress: %+v", name, prog)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}

	// An unreadable staging file fails the import without partial apply.
	unreadable := filepath.Join(staging, "unreadable.spb")
	if err := os.WriteFile(unreadable, goodRaw, 0000); err != nil {
		t.Fatal(err)
	}
	if _, err := high.BridgeImport(0); err == nil {
		t.Fatalf("unreadable artifact accepted")
	}
	if got := contacts(t, high, 0); len(got) != 0 {
		t.Fatalf("unreadable artifact partially applied: %v", got)
	}
	if err := os.Chmod(unreadable, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(unreadable); err != nil {
		t.Fatal(err)
	}

	// The importer still accepts the genuine bundle afterwards.
	if err := os.WriteFile(filepath.Join(staging, good[0]), goodRaw, 0644); err != nil {
		t.Fatal(err)
	}
	if _, imported := importOnce(t, high, 0); imported != 1 {
		t.Fatalf("genuine import applied %d, want 1", imported)
	}
	assertContactsEqual(t, contacts(t, low, 0), contacts(t, high, 0))
}

func TestHighLowRestartResumeLive(t *testing.T) {
	workDir := t.TempDir()
	keys := makeLiveKeys(t, filepath.Join(workDir, "keys"))
	staging := filepath.Join(workDir, "staging")
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatal(err)
	}
	low := newLowCluster(t, "highlow-restart-low", staging, keys, liveContactsSchema())
	high := newHighCluster(t, "highlow-restart-high", staging, keys, liveContactsSchema(), 1, []int{0})

	writeContact(t, low, 0, ids.NewRowID(), "ann", 1)
	writeContact(t, low, 0, ids.NewRowID(), "bob", 2)
	if _, published := exportOnce(t, low, 0); published < 1 {
		t.Fatalf("published %d, want >= 1", published)
	}
	// Restarting the exporter loses no capture resume: nothing re-captures.
	low.StopNode(0)
	low.StartNode(0)
	low.WaitNodeReady(0)
	res, err := low.BridgeExport(0)
	if err != nil {
		t.Fatalf("post-restart export: %v", err)
	}
	if captured, _ := res["captured"].(float64); captured != 0 {
		t.Fatalf("post-restart captured %v, want 0", res)
	}
	if _, imported := importOnce(t, high, 0); imported < 1 {
		t.Fatalf("imported %d, want >= 1", imported)
	}
	// Restarting the importer loses no inbox/cursor resume: the redelivery
	// is a no-op and state holds both contacts exactly once.
	high.StopNode(0)
	high.StartNode(0)
	high.WaitNodeReady(0)
	received, imported := importOnce(t, high, 0)
	if received != 0 || imported != 0 {
		t.Fatalf("post-restart import = (%d, %d), want (0, 0)", received, imported)
	}
	assertContactsEqual(t, contacts(t, low, 0), contacts(t, high, 0))
}

func TestHighLowSchemaHoldMigrationLive(t *testing.T) {
	workDir := t.TempDir()
	keys := makeLiveKeys(t, filepath.Join(workDir, "keys"))
	staging := filepath.Join(workDir, "staging")
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatal(err)
	}
	withEmail := liveContactsSchema(schema.ColumnSchema{Name: "email", Type: schema.ColText, Nullable: true})
	low := newLowCluster(t, "highlow-schema-low", staging, keys, withEmail)
	high := newHighCluster(t, "highlow-schema-high", staging, keys, liveContactsSchema(), 1, []int{0})

	row := ids.NewRowID()
	if err := low.ExecSQL(0, `INSERT INTO contacts (id, name, score, email) VALUES (?, ?, ?, ?)`,
		hex.EncodeToString(row[:]), "ann", 1, "a@x"); err != nil {
		t.Fatalf("insert with email: %v", err)
	}
	if _, published := exportOnce(t, low, 0); published < 1 {
		t.Fatalf("published %d, want >= 1", published)
	}
	// The unknown column holds the bundle: nothing applies.
	if _, err := high.BridgeImport(0); err == nil || !strings.Contains(strings.ToLower(err.Error()), "waiting-schema") {
		t.Fatalf("held import err = %v, want a waiting-schema hold", err)
	}
	if got := contacts(t, high, 0); len(got) != 0 {
		t.Fatalf("held bundle applied: %v", got)
	}
	// The hold survives restart and still applies nothing.
	high.StopNode(0)
	high.StartNode(0)
	high.WaitNodeReady(0)
	received, imported := importOnce(t, high, 0)
	if received != 0 || imported != 0 {
		t.Fatalf("held redelivery = (%d, %d), want (0, 0)", received, imported)
	}
	prog := bridgeProgress(t, high, 0)
	holds, _ := prog[0].(map[string]any)["Holds"].([]any)
	if len(holds) != 1 {
		t.Fatalf("progress = %+v, want one hold", prog)
	}
	// The local migration releases the bundle; nothing crossed the bridge.
	if err := high.Migrate(0, withEmail); err != nil {
		t.Fatalf("migrate high: %v", err)
	}
	if _, imported := importOnce(t, high, 0); imported != 1 {
		t.Fatalf("post-migration import applied %d, want 1", imported)
	}
	res, err := high.QuerySQL(0, `SELECT name, email FROM contacts`)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range res.Rows {
		if name, _ := r[0].(string); name == "ann" {
			if email, _ := r[1].(string); email == "a@x" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("migrated import rows = %+v, want ann with email", res.Rows)
	}
}

func TestHighLowKeyRotationAndOutageResumeLive(t *testing.T) {
	workDir := t.TempDir()
	keys := makeLiveKeys(t, filepath.Join(workDir, "keys"))
	staging := filepath.Join(workDir, "staging")
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatal(err)
	}
	low := newLowCluster(t, "highlow-rotate-low", staging, keys, liveContactsSchema())
	high := newHighCluster(t, "highlow-rotate-high", staging, keys, liveContactsSchema(), 1, []int{0})

	writeContact(t, low, 0, ids.NewRowID(), "ann", 1)
	if _, published := exportOnce(t, low, 0); published < 1 {
		t.Fatalf("round 1 published %d, want >= 1", published)
	}
	// Rotate: the successor signs from here on while the importer trusts
	// both during the overlap (config edit plus restart).
	next := makeLiveKeys(t, filepath.Join(workDir, "next-keys"))
	rewriteNodeConfig(t, low, 0, map[string]any{"bridge.signer_key_file": next.files.SignerKeyFile})
	low.StopNode(0)
	low.StartNode(0)
	low.WaitNodeReady(0)
	rewriteNodeConfig(t, high, 0, map[string]any{"bridge.signer_pub_files": []string{next.files.SignerPubFile}})
	high.StopNode(0)
	high.StartNode(0)
	high.WaitNodeReady(0)
	writeContact(t, low, 0, ids.NewRowID(), "bob", 2)
	if _, published := exportOnce(t, low, 0); published < 1 {
		t.Fatalf("round 2 published %d, want >= 1", published)
	}
	if _, imported := importOnce(t, high, 0); imported != 2 {
		t.Fatalf("rotated import applied %d, want 2", imported)
	}
	// Retire the predecessor: replaying its bundle now fails closed.
	rewriteNodeConfig(t, high, 0, map[string]any{
		"bridge.signer_pub_file":  next.files.SignerPubFile,
		"bridge.signer_pub_files": []string{},
	})
	high.StopNode(0)
	high.StartNode(0)
	high.WaitNodeReady(0)
	files := stagingNames(t, staging)
	raw, err := os.ReadFile(filepath.Join(staging, files[0]))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "replay.spb"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := high.BridgeImport(0); err == nil {
		t.Fatal("retired-signer replay accepted")
	}
	if got := contacts(t, high, 0); len(got) != 2 {
		t.Fatalf("high state = %v", got)
	}
	if err := os.Remove(filepath.Join(staging, "replay.spb")); err != nil {
		t.Fatal(err)
	}

	// Prolonged outage: events queue durably, then catch up completely.
	for i := 0; i < 5; i++ {
		writeContact(t, low, 0, ids.NewRowID(), "backlog", int64(i))
	}
	res, err := low.BridgeExportWithOptions(0, harness.BridgeExportOptions{CaptureOnly: true})
	if err != nil {
		t.Fatalf("capture-only export: %v", err)
	}
	if captured, _ := res["captured"].(float64); captured != 5 {
		t.Fatalf("captured %v, want 5", res)
	}
	st, err := low.BridgeStatus(0)
	if err != nil {
		t.Fatal(err)
	}
	if pending, _ := st["pending_events"].(float64); pending != 5 {
		t.Fatalf("outage backlog = %+v, want 5 pending events", st)
	}
	if _, published := exportOnce(t, low, 0); published < 1 {
		t.Fatalf("catch-up published %d, want >= 1", published)
	}
	if _, imported := importOnce(t, high, 0); imported != 1 {
		t.Fatalf("catch-up import applied %d, want 1", imported)
	}
	assertContactsEqual(t, map[string]float64{"ann": 1, "bob": 2, "backlog": 4}, contacts(t, high, 0))
}

// rewriteNodeConfig applies dotted-path updates to a node's config file;
// the caller restarts the node for them to take effect.
func rewriteNodeConfig(t *testing.T, c *harness.Cluster, idx int, updates map[string]any) {
	t.Helper()
	path := c.Nodes[idx].ConfigFile
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	for dotted, v := range updates {
		parts := strings.Split(dotted, ".")
		m := cfg
		for _, p := range parts[:len(parts)-1] {
			next, _ := m[p].(map[string]any)
			if next == nil {
				next = make(map[string]any)
				m[p] = next
			}
			m = next
		}
		m[parts[len(parts)-1]] = v
	}
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0644); err != nil {
		t.Fatal(err)
	}
}
