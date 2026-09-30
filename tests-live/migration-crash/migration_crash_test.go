// Rolling additive migration with a SIGKILL inside the migrate window.
//
// Three daemons mesh on a two-column table. Node1 migrates first as a
// positive control (mixed-version mesh keeps replicating both ways).
// Then node2 takes SIGKILL inside its own migrate call: on restart it
// must land in a well-defined schema state (old epoch or new epoch,
// never mixed), mixed-version replication must continue, and after the
// laggards migrate the whole mesh must converge fully migrated with
// identical digests.
package migrationcrash_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func baseTables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "mc_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}
}

func evolvedTables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "mc_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
		},
	}}
}

func TestMigrationCrashLandsInDefinedState(t *testing.T) {
	killDelay := time.Duration(envMillis("SPEDSQL_MIGRATION_CRASH_KILL_DELAY_MS", 15)) * time.Millisecond
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "migration-crash",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema:      &db.SchemaConfig{Version: 1, Tables: baseTables()},
	})

	// Seed enough rows that manifest publication plus the materializer
	// rebuild spans a real window for SIGKILL to land inside.
	const seedRows = 500
	for i := 0; i < seedRows; i++ {
		id := fmt.Sprintf("%032x", 9000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO mc_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("n-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	waitNamesConverged(t, cluster, seedRows, 60*time.Second)

	// Pin node3 on the old schema with peer isolation: adoption is fast
	// (~1s once a revision exists), so without isolation there would be no
	// deterministic mixed-version window after the crash.
	for _, pair := range [][2]int{{2, 0}, {0, 2}, {2, 1}, {1, 2}} {
		if err := cluster.RemovePeer(pair[0], pair[1]); err != nil {
			t.Fatalf("isolate node3: %v", err)
		}
	}

	// Timed kill: SIGKILL node2 inside its own migrate call, which is the
	// mesh's FIRST publication of the new revision — nobody holds epoch 2
	// yet, so pre-adoption cannot spoil the race and the kill genuinely
	// lands inside (or around) node2's manifest publication. Wherever it
	// lands (before persist, mid-publication, after ack) the restart must
	// resolve to exactly old or new, never mixed.
	for i := 0; i < 3; i++ {
		assertEpoch(t, cluster, i, 1)
	}
	migrateDone := make(chan error, 1)
	go func() { migrateDone <- cluster.Migrate(1, evolvedTables()) }()
	time.Sleep(killDelay)
	t.Logf("SIGKILL node2 %v into its migrate call", killDelay)
	cluster.KillNode(1)
	migrateErr := <-migrateDone
	t.Logf("in-flight migrate returned: %v", migrateErr)

	// Determine the landed state empirically. A reopen requires the
	// config declaration to match the persisted manifest exactly, so a
	// node that persisted the new revision refuses to start on the old
	// declaration (exit + mismatch in the log); a node that never
	// persisted starts fine and still reports the old epoch.
	state := restartAndClassify(t, cluster, 1)

	// Mixed-version replication continues after the crash, with a clean
	// migration as positive control. If node2 landed new it is the new
	// side; if it landed old, node1 migrates cleanly first (proving honest
	// rolling migration works). Node3 is deterministically old (isolated
	// since before the first publication). Both sides write while split,
	// then heal with versions asserted different at the heal instant, and
	// convergence across the boundary proves mixed-version exchange.
	newIdx := 1
	if state == "old" {
		if err := cluster.Migrate(0, evolvedTables()); err != nil {
			t.Fatalf("clean migrate node1 after crash: %v", err)
		}
		newIdx = 0
	}
	assertEpoch(t, cluster, newIdx, 2)
	assertEpoch(t, cluster, 2, 1)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", 9000+i)
		if err := cluster.ExecSQL(newIdx, "UPDATE mc_rows SET score=? WHERE id=?", int64(100+i), id); err != nil {
			t.Fatalf("score write: %v", err)
		}
	}
	if err := cluster.ExecSQL(2, "INSERT INTO mc_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 20001), "post-crash-old"); err != nil {
		t.Fatalf("post-crash old-schema write: %v", err)
	}
	if got := statusEpoch(t, cluster.Nodes[newIdx].APIAddr); got != 2 {
		t.Fatalf("new side epoch = %d at heal, want 2", got)
	}
	if got := statusEpoch(t, cluster.Nodes[2].APIAddr); got != 1 {
		t.Fatalf("node3 epoch = %d at heal, want 1 (isolation leaked)", got)
	}
	for _, pair := range [][2]int{{2, 0}, {0, 2}, {2, 1}, {1, 2}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("heal node3: %v", err)
		}
	}
	waitNamesConverged(t, cluster, seedRows+1, 60*time.Second)
	assertScore(t, cluster, newIdx, fmt.Sprintf("%032x", 9000), 100)
	t.Logf("node2 landed in %s state; mixed-version mesh still converges", state)

	// Roll the rest forward: every node ends fully migrated and bit-identical.
	for i := 0; i < 3; i++ {
		if statusEpoch(t, cluster.Nodes[i].APIAddr) != 2 {
			if err := cluster.Migrate(i, evolvedTables()); err != nil {
				t.Fatalf("migrate node%d: %v", i+1, err)
			}
		}
	}
	waitFullConverged(t, cluster, seedRows+1, 90*time.Second)
	for i := 0; i < 3; i++ {
		assertEpoch(t, cluster, i, 2)
	}
	assertScore(t, cluster, 0, fmt.Sprintf("%032x", 9001), 101)
	assertScore(t, cluster, 1, fmt.Sprintf("%032x", 9002), 102)
	assertScore(t, cluster, 2, fmt.Sprintf("%032x", 9003), 103)

	// Post-migration honest write replicates everywhere.
	if err := cluster.ExecSQL(2, "INSERT INTO mc_rows (id, name, score) VALUES (?, ?, ?)", fmt.Sprintf("%032x", 20002), "final", int64(7)); err != nil {
		t.Fatalf("post-migration write: %v", err)
	}
	waitFullConverged(t, cluster, seedRows+2, 90*time.Second)
	t.Logf("migration crash proven: node2 landed %s, all nodes fully migrated with equal digests", state)
}

// restartAndClassify restarts the killed-while-migrating node and returns
// the well-defined schema state it landed in ("old" or "new"). Any other
// outcome (won't start either way, wrong epoch, lost rows) fails.
func restartAndClassify(t *testing.T, c *harness.Cluster, idx int) string {
	t.Helper()
	node := c.Nodes[idx]

	c.StartNode(idx)
	if up := waitUpOrExit(t, node, 20*time.Second); up {
		// The daemon answers healthz before unlock; the schema
		// manifest opens at unlock time, so a persisted new
		// revision surfaces as an unlock failure, not a dead
		// process.
		if uerr := tryUnlock(c, idx); uerr != nil {
			if !isMismatch(uerr.Error()) {
				t.Fatalf("node%d unlock failed without a schema-mismatch marker: %v", idx+1, uerr)
			}
			return restartOnNewDeclaration(t, c, idx, "unlock refused old declaration: "+uerr.Error())
		}
		c.WaitNodeReady(idx)
		assertEpoch(t, c, idx, 1)
		// Old state must be exactly old: pre-crash rows intact under
		// the original names, no new-column content visible.
		n, err := c.QueryRowCount(idx, "mc_rows")
		if err != nil {
			t.Fatalf("node%d old-state count: %v", idx+1, err)
		}
		if n != 500 {
			t.Fatalf("node%d old-state rows = %d, want 500", idx+1, n)
		}
		if _, err := c.QuerySQL(idx, "SELECT score FROM mc_rows LIMIT 1"); err == nil {
			t.Fatalf("node%d landed old yet exposes the new score column (mixed state)", idx+1)
		}
		return "old"
	}
	// The process died on the old declaration: it must have persisted
	// the new revision. Confirm the mismatch in the log (proves this
	// branch is real, not a vacuous fallback), then restart on the new
	// declaration.
	raw := mustRead(t, node.LogFile)
	if !isMismatch(raw) {
		t.Fatalf("node%d exited on old config without a schema-mismatch marker; refusing blind fallback (log tail: %s)",
			idx+1, tail(raw, 2000))
	}
	return restartOnNewDeclaration(t, c, idx, "process exited on old declaration")
}

// restartOnNewDeclaration persists the migrated declaration as the node's
// configuration (as the Migrate contract requires of the application) and
// restarts. The node must come up exactly on the new epoch with all
// pre-crash rows intact.
func restartOnNewDeclaration(t *testing.T, c *harness.Cluster, idx int, why string) string {
	t.Helper()
	node := c.Nodes[idx]
	t.Logf("node%d %s; restarting on the new declaration", idx+1, why)
	c.KillNode(idx)
	rewriteSchemaConfig(t, node.ConfigFile, 2, evolvedTables())
	c.StartNode(idx)
	if up := waitUpOrExit(t, node, 20*time.Second); !up {
		t.Fatalf("node%d will not start on the new declaration either (mixed/corrupt state); log tail: %s",
			idx+1, tail(mustRead(t, node.LogFile), 2000))
	}
	c.UnlockNode(idx, node.KeyHex)
	c.WaitNodeReady(idx)
	assertEpoch(t, c, idx, 2)
	n, err := c.QueryRowCount(idx, "mc_rows")
	if err != nil {
		t.Fatalf("node%d new-state count: %v", idx+1, err)
	}
	if n != 500 {
		t.Fatalf("node%d new-state rows = %d, want 500", idx+1, n)
	}
	return "new"
}

// tryUnlock attempts one unlock like harness.UnlockNode but returns the
// failure instead of failing the test, so the caller can classify a
// schema-mismatch refusal.
func tryUnlock(c *harness.Cluster, idx int) error {
	node := c.Nodes[idx]
	payload, _ := json.Marshal(map[string]string{
		"key_hex": node.KeyHex,
		"cipher":  "chacha20",
	})
	url := fmt.Sprintf("https://%s/v1/admin/unlock", node.APIAddr)
	deadline := time.Now().Add(10 * time.Second)
	var lastFailure string
	for time.Now().Before(deadline) {
		resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
		if err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
				_ = resp.Body.Close()
				return nil
			}
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			lastFailure = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
			// A definitive refusal needs no retry loop.
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusBadRequest {
				return fmt.Errorf("unlock node %s: %s", node.Label, lastFailure)
			}
		} else {
			lastFailure = err.Error()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("unlock node %s: %s", node.Label, lastFailure)
}

func isMismatch(s string) bool {
	return strings.Contains(s, "migrate with Migrate, not config drift") ||
		strings.Contains(s, "SchemaMismatch") ||
		strings.Contains(s, "schema mismatch") ||
		strings.Contains(s, "stored epoch")
}

// waitUpOrExit polls until the node answers healthz (true) or its process
// exits (false).
func waitUpOrExit(t *testing.T, node *harness.Node, timeout time.Duration) bool {
	t.Helper()
	exited := make(chan struct{})
	go func() {
		if node.Process != nil {
			_ = node.Process.Wait()
		}
		close(exited)
	}()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			return false
		default:
		}
		resp, err := client.Get("https://" + node.APIAddr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	select {
	case <-exited:
		return false
	default:
		t.Fatalf("node %s neither healthy nor exited within %v", node.Label, timeout)
		return false
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

// rewriteSchemaConfig persists the migrated declaration as the node's
// configuration, using the exact marshal shape the harness wrote.
func rewriteSchemaConfig(t *testing.T, configFile string, version uint64, tables []schema.TableSchema) {
	t.Helper()
	raw, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	decl, err := json.Marshal(&db.SchemaConfig{Version: version, Tables: tables})
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}
	var declAny any
	if err := json.Unmarshal(decl, &declAny); err != nil {
		t.Fatalf("recode schema: %v", err)
	}
	cfg["schema"] = declAny
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	if err := os.WriteFile(configFile, out, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

func assertEpoch(t *testing.T, c *harness.Cluster, idx int, want uint64) {
	t.Helper()
	got := statusEpoch(t, c.Nodes[idx].APIAddr)
	if got != want {
		t.Fatalf("node%d schema_epoch = %d, want %d", idx+1, got, want)
	}
}

func statusEpoch(t *testing.T, apiAddr string) uint64 {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	defer resp.Body.Close()
	var st struct {
		SchemaEpoch uint64 `json:"schema_epoch"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	return st.SchemaEpoch
}

// waitNamesConverged polls id/name convergence (tolerates schema skew on
// the new column during the mixed-version window).
func waitNamesConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		counts := map[string]int{}
		for i := range c.Nodes {
			res, err := c.QuerySQL(i, "SELECT name FROM mc_rows ORDER BY name")
			if err != nil || len(res.Rows) != want {
				ok = false
				break
			}
			key := fmt.Sprintf("%v", res.Rows)
			counts[key]++
		}
		if ok && len(counts) == 1 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %d names within %v", want, timeout)
}

// waitFullConverged polls full-row digest equality once all peers share
// the evolved schema.
func waitFullConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "mc_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "mc_rows", "id")
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
	for i := range c.Nodes {
		n, nerr := c.QueryRowCount(i, "mc_rows")
		d, derr := c.ComputeTableDigest(i, "mc_rows", "id")
		ep := statusEpoch(t, c.Nodes[i].APIAddr)
		t.Logf("node %d at timeout: count=%d countErr=%v digest=%.16s digestErr=%v epoch=%d", i, n, nerr, d, derr, ep)
	}
	dumpRowDiff(t, c)
	t.Fatalf("nodes did not fully converge within %v", timeout)
}

// dumpRowDiff logs the first ordered full-row differences between node1
// and the others (failure diagnostics).
func dumpRowDiff(t *testing.T, c *harness.Cluster) {
	t.Helper()
	base, err := c.QuerySQL(0, "SELECT id, name, score FROM mc_rows ORDER BY name")
	if err != nil {
		t.Logf("divergence: node1 rows query: %v", err)
		return
	}
	for idx := 1; idx < len(c.Nodes); idx++ {
		other, err := c.QuerySQL(idx, "SELECT id, name, score FROM mc_rows ORDER BY name")
		if err != nil {
			t.Logf("divergence: node%d rows query: %v", idx+1, err)
			continue
		}
		if len(base.Rows) != len(other.Rows) {
			t.Logf("divergence: node1 rows=%d node%d rows=%d", len(base.Rows), idx+1, len(other.Rows))
			continue
		}
		shown := 0
		for i := range base.Rows {
			a, b := fmt.Sprintf("%v", base.Rows[i]), fmt.Sprintf("%v", other.Rows[i])
			if a != b {
				t.Logf("divergence: row %d node1=%s node%d=%s", i, a, idx+1, b)
				shown++
				if shown >= 5 {
					break
				}
			}
		}
		if shown == 0 {
			t.Logf("divergence: node%d full rows match node1 (digest skew is encoding/order, not content)", idx+1)
		}
	}
	// SELECT * shape per node: column order/arity is what the digest sees.
	for idx := 0; idx < len(c.Nodes); idx++ {
		star, err := c.QuerySQL(idx, "SELECT * FROM mc_rows ORDER BY name LIMIT 2")
		if err != nil {
			t.Logf("divergence: node%d SELECT *: %v", idx+1, err)
			continue
		}
		t.Logf("divergence: node%d SELECT * columns=%v row0=%v", idx+1, star.Columns, firstRow(star))
	}
}

func firstRow(res *harness.QueryResult) any {
	if len(res.Rows) == 0 {
		return nil
	}
	return res.Rows[0]
}

func assertScore(t *testing.T, c *harness.Cluster, idx int, idHex string, want int64) {
	t.Helper()
	res, err := c.QuerySQL(idx, "SELECT score FROM mc_rows WHERE id=?", idHex)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("node %d score read: %+v %v", idx, res, err)
	}
	got, _ := res.Rows[0][0].(float64)
	if int64(got) != want {
		t.Fatalf("node %d score = %v, want %d", idx, got, want)
	}
}

func envMillis(name string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(name)); err == nil && value >= 0 {
		return value
	}
	return fallback
}
