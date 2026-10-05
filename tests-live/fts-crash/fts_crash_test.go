// FTS-crash acceptance (single node): FTS5 structures are local-only,
// in-memory derived state (architecture/query-and-search.md sections 23
// and 65: never replicated, rebuilt from base tables). This suite proves
// crash/rebuild: an FTS index over known docs matches exactly pre-crash,
// the node is SIGKILLed mid-index-write, and after restart + rebuild from
// the durable base table every MATCH query returns the exact full row
// set with no duplicates and PRAGMA integrity_check is clean.
//
// Design notes (from studying the product path):
//   - The FTS table definition is config LocalDDL (schema carried by the
//     node, re-applied on every open/rebuild, never replicated). Exec
//     rejects all schema changes by design (sqlengine/guard.go), so the
//     suite declares docs_fts in SchemaConfig.LocalDDL.
//   - FTS content is app-maintained local state: capture skips
//     non-registry tables, so dual writes (base row + FTS row) through
//     the normal ExecSQL path replicate the base row and index locally.
//     CREATE TRIGGER cannot go through ExecSQL: the trigger body carries
//     a second statement and the engine rejects multi-statement queries
//     (sqlengine/singlestmt.go).
//   - A scratch-engine FTS5 capability probe runs before cluster setup:
//     absent FTS5 skips honestly instead of failing daemon startup (the
//     config LocalDDL would not apply).
//   - After the crash the suite asserts the definition survived (config
//     re-applied at open) while the in-memory content is actually gone,
//     rebuilds content from the base table, and only then asserts
//     MATCH/base equivalence.
package ftscrash_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/sqlengine"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

// Distinct tokens: no token is a substring of another, so FTS5 MATCH and
// SQL LIKE agree exactly on space-separated bodies.
var tokens = []string{"tokalpha", "tokbravo", "tokcharlie", "tokdelta"}

const commonToken = "tokcommon"

func TestFTSCrashRebuildMatchesBase(t *testing.T) {
	seedDocs := envInt("MURMUR_FTS_CRASH_SEED", 200)
	// Capability probe before cluster setup: the FTS definition ships
	// as config LocalDDL, which would fail daemon startup outright on
	// a backend without FTS5.
	if !fts5Available() {
		t.Skip("FTS5 unavailable on this backend, skipping")
	}
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "fts-crash",
		NumNodes: 1,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "docs",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "title", Type: schema.ColText, Nullable: true},
				{Name: "body", Type: schema.ColText, Nullable: true},
			},
		}},
			LocalDDL: []string{`CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts USING fts5(id UNINDEXED, title, body)`},
		},
	})

	// The config LocalDDL applied at open: definition present, empty.
	if n := countQuery(t, cluster, "SELECT count(*) FROM sqlite_master WHERE name = 'docs_fts'"); n != 1 {
		t.Fatalf("docs_fts missing after open with config LocalDDL (count=%d)", n)
	}

	// Seed known docs with dual writes (base + FTS row each).
	for i := 0; i < seedDocs; i++ {
		insertDoc(t, cluster, fmt.Sprintf("%032x", i), fmt.Sprintf("doc %d", i), bodyFor(i))
	}

	// Positive control: MATCH returns the exact expected rows pre-crash.
	// Expected sets are derived from the base table (LIKE) and cross-
	// checked against the deterministic generator (independent of FTS).
	for _, tok := range append([]string{commonToken}, tokens...) {
		match := matchTitles(t, cluster, tok)
		like := likeTitles(t, cluster, tok)
		if !equalStrings(match, like) {
			t.Fatalf("pre-crash MATCH %q = %d rows, base LIKE = %d rows", tok, len(match), len(like))
		}
		var gen []string
		for i := 0; i < seedDocs; i++ {
			if strings.Contains(bodyFor(i), tok) {
				gen = append(gen, fmt.Sprintf("doc %d", i))
			}
		}
		if len(match) != len(gen) {
			t.Fatalf("pre-crash MATCH %q = %d rows, generator expects %d", tok, len(match), len(gen))
		}
	}
	if match := matchTitles(t, cluster, "nomatchtoken"); len(match) != 0 {
		t.Fatalf("pre-crash MATCH nomatchtoken = %v, want empty", match)
	}
	if n := countQuery(t, cluster, "SELECT count(*) FROM docs_fts"); n != seedDocs {
		t.Fatalf("pre-crash FTS row count = %d, want %d", n, seedDocs)
	}
	t.Logf("positive control: %d docs indexed, MATCH exact pre-crash", seedDocs)

	// SIGKILL mid-index-write, coordinated like files-crash: the kill
	// only counts when a write errored/timed out; between-writes kills
	// retry instead of passing as proof.
	killed := false
	for round := 1; round <= 3 && !killed; round++ {
		var inFlight, errored atomic.Int64
		stop := make(chan struct{})
		done := make(chan struct{})
		go func(round int) {
			defer close(done)
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := 1<<20*round + n
				inFlight.Add(1)
				err := insertDocErr(cluster, fmt.Sprintf("%032x", id),
					fmt.Sprintf("crash-r%d-n%d", round, n), bodyFor(id))
				inFlight.Add(-1)
				if err != nil {
					errored.Add(1)
					return
				}
				n++
			}
		}(round)
		if !waitInflight(&inFlight, 10*time.Second) {
			close(stop)
			<-done
			t.Fatalf("round %d: no index write ever in flight", round)
		}
		t.Logf("round %d: SIGKILL with %d index write(s) in flight", round, inFlight.Load())
		cluster.KillNode(0)
		close(stop)
		<-done
		cluster.StartNode(0)
		cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
		cluster.WaitNodeReady(0)
		if errored.Load() == 0 {
			t.Logf("round %d: kill landed between writes; retrying", round)
			continue
		}
		killed = true
		t.Logf("round %d: killed mid-index-write (%d errored writes)", round, errored.Load())
	}
	if !killed {
		t.Fatal("no valid mid-write kill in 3 rounds")
	}

	// The crash wiped the in-memory FTS content while the definition
	// survived via config LocalDDL (re-applied at open): prove both
	// (no silent persistence assumption either way), then rebuild the
	// content from the base table.
	if n := countQuery(t, cluster, "SELECT count(*) FROM sqlite_master WHERE name = 'docs_fts'"); n != 1 {
		t.Fatalf("post-crash sqlite_master lists docs_fts %d times, want 1 (config LocalDDL must re-apply at open)", n)
	}
	if n := countQuery(t, cluster, "SELECT count(*) FROM docs_fts"); n != 0 {
		t.Fatalf("post-crash FTS content = %d rows, want 0 (in-memory loss)", n)
	}
	t.Log("post-crash: FTS definition restored from config, content gone with the in-memory engine, as designed")
	if err := cluster.ExecSQL(0,
		`INSERT INTO docs_fts(id, title, body) SELECT id, title, body FROM docs`); err != nil {
		t.Fatalf("rebuild FTS from base: %v", err)
	}

	// Full proof: every MATCH returns the exact base-table row set (LIKE-
	// derived, independent of FTS), no duplicates, exact counts, and a
	// clean integrity check.
	baseCount := countQuery(t, cluster, "SELECT count(*) FROM docs")
	if n := countQuery(t, cluster, "SELECT count(*) FROM docs_fts"); n != baseCount {
		t.Fatalf("post-rebuild FTS count = %d, base count = %d", n, baseCount)
	}
	if n := countQuery(t, cluster, "SELECT count(DISTINCT title) FROM docs_fts"); n != baseCount {
		t.Fatalf("post-rebuild distinct FTS titles = %d, base count = %d (duplicates)", n, baseCount)
	}
	for _, tok := range append([]string{commonToken}, tokens...) {
		match := matchTitles(t, cluster, tok)
		like := likeTitles(t, cluster, tok)
		if !equalStrings(match, like) {
			t.Fatalf("post-rebuild MATCH %q = %d rows, base LIKE = %d rows", tok, len(match), len(like))
		}
		t.Logf("token %q: %d rows, MATCH == base exactly", tok, len(match))
	}
	if match := matchTitles(t, cluster, "nomatchtoken"); len(match) != 0 {
		t.Fatalf("MATCH nomatchtoken = %v, want empty", match)
	}
	checkIntegrity(t, cluster)
	t.Log("fts-crash proven: crash/rebuild restores exact search results, integrity clean")
}

// insertDoc writes one doc to the base table and the FTS index.
func insertDoc(t *testing.T, c *harness.Cluster, id, title, body string) {
	t.Helper()
	if err := insertDocErr(c, id, title, body); err != nil {
		t.Fatalf("insert %s: %v", title, err)
	}
}

func insertDocErr(c *harness.Cluster, id, title, body string) error {
	if err := c.ExecSQL(0, "INSERT INTO docs (id, title, body) VALUES (?, ?, ?)", id, title, body); err != nil {
		return err
	}
	return c.ExecSQL(0, "INSERT INTO docs_fts (id, title, body) VALUES (?, ?, ?)", id, title, body)
}

// bodyFor renders deterministic doc text: the common token plus one
// rotating token plus filler.
func bodyFor(i int) string {
	return fmt.Sprintf("%s %s docnum %d filler text for full text search crash testing", commonToken, tokens[i%len(tokens)], i)
}

// matchTitles runs an FTS MATCH query, ordered by title.
func matchTitles(t *testing.T, c *harness.Cluster, tok string) []string {
	t.Helper()
	res, err := c.QuerySQL(0, "SELECT title FROM docs_fts WHERE docs_fts MATCH ? ORDER BY title", tok)
	if err != nil {
		t.Fatalf("MATCH %q: %v", tok, err)
	}
	return firstColStrings(res)
}

// likeTitles derives the expected row set from the base table, ordered.
func likeTitles(t *testing.T, c *harness.Cluster, tok string) []string {
	t.Helper()
	res, err := c.QuerySQL(0, "SELECT title FROM docs WHERE body LIKE ? ORDER BY title", "%"+tok+"%")
	if err != nil {
		t.Fatalf("LIKE %q: %v", tok, err)
	}
	return firstColStrings(res)
}

func countQuery(t *testing.T, c *harness.Cluster, q string) int {
	t.Helper()
	res, err := c.QuerySQL(0, q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) == 0 {
		t.Fatalf("query %q returned %v rows, want 1", q, len(res.Rows))
	}
	return int(toInt64(t, res.Rows[0][0]))
}

// checkIntegrity requires PRAGMA integrity_check to report ok.
func checkIntegrity(t *testing.T, c *harness.Cluster) {
	t.Helper()
	res, err := c.QuerySQL(0, "PRAGMA integrity_check")
	if err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("integrity_check returned no rows")
	}
	for _, row := range res.Rows {
		if s := fmt.Sprint(row[0]); !strings.EqualFold(s, "ok") {
			t.Fatalf("integrity_check reports %q, want ok", s)
		}
	}
}

func firstColStrings(res *harness.QueryResult) []string {
	out := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		out = append(out, fmt.Sprint(row[0]))
	}
	return out
}

func toInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	default:
		i, err := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		if err != nil {
			t.Fatalf("cell %v (%T) is not an integer", v, v)
		}
		return i
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func waitInflight(inFlight *atomic.Int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if inFlight.Load() > 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

// fts5Available probes the linked SQLite backend (same driver the
// daemons use) for FTS5 via a scratch in-memory engine.
func fts5Available() bool {
	reg, err := schema.BuildRegistry(1, nil)
	if err != nil {
		return false
	}
	eng, err := sqlengine.Open(reg, nil, nil, 0)
	if err != nil {
		return false
	}
	defer eng.Close()
	used := 0
	err = eng.QueryRowContext(context.Background(),
		"SELECT sqlite_compileoption_used('ENABLE_FTS5')", nil,
		func(r *sql.Row) error { return r.Scan(&used) })
	return err == nil && used == 1
}
