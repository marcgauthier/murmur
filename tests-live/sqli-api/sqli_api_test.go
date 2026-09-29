// SQLi/API containment: hostile SQL text through the HTTP service API must
// stay contained — stacked statements never execute past the first, bound
// arguments are never interpolated (comment escapes, UNION smuggling, and
// quote-breakout payloads stay literal), oversized identifiers fail cleanly,
// file routes treat names as opaque keys (path traversal cannot escape), and
// error responses leak no schema or stack details. Legit writes and reads
// keep working throughout (positive control).
package sqliapi_test

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

var sqliSchema = &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{
	{
		Name: "sqli_users",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	},
	{
		Name: "sqli_secrets",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "secret", Type: schema.ColText, Nullable: true},
		},
	},
}}

// leakMarkers must never appear in API error responses: no goroutine/stack
// traces, no Go source positions, no schema DDL, no key material.
var leakMarkers = []string{
	"goroutine ", "panic:", "panic(", ".go:", "stack trace", "runtime error",
	"CREATE TABLE", "BEGIN IMMEDIATE", "key_hex", "BEGIN;",
}

func TestSQLiAPIContainment(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "sqli-api",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema:      sqliSchema,
		Files:       &harness.FilesOptions{ObjectKeyHex: "3333333333333333333333333333333333333333333333333333333333333333", MaxFileBytes: 4 << 20, FetchIntervalMs: -1},
	})
	var errTexts []string
	noteErr := func(err error) {
		if err != nil {
			errTexts = append(errTexts, err.Error())
		}
	}

	// --- Positive control (pre-attack): legit writes replicate, digests match.
	if err := cluster.ExecSQL(0, "INSERT INTO sqli_users (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 1), "alice"); err != nil {
		t.Fatalf("legit insert user: %v", err)
	}
	if err := cluster.ExecSQL(0, "INSERT INTO sqli_users (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 2), "bob"); err != nil {
		t.Fatalf("legit insert user: %v", err)
	}
	if err := cluster.ExecSQL(0, "INSERT INTO sqli_secrets (id, secret) VALUES (?, ?)", fmt.Sprintf("%032x", 10), "crown-jewels"); err != nil {
		t.Fatalf("legit insert secret: %v", err)
	}
	waitConverged(t, cluster, "sqli_users", 2, 30*time.Second)
	waitConverged(t, cluster, "sqli_secrets", 1, 30*time.Second)
	secretsDigest := tableDigest(t, cluster, 0, "sqli_secrets", "secret")

	// --- 1. Stacked statements in exec text: only the first may apply.
	stackID := fmt.Sprintf("%032x", 3)
	noteErr(cluster.ExecSQL(0,
		"INSERT INTO sqli_users (id, name) VALUES (?, ?); DELETE FROM sqli_secrets", stackID, "mallory"))
	assertSecretsIntact := func(ctx string) {
		t.Helper()
		for i := range cluster.Nodes {
			n, err := cluster.QueryRowCount(i, "sqli_secrets")
			if err != nil || n != 1 {
				t.Fatalf("%s: node %d secrets count = %d, err = %v, want 1 (cross-table effect?)", ctx, i, n, err)
			}
		}
		// Give async replication a window in which a leaked write would land.
		time.Sleep(2 * time.Second)
		for i := range cluster.Nodes {
			if d := tableDigest(t, cluster, i, "sqli_secrets", "secret"); d != secretsDigest {
				t.Fatalf("%s: node %d secrets digest moved %s -> %s (cross-table effect)", ctx, i, secretsDigest, d)
			}
		}
		if _, err := cluster.QuerySQL(0, "SELECT secret FROM sqli_secrets WHERE secret = ?", "crown-jewels"); err != nil {
			t.Fatalf("%s: secrets table unreadable: %v", ctx, err)
		}
	}
	assertSecretsIntact("after stacked DELETE")
	// Stacked DROP with inline values (no placeholders to dispute).
	noteErr(cluster.ExecSQL(0,
		"INSERT INTO sqli_users (id, name) VALUES (x'"+fmt.Sprintf("%032x", 4)+"', 'stacked-drop'); DROP TABLE sqli_secrets; --"))
	assertSecretsIntact("after stacked DROP")

	// --- 2. Comment escapes and quote breakouts via bound args stay literal.
	for _, evil := range []string{"' OR '1'='1", "' OR 1=1 --", "admin'--", "' UNION SELECT 1 --", "\\'; DROP TABLE sqli_users; --"} {
		res, err := cluster.QuerySQL(0, "SELECT id, name FROM sqli_users WHERE name = ?", evil)
		if err != nil {
			t.Fatalf("bound comment-escape query %q: %v", evil, err)
		}
		if len(res.Rows) != 0 {
			t.Fatalf("bound arg %q matched %d rows, want 0 (interpolated?)", evil, len(res.Rows))
		}
	}
	// Evil values stored through args round-trip literally and cause no effects.
	nasty := "x'; DROP TABLE sqli_secrets; --"
	if err := cluster.ExecSQL(0, "INSERT INTO sqli_users (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 5), nasty); err != nil {
		t.Fatalf("insert evil literal: %v", err)
	}
	res, err := cluster.QuerySQL(0, "SELECT name FROM sqli_users WHERE name = ?", nasty)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("evil literal round-trip: rows=%v err=%v, want exactly the stored value", res, err)
	}
	if got := fmt.Sprintf("%v", res.Rows[0][0]); got != nasty {
		t.Fatalf("evil literal mangled: %q != %q", got, nasty)
	}
	assertSecretsIntact("after evil-literal insert")

	// --- 3. UNION exfiltration smuggled through args exfiltrates nothing.
	unionPayloads := []string{
		"' UNION SELECT id, secret FROM sqli_secrets --",
		"alice' UNION SELECT id, secret FROM sqli_secrets --",
		"' UNION ALL SELECT id, secret FROM sqli_secrets /*",
	}
	for _, evil := range unionPayloads {
		res, err := cluster.QuerySQL(0, "SELECT id, name FROM sqli_users WHERE name = ?", evil)
		if err != nil {
			t.Fatalf("bound UNION query: %v", err)
		}
		if len(res.Rows) != 0 {
			t.Fatalf("bound UNION payload %q returned %d rows, want 0", evil, len(res.Rows))
		}
		raw := fmt.Sprintf("%v", res.Rows)
		if strings.Contains(raw, "crown-jewels") {
			t.Fatalf("bound UNION payload %q leaked secret bytes", evil)
		}
	}

	// Non-vacuous control: the same payloads ARE live when interpolated, so
	// the empty results above prove arg binding, not dead payloads.
	interp, err := cluster.QuerySQL(0, "SELECT id, name FROM sqli_users WHERE name = '' OR '1'='1'")
	if err != nil {
		t.Fatalf("interpolated control query: %v", err)
	}
	if len(interp.Rows) == 0 {
		t.Fatal("interpolated tautology returned 0 rows: control payload is dead, suite is vacuous")
	}
	t.Logf("interpolation control: tautology matched %d rows (attack live), bound form matched 0 (contained)", len(interp.Rows))

	// --- 4. Oversized identifiers: clean error, node keeps serving.
	longIdent := strings.Repeat("q", 3000)
	noteErr(cluster.ExecSQL(0, "INSERT INTO "+longIdent+" (id) VALUES (1)"))
	if err := cluster.ExecSQL(0, "INSERT INTO "+longIdent+" (id) VALUES (1)"); err == nil {
		t.Fatal("3KB identifier accepted, want an error")
	}
	noteErr(expectQueryErr(t, cluster, "SELECT * FROM "+longIdent))
	// A huge-but-valid query must not hang or crash the node: it matches
	// nothing (or errors cleanly), and the node keeps serving after.
	hugeQuery := "SELECT * FROM sqli_users WHERE name = '" + strings.Repeat("z", 64<<10) + "'"
	if res, err := cluster.QuerySQL(0, hugeQuery); err != nil {
		noteErr(err)
	} else if len(res.Rows) != 0 {
		t.Fatalf("64KB literal query matched %d rows, want 0", len(res.Rows))
	}
	// Node still serves legit traffic after the oversized attempts.
	if n, err := cluster.QueryRowCount(0, "sqli_users"); err != nil || n < 2 {
		t.Fatalf("node not serving after oversized identifiers: count=%d err=%v", n, err)
	}

	// --- 5. Syntax-error and missing-table responses (leak-scan material).
	noteErr(cluster.ExecSQL(0, "SELEC FROM WHERE WHATEVER"))
	noteErr(expectQueryErr(t, cluster, "SELECT * FROM nope_missing WHERE x = 1"))
	noteErr(cluster.ExecSQL(0, "INSERT INTO nope_missing (id) VALUES (1)"))
	// One raw request to pin status/body shape: non-200, no markers.
	body, code := rawExec(t, cluster, 0, `{"query":"INSERT INTO nope_missing (id) VALUES (1)"}`)
	if code == http.StatusOK {
		t.Fatal("raw bad exec returned 200, want an error status")
	}
	errTexts = append(errTexts, body)

	// --- 6. File routes: legit round-trip, traversal contained.
	legit := []byte("legit file bytes for sqli-api")
	if _, err := cluster.UploadFile(0, "sqli-legit.txt", legit); err != nil {
		t.Fatalf("legit file upload: %v", err)
	}
	st, err := cluster.StatFile(0, "sqli-legit.txt")
	if err != nil {
		t.Fatalf("legit file status: %v", err)
	}
	if !st.Exists || !st.Available || st.Size != int64(len(legit)) {
		t.Fatalf("legit file status = %+v, want exists+available with size %d", st, len(legit))
	}
	got, err := cluster.DownloadFile(0, "sqli-legit.txt")
	if err != nil {
		t.Fatalf("legit file download: %v", err)
	}
	if !bytes.Equal(got, legit) {
		t.Fatal("legit file bytes mangled in round-trip")
	}
	traversals := []string{
		"../../../../etc/passwd",
		"..\\..\\windows\\win.ini",
		"%2e%2e%2fetc%2fpasswd",
		"/etc/passwd",
		"sqli-legit.txt/../../etc/passwd",
		"..",
	}
	for _, evil := range traversals {
		if data, err := cluster.DownloadFile(0, evil); err == nil {
			if len(data) != 0 {
				t.Fatalf("traversal download %q returned %d bytes, want error or empty", evil, len(data))
			}
		} else {
			errTexts = append(errTexts, err.Error())
		}
		if fstat, err := cluster.StatFile(0, evil); err == nil && (fstat.Exists || fstat.Available) {
			t.Fatalf("traversal status %q reports exists/available: %+v", evil, fstat)
		}
		hits, err := cluster.SearchFiles(0, "", evil)
		if err != nil {
			noteErr(err)
		} else {
			for _, h := range hits {
				if h.Name == evil && h.Available {
					t.Fatalf("traversal search %q unexpectedly available", evil)
				}
			}
		}
		noteErr(cluster.DeleteFile(0, evil))
	}
	// Traversal names are opaque keys: upload under one round-trips as data
	// and creates no filesystem entry outside the object store.
	marker := "evil-traversal-marker-sqli"
	travName := "../../" + marker
	payload := []byte("traversal payload stays keyed, not pathed")
	if _, err := cluster.UploadFile(0, travName, payload); err != nil {
		t.Fatalf("traversal-name upload: %v", err)
	}
	got, err = cluster.DownloadFile(0, travName)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("traversal-name round-trip: bytes=%q err=%v", got, err)
	}
	if err := cluster.DeleteFile(0, travName); err != nil {
		t.Fatalf("traversal-name delete: %v", err)
	}
	if err := filepath.WalkDir(cluster.RuntimeDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if strings.Contains(d.Name(), marker) {
			t.Errorf("traversal escaped to filesystem: %s", p)
		}
		return nil
	}); err != nil {
		t.Fatalf("walk runtime dir: %v", err)
	}

	// --- 7. Error responses leak no schema or stack details.
	if len(errTexts) == 0 {
		t.Fatal("no error responses collected: leak scan is vacuous")
	}
	for _, text := range errTexts {
		for _, m := range leakMarkers {
			if strings.Contains(text, m) {
				t.Fatalf("error response leaks %q: %.300s", m, text)
			}
		}
	}
	t.Logf("scanned %d error responses for %d leak markers: clean", len(errTexts), len(leakMarkers))

	// --- Positive control (post-attack): writes converge, digests match.
	if err := cluster.ExecSQL(1, "INSERT INTO sqli_users (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 6), "post-attack"); err != nil {
		t.Fatalf("post-attack insert: %v", err)
	}
	nUsers, _ := cluster.QueryRowCount(1, "sqli_users")
	waitConverged(t, cluster, "sqli_users", nUsers, 30*time.Second)
	waitConverged(t, cluster, "sqli_secrets", 1, 30*time.Second)
}

func expectQueryErr(t *testing.T, cluster *harness.Cluster, q string, args ...any) error {
	t.Helper()
	_, err := cluster.QuerySQL(0, q, args...)
	if err == nil {
		t.Fatalf("query %.80q... succeeded, want an error", q)
	}
	return err
}

func rawExec(t *testing.T, cluster *harness.Cluster, idx int, payload string) (string, int) {
	t.Helper()
	url := fmt.Sprintf("https://%s/v1/exec", cluster.Nodes[idx].APIAddr)
	resp, err := http.Post(url, "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("raw exec POST: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return string(raw), resp.StatusCode
}

func tableDigest(t *testing.T, cluster *harness.Cluster, idx int, table, orderBy string) string {
	t.Helper()
	d, err := cluster.ComputeTableDigest(idx, table, orderBy)
	if err != nil {
		t.Fatalf("node %d %s digest: %v", idx, table, err)
	}
	return d
}

func waitConverged(t *testing.T, c *harness.Cluster, table string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, table)
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, table, "name")
			if table == "sqli_secrets" {
				d, err = c.ComputeTableDigest(i, table, "secret")
			}
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
	t.Fatalf("nodes did not converge on %d %s rows with equal digests within %v", want, table, timeout)
}
