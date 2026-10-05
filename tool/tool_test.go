package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/tool/cmd"
	"github.com/marcgauthier/murmur/tool/format"
)

func runCLI(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	ctx := context.Background()

	// Parse global options and find subcommand
	opts, remaining := cmd.ParseGlobalOptions(args)
	if len(remaining) == 0 {
		return "", "", nil
	}

	commandName := remaining[0]
	subArgs := remaining[1:]

	targetCmd := cmd.Lookup(commandName)
	if targetCmd == nil {
		return "", "", nil
	}

	err := targetCmd.Run(ctx, opts, subArgs, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func TestCLI_EndToEnd(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "murmur_cli_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "db")

	// 1. Init
	t.Run("Init", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "init", dataDir)
		if err != nil {
			t.Fatalf("init failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "Initialized") {
			t.Errorf("unexpected init output: %s", stdout)
		}
	})

	// 2. Query DDL & Insert
	t.Run("Query_DDL_And_Insert", func(t *testing.T) {
		_, stderr, err := runCLI(t, "query", dataDir, "CREATE TABLE users (id BLOB PRIMARY KEY, name TEXT, age INTEGER);")
		if err != nil {
			t.Fatalf("create table failed: %v (stderr: %s)", err, stderr)
		}

		insertSQL := "INSERT INTO users (id, name, age) VALUES (x'0102030405060708090a0b0c0d0e0f10', 'Alice', 30);"
		_, stderr, err = runCLI(t, "query", dataDir, insertSQL)
		if err != nil {
			t.Fatalf("insert failed: %v (stderr: %s)", err, stderr)
		}

		// Select
		stdout, stderr, err := runCLI(t, "--json", "query", dataDir, "SELECT name, age FROM users;")
		if err != nil {
			t.Fatalf("select failed: %v (stderr: %s)", err, stderr)
		}

		var rows []map[string]any
		if err := json.Unmarshal([]byte(stdout), &rows); err != nil {
			t.Fatalf("failed to unmarshal JSON output %q: %v", stdout, err)
		}
		if len(rows) != 1 || rows[0]["name"] != "Alice" {
			t.Fatalf("unexpected query result: %v", rows)
		}
	})

	// 3. Schema
	t.Run("Schema", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "--json", "schema", dataDir)
		if err != nil {
			t.Fatalf("schema failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "users") {
			t.Errorf("schema output does not contain users: %s", stdout)
		}
	})

	// 4. Import & Export
	t.Run("Import_Export", func(t *testing.T) {
		csvFile := filepath.Join(tempDir, "import.csv")
		csvData := "id,name,age\n0202030405060708090a0b0c0d0e0f10,Bob,25\n"
		if err := os.WriteFile(csvFile, []byte(csvData), 0o600); err != nil {
			t.Fatalf("failed to write csv: %v", err)
		}

		_, stderr, err := runCLI(t, "import", dataDir, "users", csvFile, "--format=csv")
		if err != nil {
			t.Fatalf("import failed: %v (stderr: %s)", err, stderr)
		}

		exportCSV := filepath.Join(tempDir, "export.csv")
		_, stderr, err = runCLI(t, "export", dataDir, "users", "--format=csv", "--output="+exportCSV)
		if err != nil {
			t.Fatalf("export failed: %v (stderr: %s)", err, stderr)
		}

		expData, err := os.ReadFile(exportCSV)
		if err != nil {
			t.Fatalf("failed to read exported csv: %v", err)
		}
		if !strings.Contains(string(expData), "Bob") || !strings.Contains(string(expData), "Alice") {
			t.Errorf("exported csv missing rows: %s", string(expData))
		}
	})

	// 5. Dump
	t.Run("Dump", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "dump", dataDir)
		if err != nil {
			t.Fatalf("dump failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "users") || !strings.Contains(stdout, "INSERT INTO users") {
			t.Errorf("dump missing expected SQL: %s", stdout)
		}
	})

	// 6. Inspect & Keys
	t.Run("Inspect_And_Keys", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "--json", "inspect", dataDir)
		if err != nil {
			t.Fatalf("inspect failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "data_dir") {
			t.Errorf("unexpected inspect output: %s", stdout)
		}

		stdout, stderr, err = runCLI(t, "--json", "keys", dataDir)
		if err != nil {
			t.Fatalf("keys failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "unlocked") {
			t.Errorf("unexpected keys output: %s", stdout)
		}
	})

	// 7. Verify & Repair
	t.Run("Verify_And_Repair", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "verify", dataDir, "--quick")
		if err != nil {
			t.Fatalf("verify failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(strings.ToLower(stdout), "verified") {
			t.Errorf("verify did not pass: %s", stdout)
		}

		stdout, stderr, err = runCLI(t, "repair", dataDir, "--force")
		if err != nil {
			t.Fatalf("repair failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(strings.ToLower(stdout), "rebuilt") {
			t.Errorf("unexpected repair output: %s", stdout)
		}
	})

	// 8. Doctor
	t.Run("Doctor", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "--json", "doctor", dataDir)
		if err != nil {
			t.Fatalf("doctor failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "HEALTHY") {
			t.Errorf("doctor status not healthy: %s", stdout)
		}
	})

	// 9. Backup & Restore
	t.Run("Backup_And_Restore", func(t *testing.T) {
		backupArchive := filepath.Join(tempDir, "murmur_backup.tar.gz")
		stdout, stderr, err := runCLI(t, "backup", "create", dataDir, backupArchive)
		if err != nil {
			t.Fatalf("backup create failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "created successfully") {
			t.Errorf("unexpected backup create output: %s", stdout)
		}

		// Info
		stdout, stderr, err = runCLI(t, "--json", "backup", "info", backupArchive)
		if err != nil {
			t.Fatalf("backup info failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "schema_version") {
			t.Errorf("unexpected backup info output: %s", stdout)
		}

		// Verify
		stdout, stderr, err = runCLI(t, "backup", "verify", backupArchive)
		if err != nil {
			t.Fatalf("backup verify failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(strings.ToLower(stdout), "verified") {
			t.Errorf("unexpected backup verify output: %s", stdout)
		}

		// Restore
		restoreDir := filepath.Join(tempDir, "restored_db")
		stdout, stderr, err = runCLI(t, "backup", "restore", backupArchive, restoreDir)
		if err != nil {
			t.Fatalf("backup restore failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "restored successfully") {
			t.Errorf("unexpected backup restore output: %s", stdout)
		}

		// Query restored
		stdout, stderr, err = runCLI(t, "--json", "query", restoreDir, "SELECT count(*) AS cnt FROM users;")
		if err != nil {
			t.Fatalf("query restored failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "2") {
			t.Errorf("expected 2 users in restored db, got: %s", stdout)
		}
	})

	// 10. Bench
	t.Run("Bench", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "bench", "--operations=100")
		if err != nil {
			t.Fatalf("bench failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "Benchmark Results") {
			t.Errorf("unexpected bench output: %s", stdout)
		}
	})
}

func TestFormatters(t *testing.T) {
	headers := []string{"ID", "Name", "Active"}
	rows := [][]string{
		{"1", "Alice", "true"},
		{"2", "Bob", "false"},
	}

	var buf bytes.Buffer
	format.RenderTable(&buf, headers, rows, false)
	if !strings.Contains(buf.String(), "+----+-------+--------+") {
		t.Errorf("unexpected ascii table output: %s", buf.String())
	}

	buf.Reset()
	format.RenderTable(&buf, headers, rows, true)
	if !strings.Contains(buf.String(), "Alice") || !strings.Contains(buf.String(), "Active") {
		t.Errorf("unexpected markdown table output: %s", buf.String())
	}

	buf.Reset()
	format.RenderJSONError(&buf, os.ErrNotExist)
	if !strings.Contains(buf.String(), `"error":`) {
		t.Errorf("unexpected json error output: %s", buf.String())
	}
}

func TestCLI_Execute(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := cmd.Execute([]string{"--version"}, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), "murmur version") {
		t.Errorf("expected version output, got code %d, out: %s", code, stdout.String())
	}

	stdout.Reset()
	code = cmd.Execute([]string{"--help"}, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), "Murmur CLI") {
		t.Errorf("expected help output, got code %d, out: %s", code, stdout.String())
	}

	stderr.Reset()
	code = cmd.Execute([]string{"nonexistent-command"}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "unknown command") {
		t.Errorf("expected unknown command error, got code %d, err: %s", code, stderr.String())
	}
}

func TestRemoteCommands(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/status", "/v1/admin/status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":         "ok",
				"node_id":        "node-001",
				"uptime_seconds": 3600,
				"memory_bytes":   10485760,
				"tombstones":     5,
				"lag": []map[string]any{
					{"peer_id": "node-002", "lag_bytes": 0, "lag_ops": 0},
				},
			})
		case "/v1/debug/peers":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"name": "node-001", "addr": "127.0.0.1:8443", "status": "alive"},
			})
		case "/v1/query":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"columns": []string{"id", "val"},
				"rows":    [][]string{{"1", "test"}},
			})
		case "/v1/exec":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"rows_affected": 1,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Run("Remote_Status", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "--json", "status", ts.URL)
		if err != nil {
			t.Fatalf("remote status failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "node-001") {
			t.Errorf("unexpected status output: %s", stdout)
		}
	})

	t.Run("Remote_Cluster", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "--json", "cluster", ts.URL)
		if err != nil {
			t.Fatalf("remote cluster failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "node-001") {
			t.Errorf("unexpected cluster output: %s", stdout)
		}
	})

	t.Run("Remote_Lag", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "--json", "lag", ts.URL)
		if err != nil {
			t.Fatalf("remote lag failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "node-002") {
			t.Errorf("unexpected lag output: %s", stdout)
		}
	})

	t.Run("Remote_GC", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "--json", "gc", ts.URL)
		if err != nil {
			t.Fatalf("remote gc failed: %v (stderr: %s)", err, stderr)
		}
		if !strings.Contains(stdout, "tombstones") {
			t.Errorf("unexpected gc output: %s", stdout)
		}
	})
}

func TestShell(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "murmur_shell_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "db")
	_, _, err = runCLI(t, "init", dataDir)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	commands := []string{
		".help",
		".mode table",
		".mode csv",
		".mode json",
		".headers on",
		".timer on",
		"CREATE TABLE items (id BLOB PRIMARY KEY, title TEXT);",
		"INSERT INTO items (id, title) VALUES (x'0102030405060708090a0b0c0d0e0f11', 'Widget');",
		"SELECT title FROM items;",
		".tables",
		".schema items",
		".status",
		".dump",
		".quit",
	}

	input := strings.Join(commands, "\n") + "\n"
	shellCmd := &cmd.ShellCommand{In: strings.NewReader(input)}

	var stdout, stderr bytes.Buffer
	err = shellCmd.Run(context.Background(), cmd.GlobalOptions{}, []string{dataDir}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("shell execution failed: %v (stderr: %s)", err, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, "Widget") {
		t.Errorf("shell missing query output: %s", out)
	}
	if !strings.Contains(out, "items") {
		t.Errorf("shell missing .tables output: %s", out)
	}
	if !strings.Contains(out, "CREATE TABLE") {
		t.Errorf("shell missing .schema output: %s", out)
	}
}

func TestMainEntry(t *testing.T) {
	if os.Getenv("TEST_MURMUR_MAIN") == "1" {
		os.Args = []string{"murmur", "--version"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainEntry$")
	cmd.Env = append(os.Environ(), "TEST_MURMUR_MAIN=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("main failed: %v, out: %s", err, string(out))
	}
	if !strings.Contains(string(out), "murmur version") {
		t.Errorf("unexpected output from main: %s", string(out))
	}
}

