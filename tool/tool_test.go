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

	t.Run("Init", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "init", dataDir)
		if err != nil || !strings.Contains(stdout, "Initialized") {
			t.Fatalf("init failed: %v (stdout=%s stderr=%s)", err, stdout, stderr)
		}
	})

	t.Run("OfflineDiagnostics", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "--json", "verify", dataDir)
		if err != nil || !strings.Contains(stdout, `"valid": true`) || !strings.Contains(stdout, "Schema is not bound yet") {
			t.Fatalf("verify did not report valid unbound Spool: err=%v stdout=%s stderr=%s", err, stdout, stderr)
		}
		stdout, stderr, err = runCLI(t, "--json", "inspect", dataDir)
		if err != nil || !strings.Contains(stdout, "data_dir") {
			t.Fatalf("inspect failed: err=%v stdout=%s stderr=%s", err, stdout, stderr)
		}
		stdout, stderr, err = runCLI(t, "--json", "keys", dataDir)
		if err != nil || !strings.Contains(stdout, `"unlocked": true`) {
			t.Fatalf("keys failed: err=%v stdout=%s stderr=%s", err, stdout, stderr)
		}
		stdout, stderr, err = runCLI(t, "--json", "repair", dataDir)
		if err != nil || !strings.Contains(stdout, `"durable_state_checked": true`) {
			t.Fatalf("repair failed: err=%v stdout=%s stderr=%s", err, stdout, stderr)
		}
		stdout, stderr, err = runCLI(t, "--json", "doctor", dataDir)
		if err != nil || !strings.Contains(stdout, `"overall": "DEGRADED"`) {
			t.Fatalf("doctor should report the unbound schema/materializer: err=%v stdout=%s stderr=%s", err, stdout, stderr)
		}
	})

	t.Run("BackupAndRestore", func(t *testing.T) {
		archive := filepath.Join(tempDir, "murmur_backup.tar.gz")
		stdout, stderr, err := runCLI(t, "backup", "create", dataDir, archive)
		if err != nil || !strings.Contains(stdout, "created successfully") {
			t.Fatalf("backup create failed: err=%v stdout=%s stderr=%s", err, stdout, stderr)
		}
		stdout, stderr, err = runCLI(t, "--json", "backup", "info", archive)
		if err != nil || !strings.Contains(stdout, "schema_version") {
			t.Fatalf("backup info failed: err=%v stdout=%s stderr=%s", err, stdout, stderr)
		}
		stdout, stderr, err = runCLI(t, "backup", "verify", archive)
		if err != nil || !strings.Contains(strings.ToLower(stdout), "verified") {
			t.Fatalf("backup verify failed: err=%v stdout=%s stderr=%s", err, stdout, stderr)
		}
		restoreDir := filepath.Join(tempDir, "restored_db")
		stdout, stderr, err = runCLI(t, "backup", "restore", archive, restoreDir)
		if err != nil || !strings.Contains(stdout, "restored successfully") {
			t.Fatalf("backup restore failed: err=%v stdout=%s stderr=%s", err, stdout, stderr)
		}
		if _, err := os.Stat(filepath.Join(restoreDir, "schema.json")); !os.IsNotExist(err) {
			t.Fatalf("restore recreated a legacy schema file (stat err=%v)", err)
		}
	})

	t.Run("TypedBenchmark", func(t *testing.T) {
		stdout, stderr, err := runCLI(t, "--json", "bench", "--duration=10ms")
		if err != nil || !strings.Contains(stdout, "rime_record_writes_per_sec") {
			t.Fatalf("typed benchmark failed: err=%v stdout=%s stderr=%s", err, stdout, stderr)
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
