package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tool/client"
)

func TestAllCommandMetadata(t *testing.T) {
	for name, command := range commandRegistry {
		if command.Name() == "" {
			t.Errorf("command %s has empty Name()", name)
		}
		if command.Description() == "" {
			t.Errorf("command %s has empty Description()", name)
		}
		if command.Usage() == "" {
			t.Errorf("command %s has empty Usage()", name)
		}
	}
}

func TestRoot_Execute_FlagsAndOptions(t *testing.T) {
	var stdout, stderr bytes.Buffer

	// Help and Version
	code := Execute([]string{"--help"}, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), "Usage:") {
		t.Errorf("expected help output, got %d: %s", code, stdout.String())
	}

	stdout.Reset()
	code = Execute([]string{"-h"}, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), "Usage:") {
		t.Errorf("expected short help output, got %d: %s", code, stdout.String())
	}

	stdout.Reset()
	code = Execute([]string{"--version"}, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), Version) {
		t.Errorf("expected version output, got %d: %s", code, stdout.String())
	}

	stdout.Reset()
	code = Execute([]string{"--version", "--json"}, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), Version) {
		t.Errorf("expected json version output, got %d: %s", code, stdout.String())
	}

	// No arguments prints help
	stdout.Reset()
	code = Execute([]string{}, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), "Usage:") {
		t.Errorf("expected help for empty args, got %d: %s", code, stdout.String())
	}

	// Unknown command
	stderr.Reset()
	code = Execute([]string{"unknown-cmd"}, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "unknown command") {
		t.Errorf("expected unknown command exit 1, got %d: %s", code, stderr.String())
	}

	// ParseGlobalOptions flag coverage
	args := []string{
		"--json",
		"--markdown",
		"--verbose",
		"--insecure",
		"--cert=/tmp/cert.pem",
		"--cert", "/tmp/cert2.pem",
		"--key=/tmp/key.pem",
		"--key", "/tmp/key2.pem",
		"--ca=/tmp/ca.pem",
		"--ca", "/tmp/ca2.pem",
		"--passphrase=pass123",
		"--passphrase", "pass456",
		"--key-hex=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"--key-hex", "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		"pos1", "pos2",
	}
	opts, pos := ParseGlobalOptions(args)
	if !opts.JSON || !opts.Markdown || !opts.Verbose || !opts.Insecure {
		t.Errorf("expected boolean flags to be true: %+v", opts)
	}
	if opts.CertFile != "/tmp/cert2.pem" || opts.KeyFile != "/tmp/key2.pem" || opts.CAFile != "/tmp/ca2.pem" {
		t.Errorf("expected parsed file paths, got %+v", opts)
	}
	if opts.Passphrase != "pass456" || len(opts.KeyHex) != 64 {
		t.Errorf("expected passphrase and key-hex parsed, got %+v", opts)
	}
	if len(pos) != 2 || pos[0] != "pos1" || pos[1] != "pos2" {
		t.Errorf("expected positional args, got %v", pos)
	}

	// ClientFromOpts
	client, err := ClientFromOpts("localhost:8080", GlobalOptions{Insecure: true})
	if err != nil || client == nil {
		t.Errorf("ClientFromOpts failed: %v", err)
	}

	// Error handling
	stderr.Reset()
	handleError(fmt.Errorf("test error"), GlobalOptions{JSON: true}, &stderr)
	if !strings.Contains(stderr.String(), `"error": true`) {
		t.Errorf("expected JSON error format: %s", stderr.String())
	}

	stderr.Reset()
	handleError(fmt.Errorf("test error"), GlobalOptions{JSON: false}, &stderr)
	if !strings.Contains(stderr.String(), "Error: test error") {
		t.Errorf("expected standard error format: %s", stderr.String())
	}
}

func TestInitCommand(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "murmur_init_test_*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()
	initCmd := &InitCommand{}

	// Missing args
	var stdout, stderr bytes.Buffer
	err = initCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr)
	if err == nil {
		t.Errorf("expected error for missing args, got nil")
	}

	// Standard init with JSON
	target1 := filepath.Join(tempDir, "db1")
	stdout.Reset()
	err = initCmd.Run(ctx, GlobalOptions{JSON: true}, []string{target1}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("init db1 failed: %v", err)
	}
	if !strings.Contains(stdout.String(), `"data_dir":`) {
		t.Errorf("unexpected JSON init: %s", stdout.String())
	}

	// Init with Passphrase
	target2 := filepath.Join(tempDir, "db2")
	stdout.Reset()
	err = initCmd.Run(ctx, GlobalOptions{Passphrase: "my-secret-passphrase"}, []string{target2}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("init db2 failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "Initialized new Murmur database") {
		t.Errorf("unexpected init text output: %s", stdout.String())
	}

	// Init with KeyHex
	target3 := filepath.Join(tempDir, "db3")
	stdout.Reset()
	keyHex := hex.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	err = initCmd.Run(ctx, GlobalOptions{KeyHex: keyHex}, []string{target3}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("init db3 failed: %v", err)
	}
}

func TestDDLHelper_ParsingAndExecution(t *testing.T) {
	// Column type parsing
	ddl := `CREATE TABLE test_all_types (
		id BLOB PRIMARY KEY NOT NULL,
		name TEXT DEFAULT 'unknown',
		age INTEGER,
		salary BIGINT,
		rate DOUBLE,
		ratio FLOAT,
		is_active BOOLEAN,
		created_at TIMESTAMP
	);`

	table, err := parseCreateTableDDL(ddl)
	if err != nil {
		t.Fatalf("parseCreateTableDDL failed: %v", err)
	}
	if table.Name != "test_all_types" || len(table.Columns) != 8 {
		t.Fatalf("unexpected parsed table: %+v", table)
	}
	if table.Columns[0].Type != schema.ColBlob || table.Columns[0].Nullable {
		t.Errorf("col 0 mismatch: %+v", table.Columns[0])
	}
	if table.Columns[1].Type != schema.ColText || !table.Columns[1].Nullable {
		t.Errorf("col 1 mismatch: %+v", table.Columns[1])
	}
	if table.Columns[2].Type != schema.ColInteger {
		t.Errorf("col 2 mismatch: %+v", table.Columns[2])
	}

	// Invalid syntax statements
	_, err = parseCreateTableDDL("CREATE TABLE invalid_no_paren;")
	if err == nil {
		t.Errorf("expected error for invalid CREATE TABLE syntax, got nil")
	}

	// Split statements
	sqlScript := `
		-- This is a comment
		CREATE TABLE a (id BLOB PRIMARY KEY);
		/* Multiline comment */
		INSERT INTO a (id) VALUES (x'0102030405060708090a0b0c0d0e0f10');
		SELECT * FROM a WHERE name = 'semi;colon';
	`
	stmts := splitStatements(sqlScript)
	if len(stmts) != 3 {
		t.Errorf("expected 3 statements, got %d: %v", len(stmts), stmts)
	}
}

func TestQueryCommand_LocalAndRemote(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "murmur_query_test_*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "db")
	initCmd := &InitCommand{}
	var stdout, stderr bytes.Buffer
	_ = initCmd.Run(context.Background(), GlobalOptions{}, []string{dataDir}, &stdout, &stderr)

	ctx := context.Background()
	queryCmd := &QueryCommand{}

	// Missing args
	err = queryCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr)
	if err == nil {
		t.Errorf("expected error for missing query args")
	}

	// Create table and insert
	stdout.Reset()
	err = queryCmd.Run(ctx, GlobalOptions{}, []string{dataDir, "CREATE TABLE users (id BLOB PRIMARY KEY, name TEXT);"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("create table query failed: %v", err)
	}

	stdout.Reset()
	err = queryCmd.Run(ctx, GlobalOptions{}, []string{dataDir, "INSERT INTO users (id, name) VALUES (x'0102030405060708090a0b0c0d0e0f10', 'Alice');"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("insert query failed: %v", err)
	}

	// Query with Table output
	stdout.Reset()
	err = queryCmd.Run(ctx, GlobalOptions{}, []string{dataDir, "SELECT name FROM users;"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "Alice") {
		t.Errorf("select query table output mismatch: %s", stdout.String())
	}

	// Query with Markdown output
	stdout.Reset()
	err = queryCmd.Run(ctx, GlobalOptions{Markdown: true}, []string{dataDir, "SELECT name FROM users;"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "| Alice |") {
		t.Errorf("select query markdown output mismatch: %s", stdout.String())
	}

	// Query with JSON output
	stdout.Reset()
	err = queryCmd.Run(ctx, GlobalOptions{JSON: true}, []string{dataDir, "SELECT name FROM users;"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), `"name": "Alice"`) {
		t.Errorf("select query JSON output mismatch: %s", stdout.String())
	}

	// Query with --file
	sqlFile := filepath.Join(tempDir, "query.sql")
	_ = os.WriteFile(sqlFile, []byte("SELECT count(*) AS total FROM users;"), 0o600)
	stdout.Reset()
	err = queryCmd.Run(ctx, GlobalOptions{JSON: true}, []string{dataDir, "--file=" + sqlFile}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), `"total":`) {
		t.Errorf("query with file failed: %v, out: %s", err, stdout.String())
	}

	// Remote query mock
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"columns": []string{"msg"},
			"rows":    [][]string{{"remote hello"}},
		})
	})
	mux.HandleFunc("/v1/exec", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"rows_affected": 1,
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	stdout.Reset()
	err = queryCmd.Run(ctx, GlobalOptions{JSON: true}, []string{ts.URL, "SELECT msg"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "remote hello") {
		t.Errorf("remote query failed: %v, out: %s", err, stdout.String())
	}
}

func TestImportAndExportCommands(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "murmur_io_test_*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "db")
	_ = (&InitCommand{}).Run(context.Background(), GlobalOptions{}, []string{dataDir}, io.Discard, io.Discard)
	_ = (&QueryCommand{}).Run(context.Background(), GlobalOptions{}, []string{dataDir, "CREATE TABLE logs (id BLOB PRIMARY KEY, level TEXT, msg TEXT);"}, io.Discard, io.Discard)

	ctx := context.Background()
	importCmd := &ImportCommand{}
	exportCmd := &ExportCommand{}

	// Missing args
	var stdout, stderr bytes.Buffer
	if err := importCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected import error on missing args")
	}
	if err := exportCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected export error on missing args")
	}

	// Import CSV with custom delimiter and batching
	csvFile := filepath.Join(tempDir, "data.csv")
	csvContent := "id;level;msg\n01010101010101010101010101010101;INFO;started\n02020202020202020202020202020202;WARN;check\n"
	_ = os.WriteFile(csvFile, []byte(csvContent), 0o600)

	stdout.Reset()
	err = importCmd.Run(ctx, GlobalOptions{JSON: true}, []string{
		dataDir,
		"--table=logs",
		"--delimiter=;",
		"--batch=1",
		csvFile,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if !strings.Contains(stdout.String(), `"imported": 2`) {
		t.Errorf("unexpected import JSON: %s", stdout.String())
	}

	// Export JSON
	stdout.Reset()
	err = exportCmd.Run(ctx, GlobalOptions{}, []string{dataDir, "--table=logs", "--format=json"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "started") {
		t.Errorf("export JSON failed: %v, out: %s", err, stdout.String())
	}

	// Export SQL
	stdout.Reset()
	err = exportCmd.Run(ctx, GlobalOptions{}, []string{dataDir, "--table=logs", "--format=sql"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "INSERT INTO logs") {
		t.Errorf("export SQL failed: %v, out: %s", err, stdout.String())
	}

	// Export to file
	outFile := filepath.Join(tempDir, "out.csv")
	err = exportCmd.Run(ctx, GlobalOptions{}, []string{dataDir, "--table=logs", "--format=csv", "--output=" + outFile}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("export to file failed: %v", err)
	}
	outData, _ := os.ReadFile(outFile)
	if !strings.Contains(string(outData), "started") {
		t.Errorf("exported file missing rows: %s", string(outData))
	}

	// Remote import mock
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/exec", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"rows_affected": 1})
	})
	mux.HandleFunc("/v1/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"columns": []string{"id", "level"},
			"rows":    [][]string{{"0101", "INFO"}},
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	stdout.Reset()
	err = importCmd.Run(ctx, GlobalOptions{JSON: true}, []string{ts.URL, "--table=logs", "--delimiter=;", csvFile}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("remote import failed: %v", err)
	}

	stdout.Reset()
	err = exportCmd.Run(ctx, GlobalOptions{JSON: true}, []string{ts.URL, "--table=logs", "--format=json"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "INFO") {
		t.Errorf("remote export failed: %v, out: %s", err, stdout.String())
	}
}

func TestInspectVerifyRepairDoctor(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "murmur_diag_test_*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "db")
	_ = (&InitCommand{}).Run(context.Background(), GlobalOptions{}, []string{dataDir}, io.Discard, io.Discard)
	_ = (&QueryCommand{}).Run(context.Background(), GlobalOptions{}, []string{dataDir, "CREATE TABLE items (id BLOB PRIMARY KEY, title TEXT);"}, io.Discard, io.Discard)

	ctx := context.Background()
	var stdout, stderr bytes.Buffer

	// 1. Inspect
	inspectCmd := &InspectCommand{}
	if err := inspectCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error for missing inspect args")
	}
	stdout.Reset()
	if err := inspectCmd.Run(ctx, GlobalOptions{}, []string{dataDir}, &stdout, &stderr); err != nil {
		t.Fatalf("inspect text failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "Storage Inspection") {
		t.Errorf("unexpected inspect text output: %s", stdout.String())
	}

	// 2. Verify
	verifyCmd := &VerifyCommand{}
	if err := verifyCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error for missing verify args")
	}
	stdout.Reset()
	if err := verifyCmd.Run(ctx, GlobalOptions{JSON: true}, []string{dataDir, "--full"}, &stdout, &stderr); err != nil {
		t.Fatalf("verify JSON failed: %v", err)
	}
	if !strings.Contains(stdout.String(), `"valid": true`) {
		t.Errorf("unexpected verify JSON: %s", stdout.String())
	}

	// 3. Repair (check only and force)
	repairCmd := &RepairCommand{}
	if err := repairCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error for missing repair args")
	}
	stdout.Reset()
	if err := repairCmd.Run(ctx, GlobalOptions{}, []string{dataDir}, &stdout, &stderr); err != nil {
		t.Fatalf("repair check failed: %v", err)
	}
	stdout.Reset()
	if err := repairCmd.Run(ctx, GlobalOptions{JSON: true}, []string{dataDir, "--force"}, &stdout, &stderr); err != nil {
		t.Fatalf("repair force failed: %v", err)
	}
	if !strings.Contains(stdout.String(), `"sqlite_rebuilt": true`) {
		t.Errorf("unexpected repair JSON: %s", stdout.String())
	}

	// 4. Schema
	schemaCmd := &SchemaCommand{}
	if err := schemaCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error for missing schema args")
	}
	stdout.Reset()
	if err := schemaCmd.Run(ctx, GlobalOptions{Markdown: true}, []string{dataDir}, &stdout, &stderr); err != nil {
		t.Fatalf("schema markdown failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "items") {
		t.Errorf("schema markdown missing items table: %s", stdout.String())
	}

	// 5. Keys
	keysCmd := &KeysCommand{}
	if err := keysCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error for missing keys args")
	}
	stdout.Reset()
	if err := keysCmd.Run(ctx, GlobalOptions{Markdown: true}, []string{dataDir}, &stdout, &stderr); err != nil {
		t.Fatalf("keys markdown failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "Key Registry Metadata") {
		t.Errorf("keys output missing metadata: %s", stdout.String())
	}

	// 6. Doctor
	doctorCmd := &DoctorCommand{}
	if err := doctorCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error for missing doctor args")
	}
	stdout.Reset()
	if err := doctorCmd.Run(ctx, GlobalOptions{}, []string{dataDir}, &stdout, &stderr); err != nil {
		t.Fatalf("doctor text failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "Diagnostic Scorecard") {
		t.Errorf("unexpected doctor text: %s", stdout.String())
	}
}

func TestBenchAndClusterCommands(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer

	// 1. Bench
	benchCmd := &BenchCommand{}
	stdout.Reset()
	err := benchCmd.Run(ctx, GlobalOptions{JSON: true}, []string{"--operations=50"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("bench failed: %v", err)
	}
	if !strings.Contains(stdout.String(), `"sql_inserts_per_sec":`) {
		t.Errorf("bench JSON missing fields: %s", stdout.String())
	}

	// 2. Remote Cluster, Lag, GC with table output
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":         "healthy",
			"node_id":        "node-01",
			"uptime_seconds": 120,
			"memory_bytes":   5000000,
			"tombstones":     12,
			"lag": []map[string]any{
				{"peer_id": "node-02", "lag_bytes": 100, "lag_ops": 2},
			},
		})
	})
	mux.HandleFunc("/v1/debug/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"node_id": "node-01", "addr": "127.0.0.1:8080", "status": "alive"},
			{"node_id": "node-02", "addr": "127.0.0.1:8081", "status": "alive"},
		})
	})
	mux.HandleFunc("/v1/exec", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"rows_affected": 1,
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Cluster
	clusterCmd := &ClusterCommand{}
	stdout.Reset()
	if err := clusterCmd.Run(ctx, GlobalOptions{}, []string{ts.URL}, &stdout, &stderr); err != nil {
		t.Fatalf("cluster table failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "node-01") {
		t.Errorf("cluster output missing node: %s", stdout.String())
	}

	// Lag
	lagCmd := &LagCommand{}
	stdout.Reset()
	if err := lagCmd.Run(ctx, GlobalOptions{}, []string{ts.URL}, &stdout, &stderr); err != nil {
		t.Fatalf("lag table failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "node-02") {
		t.Errorf("lag output missing peer: %s", stdout.String())
	}

	// GC
	gcCmd := &GCCommand{}
	stdout.Reset()
	if err := gcCmd.Run(ctx, GlobalOptions{}, []string{ts.URL, "--trigger"}, &stdout, &stderr); err != nil {
		t.Fatalf("gc trigger failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "triggered successfully") {
		t.Errorf("gc trigger output unexpected: %s", stdout.String())
	}
}

func TestShell_ScriptFileExecution(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "murmur_shell_script_*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "db")
	_ = (&InitCommand{}).Run(context.Background(), GlobalOptions{}, []string{dataDir}, io.Discard, io.Discard)

	scriptFile := filepath.Join(tempDir, "commands.sql")
	scriptContent := "CREATE TABLE test_script (id BLOB PRIMARY KEY, val TEXT);\nINSERT INTO test_script VALUES (x'0102030405060708090a0b0c0d0e0f10', 'from_script');\n"
	_ = os.WriteFile(scriptFile, []byte(scriptContent), 0o600)

	input := fmt.Sprintf(".read %s\nSELECT val FROM test_script;\n.quit\n", scriptFile)
	shellCmd := &ShellCommand{In: strings.NewReader(input)}

	var stdout, stderr bytes.Buffer
	err = shellCmd.Run(context.Background(), GlobalOptions{}, []string{dataDir}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("shell .read failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "from_script") {
		t.Errorf("shell missing .read output: %s", stdout.String())
	}
}

func TestDoctor_ComprehensiveBranches(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	doctorCmd := &DoctorCommand{}

	// 1. Remote doctor healthy
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "ready"})
	})
	mux.HandleFunc("/v1/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"rows": [][]string{{"1"}}})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	stdout.Reset()
	err := doctorCmd.Run(ctx, GlobalOptions{JSON: true}, []string{ts.URL}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "HEALTHY") {
		t.Errorf("remote doctor healthy failed: %v, out: %s", err, stdout.String())
	}

	// 2. Remote doctor failure
	tsFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer tsFail.Close()

	stdout.Reset()
	err = doctorCmd.Run(ctx, GlobalOptions{JSON: true}, []string{tsFail.URL}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "CRITICAL") {
		t.Errorf("remote doctor fail failed: %v, out: %s", err, stdout.String())
	}

	// 3. Local doctor with missing directory
	stdout.Reset()
	err = doctorCmd.Run(ctx, GlobalOptions{JSON: true}, []string{"/nonexistent/path/to/db"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "CRITICAL") {
		t.Errorf("local doctor missing dir failed: %v, out: %s", err, stdout.String())
	}

	// 4. Local doctor with a regular file instead of a directory
	tmpFile, _ := os.CreateTemp("", "doc_file_*")
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	stdout.Reset()
	err = doctorCmd.Run(ctx, GlobalOptions{JSON: true}, []string{tmpFile.Name()}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "CRITICAL") {
		t.Errorf("local doctor file instead of dir failed: %v, out: %s", err, stdout.String())
	}

	// 5. Local doctor with pending restore intent
	tempDir, _ := os.MkdirTemp("", "doc_intent_*")
	defer os.RemoveAll(tempDir)
	_ = (&InitCommand{}).Run(ctx, GlobalOptions{}, []string{tempDir}, io.Discard, io.Discard)
	_ = os.WriteFile(filepath.Join(tempDir, "RESTORE_INTENT"), []byte("pending"), 0o600)

	stdout.Reset()
	err = doctorCmd.Run(ctx, GlobalOptions{Markdown: true}, []string{tempDir}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "DEGRADED") {
		t.Errorf("local doctor restore intent failed: %v, out: %s", err, stdout.String())
	}
}

func TestInit_SeedAndErrors(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "init_seed_*")
	defer os.RemoveAll(tempDir)

	ctx := context.Background()
	initCmd := &InitCommand{}
	var stdout, stderr bytes.Buffer

	// Seed with valid SQL
	seedFile := filepath.Join(tempDir, "seed.sql")
	_ = os.WriteFile(seedFile, []byte("CREATE TABLE seed_tbl (id BLOB PRIMARY KEY, v TEXT);"), 0o600)

	dbDir := filepath.Join(tempDir, "seeded_db")
	stdout.Reset()
	err := initCmd.Run(ctx, GlobalOptions{JSON: true}, []string{dbDir, "--seed=" + seedFile}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), `"seed_loaded": true`) {
		t.Errorf("init with valid seed failed: %v, out: %s", err, stdout.String())
	}

	// Seed with invalid path
	stdout.Reset()
	err = initCmd.Run(ctx, GlobalOptions{}, []string{filepath.Join(tempDir, "bad_db"), "--seed=/nonexistent/seed.sql"}, &stdout, &stderr)
	if err == nil {
		t.Errorf("expected error for nonexistent seed file, got nil")
	}
}

func TestSchema_RemoteAndFiltered(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	schemaCmd := &SchemaCommand{}

	// Remote schema mock
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		q := req["query"]
		if strings.Contains(q, "PRAGMA table_info") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"columns": []string{"cid", "name", "type", "notnull", "dflt_value", "pk"},
				"rows":    [][]string{{"0", "id", "BLOB", "1", "", "1"}, {"1", "name", "TEXT", "0", "", "0"}},
			})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"columns": []string{"name", "sql"},
				"rows":    [][]string{{"remote_users", "CREATE TABLE remote_users (id BLOB PRIMARY KEY, name TEXT)"}},
			})
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Remote schema table output
	stdout.Reset()
	err := schemaCmd.Run(ctx, GlobalOptions{}, []string{ts.URL, "--table=remote_users"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "remote_users") {
		t.Errorf("remote schema table output failed: %v, out: %s", err, stdout.String())
	}

	// Local schema with specific table filter
	tempDir, _ := os.MkdirTemp("", "schema_filter_*")
	defer os.RemoveAll(tempDir)
	_ = (&InitCommand{}).Run(ctx, GlobalOptions{}, []string{tempDir}, io.Discard, io.Discard)
	_ = (&QueryCommand{}).Run(ctx, GlobalOptions{}, []string{tempDir, "CREATE TABLE custom_tbl (id BLOB PRIMARY KEY, title TEXT);"}, io.Discard, io.Discard)

	stdout.Reset()
	err = schemaCmd.Run(ctx, GlobalOptions{}, []string{tempDir, "--table=custom_tbl"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "custom_tbl") {
		t.Errorf("local schema filtered output failed: %v, out: %s", err, stdout.String())
	}
}

func TestKeys_ErrorsAndModes(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	keysCmd := &KeysCommand{}

	// Nonexistent path
	err := keysCmd.Run(ctx, GlobalOptions{}, []string{"/nonexistent/dir/keys"}, &stdout, &stderr)
	if err == nil {
		t.Errorf("expected error for nonexistent keys dir")
	}

	// Truncated KEYREGISTRY
	tempDir, _ := os.MkdirTemp("", "keys_trunc_*")
	defer os.RemoveAll(tempDir)
	_ = os.MkdirAll(filepath.Join(tempDir, "keys"), 0o700)
	_ = os.WriteFile(filepath.Join(tempDir, "keys", "KEYREGISTRY"), []byte("short"), 0o600)

	err = keysCmd.Run(ctx, GlobalOptions{}, []string{tempDir}, &stdout, &stderr)
	if err == nil {
		t.Errorf("expected error for truncated KEYREGISTRY")
	}

	// Valid init and test with wrong passphrase
	validDir := filepath.Join(tempDir, "valid_db")
	_ = (&InitCommand{}).Run(ctx, GlobalOptions{Passphrase: "correct-pass"}, []string{validDir}, io.Discard, io.Discard)

	stdout.Reset()
	err = keysCmd.Run(ctx, GlobalOptions{Passphrase: "wrong-pass", JSON: true}, []string{validDir}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("keys command failed: %v", err)
	}
}

func TestVerify_ErrorsAndDeepCheck(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	verifyCmd := &VerifyCommand{}

	// Missing dir
	_ = verifyCmd.Run(ctx, GlobalOptions{}, []string{"/nonexistent/dir"}, &stdout, &stderr)
	if !strings.Contains(stderr.String(), "FAIL") {
		t.Errorf("expected FAIL in stderr for nonexistent verify dir: %s", stderr.String())
	}

	// Valid db full verify in table mode
	tempDir, _ := os.MkdirTemp("", "verify_deep_*")
	defer os.RemoveAll(tempDir)
	_ = (&InitCommand{}).Run(ctx, GlobalOptions{}, []string{tempDir}, io.Discard, io.Discard)

	stdout.Reset()
	err := verifyCmd.Run(ctx, GlobalOptions{Markdown: true}, []string{tempDir, "--full"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "PASS") {
		t.Errorf("deep verify markdown failed: %v, out: %s", err, stdout.String())
	}
}

func TestClusterAndStatus_Comprehensive(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	statusCmd := &StatusCommand{}
	clusterCmd := &ClusterCommand{}

	// 1. Missing args
	if err := statusCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error on status without args")
	}
	if err := clusterCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error on cluster without args")
	}

	// 2. Server with full peer and status endpoints
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/admin/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"node_id": "n1", "state": "leader", "peer_count": 2})
	})
	mux.HandleFunc("/v1/debug/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"node_id": "n2", "addrs": "127.0.0.1:9001", "connected": true, "schema_agreed": true, "state": "alive"},
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Status command in Table mode
	stdout.Reset()
	err := statusCmd.Run(ctx, GlobalOptions{}, []string{ts.URL}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "Murmur Node Status") {
		t.Errorf("status table failed: %v, out: %s", err, stdout.String())
	}

	// Cluster command in Table and Markdown mode
	stdout.Reset()
	err = clusterCmd.Run(ctx, GlobalOptions{Markdown: true}, []string{ts.URL}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "Cluster Membership (1 peers)") {
		t.Errorf("cluster table failed: %v, out: %s", err, stdout.String())
	}

	// 3. Fallback when /v1/debug/peers is missing
	muxFallback := http.NewServeMux()
	muxFallback.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"member_count": 3, "peer_list": "n1,n2,n3"})
	})
	tsFallback := httptest.NewServer(muxFallback)
	defer tsFallback.Close()

	// Fallback table mode
	stdout.Reset()
	err = clusterCmd.Run(ctx, GlobalOptions{}, []string{tsFallback.URL}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "peer_list") {
		t.Errorf("cluster fallback table failed: %v, out: %s", err, stdout.String())
	}

	// Fallback JSON mode
	stdout.Reset()
	err = clusterCmd.Run(ctx, GlobalOptions{JSON: true}, []string{tsFallback.URL}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), `"member_count": 3`) {
		t.Errorf("cluster fallback json failed: %v, out: %s", err, stdout.String())
	}

	// 4. Server failures
	tsFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server dead", http.StatusInternalServerError)
	}))
	defer tsFail.Close()

	if err := statusCmd.Run(ctx, GlobalOptions{}, []string{tsFail.URL}, &stdout, &stderr); err == nil {
		t.Errorf("expected error on status failure")
	}
	if err := clusterCmd.Run(ctx, GlobalOptions{}, []string{tsFail.URL}, &stdout, &stderr); err == nil {
		t.Errorf("expected error on cluster failure")
	}
}

func TestGCCommand_Comprehensive(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	gcCmd := &GCCommand{}

	// Missing args
	if err := gcCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error on gc missing args")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/exec", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"rows_affected": 0})
	})
	mux.HandleFunc("/v1/admin/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"gc_watermark": 1000,
			"retention_epoch": 5,
			"other_stat": "abc",
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// 1. Trigger GC in text mode
	stdout.Reset()
	err := gcCmd.Run(ctx, GlobalOptions{}, []string{ts.URL, "--trigger"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "triggered successfully") {
		t.Errorf("gc trigger failed: %v, out: %s", err, stdout.String())
	}

	// 2. Trigger GC in JSON mode
	stdout.Reset()
	err = gcCmd.Run(ctx, GlobalOptions{JSON: true}, []string{ts.URL, "--trigger"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), `"triggered": true`) {
		t.Errorf("gc trigger json failed: %v, out: %s", err, stdout.String())
	}

	// 3. Inspect GC in table mode (matching gc keys)
	stdout.Reset()
	err = gcCmd.Run(ctx, GlobalOptions{}, []string{ts.URL}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "gc_watermark") {
		t.Errorf("gc status table failed: %v, out: %s", err, stdout.String())
	}

	// 4. Inspect GC without gc keys in status (fallback to all keys)
	muxNoGC := http.NewServeMux()
	muxNoGC.HandleFunc("/v1/admin/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"storage_bytes": 4096})
	})
	tsNoGC := httptest.NewServer(muxNoGC)
	defer tsNoGC.Close()

	stdout.Reset()
	err = gcCmd.Run(ctx, GlobalOptions{}, []string{tsNoGC.URL}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "storage_bytes") {
		t.Errorf("gc status no gc keys failed: %v, out: %s", err, stdout.String())
	}

	// 5. Server error on trigger
	muxErr := http.NewServeMux()
	muxErr.HandleFunc("/v1/exec", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "exec failed", http.StatusBadRequest)
	})
	tsErr := httptest.NewServer(muxErr)
	defer tsErr.Close()

	if err := gcCmd.Run(ctx, GlobalOptions{}, []string{tsErr.URL, "--trigger"}, &stdout, &stderr); err == nil {
		t.Errorf("expected error on gc trigger failure")
	}
}

func TestRepairCommand_Comprehensive(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	repairCmd := &RepairCommand{}

	// Missing args
	if err := repairCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error on repair missing args")
	}

	tempDir, _ := os.MkdirTemp("", "repair_test_*")
	defer os.RemoveAll(tempDir)

	dbDir := filepath.Join(tempDir, "db")
	_ = (&InitCommand{}).Run(ctx, GlobalOptions{}, []string{dbDir}, io.Discard, io.Discard)

	// Create a dummy RESTORE_INTENT
	_ = os.WriteFile(filepath.Join(dbDir, "RESTORE_INTENT"), []byte("pending"), 0o600)

	// 1. Dry run mode in JSON
	stdout.Reset()
	err := repairCmd.Run(ctx, GlobalOptions{JSON: true}, []string{dbDir, "--dry-run", "--clear-intent"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), `"message": "Dry run`) {
		t.Errorf("dry run json failed: %v, out: %s", err, stdout.String())
	}
	if _, err := os.Stat(filepath.Join(dbDir, "RESTORE_INTENT")); err != nil {
		t.Errorf("RESTORE_INTENT should not be deleted during dry-run")
	}

	// 2. Real repair with --clear-intent and --rebuild-sqlite in table mode
	stdout.Reset()
	err = repairCmd.Run(ctx, GlobalOptions{}, []string{dbDir, "--clear-intent", "--rebuild-sqlite"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "REPAIRED") {
		t.Errorf("repair failed: %v, out: %s", err, stdout.String())
	}
	if _, err := os.Stat(filepath.Join(dbDir, "RESTORE_INTENT")); !os.IsNotExist(err) {
		t.Errorf("RESTORE_INTENT should have been removed")
	}
}

func TestShellCommand_InteractiveAndRemote(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer

	// Missing args
	shellCmd := &ShellCommand{}
	if err := shellCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error on shell missing args")
	}

	// 1. Remote Shell
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"node_id": "remote-node-1"})
	})
	mux.HandleFunc("/v1/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		q := req["query"]
		if strings.Contains(q, "sqlite_master") && strings.Contains(q, "table") {
			_ = json.NewEncoder(w).Encode(client.QueryResult{Columns: []string{"name"}, Rows: [][]string{{"items"}}})
		} else if strings.Contains(q, "sqlite_master") {
			_ = json.NewEncoder(w).Encode(client.QueryResult{Columns: []string{"sql"}, Rows: [][]string{{"CREATE TABLE items (id INT);"}}})
		} else {
			_ = json.NewEncoder(w).Encode(client.QueryResult{Columns: []string{"col1"}, Rows: [][]string{{"val1"}}})
		}
	})
	mux.HandleFunc("/v1/exec", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(client.ExecResult{RowsAffected: 2})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	remoteScript := strings.Join([]string{
		".help",
		".mode markdown",
		".headers off",
		".timer on",
		"SELECT 1;",
		"INSERT INTO t VALUES (1);",
		".tables",
		".tables item",
		".schema",
		".schema items",
		".status",
		".mode csv",
		"SELECT 1;",
		".mode json",
		"SELECT 1;",
		".mode table",
		".headers on",
		".headers",
		".timer",
		".mode",
		".mode invalid_mode",
		".unknown_cmd",
		".read",
		".read /nonexistent/file.sql",
		".dump items",
		".exit",
	}, "\n") + "\n"

	remoteShell := &ShellCommand{In: strings.NewReader(remoteScript)}
	stdout.Reset()
	stderr.Reset()
	err := remoteShell.Run(ctx, GlobalOptions{}, []string{ts.URL}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("remote shell run failed: %v", err)
	}

	// 2. Local shorthand execution via Execute
	tempDir, _ := os.MkdirTemp("", "shell_exec_*")
	defer os.RemoveAll(tempDir)
	_ = (&InitCommand{}).Run(ctx, GlobalOptions{}, []string{tempDir}, io.Discard, io.Discard)

	// Test Execute with dataDir arg invoking shell (with immediate .quit)
	origStdin := os.Stdin
	rPipe, wPipe, _ := os.Pipe()
	os.Stdin = rPipe
	_, _ = wPipe.WriteString(".quit\n")
	_ = wPipe.Close()

	stdout.Reset()
	code := Execute([]string{tempDir}, &stdout, io.Discard)
	os.Stdin = origStdin
	if code != 0 {
		t.Errorf("Execute shorthand failed with code %d", code)
	}
}

func TestBackup_InfoAndErrors(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	backupCmd := &BackupCommand{}

	// Unknown subcommand
	if err := backupCmd.Run(ctx, GlobalOptions{}, []string{"unknown-sub"}, &stdout, &stderr); err == nil {
		t.Errorf("expected error on unknown backup subcommand")
	}

	// Missing args for info, verify, create, restore
	if err := backupCmd.Run(ctx, GlobalOptions{}, []string{"info"}, &stdout, &stderr); err == nil {
		t.Errorf("expected error on info missing args")
	}
	if err := backupCmd.Run(ctx, GlobalOptions{}, []string{"verify"}, &stdout, &stderr); err == nil {
		t.Errorf("expected error on verify missing args")
	}
	if err := backupCmd.Run(ctx, GlobalOptions{}, []string{"create"}, &stdout, &stderr); err == nil {
		t.Errorf("expected error on create missing args")
	}
	if err := backupCmd.Run(ctx, GlobalOptions{}, []string{"restore"}, &stdout, &stderr); err == nil {
		t.Errorf("expected error on restore missing args")
	}

	// Create a valid backup archive and test runInfo in table mode
	tempDir, _ := os.MkdirTemp("", "backup_table_*")
	defer os.RemoveAll(tempDir)
	dbDir := filepath.Join(tempDir, "db")
	_ = (&InitCommand{}).Run(ctx, GlobalOptions{}, []string{dbDir}, io.Discard, io.Discard)
	archivePath := filepath.Join(tempDir, "backup.tar.gz")

	_ = backupCmd.Run(ctx, GlobalOptions{}, []string{"create", dbDir, archivePath}, io.Discard, io.Discard)

	stdout.Reset()
	err := backupCmd.Run(ctx, GlobalOptions{}, []string{"info", archivePath}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "Murmur Backup Archive Info") {
		t.Errorf("backup info table mode failed: %v, out: %s", err, stdout.String())
	}
}

func TestQuery_TimingAndFormats(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	queryCmd := &QueryCommand{}

	tempDir, _ := os.MkdirTemp("", "query_formats_*")
	defer os.RemoveAll(tempDir)
	dbDir := filepath.Join(tempDir, "db")
	_ = (&InitCommand{}).Run(ctx, GlobalOptions{}, []string{dbDir}, io.Discard, io.Discard)

	// Create table
	_ = queryCmd.Run(ctx, GlobalOptions{}, []string{dbDir, "CREATE TABLE items (id BLOB PRIMARY KEY, title TEXT);"}, io.Discard, io.Discard)

	// Insert with timing
	stdout.Reset()
	err := queryCmd.Run(ctx, GlobalOptions{}, []string{dbDir, "INSERT INTO items (id, title) VALUES (x'0102030405060708090a0b0c0d0e0f12', 'Gadget');", "--timing"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "Query OK") {
		t.Errorf("query insert with timing failed: %v, out: %s", err, stdout.String())
	}

	// Select with CSV format
	stdout.Reset()
	err = queryCmd.Run(ctx, GlobalOptions{}, []string{dbDir, "SELECT title FROM items;", "--format=csv"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "Gadget") {
		t.Errorf("query select csv failed: %v, out: %s", err, stdout.String())
	}

	// Update mutation
	stdout.Reset()
	err = queryCmd.Run(ctx, GlobalOptions{}, []string{dbDir, "UPDATE items SET title = 'SuperGadget' WHERE title = 'Gadget';", "--timing"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "Query OK") {
		t.Errorf("query update failed: %v, out: %s", err, stdout.String())
	}
}

