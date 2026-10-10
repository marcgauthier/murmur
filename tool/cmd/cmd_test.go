package cmd

import (
	"bytes"
	"context"
	"crypto/ed25519"
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

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/schema"
)

type initBindingRecord struct {
	ID    murmur.RowID `rime:"primary"`
	Value string
}

func bindTypedInitSchema(t *testing.T, dataDir string) {
	t.Helper()
	nodeBytes, err := os.ReadFile(filepath.Join(dataDir, "node.id"))
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := ids.ParseNodeID(strings.TrimSpace(string(nodeBytes)))
	if err != nil {
		t.Fatal(err)
	}
	privateKeyBytes, err := os.ReadFile(filepath.Join(dataDir, "origin.key"))
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.PrivateKey(privateKeyBytes)
	registry, err := origin.NewKeyRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Add(nodeID, privateKey.Public().(ed25519.PublicKey)); err != nil {
		t.Fatal(err)
	}
	definition, err := murmur.Model[initBindingRecord](murmur.ModelOptions{
		Name: "items", TableID: 1,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Value": 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := murmur.Open(context.Background(), murmur.Config{
		Path:   dataDir,
		NodeID: nodeID,
		Schema: murmur.SchemaConfig{Version: 1},
		Tables: []murmur.TableDefinition{definition},
		Spool:  murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{
			Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "cli-key",
		},
		OriginSigning: murmur.OriginSigningConfig{PrivateKey: privateKey, TrustedKeys: registry},
	})
	if err != nil {
		t.Fatalf("first application open: %v", err)
	}
	if _, err := db.Count(context.Background(), initBindingRecord{}); err != nil {
		t.Fatalf("bind model table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}
	store, err := openOfflineStore(dataDir, GlobalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manifest, err := store.LoadSchemaManifest()
	if err != nil || manifest == nil || len(manifest.Tables) != 1 || manifest.Tables[0].Name != "items" {
		t.Fatalf("first typed open did not persist the Go schema: manifest=%+v err=%v", manifest, err)
	}
}

func TestInitStoreBindsFirstTypedSchema(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "db")
	if err := (&InitCommand{}).Run(context.Background(), GlobalOptions{}, []string{dataDir}, io.Discard, io.Discard); err != nil {
		t.Fatalf("initialize Spool: %v", err)
	}
	bindTypedInitSchema(t, dataDir)
}

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

func TestLegacyCommandsRemoved(t *testing.T) {
	for _, name := range []string{"shell", "query", "exec", "import", "export", "dump"} {
		if command := Lookup(name); command != nil {
			t.Errorf("legacy command %q is still registered: %T", name, command)
		}
	}
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	if code := Execute([]string{dir}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("data-directory shorthand must not dispatch to a shell: code=%d stderr=%s", code, stderr.String())
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
	if _, err := os.Stat(filepath.Join(target1, "schema.json")); !os.IsNotExist(err) {
		t.Errorf("native init must not create a legacy schema file (stat err=%v)", err)
	}
	initializedStore, err := openOfflineStore(target1, GlobalOptions{})
	if err != nil {
		t.Fatalf("open initialized Spool: %v", err)
	}
	manifest, err := initializedStore.LoadSchemaManifest()
	if err != nil {
		t.Fatalf("load initial schema manifest: %v", err)
	}
	if manifest != nil {
		t.Fatalf("fresh init should leave Go schema binding to the application, got manifest %+v", manifest)
	}
	if err := initializedStore.Close(); err != nil {
		t.Fatalf("close initialized Spool: %v", err)
	}
	stdout.Reset()
	if err := (&VerifyCommand{}).Run(ctx, GlobalOptions{JSON: true}, []string{target1}, &stdout, &stderr); err != nil ||
		!strings.Contains(stdout.String(), `"valid": true`) || !strings.Contains(stdout.String(), `Schema is not bound yet`) {
		t.Fatalf("verify should accept a valid unbound Spool and explain schema state: err=%v output=%s", err, stdout.String())
	}
	stdout.Reset()
	if err := (&DoctorCommand{}).Run(ctx, GlobalOptions{JSON: true}, []string{target1}, &stdout, &stderr); err != nil ||
		!strings.Contains(stdout.String(), `"name": "Schema Registration"`) || !strings.Contains(stdout.String(), `"overall": "DEGRADED"`) {
		t.Fatalf("doctor should report schema registration pending for a fresh Spool: err=%v output=%s", err, stdout.String())
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

func TestInspectVerifyRepairDoctor(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "murmur_diag_test_*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "db")
	_ = (&InitCommand{}).Run(context.Background(), GlobalOptions{}, []string{dataDir}, io.Discard, io.Discard)
	bindTypedInitSchema(t, dataDir)

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

	// 3. Repair validates Spool directly and refuses to claim a CLI rebuild.
	repairCmd := &RepairCommand{}
	if err := repairCmd.Run(ctx, GlobalOptions{}, nil, &stdout, &stderr); err == nil {
		t.Errorf("expected error for missing repair args")
	}
	stdout.Reset()
	if err := repairCmd.Run(ctx, GlobalOptions{}, []string{dataDir}, &stdout, &stderr); err != nil {
		t.Fatalf("repair check failed: %v", err)
	}
	stdout.Reset()
	if err := repairCmd.Run(ctx, GlobalOptions{JSON: true}, []string{dataDir}, &stdout, &stderr); err != nil {
		t.Fatalf("repair durable-state check failed: %v", err)
	}
	if !strings.Contains(stdout.String(), `"durable_state_checked": true`) || strings.Contains(stdout.String(), `"materializer_rebuilt": true`) {
		t.Errorf("unexpected repair JSON: %s", stdout.String())
	}
	if err := repairCmd.Run(ctx, GlobalOptions{}, []string{dataDir, "--rebuild-materializer"}, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "Go table definitions") {
		t.Errorf("repair must not claim a CLI materializer rebuild, got %v", err)
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
	err := benchCmd.Run(ctx, GlobalOptions{JSON: true}, []string{"--duration=1ms"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("bench failed: %v", err)
	}
	var benchMetrics map[string]float64
	if err := json.Unmarshal(stdout.Bytes(), &benchMetrics); err != nil {
		t.Fatalf("decode bench JSON: %v; output: %s", err, stdout.String())
	}
	if benchMetrics["rime_record_writes_per_sec"] <= 0 || benchMetrics["rime_record_reads_per_sec"] <= 0 {
		t.Errorf("bench did not measure typed RIME operations: %#v", benchMetrics)
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
	mux.HandleFunc("/v1/admin/gc", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
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

func TestInit_RejectsLegacySeed(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "init_seed_*")
	defer os.RemoveAll(tempDir)

	ctx := context.Background()
	initCmd := &InitCommand{}
	var stdout, stderr bytes.Buffer

	// Legacy schema-file seeding is removed; applications define schemas in Go.
	seedFile := filepath.Join(tempDir, "seed.sql")
	_ = os.WriteFile(seedFile, []byte("CREATE TABLE seed_tbl (id BLOB PRIMARY KEY, v TEXT);"), 0o600)

	dbDir := filepath.Join(tempDir, "seeded_db")
	stdout.Reset()
	err := initCmd.Run(ctx, GlobalOptions{JSON: true}, []string{dbDir, "--seed=" + seedFile}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "SQL seed files are no longer supported") {
		t.Errorf("expected seed rejection, got err=%v out=%s", err, stdout.String())
	}
	if _, statErr := os.Stat(dbDir); !os.IsNotExist(statErr) {
		t.Errorf("rejected seed should not create a database directory (stat err=%v)", statErr)
	}
}

func TestSchema_RemoteAndFiltered(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	schemaCmd := &SchemaCommand{}

	// Remote schema mock
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/schema", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]schema.TableSchema{{
			ID: 17, Name: "remote_users", PK: 1,
			Columns: []schema.ColumnSchema{
				{ID: 1, Name: "id", Type: schema.ColBlob},
				{ID: 2, Name: "name", Type: schema.ColText, Nullable: true},
			},
		}})
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
	bindTypedInitSchema(t, tempDir)

	stdout.Reset()
	err = schemaCmd.Run(ctx, GlobalOptions{}, []string{tempDir, "--table=items"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "items") {
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
	mux.HandleFunc("/v1/admin/gc", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v1/admin/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"gc_watermark":    1000,
			"retention_epoch": 5,
			"other_stat":      "abc",
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
	muxErr.HandleFunc("/v1/admin/gc", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gc failed", http.StatusBadRequest)
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

	// 2. Clear intent after validating direct Spool access.
	stdout.Reset()
	err = repairCmd.Run(ctx, GlobalOptions{}, []string{dbDir, "--clear-intent"}, &stdout, &stderr)
	if err != nil || !strings.Contains(stdout.String(), "REPAIRED") {
		t.Errorf("repair failed: %v, out: %s", err, stdout.String())
	}
	if _, err := os.Stat(filepath.Join(dbDir, "RESTORE_INTENT")); !os.IsNotExist(err) {
		t.Errorf("RESTORE_INTENT should have been removed")
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
