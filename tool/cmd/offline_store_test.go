package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInspectReadsManifestWithoutLegacySchemaConfig(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "db")
	if err := (&InitCommand{}).Run(context.Background(), GlobalOptions{}, []string{dataDir}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatalf("initialize fixture: %v", err)
	}
	bindTypedInitSchema(t, dataDir)
	// The runtime opener treats schema.json as the legacy configuration.
	// A direct metadata inspection must use the durable manifest instead.
	wrongSchema := `{"version":99,"tables":[{"name":"not_the_durable_schema","columns":[{"name":"id","type":"blob"}]}]}`
	if err := os.WriteFile(filepath.Join(dataDir, "schema.json"), []byte(wrongSchema), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	if err := (&InspectCommand{}).Run(context.Background(), GlobalOptions{}, []string{dataDir}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("inspect should read Spool and the durable manifest directly: %v", err)
	}
	if !strings.Contains(stdout.String(), "Schema Epoch") || !strings.Contains(stdout.String(), "User Tables") {
		t.Fatalf("inspection omitted manifest metadata: %s", stdout.String())
	}
	stdout.Reset()
	if err := (&SchemaCommand{}).Run(context.Background(), GlobalOptions{}, []string{dataDir}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("schema should read the durable manifest directly: %v", err)
	}
	if !strings.Contains(stdout.String(), "items") || strings.Contains(stdout.String(), "not_the_durable_schema") {
		t.Fatalf("schema output did not come from the durable manifest: %s", stdout.String())
	}
	stdout.Reset()
	if err := (&VerifyCommand{}).Run(context.Background(), GlobalOptions{JSON: true}, []string{dataDir}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("verify should validate durable state without loading legacy schema: %v", err)
	}
	if !strings.Contains(stdout.String(), `"valid": true`) || !strings.Contains(stdout.String(), `"materializer_checked": false`) {
		t.Fatalf("verification did not report durable-only scope: %s", stdout.String())
	}
	stdout.Reset()
	if err := (&DoctorCommand{}).Run(context.Background(), GlobalOptions{JSON: true}, []string{dataDir}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("doctor should verify durable state without loading legacy schema: %v", err)
	}
	if !strings.Contains(stdout.String(), `"name": "Durable Spool Integrity"`) ||
		!strings.Contains(stdout.String(), `"name": "Materializer Check"`) ||
		!strings.Contains(stdout.String(), `"overall": "DEGRADED"`) {
		t.Fatalf("doctor did not report durable-only verification and the unchecked materializer: %s", stdout.String())
	}
	stdout.Reset()
	if err := (&KeysCommand{}).Run(context.Background(), GlobalOptions{JSON: true}, []string{dataDir}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("keys should inspect the Spool keyring without loading legacy schema: %v", err)
	}
	if !strings.Contains(stdout.String(), `"unlocked": true`) || !strings.Contains(stdout.String(), `"key_count": 1`) {
		t.Fatalf("key inspection did not report the unlocked Spool key inventory: %s", stdout.String())
	}
	archivePath := filepath.Join(t.TempDir(), "direct-spool-backup.tar.gz")
	if err := (&BackupCommand{}).Run(context.Background(), GlobalOptions{}, []string{"create", dataDir, archivePath}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatalf("backup should checkpoint Spool without loading legacy schema: %v", err)
	}
	if _, err := os.Stat(archivePath); err != nil {
		t.Fatalf("direct Spool backup archive missing: %v", err)
	}
	restoreDir := filepath.Join(t.TempDir(), "restored")
	if err := (&BackupCommand{}).Run(context.Background(), GlobalOptions{}, []string{"restore", archivePath, restoreDir}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatalf("restore Spool backup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "schema.json")); !os.IsNotExist(err) {
		t.Fatalf("Spool backup restore must not recreate a legacy schema file (stat err=%v)", err)
	}
}
