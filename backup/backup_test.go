package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockDB implements SourceDB for testing.
type mockDB struct {
	mu          sync.Mutex
	dataDir     string
	filesDir    string
	dbID        string
	nodeID      string
	schemaEpoch uint64
	schemaVer   uint64
	schemaHash  string

	checkpointed bool
}

func (m *mockDB) HoldCommits() func() {
	return func() {}
}

func (m *mockDB) Checkpoint(_ context.Context, stagingDataDir string) (func() error, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpointed = true

	if err := os.MkdirAll(stagingDataDir, 0o700); err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(stagingDataDir, "manifest.enc")
	if err := os.WriteFile(manifestPath, []byte("spool-manifest-bytes"), 0o600); err != nil {
		return nil, err
	}
	keysPath := filepath.Join(stagingDataDir, "keys.enc")
	if err := os.WriteFile(keysPath, []byte("spool-keys-bytes"), 0o600); err != nil {
		return nil, err
	}
	for i := 1; i <= 3; i++ {
		segPath := filepath.Join(stagingDataDir, fmt.Sprintf("%06d.seg", i))
		content := []byte(fmt.Sprintf("spool-segment-data-%d", i))
		if err := os.WriteFile(segPath, content, 0o600); err != nil {
			return nil, err
		}
	}
	return func() error { return nil }, nil
}

func (m *mockDB) ClusterID() string   { return m.dbID }
func (m *mockDB) LocalNodeID() string { return m.nodeID }
func (m *mockDB) SchemaInfo() (uint64, uint64, string) {
	return m.schemaEpoch, m.schemaVer, m.schemaHash
}
func (m *mockDB) DataDir() string { return m.dataDir }

func (m *mockDB) FilesDir() string { return m.filesDir }

// testFreshNodeID is a valid UUID distinct from setupMockDB's nodeID.
const testFreshNodeID = "123e4567-e89b-12d3-a456-426614174000"

func setupMockDB(t *testing.T) *mockDB {
	t.Helper()
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	_ = os.MkdirAll(dataDir, 0o700)

	return &mockDB{
		dataDir:     dataDir,
		dbID:        "db-test-12345",
		nodeID:      "node-test-67890",
		schemaEpoch: 3,
		schemaVer:   4,
		schemaHash:  "deadbeefcafe",
	}
}

func TestLocalBackupAndRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := setupMockDB(t)

	backupDir := filepath.Join(t.TempDir(), "backups")
	localDest, err := NewLocalDestination(backupDir)
	if err != nil {
		t.Fatalf("NewLocalDestination: %v", err)
	}

	// 1. Create Backup
	meta, err := CreateBackup(ctx, db, Config{
		Destination: localDest,
		Compression: "gzip",
	})
	if err != nil {
		t.Fatalf("CreateBackup failed: %v", err)
	}

	if meta.DBID != db.dbID {
		t.Errorf("meta.DBID = %s, want %s", meta.DBID, db.dbID)
	}
	if meta.DataFilesCount != 5 { // 3 Segments + 1 manifest.enc + 1 keys.enc
		t.Errorf("meta.DataFilesCount = %d, want 5", meta.DataFilesCount)
	}

	// Verify files in backup directory
	backups, err := localDest.ListBackups(ctx, db.dbID)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}
	if len(backups) != 1 {
		t.Fatalf("expected 1 backup, got %d", len(backups))
	}

	// 2. Validate Ciphertext (No plaintext leaks in archive)
	archivePath := filepath.Join(backupDir, backups[0].Name)
	rawArchive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if bytes.Contains(rawArchive, []byte("unencrypted-secret")) {
		t.Fatal("archive contains plaintext secrets")
	}

	// 3. Restore to target directory
	targetDir := filepath.Join(t.TempDir(), "restored")

	restoredMeta, err := Restore(ctx, RestoreConfig{
		Source:       localDest,
		BackupName:   backups[0].Name,
		TargetPath:   targetDir,
		ExpectedDBID: db.dbID,
		FreshNodeID:  testFreshNodeID,
	})
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	if restoredMeta.BackupID != meta.BackupID {
		t.Errorf("restored backupID = %s, want %s", restoredMeta.BackupID, meta.BackupID)
	}

	// Verify restored Spool files
	restoredKeys, err := os.ReadFile(filepath.Join(targetDir, "data", "keys.enc"))
	if err != nil {
		t.Fatalf("restored keys.enc missing: %v", err)
	}
	if string(restoredKeys) != "spool-keys-bytes" {
		t.Errorf("unexpected keys.enc content: %q", string(restoredKeys))
	}

	restoredManifest, err := os.ReadFile(filepath.Join(targetDir, "data", "manifest.enc"))
	if err != nil {
		t.Fatalf("restored manifest.enc missing: %v", err)
	}
	if string(restoredManifest) != "spool-manifest-bytes" {
		t.Errorf("unexpected manifest.enc content: %q", string(restoredManifest))
	}

	seg1, err := os.ReadFile(filepath.Join(targetDir, "data", "000001.seg"))
	if err != nil {
		t.Fatalf("restored 000001.seg missing: %v", err)
	}
	if string(seg1) != "spool-segment-data-1" {
		t.Errorf("unexpected 000001.seg content: %q", string(seg1))
	}

	// 4. Test non-empty directory restore protection
	_, err = Restore(ctx, RestoreConfig{
		Source:      localDest,
		BackupName:  backups[0].Name,
		TargetPath:  targetDir,
		Overwrite:   false,
		FreshNodeID: testFreshNodeID,
	})
	if !errors.Is(err, ErrRestoreTargetNotEmpty) {
		t.Fatalf("expected ErrRestoreTargetNotEmpty, got %v", err)
	}

	// 5. Test overwrite success
	_, err = Restore(ctx, RestoreConfig{
		Source:      localDest,
		BackupName:  backups[0].Name,
		TargetPath:  targetDir,
		Overwrite:   true,
		FreshNodeID: testFreshNodeID,
	})
	if err != nil {
		t.Fatalf("Restore with Overwrite=true failed: %v", err)
	}
}

func TestHTTPSBackupAndRestore(t *testing.T) {
	ctx := context.Background()
	db := setupMockDB(t)

	// Mock HTTPS server
	storage := make(map[string][]byte)
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		filename := parts[len(parts)-1]

		switch r.Method {
		case http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			storage[filename] = data
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			if r.URL.Path == "/" || r.URL.Path == "" {
				// List backups
				var items []BackupInfo
				for name, d := range storage {
					items = append(items, BackupInfo{
						Name:      name,
						DBID:      db.dbID,
						CreatedAt: time.Now(),
						SizeBytes: int64(len(d)),
					})
				}
				_ = json.NewEncoder(w).Encode(items)
				return
			}
			data, ok := storage[filename]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/gzip")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)

		case http.MethodDelete:
			delete(storage, filename)
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	dest, err := NewHTTPSDestination(HTTPSOptions{
		BaseURL: srv.URL,
	})
	if err != nil {
		t.Fatalf("NewHTTPSDestination: %v", err)
	}

	// 1. Create backup over HTTPS PUT stream
	meta, err := CreateBackup(ctx, db, Config{
		Destination: dest,
	})
	if err != nil {
		t.Fatalf("CreateBackup over HTTPS failed: %v", err)
	}

	mu.Lock()
	count := len(storage)
	mu.Unlock()
	if count != 1 {
		t.Fatalf("expected 1 file stored on mock server, got %d", count)
	}

	// 2. Restore from HTTPS GET stream
	targetDir := filepath.Join(t.TempDir(), "https-restored")
	restoredMeta, err := Restore(ctx, RestoreConfig{
		Source:      dest,
		TargetPath:  targetDir,
		FreshNodeID: testFreshNodeID,
	})
	if err != nil {
		t.Fatalf("Restore from HTTPS failed: %v", err)
	}
	if restoredMeta.BackupID != meta.BackupID {
		t.Errorf("restored ID = %s, want %s", restoredMeta.BackupID, meta.BackupID)
	}
}

func TestBackupWorkerRetention(t *testing.T) {
	ctx := context.Background()
	db := setupMockDB(t)

	backupDir := filepath.Join(t.TempDir(), "worker-backups")
	dest, _ := NewLocalDestination(backupDir)

	worker := NewWorker(ScheduleConfig{
		Enabled:       true,
		Interval:      time.Hour,
		Destination:   dest,
		RetentionDays: 0,
		MaxBackups:    2, // Retain only latest 2 backups
	}, db)

	// Trigger 4 consecutive on-demand backups
	for i := 0; i < 4; i++ {
		_, err := worker.TriggerOnDemand(ctx)
		if err != nil {
			t.Fatalf("TriggerOnDemand %d failed: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	backups, err := dest.ListBackups(ctx, db.dbID)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}

	// MaxBackups was 2, so only 2 should remain
	if len(backups) != 2 {
		t.Fatalf("expected 2 backups retained, got %d", len(backups))
	}
}

func TestArchiveStreamStructure(t *testing.T) {
	ctx := context.Background()
	db := setupMockDB(t)

	backupDir := filepath.Join(t.TempDir(), "archive-test")
	dest, _ := NewLocalDestination(backupDir)

	meta, err := CreateBackup(ctx, db, Config{Destination: dest})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}

	backups, _ := dest.ListBackups(ctx, db.dbID)
	f, err := os.Open(filepath.Join(backupDir, backups[0].Name))
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	foundMeta := false
	foundKeys := false
	foundManifest := false
	var segCount int

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}

		if hdr.Name == "backup-metadata.json" {
			foundMeta = true
			var m Metadata
			if err := json.NewDecoder(tr).Decode(&m); err != nil {
				t.Fatalf("decode meta: %v", err)
			}
			if m.BackupID != meta.BackupID {
				t.Errorf("meta mismatch: %s != %s", m.BackupID, meta.BackupID)
			}
		} else if hdr.Name == "data/keys.enc" {
			foundKeys = true
		} else if hdr.Name == "data/manifest.enc" {
			foundManifest = true
		} else if strings.HasPrefix(hdr.Name, "data/") && strings.HasSuffix(hdr.Name, ".seg") {
			segCount++
		}
	}

	if !foundMeta {
		t.Error("backup-metadata.json not found in tar archive")
	}
	if !foundKeys {
		t.Error("data/keys.enc not found in tar archive")
	}
	if !foundManifest {
		t.Error("data/manifest.enc not found in tar archive")
	}
	if segCount != 3 {
		t.Errorf("expected 3 segments, got %d", segCount)
	}
}
