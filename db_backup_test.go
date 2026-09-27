package replicateddb

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/backup"
	"github.com/nomadsql/replicateddb/crypto"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/schema"
)

func testEncryptionProvider(key []byte) crypto.KeyProvider {
	return &crypto.MapProvider{
		Keys:      map[string][]byte{"k1": append([]byte(nil), key...)},
		CurrentID: "k1",
	}
}

func testBackupSchemaConfig() SchemaConfig {
	return SchemaConfig{
		Version: 1,
		Tables: []schema.TableSchema{
			{
				Name: "users",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "name", Type: schema.ColText, Nullable: true},
					{Name: "email", Type: schema.ColText, Nullable: true},
					{Name: "balance", Type: schema.ColInteger, Nullable: true},
				},
			},
		},
	}
}

func TestDBOnlineBackupAndRestoreIntegration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "primary_db")
	backupDir := filepath.Join(dir, "backups")

	masterKey := bytes.Repeat([]byte{0x42}, 32)
	provider := testEncryptionProvider(masterKey)

	cfg := Config{
		Path:       dbDir,
		NodeID:     NewNodeID(),
		DBID:       NewDBID(),
		Schema:     testBackupSchemaConfig(),
		Pebble:     DefaultPebbleConfig(),
		Encryption: EncryptionConfig{KeyID: "k1", Provider: provider},
	}

	// 1. Open primary database
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open primary DB failed: %v", err)
	}

	// 2. Insert initial rows
	const initialRows = 200
	var sampleID ids.RowID
	for i := 0; i < initialRows; i++ {
		rowID := ids.NewRowID()
		if i == 42 {
			sampleID = rowID
		}
		name := fmt.Sprintf("Alice %d", i)
		email := fmt.Sprintf("alice%d@example.com", i)
		balance := int64(1000 + i)
		_, err := db.ExecContext(ctx, "INSERT INTO users (id, name, email, balance) VALUES (?, ?, ?, ?)", rowID[:], name, email, balance)
		if err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	// 3. Run online backup concurrently with active writes
	dest, err := backup.NewLocalDestination(backupDir)
	if err != nil {
		t.Fatalf("NewLocalDestination: %v", err)
	}

	var writeWg sync.WaitGroup
	writeDone := make(chan struct{})
	var concurrentWriteErrors []error
	var mu sync.Mutex

	// Concurrent write worker
	writeWg.Add(1)
	go func() {
		defer writeWg.Done()
		for i := initialRows; i < initialRows+100; i++ {
			select {
			case <-writeDone:
				return
			default:
				rowID := ids.NewRowID()
				name := fmt.Sprintf("Concurrent %d", i)
				email := fmt.Sprintf("conc%d@example.com", i)
				_, err := db.ExecContext(ctx, "INSERT INTO users (id, name, email, balance) VALUES (?, ?, ?, ?)", rowID[:], name, email, int64(i))
				if err != nil {
					mu.Lock()
					concurrentWriteErrors = append(concurrentWriteErrors, err)
					mu.Unlock()
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Trigger online zero-downtime backup
	backupStart := time.Now()
	meta, err := db.Backup(ctx, backup.Config{
		Destination: dest,
		Compression: "gzip",
	})
	backupDuration := time.Since(backupStart)

	close(writeDone)
	writeWg.Wait()

	if err != nil {
		t.Fatalf("Online backup failed: %v", err)
	}
	if len(concurrentWriteErrors) > 0 {
		t.Fatalf("Concurrent writes encountered errors during backup: %v", concurrentWriteErrors[0])
	}

	t.Logf("Online backup succeeded in %v (files: %d, bytes: %d)", backupDuration, meta.DataFilesCount, meta.TotalBytes)

	// Close primary database
	if err := db.Close(); err != nil {
		t.Fatalf("Close primary DB failed: %v", err)
	}

	// 4. Verify backup ciphertext: raw archive must NOT contain plaintext marker
	backups, err := dest.ListBackups(ctx, cfg.DBID.String())
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}
	if len(backups) == 0 {
		t.Fatal("no backups found in backup dir")
	}

	archivePath := filepath.Join(backupDir, backups[0].Name)
	rawArchiveBytes, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read archive failed: %v", err)
	}
	// Alice 0 should not be unencrypted in raw archive
	if bytes.Contains(rawArchiveBytes, []byte("Alice 0")) {
		t.Fatal("archive contains unencrypted plaintext user data")
	}

	// 5. Restore into secondary directory
	restoredDir := filepath.Join(dir, "restored_db")
	restoredKeysDir := filepath.Join(restoredDir, "keys")

	restoreMeta, err := Restore(ctx, backup.RestoreConfig{
		Source:       dest,
		BackupName:   backups[0].Name,
		TargetPath:   restoredDir,
		KeysPath:     restoredKeysDir,
		ExpectedDBID: cfg.DBID.String(),
	})
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
	if restoreMeta.BackupID != meta.BackupID {
		t.Errorf("restored backup ID %s != %s", restoreMeta.BackupID, meta.BackupID)
	}

	// 6. Open restored database with original master key
	restoredProvider := testEncryptionProvider(masterKey)
	restoredCfg := Config{
		Path:       restoredDir,
		NodeID:     cfg.NodeID,
		DBID:       cfg.DBID,
		Schema:     testBackupSchemaConfig(),
		Pebble:     DefaultPebbleConfig(),
		Encryption: EncryptionConfig{KeyID: "k1", Provider: restoredProvider},
	}

	restoredDB, err := Open(ctx, restoredCfg)
	if err != nil {
		t.Fatalf("Open restored DB failed: %v", err)
	}
	defer restoredDB.Close()

	// 7. Verify restored database contents
	rows, err := restoredDB.QueryContext(ctx, "SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query restored DB failed: %v", err)
	}
	var count int
	if rows.Next() {
		_ = rows.Scan(&count)
	}
	rows.Close()

	// Restored count should be at least initialRows
	if count < initialRows {
		t.Fatalf("restored count %d < initial rows %d", count, initialRows)
	}
	t.Logf("Restored database successfully opened with %d verified rows!", count)

	// Verify specific row
	rowQuery, err := restoredDB.QueryContext(ctx, "SELECT email, balance FROM users WHERE id = ?", sampleID[:])
	if err != nil {
		t.Fatalf("Verify row in restored DB failed: %v", err)
	}
	var email string
	var balance int64
	if rowQuery.Next() {
		_ = rowQuery.Scan(&email, &balance)
	} else {
		t.Fatalf("sample row %x not found in restored DB", sampleID)
	}
	rowQuery.Close()

	if email != "alice42@example.com" || balance != 1042 {
		t.Errorf("unexpected user data: email=%s, balance=%d", email, balance)
	}
}

func TestDBRestoreWrongKeyFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "primary_db")
	backupDir := filepath.Join(dir, "backups")

	masterKey := bytes.Repeat([]byte{0x77}, 32)
	wrongKey := bytes.Repeat([]byte{0x88}, 32)

	cfg := Config{
		Path:       dbDir,
		NodeID:     NewNodeID(),
		DBID:       NewDBID(),
		Schema:     testBackupSchemaConfig(),
		Pebble:     DefaultPebbleConfig(),
		Encryption: EncryptionConfig{KeyID: "k1", Provider: testEncryptionProvider(masterKey)},
	}

	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	rowID := ids.NewRowID()
	_, _ = db.ExecContext(ctx, "INSERT INTO users (id, name, email, balance) VALUES (?, 'User 1', 'u1@ex.com', 100)", rowID[:])
	dest, _ := backup.NewLocalDestination(backupDir)

	meta, err := db.Backup(ctx, backup.Config{Destination: dest})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}
	_ = db.Close()

	restoredDir := filepath.Join(dir, "restored_db")
	_, err = Restore(ctx, backup.RestoreConfig{
		Source:       dest,
		BackupName:   fmt.Sprintf("nomadsql-backup-%s-%s-%s.tar.gz", meta.DBID, meta.CreatedAt.Format("20060102-150405"), meta.BackupID[:8]),
		TargetPath:   restoredDir,
		ExpectedDBID: cfg.DBID.String(),
	})
	if err != nil {
		t.Fatalf("Restore unpack failed: %v", err)
	}

	// Attempt to open with wrong key must fail closed with auth error
	wrongCfg := Config{
		Path:       restoredDir,
		NodeID:     cfg.NodeID,
		DBID:       cfg.DBID,
		Schema:     testBackupSchemaConfig(),
		Pebble:     DefaultPebbleConfig(),
		Encryption: EncryptionConfig{KeyID: "k1", Provider: testEncryptionProvider(wrongKey)},
	}

	_, err = Open(ctx, wrongCfg)
	if err == nil {
		t.Fatal("expected Open with wrong key to fail, but it succeeded!")
	}
	t.Logf("Open with wrong key failed as expected: %v", err)
}
