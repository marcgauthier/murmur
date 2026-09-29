package replicateddb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/spedsql/backup"
	"github.com/marcgauthier/spedsql/crypto"
	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/schema"
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

	// 5. Restore into secondary directory under a fresh writer identity
	restoredDir := filepath.Join(dir, "restored_db")
	restoredKeysDir := filepath.Join(restoredDir, "keys")
	freshNode := NewNodeID()

	restoreMeta, err := Restore(ctx, backup.RestoreConfig{
		Source:       dest,
		BackupName:   backups[0].Name,
		TargetPath:   restoredDir,
		KeysPath:     restoredKeysDir,
		ExpectedDBID: cfg.DBID.String(),
		FreshNodeID:  freshNode.String(),
	})
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
	if restoreMeta.BackupID != meta.BackupID {
		t.Errorf("restored backup ID %s != %s", restoreMeta.BackupID, meta.BackupID)
	}

	// 6. Open restored database with original master key and fresh identity
	restoredProvider := testEncryptionProvider(masterKey)
	restoredCfg := Config{
		Path:       restoredDir,
		NodeID:     freshNode,
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

	// The durable marker records the adoption; the fresh writer starts at
	// sequence zero while history is preserved.
	marker, ok, err := restoredDB.store.RestoreMarker()
	if err != nil || !ok {
		t.Fatalf("RestoreMarker = %+v, %v, %v", marker, ok, err)
	}
	if marker.SourceNodeID != cfg.NodeID.String() || marker.FreshNodeID != freshNode.String() {
		t.Fatalf("bad marker: %+v", marker)
	}
	if st := restoredDB.Status(); st.LocalSeq != 0 {
		t.Fatalf("restored LocalSeq = %d, want 0", st.LocalSeq)
	}

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
	freshNode := NewNodeID()
	_, err = Restore(ctx, backup.RestoreConfig{
		Source:       dest,
		BackupName:   fmt.Sprintf("nomadsql-backup-%s-%s-%s.tar.gz", meta.DBID, meta.CreatedAt.Format("20060102-150405"), meta.BackupID[:8]),
		TargetPath:   restoredDir,
		ExpectedDBID: cfg.DBID.String(),
		FreshNodeID:  freshNode.String(),
	})
	if err != nil {
		t.Fatalf("Restore unpack failed: %v", err)
	}

	// Attempt to open with wrong key must fail closed with auth error.
	// The fresh identity is used so the failure exercised is key auth,
	// not the restore-identity gate.
	wrongCfg := Config{
		Path:       restoredDir,
		NodeID:     freshNode,
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

func TestRestoreCloneIdentityEnforcement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "primary_db")

	cfg := testConfig(dbDir)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	rowA := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, rowA[:], "ann"); err != nil {
		t.Fatal(err)
	}
	peerX := NewNodeID()
	if _, err := db.store.EnsureMemberAdmitted(peerX, 1000, 5000); err != nil {
		t.Fatal(err)
	}
	if err := db.store.SetPeerExcluded(peerX, true); err != nil {
		t.Fatal(err)
	}
	dest, err := backup.NewLocalDestination(filepath.Join(dir, "backups"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Backup(ctx, backup.Config{Destination: dest}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	restoredDir := filepath.Join(dir, "restored_db")
	freshNode := NewNodeID()
	if _, err := Restore(ctx, backup.RestoreConfig{
		Source:      dest,
		TargetPath:  restoredDir,
		FreshNodeID: freshNode.String(),
	}); err != nil {
		t.Fatal(err)
	}
	intentPath := filepath.Join(restoredDir, backup.RestoreIntentFileName)
	if _, err := os.Stat(intentPath); err != nil {
		t.Fatalf("intent missing after restore: %v", err)
	}

	openAs := func(node NodeID) (*DB, error) {
		rcfg := testConfig(restoredDir)
		rcfg.NodeID = node
		return Open(ctx, rcfg)
	}
	// Same-identity rollback and wrong-identity opens are rejected.
	if rdb, err := openAs(cfg.NodeID); err == nil {
		_ = rdb.Close()
		t.Fatal("open restored data as original node succeeded")
	} else if !errors.Is(err, ErrRestoreIdentity) {
		t.Fatalf("rollback err = %v, want ErrRestoreIdentity", err)
	}
	if rdb, err := openAs(NewNodeID()); err == nil {
		_ = rdb.Close()
		t.Fatal("open restored data as third node succeeded")
	} else if !errors.Is(err, ErrRestoreIdentity) {
		t.Fatalf("wrong-identity err = %v, want ErrRestoreIdentity", err)
	}

	// The fresh identity adopts: history visible, sequence reset, intent
	// cleared, marker durable, inherited membership/exclusions cleared.
	rdb, err := openAs(freshNode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(intentPath); !os.IsNotExist(err) {
		_ = rdb.Close()
		t.Fatalf("intent not cleared: %v", err)
	}
	rows := queryAll(t, rdb, `SELECT id, name FROM contacts`)
	if len(rows) != 1 {
		_ = rdb.Close()
		t.Fatalf("want 1 restored row, got %d", len(rows))
	}
	if st := rdb.Status(); st.LocalSeq != 0 {
		_ = rdb.Close()
		t.Fatalf("LocalSeq = %d, want 0", st.LocalSeq)
	}
	if mbs, err := rdb.store.ListMembers(); err != nil || len(mbs) != 0 {
		_ = rdb.Close()
		t.Fatalf("ListMembers = %+v, %v; want cleared after restore", mbs, err)
	}
	if ex, err := rdb.store.ListExcludedPeers(); err != nil || len(ex) != 0 {
		_ = rdb.Close()
		t.Fatalf("ListExcludedPeers = %+v, %v; want cleared after restore", ex, err)
	}
	marker, ok, err := rdb.store.RestoreMarker()
	if err != nil || !ok || marker.SourceNodeID != cfg.NodeID.String() {
		_ = rdb.Close()
		t.Fatalf("marker = %+v, %v, %v", marker, ok, err)
	}
	rowB := NewRowID()
	if _, err := rdb.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, rowB[:], "bob"); err != nil {
		_ = rdb.Close()
		t.Fatal(err)
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}

	// Ordinary restart as the fresh node (no intent left behind).
	rdb, err = openAs(freshNode)
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	rows = queryAll(t, rdb, `SELECT name FROM contacts ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows after restart, got %d", len(rows))
	}
	if st := rdb.Status(); st.LocalSeq != 1 {
		t.Fatalf("LocalSeq = %d, want 1", st.LocalSeq)
	}
}

func TestReseedFlowRejectsOldCluster(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	nodeA, nodeC := NewNodeID(), NewNodeID()
	oldDB, newDB := NewDBID(), NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	// Node A runs the old cluster and takes the baseline backup.
	dbA, err := Open(ctx, replConfig(filepath.Join(dir, "node_a"), nodeA, oldDB, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	rowOld := NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, rowOld[:], "old"); err != nil {
		t.Fatal(err)
	}
	dest, err := backup.NewLocalDestination(filepath.Join(dir, "backups"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dbA.Backup(ctx, backup.Config{Destination: dest}); err != nil {
		t.Fatal(err)
	}
	if err := dbA.Close(); err != nil {
		t.Fatal(err)
	}

	// Coordinated reseed of the baseline under a fresh identity + new DBID.
	restoredDir := filepath.Join(dir, "node_c")
	if _, err := Restore(ctx, backup.RestoreConfig{
		Source:      dest,
		TargetPath:  restoredDir,
		Mode:        backup.RestoreReseed,
		FreshNodeID: nodeC.String(),
		NewDBID:     newDB.String(),
	}); err != nil {
		t.Fatal(err)
	}
	cfgC := replConfig(restoredDir, nodeC, newDB, creds[nodeC], nil)
	dbC, err := Open(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()
	if got := dbC.Status().DBID; got != newDB {
		t.Fatalf("reseeded DBID = %s, want %s", got, newDB)
	}
	marker, ok, err := dbC.store.RestoreMarker()
	if err != nil || !ok || marker.Mode != "reseed" {
		t.Fatalf("marker = %+v, %v, %v", marker, ok, err)
	}
	rows := queryAll(t, dbC, `SELECT name FROM contacts`)
	if len(rows) != 1 {
		t.Fatalf("want baseline row on C, got %d", len(rows))
	}

	// Reopen A on the old DBID and point the clusters at each other.
	dbA, err = Open(ctx, replConfig(filepath.Join(dir, "node_a"), nodeA, oldDB, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	addrC := waitForAddr(t, dbC, 5*time.Second)
	if err := dbA.AddPeer(ctx, Peer{NodeID: nodeC, Addrs: []string{addrC}}); err != nil {
		t.Fatal(err)
	}
	if err := dbC.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{addrA}}); err != nil {
		t.Fatal(err)
	}
	rowNew := NewRowID()
	if _, err := dbC.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, rowNew[:], "new"); err != nil {
		t.Fatal(err)
	}

	// Same CA, valid certificates — but the DBID differs, so no traffic
	// may flow in either direction and no session may establish.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n := dbA.Status().ConnectedPeers + dbC.Status().ConnectedPeers; n != 0 {
			t.Fatalf("old and reseeded clusters connected (%d sessions)", n)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := queryAll(t, dbA, `SELECT name FROM contacts WHERE name = 'new'`); len(got) != 0 {
		t.Fatal("reseeded row leaked into the old cluster")
	}
	if got := queryAll(t, dbC, `SELECT name FROM contacts WHERE name = 'old' AND id != ?`, rowOld[:]); len(got) != 0 {
		t.Fatal("unexpected duplicate of old row on C")
	}
	if got := queryAll(t, dbC, `SELECT name FROM contacts`); len(got) != 2 {
		t.Fatalf("want 2 rows on C (baseline + new), got %d", len(got))
	}
}

// TestReseedAbortedRebindRetries proves a rebind that dies before doing
// any work leaves a source-bound store that the next Open converges, and
// that the reseeded store reopens stably afterwards.
func TestReseedAbortedRebindRetries(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	nodeA, nodeC := NewNodeID(), NewNodeID()
	oldDB, newDB := NewDBID(), NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	dbA, err := Open(ctx, replConfig(filepath.Join(dir, "node_a"), nodeA, oldDB, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	rowOld := NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, rowOld[:], "old"); err != nil {
		t.Fatal(err)
	}
	dest, err := backup.NewLocalDestination(filepath.Join(dir, "backups"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dbA.Backup(ctx, backup.Config{Destination: dest}); err != nil {
		t.Fatal(err)
	}
	if err := dbA.Close(); err != nil {
		t.Fatal(err)
	}
	restoredDir := filepath.Join(dir, "node_c")
	if _, err := Restore(ctx, backup.RestoreConfig{
		Source:      dest,
		TargetPath:  restoredDir,
		Mode:        backup.RestoreReseed,
		FreshNodeID: nodeC.String(),
		NewDBID:     newDB.String(),
	}); err != nil {
		t.Fatal(err)
	}
	prov := &crypto.MapProvider{
		Keys:      map[string][]byte{testKeyID: testKey},
		CurrentID: testKeyID, Algorithm: crypto.DefaultAlgorithm,
	}
	var source, target [16]byte
	copy(source[:], oldDB[:])
	copy(target[:], newDB[:])
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := crypto.RebindStore(cancelled, crypto.RebindOptions{
		RegDir:   filepath.Join(restoredDir, "keys"),
		Roots:    []string{filepath.Join(restoredDir, "data")},
		Provider: prov, SourceDBID: source, NewDBID: target,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("aborted rebind err = %v, want context.Canceled", err)
	}
	// Nothing moved: the store still opens as source, not as new.
	srcReg, err := crypto.OpenRegistry(filepath.Join(restoredDir, "keys"), prov, source)
	if err != nil {
		t.Fatalf("aborted store lost source binding: %v", err)
	}
	srcReg.Close()
	if reg, err := crypto.OpenRegistry(filepath.Join(restoredDir, "keys"), prov, target); err == nil {
		reg.Close()
		t.Fatal("aborted store opens under new DBID")
	}
	// Normal open converges the rebind; data and identity hold.
	cfgC := replConfig(restoredDir, nodeC, newDB, creds[nodeC], nil)
	dbC, err := Open(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	if got := dbC.Status().DBID; got != newDB {
		t.Fatalf("reseeded DBID = %s, want %s", got, newDB)
	}
	if rows := queryAll(t, dbC, `SELECT name FROM contacts`); len(rows) != 1 {
		t.Fatalf("want baseline row, got %d", len(rows))
	}
	if err := dbC.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen is an ordinary stable open (intent consumed).
	dbC, err = Open(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()
	if got := dbC.Status().DBID; got != newDB {
		t.Fatalf("reopened DBID = %s, want %s", got, newDB)
	}
	if rows := queryAll(t, dbC, `SELECT name FROM contacts`); len(rows) != 1 {
		t.Fatalf("want baseline row after reopen, got %d", len(rows))
	}
}

func TestReplicationCertMustMatchNode(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	// Certificate issued to B but configured as A: Open must fail before
	// any replication starts.
	cfg := replConfig(t.TempDir(), nodeA, NewDBID(), creds[nodeB], nil)
	db, err := Open(ctx, cfg)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open with mismatched certificate succeeded")
	}
	if !errors.Is(err, ErrLocalIdentityMismatch) {
		t.Fatalf("err = %v, want ErrLocalIdentityMismatch", err)
	}
}
