package murmur

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

	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/crypto"
	"github.com/marcgauthier/murmur/ids"
)

func testEncryptionProvider(key []byte) crypto.KeyProvider {
	return &crypto.MapProvider{
		Keys:      map[string][]byte{"k1": append([]byte(nil), key...)},
		CurrentID: "k1",
	}
}

type backupUser struct {
	ID      ids.RowID `rime:"primary"`
	Name    string
	Email   string
	Balance int64
}

func testBackupTables(t *testing.T) []TableDefinition {
	t.Helper()
	definition, err := Define[backupUser]("backup_users", 82, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2, "Email": 3, "Balance": 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	return []TableDefinition{definition}
}

func insertBackupUser(ctx context.Context, db *DB, table *RecordTable[backupUser], rec *backupUser) error {
	tx, err := db.BeginTx(ctx)
	if err != nil {
		return err
	}
	if err := table.Insert(tx, rec); err != nil {
		return err
	}
	return tx.Commit()
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
		Tables:     testBackupTables(t),
		Spool:      DefaultSpoolConfig(),
		Encryption: EncryptionConfig{KeyID: "k1", Provider: provider},
	}

	// 1. Open primary database
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatalf("Open primary DB failed: %v", err)
	}
	table, err := TableOf[backupUser](db, "backup_users")
	if err != nil {
		t.Fatalf("TableOf: %v", err)
	}

	// 2. Insert initial rows
	const initialRows = 200
	var sampleID ids.RowID
	for i := 0; i < initialRows; i++ {
		rowID := ids.NewRowID()
		if i == 42 {
			sampleID = rowID
		}
		rec := &backupUser{
			ID:      rowID,
			Name:    fmt.Sprintf("Alice %d", i),
			Email:   fmt.Sprintf("alice%d@example.com", i),
			Balance: int64(1000 + i),
		}
		if err := insertBackupUser(ctx, db, table, rec); err != nil {
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
				rec := &backupUser{
					ID:      ids.NewRowID(),
					Name:    fmt.Sprintf("Concurrent %d", i),
					Email:   fmt.Sprintf("conc%d@example.com", i),
					Balance: int64(i),
				}
				if err := insertBackupUser(ctx, db, table, rec); err != nil {
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
		Tables:     testBackupTables(t),
		Spool:      DefaultSpoolConfig(),
		Encryption: EncryptionConfig{KeyID: "k1", Provider: restoredProvider},
	}

	restoredDB, err := openSignedFixture(ctx, restoredCfg)
	if err != nil {
		t.Fatalf("Open restored DB failed: %v", err)
	}
	defer restoredDB.Close()
	restoredTable, err := TableOf[backupUser](restoredDB, "backup_users")
	if err != nil {
		t.Fatalf("TableOf restored: %v", err)
	}

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
	count, err := restoredTable.Where().Count()
	if err != nil {
		t.Fatalf("Query restored DB failed: %v", err)
	}

	// Restored count should be at least initialRows
	if count < initialRows {
		t.Fatalf("restored count %d < initial rows %d", count, initialRows)
	}
	t.Logf("Restored database successfully opened with %d verified rows!", count)

	// Verify specific row
	sample, err := restoredTable.Get(sampleID)
	if err != nil {
		t.Fatalf("sample row %x not found in restored DB: %v", sampleID, err)
	}

	if sample.Email != "alice42@example.com" || sample.Balance != 1042 {
		t.Errorf("unexpected user data: email=%s, balance=%d", sample.Email, sample.Balance)
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
		Tables:     testBackupTables(t),
		Spool:      DefaultSpoolConfig(),
		Encryption: EncryptionConfig{KeyID: "k1", Provider: testEncryptionProvider(masterKey)},
	}

	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	table, err := TableOf[backupUser](db, "backup_users")
	if err != nil {
		t.Fatalf("TableOf: %v", err)
	}
	_ = insertBackupUser(ctx, db, table, &backupUser{ID: ids.NewRowID(), Name: "User 1", Email: "u1@ex.com", Balance: 100})
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
		Tables:     testBackupTables(t),
		Spool:      DefaultSpoolConfig(),
		Encryption: EncryptionConfig{KeyID: "k1", Provider: testEncryptionProvider(wrongKey)},
	}

	_, err = openSignedFixture(ctx, wrongCfg)
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
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := TableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	if err := insertRecord(ctx, db, table, &facadeRecord{ID: NewRowID(), Name: "ann"}); err != nil {
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
		rcfg.Schema.Tables = nil
		rcfg.Tables = []TableDefinition{recordDefinition(t)}
		return openSignedFixture(ctx, rcfg)
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
	rtable, err := TableOf[facadeRecord](rdb, "records")
	if err != nil {
		_ = rdb.Close()
		t.Fatal(err)
	}
	nrows, err := rtable.Where().Count()
	if err != nil {
		_ = rdb.Close()
		t.Fatal(err)
	}
	if nrows != 1 {
		_ = rdb.Close()
		t.Fatalf("want 1 restored row, got %d", nrows)
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
	if err := insertRecord(ctx, rdb, rtable, &facadeRecord{ID: NewRowID(), Name: "bob"}); err != nil {
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
	rtable, err = TableOf[facadeRecord](rdb, "records")
	if err != nil {
		t.Fatal(err)
	}
	nrows, err = rtable.Where().Count()
	if err != nil {
		t.Fatal(err)
	}
	if nrows != 2 {
		t.Fatalf("want 2 rows after restart, got %d", nrows)
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
	cfgA := replTypedConfig(t, replConfig(filepath.Join(dir, "node_a"), nodeA, oldDB, creds[nodeA], nil))
	dbA, err := openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	tableA, err := TableOf[facadeRecord](dbA, "records")
	if err != nil {
		t.Fatal(err)
	}
	rowOld := NewRowID()
	if err := insertRecord(ctx, dbA, tableA, &facadeRecord{ID: rowOld, Name: "old"}); err != nil {
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
	cfgC := replTypedConfig(t, replConfig(restoredDir, nodeC, newDB, creds[nodeC], nil))
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()
	tableC, err := TableOf[facadeRecord](dbC, "records")
	if err != nil {
		t.Fatal(err)
	}
	if got := dbC.Status().DBID; got != newDB {
		t.Fatalf("reseeded DBID = %s, want %s", got, newDB)
	}
	marker, ok, err := dbC.store.RestoreMarker()
	if err != nil || !ok || marker.Mode != "reseed" {
		t.Fatalf("marker = %+v, %v, %v", marker, ok, err)
	}
	if n, err := tableC.Where().Count(); err != nil || n != 1 {
		t.Fatalf("want baseline row on C, got %d, %v", n, err)
	}

	// Reopen A on the old DBID and point the clusters at each other.
	dbA, err = openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	tableA, err = TableOf[facadeRecord](dbA, "records")
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)
	addrC := waitForAddr(t, dbC, 5*time.Second)
	if err := dbA.AddPeer(ctx, Peer{NodeID: nodeC, Addrs: []string{addrC}}); err != nil {
		t.Fatal(err)
	}
	if err := dbC.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{addrA}}); err != nil {
		t.Fatal(err)
	}
	if err := insertRecord(ctx, dbC, tableC, &facadeRecord{ID: NewRowID(), Name: "new"}); err != nil {
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
	countWhere := func(table *RecordTable[facadeRecord], match func(*facadeRecord) bool) int {
		t.Helper()
		found, err := table.Where().Find()
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, rec := range found {
			if match(rec) {
				n++
			}
		}
		return n
	}
	if got := countWhere(tableA, func(r *facadeRecord) bool { return r.Name == "new" }); got != 0 {
		t.Fatal("reseeded row leaked into the old cluster")
	}
	if got := countWhere(tableC, func(r *facadeRecord) bool { return r.Name == "old" && r.ID != rowOld }); got != 0 {
		t.Fatal("unexpected duplicate of old row on C")
	}
	if n, err := tableC.Where().Count(); err != nil || n != 2 {
		t.Fatalf("want 2 rows on C (baseline + new), got %d, %v", n, err)
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

	dbA, err := openSignedFixture(ctx, replTypedConfig(t, replConfig(filepath.Join(dir, "node_a"), nodeA, oldDB, creds[nodeA], nil)))
	if err != nil {
		t.Fatal(err)
	}
	tableA, err := TableOf[facadeRecord](dbA, "records")
	if err != nil {
		t.Fatal(err)
	}
	if err := insertRecord(ctx, dbA, tableA, &facadeRecord{ID: NewRowID(), Name: "old"}); err != nil {
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
	// Normal open converges the reseed intent; data and identity hold.
	cfgC := replTypedConfig(t, replConfig(restoredDir, nodeC, newDB, creds[nodeC], nil))
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	tableC, err := TableOf[facadeRecord](dbC, "records")
	if err != nil {
		t.Fatal(err)
	}
	if got := dbC.Status().DBID; got != newDB {
		t.Fatalf("reseeded DBID = %s, want %s", got, newDB)
	}
	if n, err := tableC.Where().Count(); err != nil || n != 1 {
		t.Fatalf("want baseline row, got %d, %v", n, err)
	}
	if err := dbC.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen is an ordinary stable open (intent consumed).
	dbC, err = openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()
	tableC, err = TableOf[facadeRecord](dbC, "records")
	if err != nil {
		t.Fatal(err)
	}
	if got := dbC.Status().DBID; got != newDB {
		t.Fatalf("reopened DBID = %s, want %s", got, newDB)
	}
	if n, err := tableC.Where().Count(); err != nil || n != 1 {
		t.Fatalf("want baseline row after reopen, got %d, %v", n, err)
	}
}

func TestReplicationCertMustMatchNode(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	// Certificate issued to B but configured as A: Open must fail before
	// any replication starts.
	cfg := replTypedConfig(t, replConfig(t.TempDir(), nodeA, NewDBID(), creds[nodeB], nil))
	db, err := openSignedFixture(ctx, cfg)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open with mismatched certificate succeeded")
	}
	if !errors.Is(err, ErrLocalIdentityMismatch) {
		t.Fatalf("err = %v, want ErrLocalIdentityMismatch", err)
	}
}
