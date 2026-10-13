package murmur

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testKey() []byte { return bytes.Repeat([]byte{7}, 32) }
func newDB(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	opts := DefaultOptions()
	opts.Mode = CreateNew
	d, err := Open(path, testKey(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d, path
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func TestTransactionsValuesAndReopen(t *testing.T) {
	d, path := newDB(t)
	must(t, d.Migrate(NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,n INTEGER,r REAL,b BLOB,nullable TEXT)")))
	var escaped *Sql
	var statement *Statement
	must(t, d.Transaction(func(sql *Sql) error {
		escaped = sql
		var err error
		statement, err = sql.Prepare("INSERT INTO docs VALUES(?,?,?,?,?,?)")
		if err != nil {
			return err
		}
		_, err = statement.Exec(10, "a\x00b", int64(math.MaxInt64), math.Inf(1), []byte{}, nil)
		return err
	}))
	if _, err := escaped.Exec("DELETE FROM docs"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("escaped SQL: %v", err)
	}
	if _, err := statement.Exec(11, "x", 1, 2, []byte{0}, nil); err == nil {
		t.Fatal("escaped statement worked")
	}
	must(t, d.Read(func(sql *Sql) error {
		var id, n int64
		var title string
		var r float64
		var blob []byte
		var null Value
		err := sql.QueryRow("SELECT * FROM docs").Scan(&id, &title, &n, &r, &blob, &null)
		if err != nil {
			return err
		}
		if id != 10 || title != "a\x00b" || n != math.MaxInt64 || !math.IsInf(r, 1) || blob == nil || len(blob) != 0 || null.Kind != NullKind {
			return fmt.Errorf("value fidelity: %d %q %d %v %v %v", id, title, n, r, blob, null)
		}
		return nil
	}))
	sentinel := errors.New("rollback")
	err := d.Transaction(func(sql *Sql) error {
		_, err := sql.Exec("UPDATE docs SET title='rollback'")
		if err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	err = d.Transaction(func(sql *Sql) error {
		_, err := sql.Exec("UPDATE docs SET title='panic'")
		if err != nil {
			return err
		}
		panic("callback panic")
	})
	if !errors.Is(err, ErrCallbackPanicked) {
		t.Fatalf("panic: %v", err)
	}
	must(t, d.Read(func(sql *Sql) error {
		var title string
		if err := sql.QueryRow("SELECT title FROM docs").Scan(&title); err != nil {
			return err
		}
		if title != "a\x00b" {
			return errors.New("rollback failed")
		}
		return nil
	}))
	must(t, d.Flush())
	must(t, d.Close())
	opts := DefaultOptions()
	opened, err := Open(path, testKey(), opts)
	must(t, err)
	defer opened.Close()
	result, err := opened.Query("SELECT COUNT(*) FROM docs")
	must(t, err)
	if result.Rows[0][0].Integer != 1 {
		t.Fatal(result)
	}
}
func TestReadAuthorizationReentryAndWrongThread(t *testing.T) {
	d, _ := newDB(t)
	must(t, d.Migrate(NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)")))
	err := d.Read(func(sql *Sql) error { _, err := sql.Exec("INSERT INTO docs VALUES(1,'no')"); return err })
	if !errors.Is(err, ErrSQL) {
		t.Fatalf("read write: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- d.Transaction(func(sql *Sql) error { _, err := d.Exec("INSERT INTO docs VALUES(1,'reenter')"); return err })
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reentry deadlocked")
	}
	must(t, d.Read(func(sql *Sql) error {
		done := make(chan error, 1)
		go func() { _, err := sql.Query("SELECT 1"); done <- err }()
		err := <-done
		if !errors.Is(err, ErrInvalid) {
			return fmt.Errorf("wrong thread: %v", err)
		}
		return nil
	}))
	if _, err := d.Exec("CREATE TABLE prohibited(id INTEGER PRIMARY KEY)"); !errors.Is(err, ErrSQL) {
		t.Fatalf("ordinary DDL: %v", err)
	}
}
func TestPreparedRowsAndConcurrentCallers(t *testing.T) {
	d, _ := newDB(t)
	must(t, d.Migrate(NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)")))
	must(t, d.Transaction(func(sql *Sql) error {
		stmt, err := sql.Prepare("INSERT INTO docs VALUES(?,?)")
		if err != nil {
			return err
		}
		defer stmt.Close()
		for i := 0; i < 20; i++ {
			if _, err = stmt.Exec(i, "x"); err != nil {
				return err
			}
		}
		rows, err := sql.Query("SELECT id FROM docs ORDER BY id")
		if err != nil {
			return err
		}
		defer rows.Close()
		for i := 0; rows.Next(); i++ {
			var n int64
			if err := rows.Scan(&n); err != nil {
				return err
			}
			if n != int64(i) {
				return fmt.Errorf("row order: %d", n)
			}
		}
		return rows.Err()
	}))
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := d.Exec("INSERT INTO docs VALUES(?,?)", 100+id, "concurrent")
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	result, err := d.Query("SELECT COUNT(*) FROM docs")
	must(t, err)
	if result.Rows[0][0].Integer != 36 {
		t.Fatal(result)
	}
}
func TestSearchAndLocalOnlyPersistence(t *testing.T) {
	d, path := newDB(t)
	must(t, d.Migrate(NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,embedding BLOB)")))
	must(t, d.SetSearchIndexes([]SearchIndex{FTS5Index("docs_fts", "docs", "title"), VectorIndex("docs_vec", "docs", "embedding", 3)}))
	must(t, d.SetReplication("docs", false))
	embedding := binaryWriter{}
	for _, n := range []float32{1, 0.5, 0.25} {
		embedding.u32(math.Float32bits(n))
	}
	_, err := d.Exec("INSERT INTO docs VALUES(?,?,?)", 1001, "Replicated search", embedding.data)
	must(t, err)
	must(t, d.RefreshIndexes())
	result, err := d.Query("SELECT rowid FROM docs_fts WHERE docs_fts MATCH 'replicated'")
	must(t, err)
	if result.Rows[0][0].Integer != 1001 {
		t.Fatal(result)
	}
	result, err = d.Query("SELECT rowid,distance FROM docs_vec WHERE embedding MATCH ? AND k=1 ORDER BY distance", "[1,0.5,0.25]")
	must(t, err)
	if result.Rows[0][0].Integer != 1001 {
		t.Fatal(result)
	}
	state, err := d.ExportState()
	must(t, err)
	if len(state.Rows) != 0 {
		t.Fatal("local-only rows exported")
	}
	must(t, d.Flush())
	must(t, d.Close())
	options := DefaultOptions()
	options.SearchIndexes = []SearchIndex{FTS5Index("docs_fts", "docs", "title")}
	opened, err := Open(path, testKey(), options)
	must(t, err)
	defer opened.Close()
	enabled, err := opened.ReplicationEnabled("docs")
	must(t, err)
	if enabled {
		t.Fatal("replication setting lost")
	}
	result, err = opened.Query("SELECT COUNT(*) FROM docs_fts")
	must(t, err)
	if result.Rows[0][0].Integer != 1 {
		t.Fatal(result)
	}
}
func TestSubscriptionsAndCleanup(t *testing.T) {
	d, _ := newDB(t)
	must(t, d.Migrate(NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)")))
	initialCallbacks := callbackCount()
	sub, err := d.SubscribeRows("SELECT id,title FROM docs ORDER BY id", nil, []int{0}, DefaultSubscriptionOptions())
	must(t, err)
	event, err := sub.RecvTimeout(time.Second)
	must(t, err)
	if event.Kind != "Initial" || len(event.Rows) != 0 {
		t.Fatal(event)
	}
	_, err = d.Exec("INSERT INTO docs VALUES(1,'hello')")
	must(t, err)
	must(t, d.Flush())
	event, err = sub.RecvTimeout(time.Second)
	must(t, err)
	if event.Kind != "Update" || len(event.Rows) != 1 || len(event.Changes) != 1 || event.Changes[0].ChangeType != Added {
		t.Fatal(event)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = sub.Recv(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	must(t, sub.Close())
	if callbackCount() != initialCallbacks {
		t.Fatal("subscription callback leaked")
	}
}
func callbackCount() int { n := 0; callbacks.Range(func(_, _ any) bool { n++; return true }); return n }
func TestBackupRestoreKeysProgressMetrics(t *testing.T) {
	d, _ := newDB(t)
	must(t, d.Migrate(NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)")))
	_, err := d.Exec("INSERT INTO docs VALUES(1,'backup')")
	must(t, err)
	backup := filepath.Join(t.TempDir(), "backup")
	must(t, d.Backup(backup))
	must(t, VerifyBackup(backup, testKey()))
	oldID, err := d.DatabaseID()
	must(t, err)
	node := NodeID{42}
	restore := filepath.Join(t.TempDir(), "clone")
	must(t, Restore(backup, restore, testKey(), RestoreOptions{NewNodeID: &node}))
	opts := DefaultOptions()
	var phases []OpenPhase
	opts.OnOpenProgress = func(p OpenProgress) { phases = append(phases, p.Phase) }
	clone, err := Open(restore, testKey(), opts)
	must(t, err)
	if len(phases) == 0 {
		t.Fatal("missing progress")
	}
	id, err := clone.DatabaseID()
	must(t, err)
	if id != oldID {
		t.Fatal("database ID changed")
	}
	nid, err := clone.NodeID()
	must(t, err)
	if nid != node {
		t.Fatal("node ID not changed")
	}
	_, err = clone.SQLiteMetrics()
	must(t, err)
	_, err = clone.SpoolMetrics()
	must(t, err)
	_, err = clone.Status()
	must(t, err)
	_, err = clone.Schema()
	must(t, err)
	must(t, clone.RotateDataKey())
	_, err = clone.KeyInventory()
	must(t, err)
	must(t, clone.RotateMasterKey(Key{9}))
	must(t, clone.Close())
}
func TestRawSessionAndScheduler(t *testing.T) {
	c, err := OpenInMemory()
	must(t, err)
	defer c.Close()
	must(t, c.Run(func(sql *Sql) error { return sql.ExecuteBatch("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)") }))
	session, err := StartSession(c)
	must(t, err)
	must(t, c.Run(func(sql *Sql) error { _, err := sql.Exec("INSERT INTO docs VALUES(1,'captured')"); return err }))
	changes, err := session.Changeset()
	must(t, err)
	if len(changes) == 0 {
		t.Fatal("empty changeset")
	}
	must(t, session.Close())
	scheduler, err := NewWriterScheduler(DefaultOptions().Scheduler)
	must(t, err)
	defer scheduler.Close()
	ticket, err := scheduler.Admit(WriterLocal)
	must(t, err)
	class, err := ticket.Class()
	must(t, err)
	if class != WriterLocal {
		t.Fatal(class)
	}
	_, err = ticket.GrantedAt()
	must(t, err)
	must(t, ticket.Release())
	snapshot, err := scheduler.Snapshot()
	must(t, err)
	if snapshot.Local.Acquisitions != 1 {
		t.Fatal(snapshot)
	}
}
func TestProtocolAndCausalHelpers(t *testing.T) {
	seed := Key{7}
	public, err := PublicKey(seed)
	must(t, err)
	db := NodeID{1}
	origin := NodeID{2}
	transaction := OriginTransaction{Database: db, Origin: origin, Sequence: 1, ID: NodeID{3}, Payload: []byte{0, 1, 255}, Signature: Bytes{}}
	must(t, transaction.Sign(seed))
	must(t, transaction.Verify(db, []TrustEntry{{Node: origin, Key: public}}, 1024))
	encoded, err := Encode("Transaction", transaction)
	must(t, err)
	var decoded OriginTransaction
	must(t, Decode("Transaction", encoded, &decoded))
	if !bytes.Equal(decoded.Payload, transaction.Payload) {
		t.Fatal("codec mismatch")
	}
	decoded.Payload[0]++
	if err := decoded.Verify(db, []TrustEntry{{Node: origin, Key: public}}, 1024); err == nil {
		t.Fatal("tampering accepted")
	}
	var a, b Counter
	must(t, a.Add(db, origin, 3))
	must(t, b.Add(db, NodeID{4}, 5))
	must(t, a.Join(b))
	n, err := a.Value()
	must(t, err)
	if n != 8 {
		t.Fatal(n)
	}
	var set Set
	must(t, set.Add([]byte("x"), Tag{Database: db, Node: origin, Transaction: NodeID{3}}))
	values, err := set.Values()
	must(t, err)
	if len(values) != 1 {
		t.Fatal(values)
	}
	must(t, set.Remove([]byte("x")))
	values, err = set.Values()
	must(t, err)
	if len(values) != 0 {
		t.Fatal(values)
	}
}
