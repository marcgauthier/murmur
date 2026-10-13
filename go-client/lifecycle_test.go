package murmur

import (
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestStorageFailureRetryAndCallbackRelease(t *testing.T) {
	initial := callbackCount()
	var armed atomic.Bool
	opts := DefaultOptions()
	opts.Mode = CreateNew
	opts.CaptureInterval = time.Hour
	opts.FaultHooks = &FaultHooks{SegmentSync: func() error {
		if armed.Load() {
			return errors.New("injected sync failure")
		}
		return nil
	}}
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, testKey(), opts)
	must(t, err)
	defer db.Close()
	must(t, db.Migrate(NewMigration("CREATE TABLE items(id INTEGER PRIMARY KEY)")))
	must(t, db.Flush())
	_, err = db.Exec("INSERT INTO items VALUES(1)")
	must(t, err)
	armed.Store(true)
	if err = db.Flush(); !errors.Is(err, ErrStorage) {
		t.Fatalf("storage failure: %v", err)
	}
	if _, err = db.Exec("INSERT INTO items VALUES(2)"); err == nil {
		t.Fatal("mutation accepted after storage failure")
	}
	result, err := db.Query("SELECT id FROM items")
	must(t, err)
	if len(result.Rows) != 1 {
		t.Fatal(result)
	}
	armed.Store(false)
	must(t, db.RetryStorage())
	must(t, db.Close())
	if callbackCount() != initial {
		t.Fatal("fault callback leaked")
	}
	db, err = Open(path, testKey(), DefaultOptions())
	must(t, err)
	defer db.Close()
	result, err = db.Query("SELECT id FROM items")
	must(t, err)
	if len(result.Rows) != 1 || result.Rows[0][0].Integer != 1 {
		t.Fatal(result)
	}
}
func TestMigrationMappingAndOfflineExchange(t *testing.T) {
	a, _ := newDB(t)
	must(t, a.Migrate(NewMigration("CREATE TABLE items(id INTEGER PRIMARY KEY,title TEXT)")))
	_, err := a.Exec("INSERT INTO items VALUES(1,'offline')")
	must(t, err)
	must(t, a.SetReplication("items", false))
	before, err := a.Schema()
	must(t, err)
	must(t, a.Migrate(Migration{SQL: "ALTER TABLE items RENAME TO docs", Mappings: []TableMapping{{OldName: "items", NewName: "docs", Columns: [][2]string{{"id", "id"}, {"title", "title"}}}}}))
	after, err := a.Schema()
	must(t, err)
	if before.Tables[0].ID != after.Tables[0].ID {
		t.Fatal("stable table identity changed")
	}
	enabled, err := a.ReplicationEnabled("docs")
	must(t, err)
	if enabled {
		t.Fatal("replication setting lost on rename")
	}
	must(t, a.SetReplication("docs", true))
	state, err := a.ExportState()
	must(t, err)
	opts := DefaultOptions()
	opts.Mode = CreateNew
	opts.DatabaseID = &state.DatabaseID
	b, err := Open(filepath.Join(t.TempDir(), "db"), testKey(), opts)
	must(t, err)
	defer b.Close()
	must(t, b.Migrate(NewMigration("CREATE TABLE items(id INTEGER PRIMARY KEY,title TEXT)")))
	must(t, b.Migrate(Migration{SQL: "ALTER TABLE items RENAME TO docs", Mappings: []TableMapping{{OldName: "items", NewName: "docs", Columns: [][2]string{{"id", "id"}, {"title", "title"}}}}}))
	must(t, b.MergeState(state))
	result, err := b.Query("SELECT title FROM docs")
	must(t, err)
	if len(result.Rows) != 1 || result.Rows[0][0].Text != "offline" {
		t.Fatal(result)
	}
}
func TestSubscriptionBinaryValuesAndResumeReset(t *testing.T) {
	db, _ := newDB(t)
	must(t, db.Migrate(NewMigration("CREATE TABLE samples(id INTEGER PRIMARY KEY,n REAL,b BLOB)")))
	_, err := db.Exec("INSERT INTO samples VALUES(?,?,?)", 1, math.Inf(1), []byte{0, 1, 0})
	must(t, err)
	opts := DefaultSubscriptionOptions()
	opts.ResumeFrom = &SubscriptionCursor{DatabaseID: NodeID{99}}
	_, err = db.SubscribeRows("SELECT * FROM samples", nil, []int{0}, opts)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expired cursor: %v", err)
	}
	opts.ResumeFrom = nil
	sub, err := db.SubscribeRows("SELECT * FROM samples", nil, []int{0}, opts)
	must(t, err)
	defer sub.Close()
	event, err := sub.RecvTimeout(time.Second)
	must(t, err)
	if event.Kind != "Initial" || len(event.Rows) != 1 || !math.IsInf(event.Rows[0][1].Real, 1) || !reflect.DeepEqual(event.Rows[0][2].Blob, []byte{0, 1, 0}) {
		t.Fatal(event)
	}
	must(t, db.Migrate(NewMigration("CREATE INDEX sample_n ON samples(n)")))
	event, err = sub.RecvTimeout(time.Second)
	must(t, err)
	if event.Kind != "Reset" || event.Reason == "" {
		t.Fatal(event)
	}
}
func TestMapBackedCodecs(t *testing.T) {
	peer := PeerRetention{Node: NodeID{1}, LastSeenMillis: math.MaxUint64, Acknowledged: []NodeSequence{{Node: NodeID{2}, Sequence: math.MaxUint64}}}
	bytes, err := Encode("PeerRetention", peer)
	must(t, err)
	var restored PeerRetention
	must(t, Decode("PeerRetention", bytes, &restored))
	if !reflect.DeepEqual(peer, restored) {
		t.Fatal(restored)
	}
	c := Counter{}
	must(t, c.Add(NodeID{1}, NodeID{2}, -7))
	bytes, err = Encode("Counter", c)
	must(t, err)
	var decoded Counter
	must(t, Decode("Counter", bytes, &decoded))
	n, err := decoded.Value()
	must(t, err)
	if n != -7 {
		t.Fatal(n)
	}
	set := Set{}
	must(t, set.Add([]byte("x"), Tag{Database: NodeID{1}, Node: NodeID{2}}))
	bytes, err = Encode("Set", set)
	must(t, err)
	var decodedSet Set
	must(t, Decode("Set", bytes, &decodedSet))
	values, err := decodedSet.Values()
	must(t, err)
	if len(values) != 1 || string(values[0]) != "x" {
		t.Fatal(values)
	}
}
