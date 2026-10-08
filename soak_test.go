package murmur

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

type soakRecord struct {
	ID    RowID `rime:"primary"`
	Name  string
	Phone string
	Score int64
}

func soakRecordDefinition(t *testing.T) TableDefinition {
	t.Helper()
	definition, err := Define[soakRecord]("contacts", 80, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2, "Phone": 3, "Score": 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func soakReplConfig(t *testing.T, path string, node NodeID, dbid DBID, tls *TLSCredential) Config {
	t.Helper()
	cfg := replConfig(path, node, dbid, tls, nil)
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{soakRecordDefinition(t)}
	cfg.Logger = soakTestLogger{t: t}
	return cfg
}

type soakTestLogger struct{ t testing.TB }

func (l soakTestLogger) Debug(string, ...any)          {}
func (l soakTestLogger) Info(string, ...any)           {}
func (l soakTestLogger) Warn(string, ...any)           {}
func (l soakTestLogger) Error(msg string, args ...any) { l.t.Logf("ERROR %s %v", msg, args) }

// TestSoakTwoNodes runs random typed operations against two replicating nodes
// and requires final convergence. Skipped with -short. Duration defaults to
// 30s (MURMUR_SOAK_SECONDS overrides).
func TestSoakTwoNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in short mode")
	}
	seconds := 30
	if v := os.Getenv("MURMUR_SOAK_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			seconds = n
		}
	}
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	dbA, err := openSignedFixture(ctx, soakReplConfig(t, t.TempDir(), nodeA, dbid, creds[nodeA]))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	tableA, err := TableOf[soakRecord](dbA, "contacts")
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)
	cfgB := soakReplConfig(t, t.TempDir(), nodeB, dbid, creds[nodeB])
	cfgB.Replication.Peers = []Peer{{NodeID: nodeA, Addrs: []string{addrA}}}
	dbB, err := openSignedFixture(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()
	tableB, err := TableOf[soakRecord](dbB, "contacts")
	if err != nil {
		t.Fatal(err)
	}

	var poolMu sync.Mutex
	var pool []RowID
	var ops atomic.Uint64
	var firstErr atomic.Value // error
	stop := make(chan struct{})
	var wg sync.WaitGroup
	worker := func(db *DB, table *RecordTable[soakRecord], seed int64) {
		defer wg.Done()
		rng := rand.New(rand.NewSource(seed))
		for {
			select {
			case <-stop:
				return
			default:
			}
			if firstErr.Load() != nil {
				return
			}
			if err := soakOp(ctx, db, table, rng, &poolMu, &pool); err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			ops.Add(1)
		}
	}
	wg.Add(2)
	go worker(dbA, tableA, 1)
	go worker(dbB, tableB, 2)
	time.Sleep(time.Duration(seconds) * time.Second)
	close(stop)
	wg.Wait()
	if err, ok := firstErr.Load().(error); ok && err != nil {
		t.Fatalf("soak op failed: %v", err)
	}
	t.Logf("soak completed %d operations", ops.Load())

	deadline := time.Now().Add(60 * time.Second)
	for {
		a, errA := soakSnapshot(tableA)
		b, errB := soakSnapshot(tableB)
		if errA != nil || errB != nil {
			t.Fatalf("read convergence snapshots: A=%v B=%v", errA, errB)
		}
		if equalSoakSnapshots(a, b) {
			t.Logf("converged on %d rows", len(a))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no convergence: A=%d rows B=%d rows", len(a), len(b))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func soakOp(ctx context.Context, db *DB, table *RecordTable[soakRecord], rng *rand.Rand, mu *sync.Mutex, pool *[]RowID) error {
	roll := rng.Intn(100)
	switch {
	case roll < 40:
		value := &soakRecord{ID: NewRowID(), Name: fmt.Sprintf("n%d", rng.Intn(100000)), Phone: fmt.Sprintf("p%d", rng.Intn(100000)), Score: int64(rng.Intn(1000))}
		if err := soakWrite(ctx, db, func(tx *Tx) error { return table.Insert(tx, value) }); err != nil {
			return err
		}
		mu.Lock()
		*pool = append(*pool, value.ID)
		mu.Unlock()
	case roll < 80:
		mu.Lock()
		if len(*pool) == 0 {
			mu.Unlock()
			return nil
		}
		id := (*pool)[rng.Intn(len(*pool))]
		mu.Unlock()
		switch rng.Intn(3) {
		case 0:
			phone := fmt.Sprintf("p%d", rng.Intn(100000))
			return soakUpdate(ctx, db, table, id, func(value *soakRecord) { value.Phone = phone })
		case 1:
			name, score := fmt.Sprintf("n%d", rng.Intn(100000)), int64(rng.Intn(1000))
			return soakUpdate(ctx, db, table, id, func(value *soakRecord) { value.Name, value.Score = name, score })
		default:
			return soakUpdate(ctx, db, table, id, func(value *soakRecord) { value.Score++ })
		}
	case roll < 90:
		mu.Lock()
		if len(*pool) == 0 {
			mu.Unlock()
			return nil
		}
		id := (*pool)[rng.Intn(len(*pool))]
		mu.Unlock()
		err := soakWrite(ctx, db, func(tx *Tx) error { return table.Delete(tx, id) })
		if errors.Is(err, rime.ErrNotFound) {
			return nil
		}
		return err
	default:
		_, err := table.Where().Limit(50).Find()
		return err
	}
	return nil
}

func soakUpdate(ctx context.Context, db *DB, table *RecordTable[soakRecord], id RowID, update func(*soakRecord)) error {
	err := soakWrite(ctx, db, func(tx *Tx) error {
		return table.Update(tx, id, func(value *soakRecord) error {
			update(value)
			return nil
		})
	})
	if errors.Is(err, rime.ErrNotFound) { // the row may not have replicated here yet
		return nil
	}
	return err
}

func soakWrite(ctx context.Context, db *DB, fn func(*Tx) error) error {
	var lastErr error
	for attempt := 0; attempt < 10000; attempt++ {
		err := db.WriteTxContext(ctx, fn)
		if err == nil {
			return nil
		}
		if !errors.Is(err, rime.ErrConflict) && !errors.Is(err, rime.ErrSnapshotUnavailable) && !errors.Is(err, ErrNotReady) {
			return err
		}
		lastErr = err
		delay := time.Duration(attempt+1) * 50 * time.Microsecond
		if delay > time.Millisecond {
			delay = time.Millisecond
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("murmur: typed soak write exceeded conflict retry limit in state %s: %w", db.Status().State, lastErr)
}

func soakSnapshot(table *RecordTable[soakRecord]) (map[RowID]soakRecord, error) {
	rows, err := table.Where().Find()
	if err != nil {
		return nil, err
	}
	snapshot := make(map[RowID]soakRecord, len(rows))
	for _, row := range rows {
		snapshot[row.ID] = *row
	}
	return snapshot, nil
}

func equalSoakSnapshots(a, b map[RowID]soakRecord) bool {
	if len(a) != len(b) {
		return false
	}
	for id, value := range a {
		if b[id] != value {
			return false
		}
	}
	return true
}
