package replicateddb

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSoakTwoNodes runs random operations against two replicating nodes and
// requires final convergence. Skipped with -short. Duration defaults to 30s
// (REPLICATEDDB_SOAK_SECONDS overrides).
func TestSoakTwoNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in short mode")
	}
	seconds := 30
	if v := os.Getenv("REPLICATEDDB_SOAK_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			seconds = n
		}
	}
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	dbA, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	dbB, err := Open(ctx, replConfig(t.TempDir(), nodeB, dbid, creds[nodeB], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()
	if err := dbB.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{addrA}}); err != nil {
		t.Fatal(err)
	}

	// Shared row-ID pool (inserts append; updates/deletes pick randomly).
	var poolMu sync.Mutex
	var pool []RowID

	var ops atomic.Uint64
	var firstErr atomic.Value // error
	stop := make(chan struct{})
	var wg sync.WaitGroup
	worker := func(db *DB, seed int64) {
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
			if err := soakOp(ctx, db, rng, &poolMu, &pool); err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			ops.Add(1)
		}
	}
	wg.Add(2)
	go worker(dbA, 1)
	go worker(dbB, 2)
	time.Sleep(time.Duration(seconds) * time.Second)
	close(stop)
	wg.Wait()
	if err, ok := firstErr.Load().(error); ok && err != nil {
		t.Fatalf("soak op failed: %v", err)
	}
	t.Logf("soak completed %d ops", ops.Load())

	// Convergence: both nodes must reach identical query-visible state.
	deadline := time.Now().Add(60 * time.Second)
	for {
		a, b := dumpSQL(t, dbA), dumpSQL(t, dbB)
		if fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) {
			t.Logf("converged on %d rows", len(a))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no convergence: A=%d rows B=%d rows", len(a), len(b))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func soakOp(ctx context.Context, db *DB, rng *rand.Rand, mu *sync.Mutex, pool *[]RowID) error {
	roll := rng.Intn(100)
	switch {
	case roll < 40: // insert
		id := NewRowID()
		name := fmt.Sprintf("n%d", rng.Intn(100000))
		phone := fmt.Sprintf("p%d", rng.Intn(100000))
		if _, err := db.ExecContext(ctx,
			`INSERT INTO contacts (id, name, phone, score) VALUES (?, ?, ?, ?)`,
			id[:], name, phone, rng.Intn(1000)); err != nil {
			return err
		}
		mu.Lock()
		*pool = append(*pool, id)
		mu.Unlock()
	case roll < 80: // update random row (may not exist here yet: no-op write)
		mu.Lock()
		if len(*pool) == 0 {
			mu.Unlock()
			return nil
		}
		id := (*pool)[rng.Intn(len(*pool))]
		mu.Unlock()
		switch rng.Intn(3) {
		case 0:
			_, err := db.ExecContext(ctx, `UPDATE contacts SET phone = ? WHERE id = ?`,
				fmt.Sprintf("p%d", rng.Intn(100000)), id[:])
			return err
		case 1:
			_, err := db.ExecContext(ctx, `UPDATE contacts SET name = ?, score = ? WHERE id = ?`,
				fmt.Sprintf("n%d", rng.Intn(100000)), rng.Intn(1000), id[:])
			return err
		default:
			_, err := db.ExecContext(ctx, `UPDATE contacts SET score = score + 1 WHERE id = ?`, id[:])
			return err
		}
	case roll < 90: // delete random row
		mu.Lock()
		if len(*pool) == 0 {
			mu.Unlock()
			return nil
		}
		id := (*pool)[rng.Intn(len(*pool))]
		mu.Unlock()
		_, err := db.ExecContext(ctx, `DELETE FROM contacts WHERE id = ?`, id[:])
		return err
	default: // read
		rows, err := db.QueryContext(ctx, `SELECT id, name FROM contacts LIMIT 50`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id []byte
			var name any
			if err := rows.Scan(&id, &name); err != nil {
				rows.Close()
				return err
			}
		}
		err = rows.Err()
		rows.Close()
		return err
	}
	return nil
}
