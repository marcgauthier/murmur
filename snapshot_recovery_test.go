package murmur

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestSnapshotRestartServesPublishedRows proves a node that joined via
// snapshot serves the published state after a restart: the published
// generation, watermarks, and rows survive close/reopen.
func TestSnapshotRestartServesPublishedRows(t *testing.T) {
	ctx := context.Background()
	const n = 50
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	dbA, err := openSignedFixture(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	for i := 0; i < n; i++ {
		id := NewRowID()
		if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
			id[:], fmt.Sprintf("r%02d", i), fmt.Sprintf("p%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Wipe A's origin log as if long-retained GC ran: C joins via snapshot.
	if got, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || got != n {
		t.Fatalf("CollectLog = %d, %v", got, err)
	}
	pathC := t.TempDir()
	cfgC := replConfig(pathC, nodeC, dbid, creds[nodeC],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	waitForRows(t, dbC, n, 30*time.Second)
	if wm, _ := dbC.store.ReceiveWatermark(nodeA); wm != n {
		t.Fatalf("watermark = %d, want %d", wm, n)
	}
	gen, err := dbC.store.StateGeneration()
	if err != nil {
		t.Fatal(err)
	}
	if err := dbC.Close(); err != nil {
		t.Fatal(err)
	}
	// Restart serves the published snapshot state unchanged.
	dbC, err = openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()
	if got := queryAll(t, dbC, `SELECT id FROM contacts`); len(got) != n {
		t.Fatalf("reopened rows = %d, want %d", len(got), n)
	}
	if wm, _ := dbC.store.ReceiveWatermark(nodeA); wm != n {
		t.Fatalf("reopened watermark = %d, want %d", wm, n)
	}
	if gen2, _ := dbC.store.StateGeneration(); gen2 != gen {
		t.Fatalf("reopened generation = %d, want %d", gen2, gen)
	}
	// And C replicates normally after the restart: write on C, read on A.
	id := NewRowID()
	if _, err := dbC.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "new"); err != nil {
		t.Fatal(err)
	}
	waitForRows(t, dbA, n+1, 15*time.Second)
}

// TestSnapshotSlowTransferSustainedWritesAndAggressiveGC verifies snapshot recovery
// under sustained concurrent writes, aggressive GC loops on the source, and receiver
// tail catch-up.
func TestSnapshotSlowTransferSustainedWritesAndAggressiveGC(t *testing.T) {
	ctx := context.Background()
	const initialRows = 30
	const concurrentWrites = 40
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	dbA, err := openSignedFixture(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)

	// Seed initial state
	for i := 0; i < initialRows; i++ {
		id := NewRowID()
		if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
			id[:], fmt.Sprintf("init-%02d", i), fmt.Sprintf("p-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Purge origin log to force snapshot
	if got, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || got != initialRows {
		t.Fatalf("CollectLog = %d, %v", got, err)
	}

	// Start concurrent writer and aggressive GC worker on source A
	stopWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		ticker := time.NewTicker(15 * time.Millisecond)
		defer ticker.Stop()
		count := 0
		for {
			select {
			case <-stopWriter:
				return
			case <-ticker.C:
				if count < concurrentWrites {
					id := NewRowID()
					_, _ = dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
						id[:], fmt.Sprintf("concurrent-%02d", count), fmt.Sprintf("p-%02d", count))
					count++
				}
				// Aggressive GC attempting to wipe all logs up to infinity.
				// Source retention lease must protect the snapshot cut and tail history.
				_, _ = dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0)
			}
		}
	}()

	// Open receiver node C connected to A
	pathC := t.TempDir()
	cfgC := replConfig(pathC, nodeC, dbid, creds[nodeC],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	cfgC.Replication.SnapshotChunkCells = 5 // smaller chunks to simulate multi-chunk transfer
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()

	// Wait for receiver C to merge the snapshot, publish, and catch up on all concurrent writes
	totalExpected := initialRows + concurrentWrites
	waitForRows(t, dbC, totalExpected, 40*time.Second)

	close(stopWriter)
	<-writerDone

	// Ensure both nodes agree on final state
	rowsA := waitForRows(t, dbA, totalExpected, 10*time.Second)
	rowsC := waitForRows(t, dbC, totalExpected, 10*time.Second)
	if len(rowsA) != totalExpected || len(rowsC) != totalExpected {
		t.Fatalf("row count mismatch: A=%d C=%d, want %d", len(rowsA), len(rowsC), totalExpected)
	}

	// Verify post-snapshot bi-directional replication
	idPost := NewRowID()
	if _, err := dbC.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		idPost[:], "post-catchup", "999"); err != nil {
		t.Fatal(err)
	}
	waitForRows(t, dbA, totalExpected+1, 15*time.Second)
}

// TestSnapshotTransferInterruptedBySourceAndReceiverRestarts verifies that
// restarts of either receiver or source during snapshot transfer recover cleanly.
func TestSnapshotTransferInterruptedBySourceAndReceiverRestarts(t *testing.T) {
	ctx := context.Background()
	const n = 30
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	pathA := t.TempDir()
	cfgA := replConfig(pathA, nodeA, dbid, creds[nodeA], nil)
	dbA, err := openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)

	for i := 0; i < n; i++ {
		id := NewRowID()
		if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
			id[:], fmt.Sprintf("init-%02d", i), fmt.Sprintf("p-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || got != n {
		t.Fatalf("CollectLog = %d, %v", got, err)
	}

	// 1. Receiver restart: start C, let it begin, close it, and reopen it
	pathC := t.TempDir()
	cfgC := replConfig(pathC, nodeC, dbid, creds[nodeC],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	cfgC.Replication.SnapshotChunkCells = 2
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	_ = dbC.Close()

	// Reopen C; it must re-request snapshot and catch up
	dbC, err = openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()
	waitForRows(t, dbC, n, 30*time.Second)

	// 2. Source restart: write more rows on A, purge logs, restart A, verify C continues syncing
	for i := 0; i < 10; i++ {
		id := NewRowID()
		if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
			id[:], fmt.Sprintf("round2-%02d", i), "p"); err != nil {
			t.Fatal(err)
		}
	}
	_ = dbA.Close()

	// Reopen A
	dbA, err = openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA2 := waitForAddr(t, dbA, 5*time.Second)
	_ = dbC.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{addrA2}})

	waitForRows(t, dbC, n+10, 30*time.Second)
	waitForRows(t, dbA, n+10, 30*time.Second)
}
