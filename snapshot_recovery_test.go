package replicateddb

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

	dbA, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
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
	dbC, err := Open(ctx, cfgC)
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
	dbC, err = Open(ctx, cfgC)
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
