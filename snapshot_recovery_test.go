package murmur

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
)

type snapshotRecord struct {
	ID    ids.RowID `rime:"primary"`
	Name  string
	Phone string
}

func snapshotRecordDefinition(t *testing.T) TableDefinition {
	t.Helper()
	definition, err := Define[snapshotRecord]("contacts", 1, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2, "Phone": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func snapshotReplConfig(t *testing.T, path string, node NodeID, dbid DBID, tls *TLSCredential, peers []Peer) Config {
	t.Helper()
	cfg := replConfig(path, node, dbid, tls, peers)
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{snapshotRecordDefinition(t)}
	return cfg
}

func waitForTypedSnapshotRows(t testing.TB, db *DB, want int, timeout time.Duration) []*snapshotRecord {
	t.Helper()
	table, err := TableOf[snapshotRecord](db, "contacts")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := table.Where().Find()
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == want {
			return rows
		}
		time.Sleep(20 * time.Millisecond)
	}
	rows, err := table.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("timed out waiting for %d typed rows; got %d", want, len(rows))
	return nil
}

func writeSnapshotRecord(t testing.TB, db *DB, table *RecordTable[snapshotRecord], value *snapshotRecord) {
	t.Helper()
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Insert(tx, value)
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotRestartServesPublishedRows proves a node that joined via
// snapshot serves the published typed state after a restart.
func TestSnapshotRestartServesPublishedRows(t *testing.T) {
	ctx := context.Background()
	const n = 50
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	cfgA := snapshotReplConfig(t, t.TempDir(), nodeA, dbid, creds[nodeA], nil)
	dbA, err := openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	tableA, err := TableOf[snapshotRecord](dbA, "contacts")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		writeSnapshotRecord(t, dbA, tableA, &snapshotRecord{ID: NewRowID(), Name: fmt.Sprintf("r%02d", i), Phone: fmt.Sprintf("p%02d", i)})
	}
	// Wipe A's origin log as if long-retained GC ran: C joins via snapshot.
	if got, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || got != n {
		t.Fatalf("CollectLog = %d, %v", got, err)
	}
	pathC := t.TempDir()
	cfgC := snapshotReplConfig(t, pathC, nodeC, dbid, creds[nodeC], []Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	waitForTypedSnapshotRows(t, dbC, n, 30*time.Second)
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
	if got := waitForTypedSnapshotRows(t, dbC, n, 5*time.Second); len(got) != n {
		t.Fatalf("reopened rows = %d, want %d", len(got), n)
	}
	if wm, _ := dbC.store.ReceiveWatermark(nodeA); wm != n {
		t.Fatalf("reopened watermark = %d, want %d", wm, n)
	}
	if gen2, _ := dbC.store.StateGeneration(); gen2 != gen {
		t.Fatalf("reopened generation = %d, want %d", gen2, gen)
	}
	// And C replicates normally after the restart: write on C, read on A.
	tableC, err := TableOf[snapshotRecord](dbC, "contacts")
	if err != nil {
		t.Fatal(err)
	}
	writeSnapshotRecord(t, dbC, tableC, &snapshotRecord{ID: NewRowID(), Name: "new"})
	waitForTypedSnapshotRows(t, dbA, n+1, 15*time.Second)
}

// TestSnapshotSlowTransferSustainedWritesAndAggressiveGC verifies typed writes
// continue while snapshot transfer and source log collection are active.
func TestSnapshotSlowTransferSustainedWritesAndAggressiveGC(t *testing.T) {
	ctx := context.Background()
	const initialRows = 30
	const concurrentWrites = 40
	const receiverWrites = 40
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)
	cfgA := snapshotReplConfig(t, t.TempDir(), nodeA, dbid, creds[nodeA], nil)
	dbA, err := openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	tableA, err := TableOf[snapshotRecord](dbA, "contacts")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < initialRows; i++ {
		writeSnapshotRecord(t, dbA, tableA, &snapshotRecord{ID: NewRowID(), Name: fmt.Sprintf("init-%02d", i), Phone: fmt.Sprintf("p-%02d", i)})
	}
	if got, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || got != initialRows {
		t.Fatalf("CollectLog = %d, %v", got, err)
	}

	stopWriter := make(chan struct{})
	writerStopped := false
	stopSourceWriter := func() {
		if !writerStopped {
			close(stopWriter)
			writerStopped = true
		}
	}
	writerDone := make(chan struct{})
	writerErr := make(chan error, 1)
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
					value := &snapshotRecord{ID: NewRowID(), Name: fmt.Sprintf("concurrent-%02d", count), Phone: fmt.Sprintf("p-%02d", count)}
					if err := dbA.WriteTxContext(ctx, func(tx *Tx) error { return tableA.Insert(tx, value) }); err != nil {
						select {
						case writerErr <- err:
						default:
						}
						return
					}
					count++
				}
				_, _ = dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0)
			}
		}
	}()

	pathC := t.TempDir()
	cfgC := snapshotReplConfig(t, pathC, nodeC, dbid, creds[nodeC], []Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	cfgC.Replication.SnapshotChunkCells = 5
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		stopSourceWriter()
		<-writerDone
		t.Fatal(err)
	}
	tableC, err := TableOf[snapshotRecord](dbC, "contacts")
	if err != nil {
		t.Fatal(err)
	}
	receiverDone := make(chan struct{})
	receiverErr := make(chan error, 1)
	go func() {
		defer close(receiverDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < receiverWrites; i++ {
			<-ticker.C
			value := &snapshotRecord{ID: NewRowID(), Name: fmt.Sprintf("receiver-%02d", i)}
			if err := dbC.WriteTxContext(ctx, func(tx *Tx) error { return tableC.Insert(tx, value) }); err != nil {
				receiverErr <- err
				return
			}
		}
	}()
	defer func() {
		stopSourceWriter()
		_ = dbC.Close()
		<-writerDone
		<-receiverDone
	}()
	totalExpected := initialRows + concurrentWrites + receiverWrites
	<-receiverDone
	select {
	case err := <-receiverErr:
		t.Fatalf("receiver typed write failed during snapshot: %v", err)
	default:
	}
	waitForTypedSnapshotRows(t, dbC, totalExpected, 40*time.Second)
	stopSourceWriter()
	<-writerDone
	select {
	case err := <-writerErr:
		t.Fatalf("concurrent typed write failed during snapshot: %v", err)
	default:
	}
	if got := waitForTypedSnapshotRows(t, dbA, totalExpected, 10*time.Second); len(got) != totalExpected {
		t.Fatalf("source rows = %d, want %d", len(got), totalExpected)
	}
	if got := waitForTypedSnapshotRows(t, dbC, totalExpected, 10*time.Second); len(got) != totalExpected {
		t.Fatalf("receiver rows = %d, want %d", len(got), totalExpected)
	}
	writeSnapshotRecord(t, dbC, tableC, &snapshotRecord{ID: NewRowID(), Name: "post-catchup", Phone: "999"})
	waitForTypedSnapshotRows(t, dbA, totalExpected+1, 15*time.Second)
}

// TestSnapshotTransferInterruptedBySourceAndReceiverRestarts verifies that
// restarts of either receiver or source during typed snapshot transfer recover.
func TestSnapshotTransferInterruptedBySourceAndReceiverRestarts(t *testing.T) {
	ctx := context.Background()
	const n = 30
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	pathA := t.TempDir()
	cfgA := snapshotReplConfig(t, pathA, nodeA, dbid, creds[nodeA], nil)
	dbA, err := openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)
	tableA, err := TableOf[snapshotRecord](dbA, "contacts")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		writeSnapshotRecord(t, dbA, tableA, &snapshotRecord{ID: NewRowID(), Name: fmt.Sprintf("init-%02d", i), Phone: fmt.Sprintf("p-%02d", i)})
	}
	if got, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || got != n {
		t.Fatalf("CollectLog = %d, %v", got, err)
	}

	pathC := t.TempDir()
	cfgC := snapshotReplConfig(t, pathC, nodeC, dbid, creds[nodeC], []Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	cfgC.Replication.SnapshotChunkCells = 2
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	_ = dbC.Close()
	dbC, err = openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()
	waitForTypedSnapshotRows(t, dbC, n, 30*time.Second)

	for i := 0; i < 10; i++ {
		writeSnapshotRecord(t, dbA, tableA, &snapshotRecord{ID: NewRowID(), Name: fmt.Sprintf("round2-%02d", i), Phone: "p"})
	}
	_ = dbA.Close()
	dbA, err = openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA2 := waitForAddr(t, dbA, 5*time.Second)
	_ = dbC.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{addrA2}})
	waitForTypedSnapshotRows(t, dbC, n+10, 30*time.Second)
	waitForTypedSnapshotRows(t, dbA, n+10, 30*time.Second)
}
