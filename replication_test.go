package murmur

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/transport"
)

// testClusterCA issues node credentials for tests.
func testClusterCA(t testing.TB, nodes ...NodeID) (*transport.CA, map[NodeID]*TLSCredential) {
	t.Helper()
	ca, err := transport.GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[NodeID]*TLSCredential, len(nodes))
	for _, n := range nodes {
		certPEM, keyPEM, err := ca.IssueNode(n, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		out[n] = &TLSCredential{CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM}
	}
	return ca, out
}

func replConfig(path string, node NodeID, dbid DBID, tls *TLSCredential, peers []Peer) Config {
	cfg := testConfig(path)
	cfg.NodeID = node
	cfg.DBID = dbid
	cfg.Replication = ReplicationConfig{
		ListenAddr:      "127.0.0.1:0",
		TLS:             tls,
		Peers:           peers,
		SendInterval:    20 * time.Millisecond,
		DialInterval:    200 * time.Millisecond,
		AckInterval:     100 * time.Millisecond,
		MinLogRetention: time.Hour,
	}
	return cfg
}

func waitForAddr(t testing.TB, db *DB, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if a := db.repl.Addr(); a != "" {
			return a
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("listener never came up")
	return ""
}

func replTypedConfig(t *testing.T, cfg Config) Config {
	t.Helper()
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	return cfg
}

func TestTwoNodeReplication(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	dbA, err := openSignedFixture(ctx, replTypedConfig(t, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	tableA, err := tableOf[facadeRecord](dbA, "records")
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)

	cfgB := replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbB, err := openSignedFixture(ctx, replTypedConfig(t, cfgB))
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()
	tableB, err := tableOf[facadeRecord](dbB, "records")
	if err != nil {
		t.Fatal(err)
	}

	id := NewRowID()
	if err := insertRecord(ctx, dbA, tableA, &facadeRecord{ID: id, Name: "111"}); err != nil {
		t.Fatal(err)
	}
	// B converges without any manual sync.
	waitForRecordName(t, tableB, id, "111", 15*time.Second)

	// Reverse direction.
	id2 := NewRowID()
	if err := insertRecord(ctx, dbB, tableB, &facadeRecord{ID: id2, Name: "222"}); err != nil {
		t.Fatal(err)
	}
	waitForRecordName(t, tableA, id2, "222", 15*time.Second)

	// Both hold both rows.
	if n, err := tableA.Where().Count(); err != nil || n != 2 {
		t.Fatalf("A has %d rows, %v", n, err)
	}
	if n, err := tableB.Where().Count(); err != nil || n != 2 {
		t.Fatalf("B has %d rows, %v", n, err)
	}
	st := dbA.Status()
	if st.ConnectedPeers < 1 {
		t.Fatalf("A peers: %+v", st)
	}
}

func TestOfflineConflictConverges(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	// Same row created on both while disconnected (offline writes).
	id := NewRowID()
	dbA, err := openSignedFixture(ctx, replTypedConfig(t, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	tableA, err := tableOf[facadeRecord](dbA, "records")
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)

	dbB, err := openSignedFixture(ctx, replTypedConfig(t, replConfig(t.TempDir(), nodeB, dbid, creds[nodeB], nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()
	tableB, err := tableOf[facadeRecord](dbB, "records")
	if err != nil {
		t.Fatal(err)
	}

	if err := insertRecord(ctx, dbA, tableA, &facadeRecord{ID: id, Name: "from-A"}); err != nil {
		t.Fatal(err)
	}
	if err := insertRecord(ctx, dbB, tableB, &facadeRecord{ID: id, Name: "from-B"}); err != nil {
		t.Fatal(err)
	}

	// Connect: both directions converge deterministically (HLC + NodeID
	// tie-break). The winner is whichever version is greater; both nodes
	// must agree.
	if err := dbB.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{addrA}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		a, aErr := tableA.Get(id)
		b, bErr := tableB.Get(id)
		if aErr == nil && bErr == nil && a.Name == b.Name {
			t.Logf("both converged to name=%v", a.Name)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no convergence: A=%v,%v B=%v,%v", a, aErr, b, bErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Row count is 1 on both (same UUID, not duplicated).
	if n, err := tableA.Where().Count(); err != nil || n != 1 {
		t.Fatalf("A has %d rows, %v", n, err)
	}
	if err := dbB.ForceSync(ctx, nodeA); err != nil {
		t.Fatal(err)
	}
	if err := dbB.RemovePeer(ctx, nodeA); err != nil {
		t.Fatal(err)
	}
}

func TestReplicationSchemaMismatchRejected(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	// Strict policy on both nodes: sessions refuse without sync.
	strict := false
	cfgA := replTypedConfig(t, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	cfgA.Schema.AcceptRemoteSchema = &strict
	dbA, err := openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	tableA, err := tableOf[facadeRecord](dbA, "records")
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)

	// B runs a different schema epoch: the session must be refused and both
	// nodes must stay healthy.
	cfgB := replTypedConfig(t, replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}}))
	cfgB.Schema.Version = 2
	cfgB.Schema.AcceptRemoteSchema = &strict
	dbB, err := openSignedFixture(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()
	tableB, err := tableOf[facadeRecord](dbB, "records")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second) // allow several dial/handshake attempts
	if got := dbB.Status().ConnectedPeers; got != 0 {
		t.Fatalf("B connected despite schema mismatch: %d", got)
	}
	if got := dbA.Status().ConnectedPeers; got != 0 {
		t.Fatalf("A connected despite schema mismatch: %d", got)
	}
	// Both still serve local writes.
	if err := insertRecord(ctx, dbA, tableA, &facadeRecord{ID: NewRowID()}); err != nil {
		t.Fatal(err)
	}
	if err := insertRecord(ctx, dbB, tableB, &facadeRecord{ID: NewRowID()}); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotJoinAfterLogGC(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	dbA, err := openSignedFixture(ctx, replTypedConfig(t, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	tableA, err := tableOf[facadeRecord](dbA, "records")
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)

	for i := 0; i < 10; i++ {
		if err := insertRecord(ctx, dbA, tableA, &facadeRecord{ID: NewRowID(), Name: fmt.Sprintf("n%02d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Wipe A's origin log as if long-retained GC ran: a fresh node can only
	// join via snapshot.
	if n, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || n != 10 {
		t.Fatalf("CollectLog = %d, %v", n, err)
	}

	cfgC := replConfig(t.TempDir(), nodeC, dbid, creds[nodeC],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbC, err := openSignedFixture(ctx, replTypedConfig(t, cfgC))
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()
	tableC, err := tableOf[facadeRecord](dbC, "records")
	if err != nil {
		t.Fatal(err)
	}

	got := waitForRecordNames(t, tableC, 10, 20*time.Second)
	if got[0] != "n00" {
		t.Fatalf("first row = %v", got[0])
	}
	// C adopted A's watermarks from the snapshot manifest.
	if wm, _ := dbC.store.ReceiveWatermark(nodeA); wm != 10 {
		t.Fatalf("C watermark for A = %d, want 10", wm)
	}
	// And C can now replicate normally: write on C, read on A.
	id := NewRowID()
	if err := insertRecord(ctx, dbC, tableC, &facadeRecord{ID: id, Name: "new"}); err != nil {
		t.Fatal(err)
	}
	waitForRecordName(t, tableA, id, "new", 15*time.Second)
}

// waitForRecordNames waits for want typed rows and returns their names
// sorted ascending, mirroring waitForRows' ORDER BY name.
func waitForRecordNames(t *testing.T, table *recordTable[facadeRecord], want int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := table.Where().Find()
		if err == nil && len(rows) == want {
			names := make([]string, 0, len(rows))
			for _, row := range rows {
				names = append(names, row.Name)
			}
			sort.Strings(names)
			return names
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d rows", want)
	return nil
}

func TestSnapshotTailRepairConcurrentWritesAndGC(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	definition := recordDefinition(t)
	cfgA := replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil)
	cfgA.Schema.Tables = nil
	cfgA.Tables = []TableDefinition{definition}
	dbA, err := openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	tableA, err := tableOf[facadeRecord](dbA, "records")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		value := &facadeRecord{ID: NewRowID(), Name: fmt.Sprintf("n%02d", i)}
		if err := dbA.WriteTxContext(ctx, func(tx *Tx) error { return tableA.Insert(tx, value) }); err != nil {
			t.Fatal(err)
		}
	}
	// Wipe A's origin log for the first 10 rows so C must join via snapshot.
	if n, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || n != 10 {
		t.Fatalf("CollectLog = %d, %v", n, err)
	}

	// Concurrently write new rows on A and trigger GC aggressively while C connects and syncs.
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
		count := 10
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopWriter:
				return
			case <-ticker.C:
				if count < 20 {
					value := &facadeRecord{ID: NewRowID(), Name: fmt.Sprintf("n%02d", count)}
					if err := dbA.WriteTxContext(ctx, func(tx *Tx) error { return tableA.Insert(tx, value) }); err != nil {
						writerErr <- err
						return
					}
					count++
				}
				// Aggressive GC attempting to wipe all logs up to infinity.
				// Retention leases during snapshot export must protect the tail.
				_, _ = dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0)
			}
		}
	}()
	defer func() {
		stopSourceWriter()
		<-writerDone
	}()

	cfgC := replConfig(t.TempDir(), nodeC, dbid, creds[nodeC],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	cfgC.Schema.Tables = nil
	cfgC.Tables = []TableDefinition{definition}
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()

	// Wait for C to catch up all 20 rows (snapshot + tail repair).
	tableC, err := tableOf[facadeRecord](dbC, "records")
	if err != nil {
		t.Fatal(err)
	}
	if got := waitForRecordNames(t, tableC, 20, 20*time.Second); len(got) != 20 {
		t.Fatalf("receiver rows=%d, want 20", len(got))
	}
	stopSourceWriter()
	<-writerDone
	select {
	case err := <-writerErr:
		t.Fatalf("typed source write during snapshot failed: %v", err)
	default:
	}

	// Verify both nodes have 20 rows and can exchange further mutations.
	rowsA := waitForRecordNames(t, tableA, 20, 10*time.Second)
	rowsC := waitForRecordNames(t, tableC, 20, 10*time.Second)
	if len(rowsA) != 20 || len(rowsC) != 20 {
		t.Fatalf("row count mismatch: A=%d C=%d", len(rowsA), len(rowsC))
	}
	value := &facadeRecord{ID: NewRowID(), Name: "post-catchup"}
	if err := dbC.WriteTxContext(ctx, func(tx *Tx) error { return tableC.Insert(tx, value) }); err != nil {
		t.Fatal(err)
	}
	if got := waitForRecordNames(t, tableA, 21, 10*time.Second); len(got) != 21 {
		t.Fatalf("source rows after reverse write=%d, want 21", len(got))
	}
}

// TestBidirectionalBulkConverges writes hundreds of rows concurrently on
// both nodes and requires convergence plus clean shutdown. It regresses a
// duplex deadlock where both read loops bulk-sent inside Need handling and
// wedged both flow windows with nobody reading (plus a Close hang on the
// wedged session).
func TestBidirectionalBulkConverges(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	dbA, err := openSignedFixture(ctx, replTypedConfig(t, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	tableA, err := tableOf[facadeRecord](dbA, "records")
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)
	cfgB := replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbB, err := openSignedFixture(ctx, replTypedConfig(t, cfgB))
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()
	tableB, err := tableOf[facadeRecord](dbB, "records")
	if err != nil {
		t.Fatal(err)
	}

	const n = 250
	errCh := make(chan error, 2)
	go func() {
		for i := 0; i < n; i++ {
			if err := insertRecord(ctx, dbB, tableB, &facadeRecord{ID: NewRowID(), Name: "b"}); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()
	for i := 0; i < n; i++ {
		if err := insertRecord(ctx, dbA, tableA, &facadeRecord{ID: NewRowID(), Name: "a"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	waitForRecordCount(t, tableA, 2*n, 60*time.Second)
	waitForRecordCount(t, tableB, 2*n, 60*time.Second)
}
