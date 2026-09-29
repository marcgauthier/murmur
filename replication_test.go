package replicateddb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/transport"
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

func waitForRows(t testing.TB, db *DB, want int, timeout time.Duration) [][]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := db.QueryContext(context.Background(), `SELECT name, phone FROM contacts ORDER BY name`)
		if err != nil {
			if errors.Is(err, ErrMaterializerDirty) {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			t.Fatal(err)
		}
		var got [][]any
		cols := rows.Columns()
		for rows.Next() {
			dest := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range dest {
				ptrs[i] = &dest[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			got = append(got, dest)
		}
		rows.Close()
		if len(got) == want {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d rows", want)
	return nil
}

func waitForValue(t *testing.T, db *DB, id RowID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := queryAll(t, db, `SELECT phone FROM contacts WHERE id = ?`, id[:])
		if len(got) == 1 && fmt.Sprintf("%v", got[0][0]) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for phone=%q", want)
}

func TestTwoNodeReplication(t *testing.T) {
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

	cfgB := replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbB, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	id := NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		id[:], "ann", "111"); err != nil {
		t.Fatal(err)
	}
	// B converges without any manual sync.
	waitForValue(t, dbB, id, "111", 15*time.Second)

	// Reverse direction.
	id2 := NewRowID()
	if _, err := dbB.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		id2[:], "bob", "222"); err != nil {
		t.Fatal(err)
	}
	waitForValue(t, dbA, id2, "222", 15*time.Second)

	// Both hold both rows.
	if n := len(queryAll(t, dbA, `SELECT id FROM contacts`)); n != 2 {
		t.Fatalf("A has %d rows", n)
	}
	if n := len(queryAll(t, dbB, `SELECT id FROM contacts`)); n != 2 {
		t.Fatalf("B has %d rows", n)
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

	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		id[:], "ann", "from-A"); err != nil {
		t.Fatal(err)
	}
	if _, err := dbB.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		id[:], "ann", "from-B"); err != nil {
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
		a := queryAll(t, dbA, `SELECT phone FROM contacts WHERE id = ?`, id[:])
		b := queryAll(t, dbB, `SELECT phone FROM contacts WHERE id = ?`, id[:])
		if len(a) == 1 && len(b) == 1 && fmt.Sprintf("%v", a[0][0]) == fmt.Sprintf("%v", b[0][0]) {
			t.Logf("both converged to phone=%v", a[0][0])
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no convergence: A=%v B=%v", a, b)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Row count is 1 on both (same UUID, not duplicated).
	if n := len(queryAll(t, dbA, `SELECT id FROM contacts`)); n != 1 {
		t.Fatalf("A has %d rows", n)
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
	cfgA := replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil)
	cfgA.Schema.AcceptRemoteSchema = &strict
	dbA, err := Open(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)

	// B runs a different schema epoch: the session must be refused and both
	// nodes must stay healthy.
	cfgB := replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	cfgB.Schema.Version = 2
	cfgB.Schema.AcceptRemoteSchema = &strict
	dbB, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	time.Sleep(time.Second) // allow several dial/handshake attempts
	if got := dbB.Status().ConnectedPeers; got != 0 {
		t.Fatalf("B connected despite schema mismatch: %d", got)
	}
	if got := dbA.Status().ConnectedPeers; got != 0 {
		t.Fatalf("A connected despite schema mismatch: %d", got)
	}
	// Both still serve local writes.
	idA, idB := NewRowID(), NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id) VALUES (?)`, idA[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := dbB.ExecContext(ctx, `INSERT INTO contacts (id) VALUES (?)`, idB[:]); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotJoinAfterLogGC(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	dbA, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)

	for i := 0; i < 10; i++ {
		id := NewRowID()
		if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
			id[:], fmt.Sprintf("n%02d", i), fmt.Sprintf("p%02d", i)); err != nil {
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
	dbC, err := Open(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()

	got := waitForRows(t, dbC, 10, 20*time.Second)
	if fmt.Sprintf("%v", got[0][0]) != "n00" {
		t.Fatalf("first row = %v", got[0])
	}
	// C adopted A's watermarks from the snapshot manifest.
	if wm, _ := dbC.store.ReceiveWatermark(nodeA); wm != 10 {
		t.Fatalf("C watermark for A = %d, want 10", wm)
	}
	// And C can now replicate normally: write on C, read on A.
	id := NewRowID()
	if _, err := dbC.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "new"); err != nil {
		t.Fatal(err)
	}
	waitForRows(t, dbA, 11, 15*time.Second)
}

func TestSnapshotTailRepairConcurrentWritesAndGC(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeC)

	dbA, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)

	for i := 0; i < 10; i++ {
		id := NewRowID()
		if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
			id[:], fmt.Sprintf("n%02d", i), fmt.Sprintf("p%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Wipe A's origin log for the first 10 rows so C must join via snapshot.
	if n, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || n != 10 {
		t.Fatalf("CollectLog = %d, %v", n, err)
	}

	// Concurrently write new rows on A and trigger GC aggressively while C connects and syncs.
	stopWriter := make(chan struct{})
	writerDone := make(chan struct{})
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
					id := NewRowID()
					_, _ = dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
						id[:], fmt.Sprintf("n%02d", count), fmt.Sprintf("p%02d", count))
					count++
				}
				// Aggressive GC attempting to wipe all logs up to infinity.
				// Retention leases during snapshot export must protect the tail.
				_, _ = dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0)
			}
		}
	}()

	cfgC := replConfig(t.TempDir(), nodeC, dbid, creds[nodeC],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbC, err := Open(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	defer dbC.Close()

	// Wait for C to catch up all 20 rows (snapshot + tail repair).
	waitForRows(t, dbC, 20, 20*time.Second)

	close(stopWriter)
	<-writerDone

	// Verify both nodes have 20 rows and can exchange further mutations.
	rowsA := waitForRows(t, dbA, 20, 10*time.Second)
	rowsC := waitForRows(t, dbC, 20, 10*time.Second)
	if len(rowsA) != 20 || len(rowsC) != 20 {
		t.Fatalf("row count mismatch: A=%d C=%d", len(rowsA), len(rowsC))
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

	dbA, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)
	cfgB := replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbB, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	const n = 250
	errCh := make(chan error, 2)
	go func() {
		for i := 0; i < n; i++ {
			id := NewRowID()
			if _, err := dbB.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`,
				id[:], "b"); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()
	for i := 0; i < n; i++ {
		id := NewRowID()
		if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`,
			id[:], "a"); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	waitForRows(t, dbA, 2*n, 60*time.Second)
	waitForRows(t, dbB, 2*n, 60*time.Second)
}
