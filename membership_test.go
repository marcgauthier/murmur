package replicateddb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/state"
)

func openPairForMembership(t *testing.T, mutate func(*Config)) (nodeA, nodeB NodeID, dbA, dbB *DB) {
	t.Helper()
	ctx := context.Background()
	nodeA, nodeB = NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	cfgA := replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil)
	if mutate != nil {
		mutate(&cfgA)
	}
	var err error
	dbA, err = Open(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)

	cfgB := replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbB, err = Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	return nodeA, nodeB, dbA, dbB
}

// pollMember waits for a member record satisfying want.
func pollMember(t *testing.T, db *DB, peer NodeID, timeout time.Duration, want func(state.MemberRecord) bool) state.MemberRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rec, err := db.store.GetMember(peer)
		if err != nil {
			t.Fatal(err)
		}
		if want(rec) {
			return rec
		}
		time.Sleep(20 * time.Millisecond)
	}
	rec, _ := db.store.GetMember(peer)
	t.Fatalf("member %s never satisfied condition: %+v", peer, rec)
	return rec
}

// pollMemberAdmissions waits until the manager has counted at least want
// admissions. The counter trails the durable record (the attach
// goroutine increments after the commit), so tests must poll it.
func pollMemberAdmissions(t *testing.T, db *DB, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := db.Status().Replication.MemberAdmissions; got >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("admissions did not reach %d within %v (last %d)", want, timeout, db.Status().Replication.MemberAdmissions)
}

// pollMemberAdmissionsExact waits until the admission count equals want.
// A duplicate admission fails the wait instead of passing silently.
func pollMemberAdmissionsExact(t *testing.T, db *DB, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := db.Status().Replication.MemberAdmissions; got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("admissions did not equal %d within %v (last %d)", want, timeout, db.Status().Replication.MemberAdmissions)
}

// TestMemberAdmissionOnHandshake proves the first successful authenticated
// handshake persists an active admission with a retention deadline.
func TestMemberAdmissionOnHandshake(t *testing.T) {
	nodeA, nodeB, dbA, dbB := openPairForMembership(t, nil)
	defer dbA.Close()
	defer dbB.Close()

	before := time.Now().UnixMilli()
	rec := pollMember(t, dbA, nodeB, 10*time.Second, func(r state.MemberRecord) bool {
		return r.Status == state.MemberActive
	})
	if rec.FirstAdmittedAt < before || rec.Excluded {
		t.Fatalf("A's record for B = %+v", rec)
	}
	wantDeadline := rec.FirstAdmittedAt + dbA.cfg.Replication.MaxOfflineLogRetention.Milliseconds()
	if rec.RetentionDeadline != wantDeadline {
		t.Fatalf("deadline = %d, want %d", rec.RetentionDeadline, wantDeadline)
	}
	pollMember(t, dbB, nodeA, 10*time.Second, func(r state.MemberRecord) bool {
		return r.Status == state.MemberActive
	})

	// The admission counter is incremented by the attach goroutine
	// after the record commit lands, so a Status sampled between the
	// two observes the record without the count. Poll the counter;
	// only the record itself is synchronous with the poll above.
	pollMemberAdmissions(t, dbA, 1, 10*time.Second)
	st := dbA.Status()
	if st.Replication.GatingMembers != 1 {
		t.Fatalf("admissions=%d gating=%d", st.Replication.MemberAdmissions, st.Replication.GatingMembers)
	}
	if len(st.Peers) != 1 || st.Peers[0].Retired || st.Peers[0].Excluded {
		t.Fatalf("peer diagnostics = %+v", st.Peers)
	}
}

// TestAckDeadlineRenewalOnlyOnAdvance proves repeated unchanged
// acknowledgements do not renew the retention deadline, while advancing
// progress does.
func TestAckDeadlineRenewalOnlyOnAdvance(t *testing.T) {
	ctx := context.Background()
	nodeA, _, dbA, dbB := openPairForMembership(t, nil)
	defer dbA.Close()
	defer dbB.Close()

	id1 := NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`, id1[:], "ann", "111"); err != nil {
		t.Fatal(err)
	}
	waitForValue(t, dbB, id1, "111", 15*time.Second)

	rec1 := pollMember(t, dbA, dbB.cfg.NodeID, 10*time.Second, func(r state.MemberRecord) bool {
		return r.LastProgressAt > 0
	})
	// Let several ack ticks (100ms) repeat the unchanged watermark.
	time.Sleep(350 * time.Millisecond)
	recStill, err := dbA.store.GetMember(dbB.cfg.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if recStill.RetentionDeadline != rec1.RetentionDeadline || recStill.LastProgressAt != rec1.LastProgressAt {
		t.Fatalf("deadline renewed without progress: %+v -> %+v", rec1, recStill)
	}

	id2 := NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`, id2[:], "bob", "222"); err != nil {
		t.Fatal(err)
	}
	waitForValue(t, dbB, id2, "222", 15*time.Second)
	rec2 := pollMember(t, dbA, dbB.cfg.NodeID, 10*time.Second, func(r state.MemberRecord) bool {
		return r.LastProgressAt > rec1.LastProgressAt
	})
	if rec2.RetentionDeadline <= rec1.RetentionDeadline {
		t.Fatalf("deadline not renewed on advance: %+v -> %+v", rec1, rec2)
	}
	_ = nodeA
}

// TestGCRetentionDeadlineIndependentOfLastSeen proves log GC gates on
// persisted deadlines even while the peer stays connected with fresh
// session activity.
func TestGCRetentionDeadlineIndependentOfLastSeen(t *testing.T) {
	ctx := context.Background()
	nodeA, _, dbA, dbB := openPairForMembership(t, func(cfg *Config) {
		cfg.Replication.MaxOfflineLogRetention = 400 * time.Millisecond
		cfg.Replication.MinLogRetention = time.Millisecond
		cfg.Replication.MinRetainedBatches = 1
	})
	defer dbA.Close()
	defer dbB.Close()

	for _, tc := range [][2]string{{"ann", "111"}, {"bob", "222"}} {
		id := NewRowID()
		if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`, id[:], tc[0], tc[1]); err != nil {
			t.Fatal(err)
		}
		waitForValue(t, dbB, id, tc[1], 15*time.Second)
	}
	peerB := dbB.cfg.NodeID
	rec := pollMember(t, dbA, peerB, 10*time.Second, func(r state.MemberRecord) bool {
		if ack, err := dbA.store.PeerAck(peerB, nodeA); err != nil || ack != 2 {
			return false
		}
		return r.LastProgressAt > 0
	})

	// Wait past the persisted deadline while B stays connected and
	// chatting (repeat acks must not renew it).
	deadline := time.UnixMilli(rec.RetentionDeadline).Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	st := dbA.Status()
	if len(st.Peers) != 1 || !st.Peers[0].Connected {
		t.Fatalf("B should stay connected: %+v", st.Peers)
	}
	if age := time.Since(st.Peers[0].LastSeen); age > 5*time.Second {
		t.Fatalf("B's lastSeen went stale: %v", age)
	}
	recAfter, err := dbA.store.GetMember(peerB)
	if err != nil {
		t.Fatal(err)
	}
	if recAfter.RetentionDeadline != rec.RetentionDeadline {
		t.Fatalf("deadline renewed without progress: %+v -> %+v", rec, recAfter)
	}

	dbA.gcOnce(false)

	// Batch 1 is collected while batch 2 survives: the expired member
	// pins nothing despite its fresh session.
	if _, err := dbA.store.LogScan(nodeA, 1, 100, 1<<20, func(b *codec.MutationBatch) error { return nil }); !errors.Is(err, state.ErrLogGone) {
		t.Fatalf("LogScan from 1 err = %v, want ErrLogGone", err)
	}
	var seqs []uint64
	if _, err := dbA.store.LogScan(nodeA, 2, 100, 1<<20, func(b *codec.MutationBatch) error {
		seqs = append(seqs, b.Sequence)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 1 || seqs[0] != 2 {
		t.Fatalf("origin log after expiry GC = %v, want [2]", seqs)
	}
	if wm, err := dbA.store.ReceiveWatermark(nodeA); err != nil || wm != 2 {
		t.Fatalf("watermark = %d, %v", wm, err)
	}
}

// TestRemovePeerRetiresAcrossRestart proves retirement persists across
// restart, refuses rejoining handshakes, and clears on AddPeer with a
// fresh admission.
func TestRemovePeerRetiresAcrossRestart(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	cfgA := replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil)
	dbA, err := Open(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	addrA := waitForAddr(t, dbA, 5*time.Second)
	cfgB := replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbB, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()
	pollMember(t, dbA, nodeB, 10*time.Second, func(r state.MemberRecord) bool {
		return r.Status == state.MemberActive
	})

	if err := dbA.RemovePeer(ctx, nodeB); err != nil {
		t.Fatal(err)
	}
	rec, err := dbA.store.GetMember(nodeB)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != state.MemberRetired || !dbA.IsPeerExcluded(nodeB) {
		t.Fatalf("after remove: %+v excluded=%v", rec, dbA.IsPeerExcluded(nodeB))
	}
	if got := dbA.Metrics().PeersRemoved; got != 1 {
		t.Fatalf("PeersRemoved = %d, want 1", got)
	}
	if err := dbA.Close(); err != nil {
		t.Fatal(err)
	}

	dbA2, err := Open(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA2.Close()
	if !dbA2.IsPeerExcluded(nodeB) {
		t.Fatalf("exclusion lost across restart")
	}
	rec, err = dbA2.store.GetMember(nodeB)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != state.MemberRetired {
		t.Fatalf("retirement lost across restart: %+v", rec)
	}
	// B keeps dialing the old address; nothing connects while retired.
	time.Sleep(time.Second)
	if got := dbB.Status().ConnectedPeers; got != 0 {
		t.Fatalf("B connected while retired: %+v", dbB.Status().Peers)
	}

	// Readmit on A and point B at A's new listener address.
	addrA2 := waitForAddr(t, dbA2, 5*time.Second)
	if err := dbA2.AddPeer(ctx, Peer{NodeID: nodeB, Addrs: []string{addrA2}}); err != nil {
		t.Fatal(err)
	}
	if err := dbB.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{addrA2}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if dbB.Status().ConnectedPeers == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("B never reconnected after readmission")
		}
		time.Sleep(20 * time.Millisecond)
	}
	rec = pollMember(t, dbA2, nodeB, 10*time.Second, func(r state.MemberRecord) bool {
		return r.Status == state.MemberActive
	})
	if dbA2.IsPeerExcluded(nodeB) {
		t.Fatalf("exclusion not cleared: %+v", rec)
	}
	// As above, the counter lags the record commit across goroutines;
	// poll for exactly one admission rather than asserting immediately.
	pollMemberAdmissionsExact(t, dbA2, 1, 10*time.Second)
}
