package replicateddb

import (
	"context"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/state"
)

// TestStatusWriterMetrics proves local commits, write acquisitions, and
// storage gauges land in Status/Metrics.
func TestStatusWriterMetrics(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, name := range []string{"ann", "bob", "cid"} {
		id := NewRowID()
		if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], name); err != nil {
			t.Fatal(err)
		}
	}

	m := db.Metrics()
	if m.LocalCommits != 3 {
		t.Fatalf("LocalCommits = %d, want 3", m.LocalCommits)
	}
	if m.LocalCommitMutations == 0 {
		t.Fatalf("LocalCommitMutations = 0")
	}
	if m.WriteAcquisitions != 3 {
		t.Fatalf("WriteAcquisitions = %d, want 3", m.WriteAcquisitions)
	}
	if m.LocalCommitLatencyNanos == 0 {
		t.Fatalf("LocalCommitLatencyNanos = 0")
	}

	st := db.Status()
	if len(st.Peers) != 0 || st.PendingSend != 0 || st.PendingApply != 0 {
		t.Fatalf("idle status: peers=%d send=%d apply=%d", len(st.Peers), st.PendingSend, st.PendingApply)
	}
	if st.PebbleSizeBytes == 0 {
		t.Fatalf("PebbleSizeBytes = 0")
	}
	if st.Metrics.LocalCommits != 3 {
		t.Fatalf("Status.Metrics.LocalCommits = %d, want 3", st.Metrics.LocalCommits)
	}
	// Pinned to the product constant, not a literal: the persistent
	// format version intentionally moves (2 -> 3 for the downgrade
	// guard), and this assertion covers the store-to-status plumbing.
	if st.FormatFormat != state.FormatVersion {
		t.Fatalf("Status.FormatFormat = %d, want %d", st.FormatFormat, state.FormatVersion)
	}
	if st.Replication.SessionsOpened != 0 {
		t.Fatalf("SessionsOpened = %d without replication", st.Replication.SessionsOpened)
	}
}

// TestReplicationDiagnostics proves two-node traffic, per-peer records, lag
// convergence, and membership-operation counters.
func TestReplicationDiagnostics(t *testing.T) {
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

	idA := NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		idA[:], "ann", "111"); err != nil {
		t.Fatal(err)
	}
	waitForValue(t, dbB, idA, "111", 15*time.Second)

	idB := NewRowID()
	if _, err := dbB.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		idB[:], "bob", "222"); err != nil {
		t.Fatal(err)
	}
	waitForValue(t, dbA, idB, "222", 15*time.Second)

	// Acks, pongs, and watermark convergence trail data; poll for them.
	deadline := time.Now().Add(15 * time.Second)
	for {
		stA, stB := dbA.Status(), dbB.Status()
		if stA.Replication.AcksReceived > 0 && stB.Replication.AcksReceived > 0 &&
			stA.Replication.PongsReceived > 0 && stB.Replication.PongsReceived > 0 &&
			lagSum(stA) == 0 && lagSum(stB) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("acks/pongs/lag never converged: A=%+v B=%+v",
				stA.Replication, stB.Replication)
		}
		time.Sleep(20 * time.Millisecond)
	}

	stA, stB := dbA.Status(), dbB.Status()
	for name, st := range map[string]Status{"A": stA, "B": stB} {
		r := st.Replication
		if r.SessionsOpened == 0 || r.FramesReceived == 0 {
			t.Fatalf("%s: sessions=%d frames=%d", name, r.SessionsOpened, r.FramesReceived)
		}
		if r.BatchesSent == 0 || r.BatchesReceived == 0 {
			t.Fatalf("%s: sent=%d received=%d", name, r.BatchesSent, r.BatchesReceived)
		}
		if r.MutationsSent == 0 || r.MutationsReceived == 0 {
			t.Fatalf("%s: muts sent=%d received=%d", name, r.MutationsSent, r.MutationsReceived)
		}
		if r.FrameBytesSent == 0 || r.FrameBytesReceived == 0 {
			t.Fatalf("%s: bytes sent=%d received=%d", name, r.FrameBytesSent, r.FrameBytesReceived)
		}
		if r.AcksSent == 0 || r.NeedsSent == 0 || r.PingsSent == 0 {
			t.Fatalf("%s: acks=%d needs=%d pings=%d", name, r.AcksSent, r.NeedsSent, r.PingsSent)
		}
		if st.ConnectedPeers != 1 || st.QUICConnections != 1 || len(st.Peers) != 1 {
			t.Fatalf("%s: connected=%d quic=%d peers=%d", name, st.ConnectedPeers, st.QUICConnections, len(st.Peers))
		}
		p := st.Peers[0]
		if !p.Connected || !p.SchemaAgreed {
			t.Fatalf("%s: peer %+v", name, p)
		}
		if p.LastHandshake.IsZero() || p.LastSend.IsZero() || p.LastRecv.IsZero() {
			t.Fatalf("%s: peer timestamps %+v", name, p)
		}
		if p.BytesSent == 0 || p.BytesReceived == 0 {
			t.Fatalf("%s: peer bytes %d/%d", name, p.BytesSent, p.BytesReceived)
		}
		if st.Metrics.RemoteApplies == 0 || st.Metrics.RemoteApplyMutations == 0 {
			t.Fatalf("%s: remote applies=%d muts=%d", name, st.Metrics.RemoteApplies, st.Metrics.RemoteApplyMutations)
		}
	}
	// A learned B through inbound discovery; B dialed A from static config.
	if !stA.Peers[0].Dynamic {
		t.Fatalf("A's peer should be dynamic: %+v", stA.Peers[0])
	}
	if stB.Peers[0].Dynamic {
		t.Fatalf("B's peer should be configured: %+v", stB.Peers[0])
	}
	if stA.Replication.Accepts == 0 {
		t.Fatalf("A accepts = 0")
	}
	if stB.Replication.Dials == 0 {
		t.Fatalf("B dials = 0")
	}

	// Membership operations are counted.
	if err := dbB.ForceSync(ctx, nodeA); err != nil {
		t.Fatal(err)
	}
	if got := dbB.Metrics().ForceSyncs; got != 1 {
		t.Fatalf("B ForceSyncs = %d, want 1", got)
	}
	if err := dbA.AddPeer(ctx, Peer{NodeID: nodeB, Addrs: []string{"127.0.0.1:1"}}); err != nil {
		t.Fatal(err)
	}
	if got := dbA.Metrics().PeersAdded; got != 1 {
		t.Fatalf("A PeersAdded = %d, want 1", got)
	}
}

func lagSum(st Status) uint64 {
	var sum uint64
	for _, p := range st.Peers {
		for _, lag := range p.LagByOrigin {
			sum += lag
		}
	}
	return sum
}
