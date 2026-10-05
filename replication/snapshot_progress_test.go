package replication

import (
	"context"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/state"
)

// snapshotTestManager builds an unstarted manager with a short snapshot
// request timeout and an installed context, so the stall watchdog and the
// busy-deferral path can be driven without network traffic.
func snapshotTestManager(t *testing.T, timeout time.Duration) *Manager {
	t.Helper()
	cluster := newTestCluster(t)
	localID := ids.NewNodeID()
	st, err := openSignedFixture(t.TempDir(), localID, cluster.dbid, state.Options{Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	mgr, err := NewManager(ManagerConfig{
		Store: st, Applier: &fakeApplier{}, Creds: cluster.creds(t, localID),
		Local: localID, DBID: cluster.dbid, Fanout: 2,
		SnapshotRequestTimeout: timeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mgr.ctx = ctx
	mgr.log = mgr.logger()
	return mgr
}

func liveTestSession() *peerSession {
	return &peerSession{done: make(chan struct{})}
}

func drainCtrl(t *testing.T, p *peerState) (ctrlFrame, bool) {
	t.Helper()
	select {
	case f := <-p.ctrlCh:
		if f.lease != nil {
			f.lease.Release()
		}
		return f, true
	default:
		return ctrlFrame{}, false
	}
}

// A snapshot wait with no progress past the timeout must be re-requested:
// the original request may have been lost, deferred, or abandoned.
func TestStalledSnapshotReRequested(t *testing.T) {
	mgr := snapshotTestManager(t, 50*time.Millisecond)
	peer := ids.NewNodeID()
	mgr.AddPeer(peer, []string{"127.0.0.1:19101"})
	mgr.mu.Lock()
	p := mgr.peers[peer]
	mgr.mu.Unlock()
	p.mu.Lock()
	p.session = liveTestSession()
	p.mu.Unlock()

	if already := markSnapshotRequestedFixture(mgr, p); already {
		t.Fatal("first request reported already-waiting")
	}
	p.mu.Lock()
	p.lastSnapProgress = time.Now().Add(-time.Hour)
	p.mu.Unlock()

	mgr.retryStalledSnapshots()

	f, ok := drainCtrl(t, p)
	if !ok || f.typ != MsgSnapshotRequest {
		t.Fatalf("stalled snapshot not re-requested (frame present=%v type=%d)", ok, f.typ)
	}
	if got := mgr.Stats().SnapshotReRequested; got != 1 {
		t.Fatalf("SnapshotReRequested=%d, want 1", got)
	}
	if got := mgr.Stats().SnapshotRequestsSent; got != 1 {
		t.Fatalf("SnapshotRequestsSent=%d, want 1", got)
	}
	p.mu.Lock()
	since := time.Since(p.lastSnapProgress)
	p.mu.Unlock()
	if since > 5*time.Second {
		t.Fatalf("re-request did not reset the progress timer (%v ago)", since)
	}
}

// A transfer that keeps receiving manifests/chunks must never be disturbed.
func TestMovingSnapshotNotReRequested(t *testing.T) {
	mgr := snapshotTestManager(t, 50*time.Millisecond)
	peer := ids.NewNodeID()
	mgr.AddPeer(peer, []string{"127.0.0.1:19102"})
	mgr.mu.Lock()
	p := mgr.peers[peer]
	mgr.mu.Unlock()
	p.mu.Lock()
	p.session = liveTestSession()
	p.mu.Unlock()
	markSnapshotRequestedFixture(mgr, p)
	mgr.markSnapshotProgress(p)

	mgr.retryStalledSnapshots()

	if f, ok := drainCtrl(t, p); ok {
		t.Fatalf("moving snapshot re-requested (type=%d)", f.typ)
	}
	if got := mgr.Stats().SnapshotReRequested; got != 0 {
		t.Fatalf("SnapshotReRequested=%d, want 0", got)
	}
}

// Source-requested busy backoff suppresses the watchdog until it lapses.
func TestSnapshotBusyBackoffHonored(t *testing.T) {
	mgr := snapshotTestManager(t, time.Millisecond)
	peer := ids.NewNodeID()
	mgr.AddPeer(peer, []string{"127.0.0.1:19103"})
	mgr.mu.Lock()
	p := mgr.peers[peer]
	mgr.mu.Unlock()
	p.mu.Lock()
	p.session = liveTestSession()
	p.mu.Unlock()
	markSnapshotRequestedFixture(mgr, p)

	mgr.onError(p, nil, EncodeError(nil, ErrSnapshotBusy, "snapshot already in flight"))
	if got := mgr.Stats().SnapshotBusyReceived; got != 1 {
		t.Fatalf("SnapshotBusyReceived=%d, want 1", got)
	}
	p.mu.Lock()
	backoff := p.snapRetryAfter
	p.lastSnapProgress = time.Now().Add(-time.Hour)
	p.mu.Unlock()
	if time.Until(backoff) <= 0 {
		t.Fatal("busy deferral did not set a future backoff")
	}

	mgr.retryStalledSnapshots()
	if f, ok := drainCtrl(t, p); ok {
		t.Fatalf("watchdog fired during busy backoff (type=%d)", f.typ)
	}

	// Once the backoff lapses the stalled wait is re-requested.
	p.mu.Lock()
	p.snapRetryAfter = time.Now().Add(-time.Second)
	p.mu.Unlock()
	mgr.retryStalledSnapshots()
	if f, ok := drainCtrl(t, p); !ok || f.typ != MsgSnapshotRequest {
		t.Fatalf("stalled wait not re-requested after backoff (present=%v)", ok)
	}
}

// No live session means redial/handshake owns recovery; the watchdog must
// not pile requests into a dead peer's queue.
func TestStalledSnapshotWithoutSessionNotReRequested(t *testing.T) {
	mgr := snapshotTestManager(t, time.Millisecond)
	peer := ids.NewNodeID()
	mgr.AddPeer(peer, []string{"127.0.0.1:19104"})
	mgr.mu.Lock()
	p := mgr.peers[peer]
	mgr.mu.Unlock()
	markSnapshotRequestedFixture(mgr, p)
	p.mu.Lock()
	p.lastSnapProgress = time.Now().Add(-time.Hour)
	p.mu.Unlock()

	mgr.retryStalledSnapshots()

	if f, ok := drainCtrl(t, p); ok {
		t.Fatalf("sessionless wait re-requested (type=%d)", f.typ)
	}
}

// A source with a transfer already in flight must answer with an explicit
// deferral instead of dropping the request silently.
func TestSnapshotBusySendsDeferral(t *testing.T) {
	mgr := snapshotTestManager(t, time.Second)
	peer := ids.NewNodeID()
	mgr.AddPeer(peer, []string{"127.0.0.1:19105"})
	mgr.mu.Lock()
	p := mgr.peers[peer]
	mgr.mu.Unlock()
	p.snapSend.Store(true)

	mgr.sendSnapshot(p, nil)

	f, ok := drainCtrl(t, p)
	if !ok || f.typ != MsgError {
		t.Fatalf("busy source sent no error frame (present=%v)", ok)
	}
	code, _, err := DecodeError(f.payload)
	if err != nil || code != ErrSnapshotBusy {
		t.Fatalf("deferral code=%d err=%v, want ErrSnapshotBusy", code, err)
	}
	if got := mgr.Stats().SnapshotsBusyDeferred; got != 1 {
		t.Fatalf("SnapshotsBusyDeferred=%d, want 1", got)
	}
}

// Repeat requests never extend the stall deadline.
func TestMarkSnapshotRequestedIdempotent(t *testing.T) {
	mgr := snapshotTestManager(t, time.Second)
	peer := ids.NewNodeID()
	mgr.AddPeer(peer, []string{"127.0.0.1:19106"})
	mgr.mu.Lock()
	p := mgr.peers[peer]
	mgr.mu.Unlock()

	if already := markSnapshotRequestedFixture(mgr, p); already {
		t.Fatal("first request reported already-waiting")
	}
	p.mu.Lock()
	first := p.awaitingSince
	p.mu.Unlock()
	if first.IsZero() {
		t.Fatal("first request did not stamp the wait start")
	}
	time.Sleep(5 * time.Millisecond)
	if already := markSnapshotRequestedFixture(mgr, p); !already {
		t.Fatal("repeat request did not report already-waiting")
	}
	p.mu.Lock()
	second := p.awaitingSince
	progress := p.lastSnapProgress
	p.mu.Unlock()
	if !second.Equal(first) || !progress.Equal(first) {
		t.Fatal("repeat request moved the stall deadline")
	}
}

// In-flight transfer progress is visible per peer.
func TestSnapshotProgressDiagnostics(t *testing.T) {
	mgr := snapshotTestManager(t, time.Second)
	peer := ids.NewNodeID()
	mgr.AddPeer(peer, []string{"127.0.0.1:19107"})
	mgr.mu.Lock()
	p := mgr.peers[peer]
	mgr.mu.Unlock()
	markSnapshotRequestedFixture(mgr, p)
	p.mu.Lock()
	p.snapRecv = &snapRecvState{
		manifest:       &codec.SnapshotManifest{ChunkCount: 7},
		chunksReceived: 3,
	}
	p.mu.Unlock()

	var found *PeerStatus
	for _, st := range mgr.PeerStatus() {
		if st.NodeID == peer {
			s := st
			found = &s
		}
	}
	if found == nil {
		t.Fatal("peer missing from PeerStatus")
	}
	if !found.AwaitingSnapshot {
		t.Fatal("AwaitingSnapshot=false during an active wait")
	}
	if found.SnapshotChunksReceived != 3 || found.SnapshotChunksTotal != 7 {
		t.Fatalf("snapshot progress=%d/%d, want 3/7",
			found.SnapshotChunksReceived, found.SnapshotChunksTotal)
	}
}
