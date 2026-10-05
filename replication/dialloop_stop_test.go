package replication

import (
	"context"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/state"
)

// Removing a peer must stop its detached dialLoop state so a later re-add
// owns the only dialer for that ID. Before the fix, RemovePeer deleted the
// peerState from the map but left its dialLoop running; re-adding created a
// second dialer and the two loops attached competing sessions, flapping
// ConnectedPeers after partition heal (tests-live/chaos-load).
func TestRemovePeerStopsDetachedDialState(t *testing.T) {
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
	})
	if err != nil {
		t.Fatal(err)
	}

	peer := ids.NewNodeID()
	mgr.AddPeer(peer, []string{"127.0.0.1:19001"})
	mgr.mu.Lock()
	first := mgr.peers[peer]
	mgr.mu.Unlock()
	if first == nil {
		t.Fatal("AddPeer did not create peer state")
	}

	mgr.RemovePeer(peer)
	if !first.stopped.Load() {
		t.Fatal("RemovePeer did not stop the detached peer state")
	}

	mgr.AddPeer(peer, []string{"127.0.0.1:19001"})
	mgr.mu.Lock()
	second := mgr.peers[peer]
	mgr.mu.Unlock()
	if second == nil || second == first {
		t.Fatal("re-add did not install a fresh peer state")
	}
	if second.stopped.Load() {
		t.Fatal("fresh peer state starts stopped")
	}

	// OnPeerLeft detaches the same way and must also stop the dialer.
	mgr.OnPeerLeft(peer)
	if !second.stopped.Load() {
		t.Fatal("OnPeerLeft did not stop the detached peer state")
	}
}

// A stopped dialLoop must exit promptly, both when already stopped at
// start and when stopped mid-run.
func TestDialLoopExitsWhenStopped(t *testing.T) {
	cluster := newTestCluster(t)
	localID := ids.NewNodeID()
	st, err := openSignedFixture(t.TempDir(), localID, cluster.dbid, state.Options{Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	mgr, err := NewManager(ManagerConfig{
		Store: st, Applier: &fakeApplier{}, Creds: cluster.creds(t, localID),
		Local: localID, DBID: cluster.dbid, Fanout: 2, DialInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Drive dialLoop directly without Run: install the manager context
	// Run would provide (no listener, no network).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.ctx = ctx

	preset := newPeerState(ids.NewNodeID(), nil, false)
	preset.stopped.Store(true)
	mgr.wg.Add(1)
	done := make(chan struct{})
	go func() { mgr.dialLoop(preset); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dialLoop with preset stopped did not return")
	}

	live := newPeerState(ids.NewNodeID(), nil, false)
	mgr.wg.Add(1)
	doneLive := make(chan struct{})
	go func() { mgr.dialLoop(live); close(doneLive) }()
	// Let the loop block in select, then stop it like RemovePeer does.
	time.Sleep(100 * time.Millisecond)
	live.stopped.Store(true)
	live.pokeWake()
	select {
	case <-doneLive:
	case <-time.After(5 * time.Second):
		t.Fatal("dialLoop did not exit after stop")
	}
}
