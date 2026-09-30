package replication

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/state"
	"github.com/marcgauthier/murmur/transport"
)

// TestManagerAccessors pins pool/membership accessors and the exclusion
// lifecycle that gates log GC.
func TestManagerAccessors(t *testing.T) {
	cluster := newTestCluster(t)
	node := ids.NewNodeID()
	mgr, _, _ := cluster.testManager(t, node, 1)

	if mgr.Pool() == nil {
		t.Fatal("Pool is nil, want manager-owned pool")
	}
	if mgr.Membership() != nil {
		t.Fatal("fresh Membership non-nil")
	}
	mgr.SetMembership(&MembershipService{})
	if mgr.Membership() == nil {
		t.Fatal("SetMembership did not install")
	}
	mgr.SetMembership(nil)
	if mgr.Membership() != nil {
		t.Fatal("SetMembership(nil) did not clear")
	}

	if got := mgr.ExcludedPeers(); len(got) != 0 {
		t.Fatalf("ExcludedPeers = %v, want empty", got)
	}
	peer := ids.NewNodeID()
	mgr.RemovePeer(peer)
	if got := mgr.ExcludedPeers(); len(got) != 1 || got[0] != peer {
		t.Fatalf("ExcludedPeers = %v", got)
	}
	mgr.AddPeer(peer, nil)
	if got := mgr.ExcludedPeers(); len(got) != 0 {
		t.Fatalf("after re-add ExcludedPeers = %v", got)
	}
	if mgr.IsPeerExcluded(peer) {
		t.Fatal("re-added peer still excluded")
	}
	// Self add/remove are silent no-ops.
	mgr.AddPeer(node, nil)
	mgr.RemovePeer(node)
}

// TestConfiguredPeers proves non-removed peers are listed for log-GC floors.
func TestConfiguredPeers(t *testing.T) {
	cluster := newTestCluster(t)
	mgr, _, _ := cluster.testManager(t, ids.NewNodeID(), 1)
	a, b := ids.NewNodeID(), ids.NewNodeID()
	mgr.AddPeer(a, nil)
	mgr.AddPeer(b, nil)
	mgr.RemovePeer(a)
	got := mgr.ConfiguredPeers()
	if len(got) != 1 || got[0].NodeID != b {
		t.Fatalf("ConfiguredPeers = %+v", got)
	}
}

// TestRefreshSchema proves a locally migrated identity installs and resets
// session agreement without any live sessions.
func TestRefreshSchema(t *testing.T) {
	cluster := newTestCluster(t)
	mgr, _, _ := cluster.testManager(t, ids.NewNodeID(), 1)
	peer := ids.NewNodeID()
	mgr.AddPeer(peer, nil)
	var hash [32]byte
	hash[0] = 2
	mgr.RefreshSchema(SchemaIdentity{Epoch: 2, Hash: hash})
	if id := mgr.currentSchema(); id.Epoch != 2 || id.Hash != hash {
		t.Fatalf("currentSchema = %+v", id)
	}
}

// TestForceSyncPaths covers pre-dial validation and the no-address
// anti-entropy short-circuit (no network).
func TestForceSyncPaths(t *testing.T) {
	cluster := newTestCluster(t)
	node := ids.NewNodeID()
	mgr, _, _ := cluster.testManager(t, node, 1)
	if err := mgr.ForceSync(context.Background(), node); err == nil {
		t.Fatal("ForceSync(local) succeeded")
	}
	unknown := ids.NewNodeID()
	if err := mgr.ForceSync(context.Background(), unknown); !errors.Is(err, ErrPeerNotFound) {
		t.Fatalf("ForceSync(unknown) = %v", err)
	}
	mgr.RemovePeer(unknown)
	if err := mgr.ForceSync(context.Background(), unknown); !errors.Is(err, ErrPeerExcluded) {
		t.Fatalf("ForceSync(excluded) = %v", err)
	}
	peer := ids.NewNodeID()
	mgr.AddPeer(peer, nil)
	if err := mgr.ForceSync(context.Background(), peer); err != nil {
		t.Fatalf("ForceSync(no addrs) = %v", err)
	}
}

// TestHandleSelectedPeerFailure proves failed selected peers are deselected
// and unknown IDs are ignored.
func TestHandleSelectedPeerFailure(t *testing.T) {
	m := &Manager{
		peers:    map[ids.NodeID]*peerState{},
		excluded: map[ids.NodeID]bool{},
		selected: map[ids.NodeID]bool{},
	}
	m.handleSelectedPeerFailure(ids.NewNodeID()) // no-op
	peer := ids.NewNodeID()
	p := newPeerState(peer, []string{"127.0.0.1:1"}, false)
	p.selected = true
	m.peers[peer] = p
	m.selected[peer] = true
	m.handleSelectedPeerFailure(peer)
	if m.selected[peer] || p.selected {
		t.Fatal("failed peer still selected")
	}
}

// TestHandshakeOutboundLive performs a real outbound handshake between two
// managers over loopback QUIC.
func TestHandshakeOutboundLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cluster := newTestCluster(t)
	nodeA, nodeB := ids.NewNodeID(), ids.NewNodeID()
	mgrA, _, _ := cluster.testManager(t, nodeA, 1)
	_, _, addrB := cluster.testManager(t, nodeB, 1)

	sess, err := transport.Dial(ctx, addrB, cluster.creds(t, nodeA), nodeB)
	if err != nil {
		t.Fatal(err)
	}
	p := newPeerState(nodeB, []string{addrB}, false)
	if !mgrA.handshakeOutbound(p, sess) {
		t.Fatal("handshakeOutbound failed")
	}
	p.mu.Lock()
	attached := p.session != nil && p.agreed
	p.mu.Unlock()
	if !attached {
		t.Fatal("session not attached/agreed after handshake")
	}
}

// TestSendErrorUnwrap pins send-error transparency for deadline matching.
func TestSendErrorUnwrap(t *testing.T) {
	inner := errors.New("i/o timeout")
	wrapped := &sendError{err: inner}
	if !errors.Is(wrapped, inner) {
		t.Fatal("Unwrap does not expose inner error")
	}
	if !strings.Contains(wrapped.Error(), "i/o timeout") {
		t.Fatalf("Error = %q", wrapped.Error())
	}
	var l nilLogger
	l.Debug("d")
	l.Info("i")
	l.Warn("w")
	l.Error("e")
}

// TestUnionSchemaIDs pins the schema-ID set union helper.
func TestUnionSchemaIDs(t *testing.T) {
	if out := unionSchemaIDs(nil, nil); out != nil {
		t.Fatalf("empty union = %v", out)
	}
	var a, b, c [32]byte
	a[0], b[0], c[0] = 1, 2, 3
	out := unionSchemaIDs([][32]byte{a, b}, [][32]byte{b, c})
	if len(out) != 3 {
		t.Fatalf("union = %d ids, want 3", len(out))
	}
}

// TestRequestProgressQueues proves progress requests are queued only for
// sessions that negotiated paging.
func TestRequestProgressQueues(t *testing.T) {
	m := &Manager{}
	p := newPeerState(ids.NewNodeID(), nil, false)
	m.requestProgress(p, ids.NodeID{})
	if len(p.ctrlCh) != 0 {
		t.Fatal("unnegotiated peer queued a request")
	}
	p.progressPages = true
	m.requestProgress(p, ids.NewNodeID())
	if len(p.ctrlCh) != 0 {
		t.Fatal("sessionless peer queued a request")
	}
	p.session = &peerSession{}
	m.requestProgress(p, ids.NodeID{})
	if len(p.ctrlCh) != 1 {
		t.Fatalf("ctrlCh depth = %d, want 1", len(p.ctrlCh))
	}
}

// TestPeerSessionSendDataClosed proves sends fail fast on closed sessions.
func TestPeerSessionSendDataClosed(t *testing.T) {
	ps := &peerSession{done: make(chan struct{})}
	close(ps.done)
	if err := ps.sendData(0, []byte("x")); err == nil {
		t.Fatal("sendData on closed session succeeded")
	}
	if err := ps.sendCtrl(MsgNeed, 0, []byte("x")); err == nil {
		t.Fatal("sendCtrl on closed session succeeded")
	}
}

// TestMembershipStreamFrame pins memberlist-stream detection on the
// replication framing path.
func TestMembershipStreamFrame(t *testing.T) {
	var msErr *ErrMembershipStream
	if msErr.Error() == "" {
		t.Fatal("empty Error string")
	}
	_, err := ReadFrame(bytes.NewReader([]byte("SPED12345678")))
	if !errors.As(err, &msErr) {
		t.Fatalf("ReadFrame(SPED) = %v, want ErrMembershipStream", err)
	}
	if len(msErr.Prefix) != frameHdrLen {
		t.Fatalf("prefix len = %d", len(msErr.Prefix))
	}
}

// TestServeChunkNeedValidation covers capability and payload validation
// without any staged data.
func TestServeChunkNeedValidation(t *testing.T) {
	m := &Manager{st: stats{}}
	p := newPeerState(ids.NewNodeID(), nil, false)
	ps := &peerSession{done: make(chan struct{})}
	if err := m.serveChunkNeed(p, ps, []byte("junk")); err == nil {
		t.Fatal("unnegotiated chunks accepted")
	}
	p.mu.Lock()
	p.caps = CapTransactionChunks
	p.mu.Unlock()
	if err := m.serveChunkNeed(p, ps, []byte("junk")); err == nil {
		t.Fatal("malformed chunk need accepted")
	}
}

// TestServeChunkNeedMissingData proves an unsatisfiable need against a live
// store ends in an error (no network involved).
func TestServeChunkNeedMissingData(t *testing.T) {
	st, err := state.Open(t.TempDir(), ids.NewNodeID(), ids.NewDBID(), state.Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	m := &Manager{st: stats{}, cfg: ManagerConfig{Store: st, MaxTransactionBytes: 1 << 20}}
	p := newPeerState(ids.NewNodeID(), nil, false)
	p.mu.Lock()
	p.caps = CapTransactionChunks
	p.mu.Unlock()
	ps := &peerSession{done: make(chan struct{})}
	close(ps.done) // any send attempt fails fast
	need := EncodeChunkNeed(nil, ChunkNeed{
		Origin: ids.NewNodeID(), Sequence: 7, TxID: ids.NewTxID(), Missing: []uint32{0},
	})
	if err := m.serveChunkNeed(p, ps, need); err == nil {
		t.Fatal("missing chunk data returned nil")
	}
}

// TestSendBatchOrChunksPaths covers the small-batch send attempt and the
// oversized-without-capability rejection.
func TestSendBatchOrChunksPaths(t *testing.T) {
	m := &Manager{st: stats{}}
	p := newPeerState(ids.NewNodeID(), nil, false)
	ps := &peerSession{done: make(chan struct{})}
	close(ps.done)
	small := validTestBatch(ids.NewNodeID(), 1)
	if err := m.sendBatchOrChunks(p, ps, small); err == nil {
		t.Fatal("send on closed session succeeded")
	}
	big := validTestBatch(ids.NewNodeID(), 2)
	big.Mutations[0].Value = codec.Text(strings.Repeat("x", MaxFrameBytes))
	if err := m.sendBatchOrChunks(p, ps, big); err == nil {
		t.Fatal("oversized batch without capability accepted")
	}
}

// TestOnSchemaManifestNoSync proves manifests without a sync engine report
// mismatch instead of stalling.
func TestOnSchemaManifestNoSync(t *testing.T) {
	m := &Manager{st: stats{}}
	p := newPeerState(ids.NewNodeID(), nil, false)
	if err := m.onSchemaManifest(p, []byte("junk")); err == nil {
		t.Fatal("malformed manifest accepted")
	}
	rev, err := schema.NewGenesis([]schema.TableSchema{{
		Name:    "t",
		Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}},
	}}, 1, ids.NewNodeID(), 1)
	if err != nil {
		t.Fatal(err)
	}
	raw := EncodeSchemaManifest(nil, &SchemaManifestMsg{Revisions: []*schema.Manifest{rev}})
	if err := m.onSchemaManifest(p, raw); err != nil {
		t.Fatalf("manifest without sync = %v", err)
	}
	if m.st.schemaConflicts.Load() != 1 {
		t.Fatalf("conflicts = %d", m.st.schemaConflicts.Load())
	}
	if len(p.ctrlCh) != 1 {
		t.Fatalf("ctrlCh depth = %d, want mismatch report", len(p.ctrlCh))
	}
}

// TestOnSchemaAckPaths covers agreement on exact identity and re-sync on
// mismatch, plus malformed input.
func TestOnSchemaAckPaths(t *testing.T) {
	mk := func() (*Manager, *peerState) {
		return &Manager{st: stats{}}, newPeerState(ids.NewNodeID(), nil, false)
	}
	m, p := mk()
	if err := m.onSchemaAck(p, []byte("junk")); err == nil {
		t.Fatal("malformed ack accepted")
	}
	m, p = mk()
	var hash [32]byte
	hash[0] = 9
	m.schemaId = SchemaIdentity{Epoch: 4, Hash: hash}
	if err := m.onSchemaAck(p, EncodeSchemaAck(nil, &SchemaAck{Version: 4, Hash: hash})); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	agreed := p.agreed
	p.mu.Unlock()
	if !agreed {
		t.Fatal("exact ack did not agree")
	}
	m, p = mk()
	m.schemaId = SchemaIdentity{Epoch: 4, Hash: hash}
	if err := m.onSchemaAck(p, EncodeSchemaAck(nil, &SchemaAck{Version: 5, Hash: hash})); err != nil {
		t.Fatal(err)
	}
	if len(p.schemaReqCh) != 1 {
		t.Fatalf("schemaReqCh depth = %d, want re-sync request", len(p.schemaReqCh))
	}
}

// TestSnapRecvLifecycle pins receiver-state allocation stability.
func TestSnapRecvLifecycle(t *testing.T) {
	m := &Manager{st: stats{}}
	p := newPeerState(ids.NewNodeID(), nil, false)
	a, b := m.snapRecvFor(p), m.snapRecvFor(p)
	if a == nil || a != b {
		t.Fatal("snapRecvFor not stable")
	}
}

func testChunkPayload(t *testing.T) []byte {
	t.Helper()
	row := ids.NewRowID()
	return EncodeSnapshotChunk(nil, &SnapshotChunk{
		Index: 0,
		Cells: []codec.SnapshotCell{{
			TableID: 1, RowID: row, ColumnID: 2,
			Version: crdt.Version{HLC: 100, NodeID: ids.NewNodeID()},
			Value:   codec.Text("v"),
		}},
	})
}

// TestOnSnapshotChunkPaths covers decode failures, zstd failures, missing
// manifests, and one applied chunk.
func TestOnSnapshotChunkPaths(t *testing.T) {
	mk := func(applier Applier) (*Manager, *peerState) {
		return &Manager{st: stats{}, cfg: ManagerConfig{
			Applier: applier, Limits: codec.DefaultLimits(), SnapshotChunkCells: 128,
		}}, newPeerState(ids.NewNodeID(), nil, false)
	}
	m, p := mk(&fakeApplier{})
	if err := m.onSnapshotChunk(p, nil, &Frame{Payload: []byte("junk")}); err == nil {
		t.Fatal("malformed chunk accepted")
	}
	m, p = mk(&fakeApplier{})
	if err := m.onSnapshotChunk(p, nil, &Frame{Flags: FlagZstd, Payload: []byte("junk")}); err == nil {
		t.Fatal("malformed zstd chunk accepted")
	}
	m, p = mk(&fakeApplier{})
	raw := testChunkPayload(t)
	if err := m.onSnapshotChunk(p, nil, &Frame{Payload: raw}); err == nil {
		t.Fatal("chunk without manifest accepted")
	}
	m, p = mk(&fakeApplier{})
	m.snapRecvFor(p).manifest = &codec.SnapshotManifest{}
	if err := m.onSnapshotChunk(p, nil, &Frame{Payload: raw}); err != nil {
		t.Fatalf("valid chunk = %v", err)
	}
	if got := m.snapRecvFor(p).chunksReceived; got != 1 {
		t.Fatalf("chunksReceived = %d", got)
	}
}

// TestOnSnapshotDonePaths covers both the re-request branch (dangling
// manifest) and the already-complete no-op.
func TestOnSnapshotDonePaths(t *testing.T) {
	mk := func() (*Manager, *peerState) {
		return &Manager{st: stats{}}, newPeerState(ids.NewNodeID(), nil, false)
	}
	m, p := mk()
	if err := m.onSnapshotDone(p, nil); err != nil {
		t.Fatal(err)
	}
	m, p = mk()
	m.snapRecvFor(p).manifest = &codec.SnapshotManifest{}
	if err := m.onSnapshotDone(p, nil); err != nil {
		t.Fatal(err)
	}
	if m.st.snapshotReRequested.Load() != 1 || len(p.ctrlCh) != 1 {
		t.Fatal("dangling manifest did not re-request")
	}
}

// TestOnPlumtreePruneRefused proves prune without negotiation fails closed.
func TestOnPlumtreePruneRefused(t *testing.T) {
	m := &Manager{}
	p := newPeerState(ids.NewNodeID(), nil, false)
	if err := m.onPlumtreePrune(p, []byte("junk")); err == nil {
		t.Fatal("unnegotiated prune accepted")
	}
}

func testMembershipService() *MembershipService {
	return &MembershipService{
		localID:      ids.NewNodeID(),
		dbid:         ids.NewDBID(),
		aliveMembers: make(map[ids.NodeID]NodeMetadata),
		eventCh:      make(chan MembershipEvent, 8),
		log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func memberNode(id ids.NodeID, dbid ids.DBID, withMeta bool) *memberlist.Node {
	n := &memberlist.Node{Name: id.String(), Addr: net.ParseIP("127.0.0.1"), Port: 9000}
	if withMeta {
		n.Meta = EncodeNodeMetadata(&NodeMetadata{DBID: dbid, Capabilities: 7, Endpoint: "127.0.0.1:9001"})
	}
	return n
}

// TestMembershipDelegates covers the SWIM delegate surface: conflict
// accounting, update validation and enqueueing, and queue-full drops.
func TestMembershipDelegates(t *testing.T) {
	s := testMembershipService()
	s.NotifyMsg([]byte("ignored"))
	s.NotifyConflict(&memberlist.Node{}, &memberlist.Node{})
	if s.Stats().Suspicions != 1 {
		t.Fatalf("suspicions = %d", s.Stats().Suspicions)
	}

	peer := ids.NewNodeID()
	s.NotifyUpdate(memberNode(peer, s.dbid, true))
	select {
	case evt := <-s.eventCh:
		if evt.Type != EventPeerUpdated || evt.NodeID != peer || evt.Endpoint != "127.0.0.1:9001" {
			t.Fatalf("event = %+v", evt)
		}
	default:
		t.Fatal("valid update not enqueued")
	}
	// Bare node without metadata falls back to Addr:Port.
	s.NotifyUpdate(memberNode(ids.NewNodeID(), s.dbid, false))
	select {
	case evt := <-s.eventCh:
		if evt.Endpoint != "127.0.0.1:9000" {
			t.Fatalf("endpoint = %q", evt.Endpoint)
		}
	default:
		t.Fatal("bare update not enqueued")
	}
	for _, tc := range []struct {
		name string
		node *memberlist.Node
	}{
		{"bad-name", &memberlist.Node{Name: "not-a-uuid", Addr: net.ParseIP("127.0.0.1"), Port: 1}},
		{"self", memberNode(s.localID, s.dbid, true)},
		{"foreign-dbid", memberNode(ids.NewNodeID(), ids.NewDBID(), true)},
	} {
		s.NotifyUpdate(tc.node)
		select {
		case evt := <-s.eventCh:
			t.Fatalf("%s: unexpected event %+v", tc.name, evt)
		default:
		}
	}

	// Unbuffered queue with no receiver drops and counts.
	blocked := testMembershipService()
	blocked.eventCh = make(chan MembershipEvent)
	blocked.NotifyUpdate(memberNode(ids.NewNodeID(), blocked.dbid, true))
	if blocked.Stats().EventDrops != 1 {
		t.Fatalf("drops = %d", blocked.Stats().EventDrops)
	}
}

// TestMembershipAccessors pins the read-only membership surface and the
// no-memberlist reconcile shortcut.
func TestMembershipAccessors(t *testing.T) {
	s := testMembershipService()
	if s.Transport() != nil {
		t.Fatal("nil transport reported non-nil")
	}
	if got := s.Members(); len(got) != 0 {
		t.Fatalf("Members = %v", got)
	}
	peer := ids.NewNodeID()
	s.aliveMembers[peer] = NodeMetadata{Endpoint: "e"}
	if s.MemberState(peer) != "alive" || s.MemberState(ids.NewNodeID()) != "unknown" {
		t.Fatal("MemberState wrong")
	}
	if got := s.Members(); len(got) != 1 || got[peer].Endpoint != "e" {
		t.Fatalf("Members = %v", got)
	}
	st := s.Stats()
	if st.NumMembers != 0 || st.NumAlive != 1 {
		t.Fatalf("Stats = %+v", st)
	}
	s.reconcile() // no memberlist attached: no-op
	if s.NumMembers() != 0 {
		t.Fatal("NumMembers without memberlist non-zero")
	}
}
