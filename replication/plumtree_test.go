package replication

import (
	"context"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/plumtree"
	"github.com/nomadsql/replicateddb/state"
)

func testPlumtreeManager(t *testing.T, count int) (*Manager, []*peerState) {
	t.Helper()
	engine, err := plumtree.New(plumtree.Config{EagerFanout: 2, MaxNeighbors: 8, MaxCacheEntries: 8, MaxCacheBytes: 1 << 20, CacheTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{peers: make(map[ids.NodeID]*peerState), selected: make(map[ids.NodeID]bool), excluded: make(map[ids.NodeID]bool), notifyCh: make(chan struct{}, 1), plumtree: engine}
	peers := make([]*peerState, 0, count)
	for i := 0; i < count; i++ {
		id := ids.NewNodeID()
		p := newPeerState(id, nil, false)
		p.plumtree = true
		p.agreed = true
		p.session = &peerSession{done: make(chan struct{})}
		p.selected = i < 2
		m.peers[id] = p
		peers = append(peers, p)
	}
	return m, peers
}

func TestPlumtreeFramesAndNegotiation(t *testing.T) {
	hint := PlumtreeHint{ID: [32]byte{1}, Origin: ids.NewNodeID(), Seq: 9}
	got, err := DecodePlumtreeHint(EncodePlumtreeHint(nil, hint))
	if err != nil || got != hint {
		t.Fatalf("hint round trip = %+v, %v", got, err)
	}
	if _, err := DecodePlumtreeHint([]byte{1}); err == nil {
		t.Fatal("accepted truncated hint")
	}
	if caps, err := NegotiateCapabilities(CapZstd | CapPlumtree); err != nil || caps != CapZstd|CapPlumtree {
		t.Fatalf("negotiated caps=%#x err=%v", caps, err)
	}
	if _, err := NegotiateCapabilities(CapRequiredMask | CapPlumtree | (uint64(1) << 40)); err == nil {
		t.Fatal("accepted unknown required capability")
	}
}

func TestPlumtreeStartPrefersSelectedPeersAndAdvertisesLazyPeers(t *testing.T) {
	m, peers := testPlumtreeManager(t, 5)
	b := &codec.MutationBatch{OriginNode: ids.NewNodeID(), Sequence: 1, TxID: ids.NewTxID()}
	m.plumtreeStart(ids.NodeID{}, b)
	var data, hints int
	for _, p := range peers {
		select {
		case frame := <-p.plumtreeCh:
			switch frame.typ {
			case MsgPlumtreeData:
				data++
			case MsgPlumtreeIHave:
				hints++
			default:
				t.Fatalf("unexpected frame %d", frame.typ)
			}
		default:
		}
	}
	if data != 2 || hints != 3 {
		t.Fatalf("eager data=%d lazy hints=%d, want 2 and 3", data, hints)
	}
}

func TestPlumtreeIHaveDelayAndGraftRepairFallback(t *testing.T) {
	m, peers := testPlumtreeManager(t, 1)
	p := peers[0]
	h := PlumtreeHint{ID: [32]byte{4}, Origin: ids.NewNodeID(), Seq: 12}
	if err := m.onPlumtreeIHave(p, EncodePlumtreeHint(nil, h)); err != nil {
		t.Fatal(err)
	}
	frame := <-p.plumtreeCh
	if frame.typ != MsgPlumtreeGraft || time.Until(frame.due) <= 0 {
		t.Fatalf("IHAVE response=%+v, expected delayed GRAFT", frame)
	}
	if err := m.onPlumtreeGraft(p, nil, EncodePlumtreeHint(nil, h)); err != nil {
		t.Fatal(err)
	}
	select {
	case queued := <-p.ctrlCh:
		if queued.typ != MsgNeed {
			t.Fatalf("cache miss fallback type=%d, want Need", queued.typ)
		}
		need, err := DecodeNeed(queued.payload)
		if err != nil || need.Origin != h.Origin || need.FromSeq != h.Seq {
			t.Fatalf("repair fallback=%+v err=%v", need, err)
		}
	default:
		t.Fatal("cache-miss GRAFT did not queue durable range repair")
	}
}

func TestPlumtreeDuplicatePrunesEagerSender(t *testing.T) {
	m, peers := testPlumtreeManager(t, 4)
	engine, err := plumtree.New(plumtree.Config{EagerFanout: 3, MaxNeighbors: 8, MaxCacheEntries: 8, MaxCacheBytes: 1 << 20, CacheTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	m.plumtree = engine
	ordered, protected := m.plumtreePeerSets(ids.NodeID{})
	m.plumtree.SetPeers(ordered, protected)
	eager, _ := m.plumtree.Neighbors()
	var sender ids.NodeID
	for _, id := range eager {
		if !containsNode(protected, id) {
			sender = id
			break
		}
	}
	if sender.IsZero() {
		t.Fatal("no unprotected eager sender available")
	}
	b := &codec.MutationBatch{OriginNode: ids.NewNodeID(), Sequence: 2, TxID: ids.NewTxID()}
	id, raw := plumtreeIdentity(b)
	if _, err := m.plumtree.Start(id, raw, ordered); err != nil {
		t.Fatal(err)
	}
	if err := m.plumtreeReceive(sender, b); err != nil {
		t.Fatal(err)
	}
	var senderPeer *peerState
	for _, peer := range peers {
		if peer.id == sender {
			senderPeer = peer
			break
		}
	}
	select {
	case frame := <-senderPeer.plumtreeCh:
		if frame.typ != MsgPlumtreePrune {
			t.Fatalf("duplicate response type=%d, want PRUNE", frame.typ)
		}
	default:
		t.Fatal("duplicate eager delivery did not queue PRUNE")
	}
}

func containsNode(peers []ids.NodeID, id ids.NodeID) bool {
	for _, peer := range peers {
		if peer == id {
			return true
		}
	}
	return false
}

func TestPlumtreeNegotiatedThreeNodeForwarding(t *testing.T) {
	cluster := newTestCluster(t)
	start := func(node ids.NodeID, fanout int) (*Manager, *state.Store, *fakeApplier) {
		t.Helper()
		st, err := state.Open(t.TempDir(), node, cluster.dbid, state.Options{Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100}})
		if err != nil {
			t.Fatal(err)
		}
		applier := &fakeApplier{}
		mgr, err := NewManager(ManagerConfig{
			Store: st, Applier: applier, Creds: cluster.creds(t, node), Local: node, DBID: cluster.dbid,
			SchemaEpoch: 1, SchemaHash: [32]byte{1}, ListenAddr: "127.0.0.1:0",
			Fanout: fanout, EnablePlumtree: true, SendInterval: 10 * time.Millisecond, AckInterval: 50 * time.Millisecond,
			Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100},
		})
		if err != nil {
			_ = st.Close()
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = mgr.Run(ctx); close(done) }()
		t.Cleanup(func() { cancel(); <-done; _ = st.Close() })
		deadline := time.Now().Add(5 * time.Second)
		for mgr.Addr() == "" && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if mgr.Addr() == "" {
			t.Fatal("manager listener did not start")
		}
		return mgr, st, applier
	}

	var aID, bID, cID ids.NodeID
	aID[0], cID[0], bID[0] = 1, 2, 3 // C can dial B; B's single selected target is A.
	a, storeA, _ := start(aID, 1)
	b, _, applyB := start(bID, 1)
	c, _, applyC := start(cID, 1)
	a.AddPeer(bID, []string{b.Addr()})
	b.AddPeer(aID, []string{a.Addr()})
	b.AddPeer(cID, []string{c.Addr()})
	c.AddPeer(bID, []string{b.Addr()})

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		ap, cp := b.peers[aID], b.peers[cID]
		b.mu.Unlock()
		connected := func(p *peerState) bool {
			if p == nil {
				return false
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			return p.session != nil && p.plumtree
		}
		if connected(ap) && connected(cp) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	b.mu.Lock()
	connectedA, connectedC := b.peers[aID], b.peers[cID]
	b.mu.Unlock()
	if connectedA == nil || connectedC == nil || !peerUsesPlumtree(connectedA) || !peerUsesPlumtree(connectedC) {
		t.Fatal("middle node did not negotiate Plumtree with both neighbors")
	}

	batch := &codec.MutationBatch{
		ProtocolVersion: ProtocolVersion, TxID: ids.NewTxID(), OriginNode: aID, Sequence: 1,
		HLC: 100, SchemaEpoch: 1, SchemaHash: [32]byte{1},
		Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Value{Type: codec.TypeText, S: "through-plumtree"}}},
	}
	if _, err := storeA.CommitLocal(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	a.NotifyLocal()
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if applyB.count() == 1 && applyC.count() == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("forwarding did not converge: middle=%d leaf=%d", applyB.count(), applyC.count())
}
