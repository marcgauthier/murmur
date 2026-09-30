package replication

import (
	"errors"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/overload"
)

// TestQueueDropCounters proves overload drops are counted. Drops are safe
// (peers re-request) but must stay observable.
func TestQueueDropCounters(t *testing.T) {
	m := &Manager{notifyCh: make(chan struct{}, 1)}
	p := newPeerState(ids.NewNodeID(), nil, false)

	for i := 0; i < cap(p.ctrlCh); i++ {
		m.queueCtrl(p, MsgPing, 0, nil)
	}
	m.queueCtrl(p, MsgPing, 0, nil)
	if got := m.Stats().CtrlDrops; got != 1 {
		t.Fatalf("CtrlDrops = %d, want 1", got)
	}

	need := EncodeNeed(nil, Need{Origin: ids.NewNodeID(), FromSeq: 1})
	for i := 0; i < cap(p.needCh); i++ {
		if err := m.onNeed(p, nil, need); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.onNeed(p, nil, need); err != nil {
		t.Fatal(err)
	}
	st := m.Stats()
	if st.NeedDrops != 1 {
		t.Fatalf("NeedDrops = %d, want 1", st.NeedDrops)
	}
	if st.NeedsReceived != uint64(cap(p.needCh)+1) {
		t.Fatalf("NeedsReceived = %d, want %d", st.NeedsReceived, cap(p.needCh)+1)
	}

	for i := 0; i < cap(p.schemaReqCh); i++ {
		m.queueSchemaRequest(p, SchemaRequest{WantCurrent: true})
	}
	m.queueSchemaRequest(p, SchemaRequest{WantCurrent: true})
	if got := m.Stats().SchemaReqDrops; got != 1 {
		t.Fatalf("SchemaReqDrops = %d, want 1", got)
	}

	req := EncodeSchemaRequest(nil, &SchemaRequest{WantCurrent: true})
	for i := 0; i < cap(p.schemaRespCh); i++ {
		if err := m.onSchemaRequest(p, req); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.onSchemaRequest(p, req); err != nil {
		t.Fatal(err)
	}
	m.queueSchemaResponse(p, SchemaRequest{WantCurrent: true})
	st = m.Stats()
	if st.SchemaRespDrops != 2 {
		t.Fatalf("SchemaRespDrops = %d, want 2", st.SchemaRespDrops)
	}
	if st.SchemaRequestsReceived != uint64(cap(p.schemaRespCh)+1) {
		t.Fatalf("SchemaRequestsReceived = %d, want %d", st.SchemaRequestsReceived, cap(p.schemaRespCh)+1)
	}
}

func TestNotifyLocalCoalescesWhenSendLoopIsBusy(t *testing.T) {
	m := &Manager{notifyCh: make(chan struct{}, 1)}
	m.notifyCh <- struct{}{}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			m.NotifyLocal()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("local commit notification blocked behind a full wake channel")
	}
	if len(m.notifyCh) != 1 {
		t.Fatalf("coalesced wake channel has %d entries", len(m.notifyCh))
	}
}

func TestPeerSendOrderRotates(t *testing.T) {
	peers := []*peerState{
		newPeerState(ids.NewNodeID(), nil, false),
		newPeerState(ids.NewNodeID(), nil, false),
		newPeerState(ids.NewNodeID(), nil, false),
	}
	first := rotatePeers(append([]*peerState(nil), peers...), "")
	second := rotatePeers(append([]*peerState(nil), peers...), first[0].id.String())
	if len(first) != len(second) || second[0].id == first[0].id {
		t.Fatalf("peer cursor did not advance: first=%s second=%s", first[0].id, second[0].id)
	}
}

func TestControlQueueBudgetLeaseReleasesAfterDrain(t *testing.T) {
	budget, err := overload.NewCounter(overload.Limit{Bytes: 24, Entries: 2, PeerBytes: 24, PeerEntries: 2, MaxPeers: 2})
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{notifyCh: make(chan struct{}, 1), queueBudget: budget}
	p := newPeerState(ids.NewNodeID(), nil, false)
	m.queueCtrl(p, MsgPing, 0, []byte{1, 2})
	if got := budget.Snapshot(); got.Bytes != 14 || got.Entries != 1 {
		t.Fatalf("queued budget=%+v", got)
	}
	f := <-p.ctrlCh
	if f.lease == nil {
		t.Fatal("queue frame has no budget lease")
	}
	f.lease.Release()
	if got := budget.Snapshot(); got != (overload.Snapshot{}) {
		t.Fatalf("released budget=%+v", got)
	}
}

func TestControlQueueBudgetRejectsAndReturnsRetryableError(t *testing.T) {
	budget, err := overload.NewCounter(overload.Limit{Bytes: 12, Entries: 1, PeerBytes: 12, PeerEntries: 1, MaxPeers: 1})
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{notifyCh: make(chan struct{}, 1), queueBudget: budget}
	p := newPeerState(ids.NewNodeID(), nil, false)
	m.queueCtrl(p, MsgPing, 0, nil)
	m.queueCtrl(p, MsgPong, 0, nil)
	if len(p.ctrlCh) != 1 || len(p.retryCh) != 1 {
		t.Fatalf("ctrl/retry queue depths=%d/%d", len(p.ctrlCh), len(p.retryCh))
	}
	code, _, err := DecodeError((<-p.retryCh).payload)
	if err != nil || code != ErrOverloadedCode {
		t.Fatalf("retry response code=%d err=%v", code, err)
	}
	f := <-p.ctrlCh
	f.lease.Release()
	if got := budget.Snapshot(); got != (overload.Snapshot{}) {
		t.Fatalf("budget after drain=%+v", got)
	}
}

func TestNeedQueueBudgetReturnsRetryHint(t *testing.T) {
	budget, err := overload.NewCounter(overload.Limit{Bytes: 32, Entries: 1, PeerBytes: 32, PeerEntries: 1, MaxPeers: 1})
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{notifyCh: make(chan struct{}, 1), queueBudget: budget}
	p := newPeerState(ids.NewNodeID(), nil, false)
	need := Need{Origin: ids.NewNodeID(), FromSeq: 7}
	m.queueCtrl(p, MsgPing, 0, nil)
	if err := m.onNeed(p, nil, EncodeNeed(nil, need)); err != nil {
		t.Fatal(err)
	}
	frame := <-p.retryCh
	code, msg, err := DecodeError(frame.payload)
	if err != nil || code != ErrOverloadedCode || msg != requestRetryHint(need.Origin, need.FromSeq) {
		t.Fatalf("retry response code=%d msg=%q err=%v", code, msg, err)
	}
}

func TestOverloadRequestRetryIsScheduledWithoutRewindingSendCursor(t *testing.T) {
	m := &Manager{notifyCh: make(chan struct{}, 1), chunkRepairNeed: make(map[ids.TxID]ChunkNeed)}
	p := newPeerState(ids.NewNodeID(), nil, false)
	origin := ids.NewNodeID()
	p.sent[origin] = 12
	m.onError(p, nil, EncodeError(nil, ErrOverloadedCode, requestRetryHint(origin, 7)))
	select {
	case frame := <-p.ctrlCh:
		if frame.typ != MsgNeed || frame.due.IsZero() || time.Until(frame.due) <= 0 {
			t.Fatalf("request retry frame = %+v", frame)
		}
		need, err := DecodeNeed(frame.payload)
		if err != nil || need.Origin != origin || need.FromSeq != 7 {
			t.Fatalf("retry need = %+v, err=%v", need, err)
		}
	default:
		t.Fatal("overloaded request was not scheduled for retry")
	}
	if p.sent[origin] != 12 {
		t.Fatalf("request rejection rewound send cursor to %d", p.sent[origin])
	}
}

func TestOverloadSendRetryRewindsRejectedSequence(t *testing.T) {
	m := &Manager{notifyCh: make(chan struct{}, 1)}
	p := newPeerState(ids.NewNodeID(), nil, false)
	origin := ids.NewNodeID()
	p.sent[origin] = 12
	m.onError(p, nil, EncodeError(nil, ErrOverloadedCode, retryHint(origin, 7)))
	if p.sent[origin] != 6 || time.Until(p.retryAfter[origin]) <= 0 {
		t.Fatalf("send retry state sent=%d retryAfter=%v", p.sent[origin], p.retryAfter[origin])
	}
}

func TestTransferRateLimiterRejectsImpossibleBurst(t *testing.T) {
	limiter, err := overload.NewRateLimiter(100, 50, 100, 50, 2)
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{transferLimiter: limiter}
	p := newPeerState(ids.NewNodeID(), nil, false)
	if err := m.waitTransfer(p, 51); !errors.Is(err, overload.ErrOverloaded) {
		t.Fatalf("oversized transfer returned %v", err)
	}
}

// TestStatsGauges proves Stats aggregates peer counts, sessions, and queue
// depths across peers.
func TestStatsGauges(t *testing.T) {
	p1 := newPeerState(ids.NewNodeID(), nil, false)
	p1.session = &peerSession{}
	p1.needCh <- queuedNeed{need: Need{Origin: ids.NewNodeID(), FromSeq: 1}}
	p1.ctrlCh <- ctrlFrame{typ: MsgPing}
	p2 := newPeerState(ids.NewNodeID(), nil, true)
	m := &Manager{peers: map[ids.NodeID]*peerState{p1.id: p1, p2.id: p2}}

	st := m.Stats()
	if st.PeerCount != 2 || st.ConnectedPeers != 1 {
		t.Fatalf("peers = %d/%d, want 2/1", st.PeerCount, st.ConnectedPeers)
	}
	if st.GatingMembers != 0 {
		t.Fatalf("GatingMembers = %d without a store, want 0", st.GatingMembers)
	}
	if st.QueuedNeed != 1 || st.QueuedCtrl != 1 {
		t.Fatalf("queued need/ctrl = %d/%d, want 1/1", st.QueuedNeed, st.QueuedCtrl)
	}
	if st.QueuedSchemaReq != 0 || st.QueuedSchemaResp != 0 {
		t.Fatalf("queued schema = %d/%d, want 0/0", st.QueuedSchemaReq, st.QueuedSchemaResp)
	}
	if n := len(m.PeerStatus()); n != 2 {
		t.Fatalf("PeerStatus len = %d, want 2", n)
	}
	for _, ps := range m.PeerStatus() {
		if ps.NodeID == p1.id && (ps.QueuedNeed != 1 || ps.QueuedCtrl != 1 || !ps.Connected || ps.Dynamic) {
			t.Fatalf("p1 status = %+v", ps)
		}
		if ps.NodeID == p2.id && (ps.Connected || !ps.Dynamic) {
			t.Fatalf("p2 status = %+v", ps)
		}
	}
}
