package plumtree

import (
	"errors"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/ids"
)

func testEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := New(Config{EagerFanout: 2, MaxNeighbors: 4, MaxCacheEntries: 2, MaxCacheBytes: 16, CacheTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestStartSelectsBoundedEagerAndLazyPeers(t *testing.T) {
	e := testEngine(t)
	peers := []ids.NodeID{ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID()}
	id := [32]byte{1}
	plan, err := e.Start(id, []byte("payload"), peers)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Eager) != 2 || len(plan.Lazy) != 2 || string(plan.Payload) != "payload" {
		t.Fatalf("unexpected initial plan: eager=%d lazy=%d payload=%q", len(plan.Eager), len(plan.Lazy), plan.Payload)
	}
	if got, want := len(mapIDs(plan.Eager, plan.Lazy)), len(peers); got != want {
		t.Fatalf("covered peers=%d want=%d", got, want)
	}
}

func TestReceiveDuplicatePrunesEagerAndGraftReturnsCachedPayload(t *testing.T) {
	e := testEngine(t)
	from, other := ids.NewNodeID(), ids.NewNodeID()
	id := [32]byte{2}
	if _, err := e.Start(id, []byte("tx"), []ids.NodeID{from, other}); err != nil {
		t.Fatal(err)
	}
	duplicate, err := e.Receive(id, []byte("tx"), from)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || !duplicate.Prune {
		t.Fatalf("duplicate plan = %+v", duplicate)
	}
	e.Prune(other)
	graft := e.Graft(id, other)
	if !graft.Graft || len(graft.Payload) != 2 || string(graft.Payload) != "tx" {
		t.Fatalf("graft plan = %+v", graft)
	}
	eager, lazy := e.Neighbors()
	if len(eager) != 1 || len(lazy) != 1 {
		t.Fatalf("neighbor sets eager=%d lazy=%d", len(eager), len(lazy))
	}
}

func TestReceiveForwardsOnlyToEagerNeighbors(t *testing.T) {
	e := testEngine(t)
	a, b := ids.NewNodeID(), ids.NewNodeID()
	_, _ = e.Start([32]byte{3}, []byte("seed"), []ids.NodeID{a, b})
	e.Prune(b)
	id := [32]byte{4}
	plan, err := e.Receive(id, []byte("new"), a)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Eager) != 0 || len(plan.Lazy) != 1 || plan.Lazy[0] != b {
		t.Fatalf("forward plan = %+v", plan)
	}
	if again, err := e.Receive(id, []byte("new"), a); err != nil || !again.Duplicate || !again.Prune {
		t.Fatalf("duplicate from newly promoted eager peer = %+v err=%v", again, err)
	}
}

func TestCacheBoundsPayloadBytesAndEntries(t *testing.T) {
	e := testEngine(t)
	if _, err := e.Start([32]byte{1}, make([]byte, 17), nil); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversize error = %v", err)
	}
	for i := byte(1); i <= 3; i++ {
		if _, err := e.Start([32]byte{i}, []byte("12345678"), nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.Graft([32]byte{1}, ids.NewNodeID()); len(got.Payload) != 0 {
		t.Fatalf("oldest cache entry retained: %q", got.Payload)
	}
	if got := e.Graft([32]byte{3}, ids.NewNodeID()); string(got.Payload) != "12345678" {
		t.Fatalf("newest cache entry absent: %q", got.Payload)
	}
}

func TestNeighborAndIdentityBounds(t *testing.T) {
	e := testEngine(t)
	peers := []ids.NodeID{ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID()}
	if _, err := e.Start([32]byte{8}, []byte("payload"), peers); err != nil {
		t.Fatal(err)
	}
	eager, lazy := e.Neighbors()
	if len(eager)+len(lazy) != 4 {
		t.Fatalf("neighbor count=%d, want max 4", len(eager)+len(lazy))
	}
	if _, err := e.Receive([32]byte{8}, []byte("different"), peers[0]); !errors.Is(err, ErrConflictingPayload) {
		t.Fatalf("conflicting payload error=%v", err)
	}
	for i := 0; i < 12; i++ {
		_ = e.Graft([32]byte{byte(20 + i)}, ids.NewNodeID())
	}
	eager, lazy = e.Neighbors()
	if len(eager)+len(lazy) > 4 {
		t.Fatalf("control messages grew neighbor sets to %d", len(eager)+len(lazy))
	}
}

func TestProtectedRingNeighborsStayEager(t *testing.T) {
	e := testEngine(t)
	p1, p2, p3 := ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID()
	e.SetPeers([]ids.NodeID{p1, p2, p3}, []ids.NodeID{p2, p3})
	eager, lazy := e.Neighbors()
	if len(eager) != 2 || len(lazy) != 1 || !has(eager, p2) || !has(eager, p3) {
		t.Fatalf("protected neighbor sets eager=%v lazy=%v", eager, lazy)
	}
	e.Prune(p2)
	eager, _ = e.Neighbors()
	if !has(eager, p2) {
		t.Fatal("PRUNE demoted a protected ring neighbor")
	}
	id := [32]byte{31}
	if _, err := e.Start(id, []byte("ring"), []ids.NodeID{p2, p3, p1}); err != nil {
		t.Fatal(err)
	}
	plan, err := e.Receive(id, []byte("ring"), p2)
	if err != nil || plan.Prune {
		t.Fatalf("duplicate protected delivery plan=%+v err=%v", plan, err)
	}
}

func TestGraftAfterCacheExpiryFallsBackToRepair(t *testing.T) {
	e, err := New(Config{EagerFanout: 2, MaxNeighbors: 4, MaxCacheEntries: 2, MaxCacheBytes: 16, CacheTTL: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	peer := ids.NewNodeID()
	if _, err := e.Start([32]byte{9}, []byte("tx"), []ids.NodeID{peer}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	plan := e.Graft([32]byte{9}, peer)
	if !plan.Graft || len(plan.Payload) != 0 {
		t.Fatalf("expired graft plan = %+v", plan)
	}
}

func mapIDs(a, b []ids.NodeID) map[ids.NodeID]struct{} {
	m := make(map[ids.NodeID]struct{})
	for _, p := range a {
		m[p] = struct{}{}
	}
	for _, p := range b {
		m[p] = struct{}{}
	}
	return m
}

func has(peers []ids.NodeID, want ids.NodeID) bool {
	for _, peer := range peers {
		if peer == want {
			return true
		}
	}
	return false
}
