package overload

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCounterEnforcesAggregateAndPeerLimitsAndReleasesOnce(t *testing.T) {
	c, err := NewCounter(Limit{Bytes: 10, Entries: 4, PeerBytes: 7, PeerEntries: 3, MaxPeers: 2})
	if err != nil {
		t.Fatal(err)
	}
	a, err := c.Acquire("a", 6, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("a", 2, 1); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("peer cap error=%v", err)
	}
	if _, err := c.Acquire("b", 5, 2); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("aggregate cap error=%v", err)
	}
	a.Release()
	a.Release()
	b, err := c.Acquire("b", 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Snapshot(); got.Bytes != 4 || got.Entries != 2 || got.Peers != 1 {
		t.Fatalf("snapshot=%+v", got)
	}
	b.Release()
	if got := c.Snapshot(); got != (Snapshot{}) {
		t.Fatalf("after release=%+v", got)
	}
}

func TestCounterBoundsPeerState(t *testing.T) {
	c, err := NewCounter(Limit{Bytes: 20, Entries: 10, PeerBytes: 10, PeerEntries: 5, MaxPeers: 1})
	if err != nil {
		t.Fatal(err)
	}
	l, err := c.Acquire("a", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("b", 1, 1); !errors.Is(err, ErrPeerLimit) {
		t.Fatalf("peer limit error=%v", err)
	}
	l.Release()
	if _, err := c.Acquire("b", 1, 1); err != nil {
		t.Fatalf("released peer slot not reused: %v", err)
	}
}

func TestRateLimiterBoundsBurstAndHonorsCancellation(t *testing.T) {
	r, err := NewRateLimiter(100, 50, 100, 50, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WaitN(context.Background(), "a", 51); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("oversize rate request=%v", err)
	}
	if err := r.WaitN(context.Background(), "a", 50); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := r.WaitN(ctx, "a", 50); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled wait=%v", err)
	}
	if err := r.WaitN(context.Background(), "b", 1); err != nil {
		t.Fatalf("second peer request failed: %v", err)
	}
	if err := r.WaitN(context.Background(), "c", 1); !errors.Is(err, ErrPeerLimit) {
		t.Fatalf("peer-state cap=%v", err)
	}
	r.ForgetPeer("a")
	if err := r.WaitN(context.Background(), "c", 1); err != nil {
		t.Fatalf("forgotten slot not reused: %v", err)
	}
}
