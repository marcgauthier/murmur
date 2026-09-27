package crdt

import (
	"testing"

	"github.com/nomadsql/replicateddb/ids"
)

func TestCompareVersion(t *testing.T) {
	lo := ids.MustNodeID("00000000-0000-0000-0000-000000000001")
	hi := ids.MustNodeID("00000000-0000-0000-0000-000000000002")
	cases := []struct {
		a, b Version
		want int
	}{
		{Version{HLC: 1, NodeID: lo}, Version{HLC: 2, NodeID: lo}, -1},
		{Version{HLC: 2, NodeID: lo}, Version{HLC: 1, NodeID: hi}, 1},
		{Version{HLC: 5, NodeID: lo}, Version{HLC: 5, NodeID: hi}, -1},
		{Version{HLC: 5, NodeID: hi}, Version{HLC: 5, NodeID: lo}, 1},
		{Version{HLC: 5, NodeID: lo}, Version{HLC: 5, NodeID: lo}, 0},
	}
	for i, c := range cases {
		if got := CompareVersion(c.a, c.b); got != c.want {
			t.Fatalf("case %d: got %d want %d", i, got, c.want)
		}
	}
}

func TestHLCMonotonicLocal(t *testing.T) {
	var c Clock
	prev := uint64(0)
	for i := 0; i < 10000; i++ {
		v := c.Now()
		if v <= prev {
			t.Fatalf("clock went backwards: %d -> %d", prev, v)
		}
		prev = v
	}
}

func TestHLCObserve(t *testing.T) {
	var c Clock
	a := c.Now()
	// Observe a far-future remote timestamp; the next local timestamp must
	// exceed it.
	remote := a + 1000
	c.Observe(remote)
	b := c.Now()
	if b <= remote {
		t.Fatalf("local %d not greater than observed remote %d", b, remote)
	}
	// Observing an old timestamp must not move the clock backwards.
	c.Observe(a)
	if got := c.Now(); got <= b {
		t.Fatalf("clock moved backwards after stale observe: %d -> %d", b, got)
	}
}

func TestHLCRestore(t *testing.T) {
	var c Clock
	c.Restore(1 << 40)
	if got := c.Now(); got <= 1<<40 {
		t.Fatalf("restored floor not honored: %d", got)
	}
}

func TestHLCCounterOverflow(t *testing.T) {
	c := &Clock{}
	// Pin the wall clock; exhausting the 16-bit counter must still advance.
	base := int64(1_700_000_000_000)
	c.now = func() int64 { return base }
	first := c.Now()
	last := first
	for i := 0; i < 70000; i++ {
		last = c.Now()
	}
	if last <= first {
		t.Fatalf("counter overflow went backwards: %d -> %d", first, last)
	}
	if WallMillis(last) < base {
		t.Fatalf("wall component moved backwards: %d < %d", WallMillis(last), base)
	}
}

func TestMergeCell(t *testing.T) {
	a := Version{HLC: 1}
	b := Version{HLC: 2}
	if MergeCell(b, a, true) != MergeTake {
		t.Fatal("newer should take")
	}
	if MergeCell(a, b, true) != MergeKeep {
		t.Fatal("older should keep")
	}
	if MergeCell(a, a, true) != MergeEqual {
		t.Fatal("equal should be equal")
	}
	if MergeCell(a, Version{}, false) != MergeTake {
		t.Fatal("missing should take")
	}
}

func TestVisible(t *testing.T) {
	old := Version{HLC: 1}
	new := Version{HLC: 2}
	if !Visible(true, new, TombstoneState{}) {
		t.Fatal("row without tombstone should be visible")
	}
	if Visible(false, new, TombstoneState{}) {
		t.Fatal("row without cells should be invisible")
	}
	if !Visible(true, new, TombstoneState{Present: true, Version: old}) {
		t.Fatal("newer cell should resurrect over older tombstone")
	}
	if Visible(true, old, TombstoneState{Present: true, Version: new}) {
		t.Fatal("newer tombstone should hide row")
	}
}
