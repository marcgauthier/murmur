package rime

import "testing"

func TestCompoundPreparedInsertionIsInvisibleAndInstallsWithoutAllocating(t *testing.T) {
	idx := &compoundIdx{fields: []string{"tenant", "status"}, root: &compoundLevel{}}
	first, second := []any{"tenant-new", "active"}, []any{"tenant-new", "inactive"}
	idx.prepareAdd(first, 32)
	idx.prepareAdd(second, 32)
	if got := idx.lookup(first); len(got) != 0 {
		t.Fatal("prepared insertion became visible before install")
	}
	next := 0
	allocs := testing.AllocsPerRun(1, func() {
		if next == 0 {
			idx.addPrepared(first, "first")
		} else {
			idx.addPrepared(second, "second")
		}
		next++
	})
	if allocs != 0 {
		t.Fatalf("prepared compound insertion allocated %.2f times during install", allocs)
	}
	if got := idx.lookup(first); len(got) != 1 {
		t.Fatalf("first installed compound path has %d keys, want 1", len(got))
	}
	if got := idx.lookup(second); len(got) != 1 {
		t.Fatalf("second installed compound path has %d keys, want 1", len(got))
	}
	idx.remove(first, "first")
	if got := idx.lookup(second); len(got) != 1 {
		t.Fatalf("sibling compound path was lost after pruning: %v", got)
	}
}

func TestCompoundRemovePrunesSharedPathsWithoutAllocating(t *testing.T) {
	idx := &compoundIdx{fields: []string{"tenant", "status"}, root: &compoundLevel{}}
	active := []any{"tenant-a", "active"}
	inactive := []any{"tenant-a", "inactive"}
	idx.add(active, "keep-active")
	idx.add(inactive, "keep-inactive")
	idx.remove(active, "missing")
	if got := idx.lookup(active); len(got) != 1 {
		t.Fatalf("missing-key removal changed active leaf: %v", got)
	}

	keys := make([]any, 102)
	for i := range keys {
		keys[i] = i
		idx.add(active, keys[i])
	}
	removed := 0
	allocs := testing.AllocsPerRun(100, func() {
		idx.remove(active, keys[removed])
		removed++
	})
	if allocs != 0 {
		t.Fatalf("compound-index removal allocated %.2f times", allocs)
	}
	for _, key := range keys[101:] {
		idx.remove(active, key)
	}
	if got := idx.lookup(active); len(got) != 1 {
		t.Fatalf("active leaf after removal = %v, want only keep-active", got)
	}
	if got := idx.lookup(inactive); len(got) != 1 {
		t.Fatalf("sibling compound branch was pruned: %v", got)
	}
	idx.remove(active, "keep-active")
	idx.remove(inactive, "keep-inactive")
	if len(idx.root.kids) != 0 || idx.total != 0 {
		t.Fatalf("empty compound index retained branches: kids=%v total=%d", idx.root.kids, idx.total)
	}
}
