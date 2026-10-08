package rime

import "testing"

func TestTrieRemovePrunesSharedPathsWithoutAllocating(t *testing.T) {
	trie := newTrieIdx()
	trie.add("alpine", "keep")
	trie.add("alpha", "remove")
	trie.add("beta", "other")
	if got := trie.lookup("al"); len(got) != 2 {
		t.Fatalf("initial shared prefix keys = %v, want 2", got)
	}
	trie.remove("alpha", "remove")
	if got := trie.lookup("alpha"); len(got) != 0 {
		t.Fatalf("removed branch lookup = %v, want empty", got)
	}
	if got := trie.lookup("al"); len(got) != 1 {
		t.Fatalf("sibling branch was pruned: %v", got)
	}
	trie.remove("missing", "keep")
	if got := trie.lookup("alpine"); len(got) != 1 {
		t.Fatalf("missing removal changed existing key: %v", got)
	}

	keys := make([]any, 102)
	for i := range keys {
		keys[i] = i
		trie.add("prune-me", keys[i])
	}
	next := 0
	allocs := testing.AllocsPerRun(100, func() {
		trie.remove("prune-me", keys[next])
		next++
	})
	if allocs != 0 {
		t.Fatalf("prefix removal allocated %.2f times; removal path should be allocation-free", allocs)
	}
	for _, key := range keys[101:] {
		trie.remove("prune-me", key)
	}
	trie.remove("alpine", "keep")
	trie.remove("beta", "other")
	if len(trie.root.keys) != 0 || len(trie.root.children) != 0 {
		t.Fatalf("empty trie retained entries: keys=%v children=%v", trie.root.keys, trie.root.children)
	}
}

func TestTriePreparedInsertionIsInvisibleAndInstallsWithoutAllocating(t *testing.T) {
	type prepared struct {
		trie *trieIdx
		key  any
	}
	items := make([]prepared, 2)
	for i := range items {
		trie := newTrieIdx()
		trie.prepareAdd("newly-prepared-path", 12)
		items[i] = prepared{trie: trie, key: i}
		if got := trie.lookup("newly"); len(got) != 0 {
			t.Fatal("prepared insertion became visible before install")
		}
	}
	next := 0
	allocs := testing.AllocsPerRun(1, func() {
		item := items[next]
		item.trie.addPrepared("newly-prepared-path", item.key)
		next++
	})
	if allocs != 0 {
		t.Fatalf("prepared prefix insertion allocated %.2f times during install", allocs)
	}
	for _, item := range items {
		if got := item.trie.lookup("newly-prepared-path"); len(got) != 1 {
			t.Fatalf("installed prefix has %d keys, want 1", len(got))
		}
	}
}

func TestTriePreparedSiblingInsertionsUseLiveSharedPath(t *testing.T) {
	trie := newTrieIdx()
	trie.prepareAdd("shared-left", 4)
	trie.prepareAdd("shared-right", 4)
	trie.addPrepared("shared-left", "left")
	trie.addPrepared("shared-right", "right")
	if got := trie.lookup("shared-"); len(got) != 2 {
		t.Fatalf("shared prefix has %d keys, want 2", len(got))
	}
	trie.remove("shared-left", "left")
	if got := trie.lookup("shared-right"); len(got) != 1 {
		t.Fatalf("sibling path was lost after pruning: %v", got)
	}
	trie.remove("shared-right", "right")
	if len(trie.root.children) != 0 {
		t.Fatal("prepared sibling paths retained an empty trie branch")
	}
}
