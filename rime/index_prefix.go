package rime

// trieNode is one node of the prefix index trie.
type trieNode struct {
	keys     map[any]struct{}
	children map[byte]*trieNode
	parent   *trieNode
	edge     byte
	keyHint  int
}

// trieIdx is a byte-wise radix trie over string field values. Every node
// carries the full key set beneath it, so prefix lookup is O(prefix) plus
// result size, and removal prunes empty branches.
type trieIdx struct {
	root *trieNode
}

func newTrieIdx() *trieIdx { return &trieIdx{root: &trieNode{}} }

func (n *trieNode) reserveKeys(total int) {
	if total <= n.keyHint {
		return
	}
	m := make(map[any]struct{}, total)
	for k := range n.keys {
		m[k] = struct{}{}
	}
	n.keys, n.keyHint = m, total
}

// prepareAdd attaches path-only trie nodes and reserves key storage before
// durability. Empty paths return no record keys, so this is logically
// invisible to lookups; Abort prunes paths that remain empty.
func (t *trieIdx) prepareAdd(s string, reserve int) {
	if reserve < 1 {
		reserve = 1
	}
	n := t.root
	n.reserveKeys(len(n.keys) + reserve)
	for i := 0; i < len(s); i++ {
		c := n.children[s[i]]
		if c == nil {
			if n.children == nil {
				n.children = make(map[byte]*trieNode)
			}
			c = &trieNode{parent: n, edge: s[i]}
			n.children[s[i]] = c
		}
		n = c
		n.reserveKeys(len(n.keys) + reserve)
	}
}

func (t *trieIdx) addPrepared(s string, key any) {
	n := t.root
	n.keys[key] = struct{}{}
	for i := 0; i < len(s); i++ {
		c := n.children[s[i]]
		if c == nil {
			panic("rime: missing prepared prefix path")
		}
		n = c
		n.keys[key] = struct{}{}
	}
}

func (t *trieIdx) removeNoPrune(s string, key any) {
	n := t.root
	for i := 0; i < len(s); i++ {
		n = n.children[s[i]]
		if n == nil {
			return
		}
	}
	if _, ok := n.keys[key]; !ok {
		return
	}
	delete(t.root.keys, key)
	n = t.root
	for i := 0; i < len(s); i++ {
		n = n.children[s[i]]
		delete(n.keys, key)
	}
}

func (t *trieIdx) prune(s string) {
	n := t.root
	for i := 0; i < len(s); i++ {
		n = n.children[s[i]]
		if n == nil {
			return
		}
	}
	for n != t.root && len(n.keys) == 0 && len(n.children) == 0 {
		parent := n.parent
		delete(parent.children, n.edge)
		n = parent
	}
}

func (t *trieIdx) add(s string, key any) {
	n := t.root
	if n.keys == nil {
		n.keys = map[any]struct{}{}
	}
	n.keys[key] = struct{}{}
	for i := 0; i < len(s); i++ {
		if n.children == nil {
			n.children = map[byte]*trieNode{}
		}
		c, ok := n.children[s[i]]
		if !ok {
			c = &trieNode{parent: n, edge: s[i]}
			n.children[s[i]] = c
		}
		n = c
		if n.keys == nil {
			n.keys = map[any]struct{}{}
		}
		n.keys[key] = struct{}{}
	}
}

func (t *trieIdx) remove(s string, key any) {
	n := t.root
	for i := 0; i < len(s); i++ {
		c, ok := n.children[s[i]]
		if !ok {
			return
		}
		n = c
	}
	if _, ok := n.keys[key]; !ok {
		return
	}
	n = t.root
	delete(n.keys, key)
	for i := 0; i < len(s); i++ {
		n = n.children[s[i]]
		delete(n.keys, key)
	}
	// Prune empty leaf branches from the bottom up.
	for n != t.root && len(n.keys) == 0 && len(n.children) == 0 {
		parent := n.parent
		delete(parent.children, n.edge)
		n = parent
	}
}

// lookup returns keys of all values with the given prefix.
func (t *trieIdx) lookup(prefix string) map[any]struct{} {
	n := t.root
	for i := 0; i < len(prefix); i++ {
		c, ok := n.children[prefix[i]]
		if !ok {
			return map[any]struct{}{}
		}
		n = c
	}
	out := make(map[any]struct{}, len(n.keys))
	for k := range n.keys {
		out[k] = struct{}{}
	}
	return out
}

func (t *trieIdx) len() int { return len(t.root.keys) }

// prefixLookup is the indexSet-guarded prefix entry point.
func (ix *indexSet) prefixLookup(field, prefix string) map[any]struct{} {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	tr, ok := ix.prefix[field]
	if !ok {
		return nil
	}
	return tr.lookup(prefix)
}
