package rime

import "cmp"

// orderedIdx is a dependency-free AVL tree of distinct field values. Each
// value owns an insertion-ordered key bucket; byKey locates entries directly.
// The owning indexSet lock guards the tree, buckets, and key directory.
// Updates never scan or shift the full index: value-tree work is O(log D),
// where D is distinct values, and bucket insertion/removal is O(1).
type orderedEntry struct {
	val any
	key any
}

type orderedItem struct {
	key        any
	owner      *orderedNode
	prev, next *orderedItem
}

type orderedNode struct {
	val         any
	left, right *orderedNode
	height      int
	first, last *orderedItem
}

type orderedIdx struct {
	cmp       func(a, b any) int
	kind      keyKind
	root      *orderedNode
	byKey     map[any]*orderedItem
	byKeyHint int
}

// Stored-value unboxers for typed ordered maintenance. Stored values always
// carry the field's normalized type (written through the same typed path),
// so a failed assertion surfaces a real bug rather than a lookup miss.
func unboxStr(v any) string  { return v.(string) }
func unboxI64(v any) int64   { return v.(int64) }
func unboxU64(v any) uint64  { return v.(uint64) }
func unboxF64(v any) float64 { return v.(float64) }

// orderedInsertOrFind descends with native comparisons, boxing the new value
// only when a genuinely new tree node is created.
func orderedInsertOrFind[K cmp.Ordered](o *orderedIdx, n *orderedNode, v K, unbox func(any) K) (*orderedNode, *orderedNode) {
	if n == nil {
		node := &orderedNode{val: v, height: 1}
		return node, node
	}
	switch c := cmp.Compare(v, unbox(n.val)); {
	case c < 0:
		var bucket *orderedNode
		n.left, bucket = orderedInsertOrFind(o, n.left, v, unbox)
		return orderedBalance(n), bucket
	case c > 0:
		var bucket *orderedNode
		n.right, bucket = orderedInsertOrFind(o, n.right, v, unbox)
		return orderedBalance(n), bucket
	default:
		return n, n
	}
}

func orderedDeleteVal[K cmp.Ordered](o *orderedIdx, n *orderedNode, v K, unbox func(any) K) *orderedNode {
	if n == nil {
		return nil
	}
	switch c := cmp.Compare(v, unbox(n.val)); {
	case c < 0:
		n.left = orderedDeleteVal(o, n.left, v, unbox)
	case c > 0:
		n.right = orderedDeleteVal(o, n.right, v, unbox)
	default:
		if n.left == nil {
			return n.right
		}
		if n.right == nil {
			return n.left
		}
		successor, right := orderedDetachMin(n.right)
		successor.left = n.left
		successor.right = right
		n = successor
	}
	return orderedBalance(n)
}

// orderedUnlink detaches key's item from its bucket. It reports whether the
// bucket emptied; the caller deletes emptied buckets.
func orderedUnlink(item *orderedItem) (emptied bool) {
	bucket := item.owner
	if item.prev == nil {
		bucket.first = item.next
	} else {
		item.prev.next = item.next
	}
	if item.next == nil {
		bucket.last = item.prev
	} else {
		item.next.prev = item.prev
	}
	return bucket.first == nil
}

func orderedLink(item, last *orderedItem, bucket *orderedNode) {
	item.owner = bucket
	item.prev = last
	item.next = nil
	if last == nil {
		bucket.first = item
	} else {
		last.next = item
	}
	bucket.last = item
}

// orderedAdd indexes key under v with no allocation when the bucket exists.
func orderedAdd[K cmp.Ordered](o *orderedIdx, key any, v K, unbox func(any) K) {
	if existing := o.byKey[key]; existing != nil {
		if unbox(existing.owner.val) == v {
			return
		}
		orderedRemove(o, key, unbox(existing.owner.val), unbox)
	}
	var bucket *orderedNode
	o.root, bucket = orderedInsertOrFind(o, o.root, v, unbox)
	item := &orderedItem{key: key}
	orderedLink(item, bucket.last, bucket)
	o.byKey[key] = item
	o.refreshByKeyHint()
}

// orderedRemove drops key from v's bucket with no allocation.
func orderedRemove[K cmp.Ordered](o *orderedIdx, key any, v K, unbox func(any) K) {
	item := o.byKey[key]
	if item == nil || unbox(item.owner.val) != v {
		return
	}
	if orderedUnlink(item) {
		o.root = orderedDeleteVal(o, o.root, v, unbox)
	}
	delete(o.byKey, key)
}

// orderedReplace moves key from oldV to newV, reusing its directory item.
// A key not indexed under oldV falls back to remove+add semantics.
func orderedReplace[K cmp.Ordered](o *orderedIdx, key any, oldV, newV K, unbox func(any) K) {
	item := o.byKey[key]
	if item == nil || unbox(item.owner.val) != oldV {
		o.remove(any(oldV), key)
		o.add(any(newV), key)
		return
	}
	if oldV == newV {
		return // same bucket; keep insertion order
	}
	if orderedUnlink(item) {
		o.root = orderedDeleteVal(o, o.root, oldV, unbox)
	}
	var nb *orderedNode
	o.root, nb = orderedInsertOrFind(o, o.root, newV, unbox)
	orderedLink(item, nb.last, nb)
}

func newOrderedIdx(c func(a, b any) int) *orderedIdx {
	return &orderedIdx{cmp: c, byKey: make(map[any]*orderedItem, 8), byKeyHint: 8}
}

func (o *orderedIdx) refreshByKeyHint() {
	for o.byKeyHint < len(o.byKey) {
		if o.byKeyHint < 1 {
			o.byKeyHint = 8
		} else {
			o.byKeyHint *= 2
		}
	}
}
func (o *orderedIdx) len() int { return len(o.byKey) }
func orderedHeight(n *orderedNode) int {
	if n == nil {
		return 0
	}
	return n.height
}
func orderedFixHeight(n *orderedNode) {
	n.height = 1 + max(orderedHeight(n.left), orderedHeight(n.right))
}
func orderedRotateLeft(n *orderedNode) *orderedNode {
	r := n.right
	n.right = r.left
	r.left = n
	orderedFixHeight(n)
	orderedFixHeight(r)
	return r
}
func orderedRotateRight(n *orderedNode) *orderedNode {
	l := n.left
	n.left = l.right
	l.right = n
	orderedFixHeight(n)
	orderedFixHeight(l)
	return l
}
func orderedBalance(n *orderedNode) *orderedNode {
	if n == nil {
		return nil
	}
	orderedFixHeight(n)
	balance := orderedHeight(n.left) - orderedHeight(n.right)
	if balance > 1 {
		if orderedHeight(n.left.left) < orderedHeight(n.left.right) {
			n.left = orderedRotateLeft(n.left)
		}
		return orderedRotateRight(n)
	}
	if balance < -1 {
		if orderedHeight(n.right.right) < orderedHeight(n.right.left) {
			n.right = orderedRotateRight(n.right)
		}
		return orderedRotateLeft(n)
	}
	return n
}
func (o *orderedIdx) insert(n *orderedNode, val any) (*orderedNode, *orderedNode) {
	if n == nil {
		node := &orderedNode{val: val, height: 1}
		return node, node
	}
	var bucket *orderedNode
	switch c := o.cmp(val, n.val); {
	case c < 0:
		n.left, bucket = o.insert(n.left, val)
	case c > 0:
		n.right, bucket = o.insert(n.right, val)
	default:
		return n, n
	}
	return orderedBalance(n), bucket
}

// insertPrepared inserts a caller-owned node when val is new. Managed
// publication constructs that node before durable commit, keeping AVL node
// allocation out of the post-durability section.
func (o *orderedIdx) insertPrepared(n *orderedNode, val any, prepared *orderedNode) (*orderedNode, *orderedNode) {
	if n == nil {
		prepared.val = val
		prepared.height = 1
		prepared.left, prepared.right = nil, nil
		prepared.first, prepared.last = nil, nil
		return prepared, prepared
	}
	var bucket *orderedNode
	switch c := o.cmp(val, n.val); {
	case c < 0:
		n.left, bucket = o.insertPrepared(n.left, val, prepared)
	case c > 0:
		n.right, bucket = o.insertPrepared(n.right, val, prepared)
	default:
		return n, n
	}
	return orderedBalance(n), bucket
}

func (o *orderedIdx) addPrepared(val, key any, node *orderedNode, preparedItem *orderedItem) {
	if existing := o.byKey[key]; existing != nil {
		if o.cmp(existing.owner.val, val) == 0 {
			return
		}
		o.replacePrepared(existing.owner.val, val, key, node, preparedItem)
		return
	}
	var bucket *orderedNode
	o.root, bucket = o.insertPrepared(o.root, val, node)
	item := preparedItem
	item.key = key
	item.owner = bucket
	item.prev, item.next = bucket.last, nil
	if bucket.last == nil {
		bucket.first = item
	} else {
		bucket.last.next = item
	}
	bucket.last = item
	o.byKey[key] = item
	o.refreshByKeyHint()
}

func (o *orderedIdx) replacePrepared(old, val, key any, node *orderedNode, preparedItem *orderedItem) {
	item := o.byKey[key]
	if item == nil || o.cmp(item.owner.val, old) != 0 {
		o.remove(old, key)
		o.addPrepared(val, key, node, preparedItem)
		return
	}
	if o.cmp(old, val) == 0 {
		return
	}
	bucket := item.owner
	if item.prev == nil {
		bucket.first = item.next
	} else {
		item.prev.next = item.next
	}
	if item.next == nil {
		bucket.last = item.prev
	} else {
		item.next.prev = item.prev
	}
	if bucket.first == nil {
		o.root = o.deleteNode(o.root, bucket.val)
	}
	var next *orderedNode
	o.root, next = o.insertPrepared(o.root, val, node)
	item.owner, item.prev, item.next = next, next.last, nil
	if next.last == nil {
		next.first = item
	} else {
		next.last.next = item
	}
	next.last = item
}
func orderedDetachMin(n *orderedNode) (*orderedNode, *orderedNode) {
	if n.left == nil {
		return n, n.right
	}
	var smallest *orderedNode
	smallest, n.left = orderedDetachMin(n.left)
	return smallest, orderedBalance(n)
}
func (o *orderedIdx) deleteNode(n *orderedNode, val any) *orderedNode {
	if n == nil {
		return nil
	}
	switch c := o.cmp(val, n.val); {
	case c < 0:
		n.left = o.deleteNode(n.left, val)
	case c > 0:
		n.right = o.deleteNode(n.right, val)
	default:
		if n.left == nil {
			return n.right
		}
		if n.right == nil {
			return n.left
		}
		// Move the successor node itself rather than copying its value/bucket;
		// directory items retain the same owner pointer through rotations.
		successor, right := orderedDetachMin(n.right)
		successor.left = n.left
		successor.right = right
		n = successor
	}
	return orderedBalance(n)
}
func (o *orderedIdx) add(val, key any) {
	if existing := o.byKey[key]; existing != nil {
		if o.cmp(existing.owner.val, val) == 0 {
			return
		}
		o.remove(existing.owner.val, key)
	}
	var bucket *orderedNode
	o.root, bucket = o.insert(o.root, val)
	item := &orderedItem{key: key, owner: bucket, prev: bucket.last}
	if bucket.last == nil {
		bucket.first = item
	} else {
		bucket.last.next = item
	}
	bucket.last = item
	o.byKey[key] = item
	o.refreshByKeyHint()
}

// replace moves key from old to val, reusing its directory item instead of
// deleting and reallocating it. When the key is not indexed under old it
// falls back to the exact remove+add sequence, so behavior never differs
// from separate calls.
func (o *orderedIdx) replace(old, val, key any) {
	item := o.byKey[key]
	if item == nil || o.cmp(item.owner.val, old) != 0 {
		o.remove(old, key)
		o.add(val, key)
		return
	}
	if o.cmp(old, val) == 0 {
		return // same bucket; keep insertion order
	}
	bucket := item.owner
	if item.prev == nil {
		bucket.first = item.next
	} else {
		item.prev.next = item.next
	}
	if item.next == nil {
		bucket.last = item.prev
	} else {
		item.next.prev = item.prev
	}
	if bucket.first == nil {
		o.root = o.deleteNode(o.root, bucket.val)
	}
	var nb *orderedNode
	o.root, nb = o.insert(o.root, val)
	item.owner = nb
	item.prev = nb.last
	item.next = nil
	if nb.last == nil {
		nb.first = item
	} else {
		nb.last.next = item
	}
	nb.last = item
}
func (o *orderedIdx) remove(val, key any) {
	item := o.byKey[key]
	if item == nil || o.cmp(item.owner.val, val) != 0 {
		return
	}
	bucket := item.owner
	if item.prev == nil {
		bucket.first = item.next
	} else {
		item.prev.next = item.next
	}
	if item.next == nil {
		bucket.last = item.prev
	} else {
		item.next.prev = item.prev
	}
	delete(o.byKey, key)
	if bucket.first == nil {
		o.root = o.deleteNode(o.root, bucket.val)
	}
}

// rangeKeys visits only the bounded subtree and matched key buckets.
// Duplicate values retain insertion order; descending traversal reverses it.
func (o *orderedIdx) rangeKeys(lo, hi any, loOpen, hiOpen, desc bool, limit int) []any {
	if limit == 0 {
		return nil
	}
	capacity := min(o.len(), 64)
	if limit >= 0 {
		capacity = min(capacity, limit)
	}
	out := make([]any, 0, capacity)
	var visit func(*orderedNode) bool
	visit = func(n *orderedNode) bool {
		if n == nil {
			return false
		}
		if lo != nil {
			c := o.cmp(n.val, lo)
			if c < 0 || (c == 0 && loOpen) {
				return visit(n.right)
			}
		}
		if hi != nil {
			c := o.cmp(n.val, hi)
			if c > 0 || (c == 0 && hiOpen) {
				return visit(n.left)
			}
		}
		first, second := n.left, n.right
		item := n.first
		if desc {
			first, second = n.right, n.left
			item = n.last
		}
		if visit(first) {
			return true
		}
		for item != nil {
			out = append(out, item.key)
			if limit >= 0 && len(out) >= limit {
				return true
			}
			if desc {
				item = item.prev
			} else {
				item = item.next
			}
		}
		return visit(second)
	}
	visit(o.root)
	return out
}
func (o *orderedIdx) extreme(wantMax bool) (orderedEntry, bool) {
	n := o.root
	if n == nil {
		return orderedEntry{}, false
	}
	if wantMax {
		for n.right != nil {
			n = n.right
		}
		return orderedEntry{val: n.val, key: n.last.key}, true
	}
	for n.left != nil {
		n = n.left
	}
	return orderedEntry{val: n.val, key: n.first.key}, true
}
func (ix *indexSet) orderedRange(field string, lo, hi any, loOpen, hiOpen, desc bool, limit int) []any {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	o := ix.ordered[field]
	if o == nil {
		return nil
	}
	return o.rangeKeys(lo, hi, loOpen, hiOpen, desc, limit)
}
func (ix *indexSet) orderedMinMax(field string, wantMax bool) []any {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	o := ix.ordered[field]
	if o == nil {
		return nil
	}
	e, ok := o.extreme(wantMax)
	if !ok {
		return nil
	}
	return []any{e.key}
}
