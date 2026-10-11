package rime

import (
	"cmp"

	"github.com/tidwall/btree"
)

// orderedIdx is a B-tree of distinct field values. Each value owns an
// insertion-ordered key bucket; byKey locates entries directly. The
// owning indexSet lock guards the tree, buckets, and key directory.
// Updates never scan or shift the full index: value-tree work is
// O(log D), where D is distinct values, and bucket insertion/removal
// is O(1). The B-tree uses no internal locks; the index lock serializes
// all access.
type orderedEntry struct {
	val any
	key any
}

type orderedItem struct {
	key        any
	owner      *orderedBucket
	prev, next *orderedItem
}

type orderedBucket struct {
	val         any
	first, last *orderedItem
	count       int
}

type orderedIdx struct {
	cmp       func(a, b any) int
	kind      keyKind
	tree      *btree.BTreeG[*orderedBucket]
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

// less orders buckets using native comparisons for typed kinds and the
// compiled comparator for open kinds (timestamps, UUIDs), preserving
// existing comparison and normalization rules.
func (o *orderedIdx) less(a, b *orderedBucket) bool {
	switch o.kind {
	case kString:
		return unboxStr(a.val) < unboxStr(b.val)
	case kInt64:
		return unboxI64(a.val) < unboxI64(b.val)
	case kUint64:
		return unboxU64(a.val) < unboxU64(b.val)
	case kFloat64:
		return unboxF64(a.val) < unboxF64(b.val)
	default:
		return o.cmp(a.val, b.val) < 0
	}
}

// find locates the bucket for val. The probe is stack-allocated and
// never retained, so lookups do not heap-allocate.
func (o *orderedIdx) find(val any) (*orderedBucket, bool) {
	probe := orderedBucket{val: val}
	return o.tree.Get(&probe)
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
	bucket.count--
	return bucket.count == 0
}

func orderedLink(item, last *orderedItem, bucket *orderedBucket) {
	item.owner = bucket
	item.prev = last
	item.next = nil
	if last == nil {
		bucket.first = item
	} else {
		last.next = item
	}
	bucket.last = item
	bucket.count++
}

// orderedAdd indexes key under v with no allocation when the bucket exists.
func orderedAdd[K cmp.Ordered](o *orderedIdx, key any, v K, unbox func(any) K) {
	if existing := o.byKey[key]; existing != nil {
		if unbox(existing.owner.val) == v {
			return
		}
		orderedRemove(o, key, unbox(existing.owner.val), unbox)
	}
	probe := orderedBucket{val: v}
	bucket, ok := o.tree.Get(&probe)
	if !ok {
		bucket = &orderedBucket{val: v}
		o.tree.Set(bucket)
	}
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
	bucket := item.owner
	if orderedUnlink(item) {
		o.tree.Delete(bucket)
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
	oldBucket := item.owner
	if orderedUnlink(item) {
		o.tree.Delete(oldBucket)
	}
	probe := orderedBucket{val: newV}
	nb, ok := o.tree.Get(&probe)
	if !ok {
		nb = &orderedBucket{val: newV}
		o.tree.Set(nb)
	}
	orderedLink(item, nb.last, nb)
}

func newOrderedIdx(c func(a, b any) int) *orderedIdx {
	o := &orderedIdx{cmp: c, byKey: make(map[any]*orderedItem, 8), byKeyHint: 8}
	// NoLocks: the owning indexSet lock guards every access. Degree 0
	// selects Tidwall's default degree of 32.
	o.tree = btree.NewBTreeGOptions[*orderedBucket](o.less, btree.Options{NoLocks: true})
	return o
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

// distinct returns the number of distinct indexed values.
func (o *orderedIdx) distinct() int {
	if o.tree == nil {
		return 0
	}
	return o.tree.Len()
}

// applyPrepared applies one managed bucket/directory edit using buckets
// resolved during preparation. It performs no B-tree insertion,
// deletion, or splitting; the candidate tree topology was prebuilt
// before durability and is installed separately. It allocates nothing.
func (o *orderedIdx) applyPrepared(effect orderedIndexEffect, key any) {
	if effect.hasOld && effect.hasNew && effect.oldBucket != nil && effect.oldBucket == effect.newBucket {
		return // same bucket; keep insertion order
	}
	if effect.hasOld && effect.hasNew {
		item := o.byKey[key]
		if item == nil {
			nb := effect.newBucket
			if nb == nil {
				panic("rime: missing prepared ordered bucket")
			}
			o.applyAddPrepared(nb, effect.item, key)
			return
		}
		orderedUnlink(item)
		nb := effect.newBucket
		if nb == nil {
			panic("rime: missing prepared ordered bucket")
		}
		orderedLink(item, nb.last, nb)
		return
	}
	if effect.hasOld {
		item := o.byKey[key]
		if item == nil {
			return
		}
		orderedUnlink(item)
		delete(o.byKey, key)
		return
	}
	if effect.hasNew {
		if existing := o.byKey[key]; existing != nil {
			// Key already indexed (stacked insert after delete
			// resolved live): move it into the prepared bucket.
			orderedUnlink(existing)
			nb := effect.newBucket
			if nb == nil {
				panic("rime: missing prepared ordered bucket")
			}
			orderedLink(existing, nb.last, nb)
			return
		}
		nb := effect.newBucket
		if nb == nil {
			panic("rime: missing prepared ordered bucket")
		}
		o.applyAddPrepared(nb, effect.item, key)
	}
}

func (o *orderedIdx) applyAddPrepared(bucket *orderedBucket, item *orderedItem, key any) {
	if bucket == nil || item == nil {
		panic("rime: missing prepared ordered bucket or item")
	}
	item.key = key
	item.owner = nil
	item.prev, item.next = nil, nil
	orderedLink(item, bucket.last, bucket)
	o.byKey[key] = item
	o.refreshByKeyHint()
}

func (o *orderedIdx) add(val, key any) {
	if existing := o.byKey[key]; existing != nil {
		if o.cmp(existing.owner.val, val) == 0 {
			return
		}
		o.remove(existing.owner.val, key)
	}
	bucket, ok := o.find(val)
	if !ok {
		bucket = &orderedBucket{val: val}
		o.tree.Set(bucket)
	}
	item := &orderedItem{key: key}
	orderedLink(item, bucket.last, bucket)
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
	oldBucket := item.owner
	if orderedUnlink(item) {
		o.tree.Delete(oldBucket)
	}
	nb, ok := o.find(val)
	if !ok {
		nb = &orderedBucket{val: val}
		o.tree.Set(nb)
	}
	orderedLink(item, nb.last, nb)
}
func (o *orderedIdx) remove(val, key any) {
	item := o.byKey[key]
	if item == nil || o.cmp(item.owner.val, val) != 0 {
		return
	}
	bucket := item.owner
	if orderedUnlink(item) {
		o.tree.Delete(bucket)
	}
	delete(o.byKey, key)
}

// normalizeBound converts a query bound to the field's storage kind.
// Mismatched types report ok=false (a lookup miss), never panic.
func (o *orderedIdx) normalizeBound(v any) (any, bool) {
	if v == nil {
		return nil, true
	}
	switch o.kind {
	case kString:
		s, ok := normStr(v)
		if !ok {
			return nil, false
		}
		return s, true
	case kInt64:
		i, ok := normI64(v)
		if !ok {
			return nil, false
		}
		return i, true
	case kUint64:
		u, ok := normU64(v)
		if !ok {
			return nil, false
		}
		return u, true
	case kFloat64:
		f, ok := normF64(v)
		if !ok {
			return nil, false
		}
		return f, true
	default:
		return v, true
	}
}

// compareBucket orders a stored bucket value against a normalized bound.
func (o *orderedIdx) compareBucket(bucketVal, bound any) int {
	switch o.kind {
	case kString:
		return cmp.Compare(unboxStr(bucketVal), bound.(string))
	case kInt64:
		return cmp.Compare(unboxI64(bucketVal), bound.(int64))
	case kUint64:
		return cmp.Compare(unboxU64(bucketVal), bound.(uint64))
	case kFloat64:
		return cmp.Compare(unboxF64(bucketVal), bound.(float64))
	default:
		return o.cmp(bucketVal, bound)
	}
}

// rangeKeys visits only the bounded values and matched key buckets.
// Duplicate values retain insertion order; descending traversal reverses it.
func (o *orderedIdx) rangeKeys(lo, hi any, loOpen, hiOpen, desc bool, limit int) []any {
	if limit == 0 {
		return nil
	}
	nlo, ok := o.normalizeBound(lo)
	if !ok {
		return nil
	}
	nhi, ok := o.normalizeBound(hi)
	if !ok {
		return nil
	}
	capacity := min(o.len(), 64)
	if limit >= 0 {
		capacity = min(capacity, limit)
	}
	out := make([]any, 0, capacity)
	if o.tree == nil || o.tree.Len() == 0 {
		return out
	}
	appendBucket := func(bucket *orderedBucket) bool {
		if desc {
			for item := bucket.last; item != nil; item = item.prev {
				out = append(out, item.key)
				if limit >= 0 && len(out) >= limit {
					return false
				}
			}
			return true
		}
		for item := bucket.first; item != nil; item = item.next {
			out = append(out, item.key)
			if limit >= 0 && len(out) >= limit {
				return false
			}
		}
		return true
	}
	if !desc {
		visit := func(bucket *orderedBucket) bool {
			if nhi != nil {
				if c := o.compareBucket(bucket.val, nhi); c > 0 || (c == 0 && hiOpen) {
					return false
				}
			}
			if nlo != nil {
				if c := o.compareBucket(bucket.val, nlo); c == 0 && loOpen {
					return true
				}
			}
			return appendBucket(bucket)
		}
		if nlo != nil {
			probe := orderedBucket{val: nlo}
			o.tree.Ascend(&probe, visit)
		} else {
			o.tree.Scan(visit)
		}
		return out
	}
	visit := func(bucket *orderedBucket) bool {
		if nlo != nil {
			if c := o.compareBucket(bucket.val, nlo); c < 0 || (c == 0 && loOpen) {
				return false
			}
		}
		if nhi != nil {
			if c := o.compareBucket(bucket.val, nhi); c == 0 && hiOpen {
				return true
			}
		}
		return appendBucket(bucket)
	}
	if nhi != nil {
		probe := orderedBucket{val: nhi}
		o.tree.Descend(&probe, visit)
	} else {
		o.tree.Reverse(visit)
	}
	return out
}
func (o *orderedIdx) extreme(wantMax bool) (orderedEntry, bool) {
	if o.tree == nil || o.tree.Len() == 0 {
		return orderedEntry{}, false
	}
	if wantMax {
		bucket, ok := o.tree.Max()
		if !ok || bucket.last == nil {
			return orderedEntry{}, false
		}
		return orderedEntry{val: bucket.val, key: bucket.last.key}, true
	}
	bucket, ok := o.tree.Min()
	if !ok || bucket.first == nil {
		return orderedEntry{}, false
	}
	return orderedEntry{val: bucket.val, key: bucket.first.key}, true
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
