package state

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/marcgauthier/murmur/spool"
	"github.com/tidwall/btree"
)

// errNotFound is the shared missing-key sentinel returned by every lookup.
var errNotFound = errors.New("state: not found")

// iterOptions bounds an iterator: LowerBound inclusive, UpperBound
// exclusive, matching the ordered range-scan contract.
type iterOptions struct {
	LowerBound []byte
	UpperBound []byte
}

// kvItem is one owned key/value pair in the B-tree. Keys keep their
// binary encoding and bytewise ordering; both slices are owned clones
// and never mutated after insertion.
type kvItem struct {
	k []byte
	v []byte
}

func lessKV(a, b kvItem) bool { return bytes.Compare(a.k, b.k) < 0 }

// newKVTree returns an empty B-tree for state storage. Internal locks
// are disabled: published generations are immutable and never mutated,
// private batch trees are single-writer. Degree 0 selects Tidwall's
// default degree of 32.
func newKVTree() *btree.BTreeG[kvItem] {
	return btree.NewBTreeGOptions[kvItem](lessKV, btree.Options{NoLocks: true})
}

// memStore is the authoritative in-RAM key/value state: a B-tree
// generation published atomically after each durable Spool commit.
// Keys keep their binary encoding and bytewise ordering. Stored
// key/value bytes are owned clones and never mutated, so lookups and
// iterators over a pinned generation are safe without copying.
type memStore struct {
	// mu serializes Copy operations on the published generation.
	// Tidwall's Copy mutates source bookkeeping (isolation id), so
	// concurrent copies of the same source would race. Readers use
	// only non-mutating tree methods and never observe that field.
	mu   sync.Mutex
	root atomic.Pointer[btree.BTreeG[kvItem]]
}

func newMemStore() *memStore {
	m := &memStore{}
	m.root.Store(newKVTree())
	return m
}

func (m *memStore) load() *btree.BTreeG[kvItem] {
	return m.root.Load()
}

func (m *memStore) publish(t *btree.BTreeG[kvItem]) {
	m.root.Store(t)
}

// get returns an owned copy of the value stored under key.
func (m *memStore) get(key []byte) ([]byte, error) {
	t := m.load()
	if t == nil {
		return nil, errNotFound
	}
	item, ok := t.Get(kvItem{k: key})
	if !ok {
		return nil, errNotFound
	}
	return append([]byte(nil), item.v...), nil
}

func (m *memStore) newBatch() *batch {
	m.mu.Lock()
	candidate := m.load().Copy()
	m.mu.Unlock()
	return &batch{tree: candidate}
}

func (m *memStore) newSnapshot() *snapshot {
	return &snapshot{tree: m.load()}
}

func (m *memStore) newIter(o *iterOptions) (*iterator, error) {
	return newIterator(m.load(), o), nil
}

// batch is one atomic multi-key mutation: a private B-tree copy for
// read-your-writes plus the ordered mutation list for Spool. The
// private tree is modified directly and published only after the
// Spool commit is durably acknowledged.
type batch struct {
	tree  *btree.BTreeG[kvItem]
	muts  []spool.Mutation
	size  int64
	count uint32
}

// Set stores an owned copy of key/value.
func (b *batch) Set(key, value []byte) error {
	k := append([]byte(nil), key...)
	v := append([]byte(nil), value...)
	b.tree.Set(kvItem{k: k, v: v})
	b.muts = append(b.muts, spool.Mutation{Key: k, Value: v})
	b.size += int64(len(k) + len(v))
	b.count++
	return nil
}

// Delete removes key and records a deletion marker.
func (b *batch) Delete(key []byte) error {
	k := append([]byte(nil), key...)
	b.tree.Delete(kvItem{k: k})
	b.muts = append(b.muts, spool.Mutation{Key: k, Deleted: true})
	b.size += int64(len(k))
	b.count++
	return nil
}

// Get reads pending writes first, then the published root.
func (b *batch) Get(key []byte) ([]byte, io.Closer, error) {
	item, ok := b.tree.Get(kvItem{k: key})
	if !ok {
		return nil, nil, errNotFound
	}
	return append([]byte(nil), item.v...), io.NopCloser(nil), nil
}

// NewIter iterates the batch's pending state: pending writes shadow
// the published root. The iterator pins a copy of the batch tree, so
// subsequent batch mutations do not affect it.
func (b *batch) NewIter(o *iterOptions) (*iterator, error) {
	return newIterator(b.tree.Copy(), o), nil
}

// Len reports the staged key/value byte count.
func (b *batch) Len() int { return int(b.size) }

// Count reports the staged mutation count.
func (b *batch) Count() uint32 { return b.count }

// Close discards an uncommitted batch.
func (b *batch) Close() error { return nil }

// candidate returns the private tree this batch would publish.
func (b *batch) candidate() *btree.BTreeG[kvItem] { return b.tree }

// snapshot pins one immutable B-tree generation for consistent
// multi-key reads. Published generations are never mutated, so no
// lifetime management is needed beyond dropping the reference.
type snapshot struct {
	tree *btree.BTreeG[kvItem]
}

func (s *snapshot) Get(key []byte) ([]byte, io.Closer, error) {
	if s.tree == nil {
		return nil, nil, errNotFound
	}
	item, ok := s.tree.Get(kvItem{k: key})
	if !ok {
		return nil, nil, errNotFound
	}
	return append([]byte(nil), item.v...), io.NopCloser(nil), nil
}

func (s *snapshot) NewIter(o *iterOptions) (*iterator, error) {
	return newIterator(s.tree, o), nil
}

func (s *snapshot) Close() error { return nil }

// iterator is a forward-only bounded iterator over one immutable
// generation. Generations never change, so Key/Value slices stay
// valid for the iterator's lifetime; callers must not mutate them.
type iterator struct {
	tree  *btree.BTreeG[kvItem]
	it    btree.IterG[kvItem]
	ready bool
	lower []byte
	upper []byte
	key   []byte
	val   []byte
	valid bool
}

func newIterator(tree *btree.BTreeG[kvItem], o *iterOptions) *iterator {
	it := &iterator{tree: tree}
	if o != nil {
		it.lower = append([]byte(nil), o.LowerBound...)
		it.upper = append([]byte(nil), o.UpperBound...)
	}
	return it
}

func (it *iterator) ensureIter() {
	if !it.ready && it.tree != nil {
		it.it = it.tree.Iter()
		it.ready = true
	}
}

// First positions at the first key at or above the lower bound.
func (it *iterator) First() bool {
	if it.tree == nil {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	it.ensureIter()
	var ok bool
	if len(it.lower) > 0 {
		ok = it.it.Seek(kvItem{k: it.lower})
	} else {
		ok = it.it.First()
	}
	if !ok {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	return it.check()
}

// SeekGE positions at the first key at or above key (and the lower
// bound, whichever is greater).
func (it *iterator) SeekGE(key []byte) bool {
	if it.tree == nil {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	if len(it.lower) > 0 && bytes.Compare(key, it.lower) < 0 {
		key = it.lower
	}
	it.ensureIter()
	if !it.it.Seek(kvItem{k: key}) {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	return it.check()
}

// Valid reports whether the iterator holds a key.
func (it *iterator) Valid() bool { return it.valid }

// Next advances to the following key.
func (it *iterator) Next() bool {
	if !it.ready {
		return it.First()
	}
	if !it.it.Next() {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	return it.check()
}

func (it *iterator) check() bool {
	item := it.it.Item()
	if len(it.upper) > 0 && bytes.Compare(item.k, it.upper) >= 0 {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	it.valid = true
	it.key, it.val = item.k, item.v
	return true
}

// Key returns the current key. The slice aliases the immutable
// generation and must not be mutated.
func (it *iterator) Key() []byte { return it.key }

// Value returns the current value. The slice aliases the immutable
// generation and must not be mutated.
func (it *iterator) Value() []byte { return it.val }

// Error always reports nil: in-memory iteration cannot fail.
func (it *iterator) Error() error { return nil }

// Close releases the iterator.
func (it *iterator) Close() error {
	if it.ready {
		it.it.Release()
		it.ready = false
	}
	it.tree = nil
	it.valid = false
	it.key, it.val = nil, nil
	return nil
}
