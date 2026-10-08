package state

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"

	iradix "github.com/hashicorp/go-immutable-radix"
	"github.com/marcgauthier/murmur/spool"
)

// errNotFound is the shared missing-key sentinel returned by every lookup.
var errNotFound = errors.New("state: not found")

// iterOptions bounds an iterator: LowerBound inclusive, UpperBound
// exclusive, matching the ordered range-scan contract.
type iterOptions struct {
	LowerBound []byte
	UpperBound []byte
}

// memStore is the authoritative in-RAM key/value state: an immutable
// radix tree published atomically after each durable Spool commit.
// Keys keep their binary encoding and bytewise ordering. Stored
// key/value bytes are owned clones and never mutated, so lookups and
// iterators over a pinned root are safe without copying.
type memStore struct {
	root atomic.Pointer[iradix.Tree]
}

func newMemStore() *memStore {
	m := &memStore{}
	m.root.Store(iradix.New())
	return m
}

func (m *memStore) load() *iradix.Tree {
	return m.root.Load()
}

func (m *memStore) publish(t *iradix.Tree) {
	m.root.Store(t)
}

// get returns an owned copy of the value stored under key.
func (m *memStore) get(key []byte) ([]byte, error) {
	t := m.load()
	if t.Root() == nil {
		return nil, errNotFound
	}
	v, ok := t.Get(key)
	if !ok {
		return nil, errNotFound
	}
	raw, ok := v.([]byte)
	if !ok {
		return nil, errors.New("state: corrupt in-memory value")
	}
	return append([]byte(nil), raw...), nil
}

func (m *memStore) newBatch() *batch {
	return &batch{txn: m.load().Txn()}
}

func (m *memStore) newSnapshot() *snapshot {
	return &snapshot{root: m.load().Root()}
}

func (m *memStore) newIter(o *iterOptions) (*iterator, error) {
	return newIterator(m.load().Root(), o), nil
}

// batch is one atomic multi-key mutation: a radix transaction for
// read-your-writes plus the ordered mutation list for Spool. The
// candidate root is built by Commit and published only after the
// Spool commit is durably acknowledged.
type batch struct {
	txn   *iradix.Txn
	muts  []spool.Mutation
	size  int64
	count uint32
}

// Set stores an owned copy of key/value.
func (b *batch) Set(key, value []byte) error {
	k := append([]byte(nil), key...)
	v := append([]byte(nil), value...)
	b.txn.Insert(k, v)
	b.muts = append(b.muts, spool.Mutation{Key: k, Value: v})
	b.size += int64(len(k) + len(v))
	b.count++
	return nil
}

// Delete removes key and records a deletion marker.
func (b *batch) Delete(key []byte) error {
	k := append([]byte(nil), key...)
	b.txn.Delete(k)
	b.muts = append(b.muts, spool.Mutation{Key: k, Deleted: true})
	b.size += int64(len(k))
	b.count++
	return nil
}

// Get reads pending writes first, then the published root.
func (b *batch) Get(key []byte) ([]byte, io.Closer, error) {
	if b.txn.Root() == nil {
		return nil, nil, errNotFound
	}
	v, ok := b.txn.Get(key)
	if !ok {
		return nil, nil, errNotFound
	}
	raw, ok := v.([]byte)
	if !ok {
		return nil, nil, errors.New("state: corrupt in-memory value")
	}
	return append([]byte(nil), raw...), io.NopCloser(nil), nil
}

// NewIter iterates the batch's pending state: pending writes shadow
// the published root.
func (b *batch) NewIter(o *iterOptions) (*iterator, error) {
	return newIterator(b.txn.Root(), o), nil
}

// Len reports the staged key/value byte count.
func (b *batch) Len() int { return int(b.size) }

// Count reports the staged mutation count.
func (b *batch) Count() uint32 { return b.count }

// Close discards an uncommitted batch.
func (b *batch) Close() error { return nil }

// candidate builds the root this batch would publish.
func (b *batch) candidate() *iradix.Tree { return b.txn.Commit() }

// snapshot pins one immutable radix root for consistent multi-key
// reads. Roots are immutable, so no lifetime management is needed
// beyond dropping the reference.
type snapshot struct {
	root *iradix.Node
}

func (s *snapshot) Get(key []byte) ([]byte, io.Closer, error) {
	if s.root == nil {
		return nil, nil, errNotFound
	}
	it := s.root.Iterator()
	it.SeekLowerBound(key)
	k, v, ok := it.Next()
	if !ok || !bytes.Equal(k, key) {
		return nil, nil, errNotFound
	}
	raw, ok := v.([]byte)
	if !ok {
		return nil, nil, errors.New("state: corrupt in-memory value")
	}
	return append([]byte(nil), raw...), io.NopCloser(nil), nil
}

func (s *snapshot) NewIter(o *iterOptions) (*iterator, error) {
	return newIterator(s.root, o), nil
}

func (s *snapshot) Close() error { return nil }

// iterator is a forward-only bounded iterator over one immutable
// root. Roots never change, so Key/Value slices stay valid for the
// iterator's lifetime; callers must not mutate them.
type iterator struct {
	root  *iradix.Node
	it    *iradix.Iterator
	lower []byte
	upper []byte
	key   []byte
	val   []byte
	valid bool
}

func newIterator(root *iradix.Node, o *iterOptions) *iterator {
	it := &iterator{root: root}
	if o != nil {
		it.lower = append([]byte(nil), o.LowerBound...)
		it.upper = append([]byte(nil), o.UpperBound...)
	}
	return it
}

// First positions at the first key at or above the lower bound.
func (it *iterator) First() bool {
	if it.root == nil {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	// A radix iterator serves one seek: repositioning requires a fresh
	// one, so every positioning call rebuilds it.
	it.it = it.root.Iterator()
	it.it.SeekLowerBound(it.lower)
	return it.advance()
}

// SeekGE positions at the first key at or above key (and the lower
// bound, whichever is greater).
func (it *iterator) SeekGE(key []byte) bool {
	if it.root == nil {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	if len(it.lower) > 0 && bytes.Compare(key, it.lower) < 0 {
		key = it.lower
	}
	it.it = it.root.Iterator()
	it.it.SeekLowerBound(key)
	return it.advance()
}

// Valid reports whether the iterator holds a key.
func (it *iterator) Valid() bool { return it.valid }

// Next advances to the following key.
func (it *iterator) Next() bool { return it.advance() }

func (it *iterator) advance() bool {
	if it.it == nil {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	k, v, ok := it.it.Next()
	if !ok {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	if len(it.upper) > 0 && bytes.Compare(k, it.upper) >= 0 {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	raw, ok := v.([]byte)
	if !ok {
		it.valid = false
		it.key, it.val = nil, nil
		return false
	}
	it.valid = true
	it.key, it.val = k, raw
	return true
}

// Key returns the current key. The slice aliases the immutable root
// and must not be mutated.
func (it *iterator) Key() []byte { return it.key }

// Value returns the current value. The slice aliases the immutable
// root and must not be mutated.
func (it *iterator) Value() []byte { return it.val }

// Error always reports nil: in-memory iteration cannot fail.
func (it *iterator) Error() error { return nil }

// Close releases the iterator.
func (it *iterator) Close() error { return nil }
