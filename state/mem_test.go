package state

import (
	"bytes"
	"errors"
	"testing"
)

// TestMemStoreOrdering requires bytewise key ordering, point
// lookups, and bounded iteration over a published root.
func TestMemStoreOrdering(t *testing.T) {
	m := newMemStore()
	b := m.newBatch()
	keys := []string{"\x01b", "\x01a", "\x02", "\x01a\x00", "\x00"}
	for _, k := range keys {
		if err := b.Set([]byte(k), []byte("v"+k)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	m.publish(b.candidate())
	for _, k := range keys {
		got, err := m.get([]byte(k))
		if err != nil {
			t.Fatalf("get %q: %v", k, err)
		}
		if string(got) != "v"+k {
			t.Fatalf("get %q = %q", k, got)
		}
	}
	if _, err := m.get([]byte("missing")); !errors.Is(err, errNotFound) {
		t.Fatalf("get missing = %v, want errNotFound", err)
	}
	snap := m.newSnapshot()
	it, err := snap.NewIter(&iterOptions{LowerBound: []byte{0x01}, UpperBound: []byte{0x02}})
	if err != nil {
		t.Fatalf("NewIter: %v", err)
	}
	defer it.Close()
	var got []string
	for it.SeekGE([]byte{0x01}); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iter: %v", err)
	}
	want := []string{"\x01a", "\x01a\x00", "\x01b"}
	if len(got) != len(want) {
		t.Fatalf("iterated %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("iterated %q, want %q", got, want)
		}
	}
}

// TestMemBatchReadYourWrites requires batches to see their pending
// writes (including deletions) while the published root stays
// unchanged until publication.
func TestMemBatchReadYourWrites(t *testing.T) {
	m := newMemStore()
	b := m.newBatch()
	if err := b.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	m.publish(b.candidate())
	b2 := m.newBatch()
	if err := b2.Set([]byte("a"), []byte("2")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b2.Set([]byte("b"), []byte("3")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b2.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	v, closer, err := b2.Get([]byte("a"))
	if err != nil {
		t.Fatalf("batch Get: %v", err)
	}
	closer.Close()
	if string(v) != "2" {
		t.Fatalf("batch Get(a) = %q, want 2", v)
	}
	if _, _, err := b2.Get([]byte("b")); !errors.Is(err, errNotFound) {
		t.Fatalf("batch Get(b) = %v, want errNotFound", err)
	}
	// Published root unchanged before publication.
	v, err = m.get([]byte("a"))
	if err != nil || string(v) != "1" {
		t.Fatalf("published a = %q, %v; want 1", v, err)
	}
	if len(b2.muts) != 3 {
		t.Fatalf("mutations = %d, want 3 (set, set, delete)", len(b2.muts))
	}
	if !b2.muts[2].Deleted || string(b2.muts[2].Key) != "b" {
		t.Fatalf("mutation 2 = %+v, want delete of b", b2.muts[2])
	}
	m.publish(b2.candidate())
	v, err = m.get([]byte("a"))
	if err != nil || string(v) != "2" {
		t.Fatalf("published a = %q, %v; want 2", v, err)
	}
	if _, err := m.get([]byte("b")); !errors.Is(err, errNotFound) {
		t.Fatalf("published b = %v, want errNotFound", err)
	}
}

// TestMemSnapshotIsolation requires a pinned snapshot to keep its
// contents while newer roots publish, and batch iterators to see
// pending state.
func TestMemSnapshotIsolation(t *testing.T) {
	m := newMemStore()
	b := m.newBatch()
	if err := b.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	m.publish(b.candidate())
	snap := m.newSnapshot()
	b2 := m.newBatch()
	if err := b2.Set([]byte("a"), []byte("2")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	m.publish(b2.candidate())
	v, closer, err := snap.Get([]byte("a"))
	if err != nil {
		t.Fatalf("snap Get: %v", err)
	}
	closer.Close()
	if string(v) != "1" {
		t.Fatalf("snapshot a = %q, want 1", v)
	}
	// Batch iterator sees pending state over the published root.
	b3 := m.newBatch()
	if err := b3.Set([]byte("0"), []byte("z")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	it, err := b3.NewIter(&iterOptions{})
	if err != nil {
		t.Fatalf("NewIter: %v", err)
	}
	defer it.Close()
	var keys []string
	for it.First(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if len(keys) != 2 || keys[0] != "0" || keys[1] != "a" {
		t.Fatalf("batch iter = %q, want [0 a]", keys)
	}
	if !bytes.Equal(it.Key(), nil) && !it.Valid() {
		t.Fatalf("exhausted iterator still valid")
	}
}

// TestMemStoreEmpty requires empty-tree reads to report not-found and
// invalid iteration instead of panicking on a nil radix root.
func TestMemStoreEmpty(t *testing.T) {
	m := newMemStore()
	if _, err := m.get([]byte("k")); !errors.Is(err, errNotFound) {
		t.Fatalf("get = %v, want not found", err)
	}
	b := m.newBatch()
	if _, _, err := b.Get([]byte("k")); !errors.Is(err, errNotFound) {
		t.Fatalf("batch Get = %v, want not found", err)
	}
	snap := m.newSnapshot()
	if _, _, err := snap.Get([]byte("k")); !errors.Is(err, errNotFound) {
		t.Fatalf("snapshot Get = %v, want not found", err)
	}
	it, err := m.newIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if it.First() || it.Valid() {
		t.Fatal("First on empty store is valid")
	}
	if it.SeekGE([]byte("k")) || it.Valid() {
		t.Fatal("SeekGE on empty store is valid")
	}
	// Deleting the only key must also leave a usable empty store.
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	m.publish(b.candidate())
	if _, err := m.get([]byte("k")); !errors.Is(err, errNotFound) {
		t.Fatalf("get after delete-all = %v, want not found", err)
	}
}
