package rime

import (
	"cmp"
	"math/rand"
	"sort"
	"strconv"
	"testing"
)

func TestOrderedIndexModel(t *testing.T) {
	o := newOrderedIdx(func(a, b any) int { return cmp.Compare(a.(int), b.(int)) })
	type entry struct{ key, value, sequence int }
	model := map[int]entry{}
	sequence := 0
	rng := rand.New(rand.NewSource(91))
	var verify func(*orderedNode) (int, int)
	verify = func(n *orderedNode) (int, int) {
		if n == nil {
			return 0, 0
		}
		lh, lc := verify(n.left)
		rh, rc := verify(n.right)
		if lh-rh > 1 || rh-lh > 1 || n.height != 1+max(lh, rh) {
			t.Fatalf("unbalanced tree at value %v: heights %d/%d stored %d", n.val, lh, rh, n.height)
		}
		if n.left != nil && o.cmp(n.left.val, n.val) >= 0 {
			t.Fatal("left ordering violation")
		}
		if n.right != nil && o.cmp(n.right.val, n.val) <= 0 {
			t.Fatal("right ordering violation")
		}
		count := 0
		var prior *orderedItem
		for item := n.first; item != nil; item = item.next {
			if item.owner != n || item.prev != prior || o.byKey[item.key] != item {
				t.Fatal("bucket directory corrupt")
			}
			prior = item
			count++
		}
		if count == 0 || prior != n.last {
			t.Fatal("empty/broken value bucket")
		}
		return n.height, lc + rc + count
	}
	for step := 0; step < 5000; step++ {
		key, value := rng.Intn(256), rng.Intn(81)-40
		switch rng.Intn(3) {
		case 0, 1:
			old, present := model[key]
			o.add(value, key)
			if !present || old.value != value {
				sequence++
				model[key] = entry{key, value, sequence}
			}
		case 2:
			if old, present := model[key]; present {
				o.remove(old.value, key)
				delete(model, key)
			} else {
				o.remove(value, key)
			}
		}
		_, count := verify(o.root)
		if count != len(model) || o.len() != len(model) {
			t.Fatalf("step %d count=%d/%d want %d", step, count, o.len(), len(model))
		}
		lo, hi := rng.Intn(100)-50, rng.Intn(100)-50
		loOpen, hiOpen := rng.Intn(2) == 0, rng.Intn(2) == 0
		desc := rng.Intn(2) == 0
		limit := rng.Intn(20) - 1
		expected := []entry{}
		for _, e := range model {
			if (e.value > lo || e.value == lo && !loOpen) && (e.value < hi || e.value == hi && !hiOpen) {
				expected = append(expected, e)
			}
		}
		sort.Slice(expected, func(i, j int) bool {
			a, b := expected[i], expected[j]
			if a.value != b.value {
				if desc {
					return a.value > b.value
				}
				return a.value < b.value
			}
			if desc {
				return a.sequence > b.sequence
			}
			return a.sequence < b.sequence
		})
		if limit >= 0 && len(expected) > limit {
			expected = expected[:limit]
		}
		got := o.rangeKeys(lo, hi, loOpen, hiOpen, desc, limit)
		if len(got) != len(expected) {
			t.Fatalf("step %d range length %d want %d", step, len(got), len(expected))
		}
		for i, key := range got {
			if key != expected[i].key {
				t.Fatalf("step %d range[%d]=%v want %v", step, i, key, expected[i].key)
			}
		}
		for _, maximum := range []bool{false, true} {
			extreme, ok := o.extreme(maximum)
			if ok != (len(model) > 0) {
				t.Fatal("extreme existence mismatch")
			}
			if ok {
				for _, e := range model {
					if maximum && e.value > extreme.val.(int) || !maximum && e.value < extreme.val.(int) {
						t.Fatal("wrong extreme value")
					}
				}
			}
		}
	}
	for key, e := range model {
		o.remove(e.value, key)
	}
	if o.root != nil || o.len() != 0 {
		t.Fatal("empty index retains tree or directory")
	}
}

// A structural work bound detects quadratic mutations without timing-sensitive
// CI thresholds. Sorted inputs and low-cardinality buckets are both covered.
func TestOrderedIndexMutationScaling(t *testing.T) {
	const n = 8192
	for _, distinct := range []int{60, n} {
		t.Run("distinct="+strconv.Itoa(distinct), func(t *testing.T) {
			comparisons := 0
			o := newOrderedIdx(func(a, b any) int { comparisons++; return cmp.Compare(a.(int), b.(int)) })
			for key := 0; key < n; key++ {
				o.add(key%distinct, key)
			}
			for key := n - 1; key >= 0; key-- {
				o.remove(key%distinct, key)
			}
			if comparisons > n*40 {
				t.Fatalf("mutations did %d comparisons for %d rows: expected O(n log n)", comparisons, n)
			}
			if o.root != nil || o.len() != 0 {
				t.Fatal("mutations leaked entries")
			}
		})
	}
}

func TestUniqueIntermediateClaims(t *testing.T) {
	type record struct {
		ID   int    `rime:"primary"`
		Name string `rime:"unique"`
	}
	db := New()
	defer db.Close()
	table, err := Register[record](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.UpsertMany([]*record{{ID: 1, Name: "one"}, {ID: 2, Name: "two"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTx(func(tx *Tx) error {
		if err := table.In(tx).Upsert(&record{ID: 1, Name: "two"}); err != nil {
			return err
		}
		return table.In(tx).Upsert(&record{ID: 1, Name: "one"})
	}); err != nil {
		t.Fatal(err)
	}
	name := SF[record](table, "Name")
	for value, key := range map[string]int{"one": 1, "two": 2} {
		rows, err := table.Where(name.Eq(value)).Find()
		if err != nil || len(rows) != 1 || rows[0].ID != key {
			t.Fatalf("final unique claim lost: %s got=%v err=%v", value, rows, err)
		}
	}
}
