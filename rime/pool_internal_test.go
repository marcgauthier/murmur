package rime

import (
	"errors"
	"testing"
)

// TestPendingWritePoolHygiene stages writes with poisoned optional fields,
// rolls back so the structs return to the pool, and requires the next
// acquisitions to observe none of the stale state.
func TestPendingWritePoolHygiene(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[qualificationRow](db)
	if err != nil {
		t.Fatal(err)
	}
	poison := errors.New("poison")
	for i := 0; i < 50; i++ {
		err := db.WriteTx(func(tx *Tx) error {
			if err := table.In(tx).Upsert(&qualificationRow{ID: "a", Value: 1}); err != nil {
				return err
			}
			p := tx.pending[0]
			p.check = func() (TxID, bool) { return 999, true }
			p.releaseUnique = func(*commitState) { panic("stale releaseUnique") }
			p.validate = func(*commitState) error { return errors.New("stale validate") }
			p.apply = func(TxID) func() { panic("stale apply") }
			p.val = &qualificationRow{ID: "stale", Value: -1}
			p.old = &qualificationRow{ID: "stale", Value: -2}
			p.uniClean = true
			p.superseded = true
			return poison
		})
		if !errors.Is(err, poison) {
			t.Fatalf("iteration %d: want poison, got %v", i, err)
		}
		if err := db.WriteTx(func(tx *Tx) error {
			if err := table.In(tx).Upsert(&qualificationRow{ID: "b", Value: 2}); err != nil {
				return err
			}
			p := tx.pending[0]
			if p.check != nil || p.releaseUnique != nil || p.validate != nil || p.apply != nil {
				t.Fatal("pooled pendingWrite leaked optional callbacks")
			}
			if p.key != "b" || p.op != OpInsert || p.del || p.old != nil {
				t.Fatalf("pooled pendingWrite leaked core state: %+v", p)
			}
			if p.uniClean || p.superseded {
				t.Fatal("pooled pendingWrite leaked commit flags")
			}
			if p.val.(*qualificationRow).ID != "b" {
				t.Fatal("pooled pendingWrite leaked staged value")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := table.Delete("b"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back row visible: %v", err)
	}
}
