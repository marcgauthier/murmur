//go:build !race

package rime

import (
	"context"
	"fmt"
	"testing"
)

type managedInstallAllocRecord struct {
	ID     string   `rime:"primary"`
	Name   string   `rime:"index,prefix"`
	Group  string   `rime:"index"`
	Rank   int      `rime:"index,ordered"`
	Unique string   `rime:"unique"`
	Active bool     `rime:"index"`
	Weight float64  `rime:"index"`
	Code   [16]byte `rime:"index"`
}

func TestManagedInstallDoesNotAllocateAfterPreparation(t *testing.T) {
	type state struct {
		db    *DB
		table *Table[managedInstallAllocRecord]
		tx    *Tx
		p     *PreparedTx
		after []commitAfter
	}
	prepare := func() state {
		db := New()
		table, err := Register[managedInstallAllocRecord](db,
			WithTableShards[managedInstallAllocRecord](1),
			WithCompound[managedInstallAllocRecord]("group_rank", "Group", "Rank"))
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 12; i++ {
			var code [16]byte
			code[0] = byte(i + 1)
			err := table.In(tx).Insert(&managedInstallAllocRecord{
				ID: fmt.Sprintf("row-%02d", i), Name: fmt.Sprintf("shared-prefix-%02d", i),
				Group: "shared-group", Rank: i % 3, Unique: fmt.Sprintf("u-%02d", i),
				Active: i%2 == 0, Weight: float64(i % 4), Code: code,
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		p, err := tx.PrepareCommit()
		if err != nil {
			t.Fatal(err)
		}
		return state{db: db, table: table, tx: tx, p: p}
	}
	states := [2]state{prepare(), prepare()}
	next := 0
	allocs := testing.AllocsPerRun(1, func() {
		var err error
		states[next].after, err = states[next].p.install()
		if err != nil {
			t.Fatalf("prepared install: %v", err)
		}
		next++
	})
	if allocs > 2 {
		t.Fatalf("managed installation allocated %.2f times after preparation; want at most 2", allocs)
	}
	for _, current := range states {
		for _, after := range current.after {
			if after.table != nil {
				after.table.fireAfterEffect(after.effect)
			} else if after.fn != nil {
				after.fn()
			}
		}
		current.tx.finish()
		name := SF[managedInstallAllocRecord](current.table, "Name")
		rows, err := current.table.Where(name.StartsWith("shared-prefix-")).Find()
		if err != nil || len(rows) != 12 {
			t.Fatalf("published rows = %d, err %v; want 12", len(rows), err)
		}
		current.db.Close()
	}
}
